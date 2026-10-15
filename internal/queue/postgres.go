package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nooby6/forgequeue/internal/job"
)

type PostgresQueue struct {
	db *pgxpool.Pool
}

func NewPostgresQueue(db *pgxpool.Pool) *PostgresQueue {
	return &PostgresQueue{db: db}
}

const jobColumns = "id,idempotency_key,queue_name,job_type,payload,status,priority,attempts,max_attempts,available_at,created_at,updated_at"

func scanJob(row pgx.Row) (job.Job, error) {
	var j job.Job
	err := row.Scan(&j.ID, &j.IdempotencyKey, &j.QueueName, &j.JobType, &j.Payload, &j.Status, &j.Priority, &j.Attempts, &j.MaxAttempts, &j.AvailableAt, &j.CreatedAt, &j.UpdatedAt)
	return j, err
}

func (q *PostgresQueue) ClaimNext(ctx context.Context, queueName, workerID string, lease time.Duration) (job.Job, error) {
	tx, err := q.db.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "UPDATE jobs SET status='pending',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE queue_name=$1 AND status='running' AND lease_expires_at<=NOW() AND attempts<max_attempts", queueName)
	if err != nil {
		return job.Job{}, fmt.Errorf("recover expired jobs: %w", err)
	}
	_, err = tx.Exec(ctx, "WITH exhausted AS (UPDATE jobs SET status='failed',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE queue_name=$1 AND status='running' AND lease_expires_at<=NOW() AND attempts>=max_attempts RETURNING id,queue_name,job_type,payload,attempts,max_attempts) INSERT INTO dead_letter_jobs (job_id,queue_name,job_type,payload,attempts,max_attempts,failure_reason,failed_at,replayed_at) SELECT id,queue_name,job_type,payload,attempts,max_attempts,'lease expired after maximum attempts',NOW(),NULL FROM exhausted ON CONFLICT (job_id) DO UPDATE SET queue_name=EXCLUDED.queue_name,job_type=EXCLUDED.job_type,payload=EXCLUDED.payload,attempts=EXCLUDED.attempts,max_attempts=EXCLUDED.max_attempts,failure_reason=EXCLUDED.failure_reason,failed_at=NOW(),replayed_at=NULL", queueName)
	if err != nil {
		return job.Job{}, fmt.Errorf("dead-letter expired jobs: %w", err)
	}

	var id string
	err = tx.QueryRow(ctx, "SELECT id FROM jobs WHERE queue_name=$1 AND status='pending' AND available_at<=NOW() ORDER BY priority DESC,available_at ASC,created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1", queueName).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, ErrNoJob
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("select job to claim: %w", err)
	}

	j, err := scanJob(tx.QueryRow(ctx, "UPDATE jobs SET status='running',attempts=attempts+1,locked_by=$2,locked_at=NOW(),lease_expires_at=NOW()+$3::interval,updated_at=NOW() WHERE id=$1 RETURNING "+jobColumns, id, workerID, lease.String()))
	if err != nil {
		return job.Job{}, fmt.Errorf("claim job: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return job.Job{}, fmt.Errorf("commit job claim: %w", err)
	}
	return j, nil
}

// RenewLease extends a job's lease only while it is still active and owned by
// the requesting worker. An expired lease cannot be revived by its former owner.
func (q *PostgresQueue) RenewLease(ctx context.Context, id, workerID string, lease time.Duration) error {
	tag, err := q.db.Exec(ctx, "UPDATE jobs SET lease_expires_at=NOW()+$3::interval,updated_at=NOW() WHERE id=$1 AND status='running' AND locked_by=$2 AND lease_expires_at>NOW()", id, workerID, lease.String())
	if err != nil {
		return fmt.Errorf("renew job lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("renew job lease: ownership lost or lease expired")
	}
	return nil
}

func (q *PostgresQueue) Complete(ctx context.Context, id, workerID string) error {
	tag, err := q.db.Exec(ctx, "UPDATE jobs SET status='succeeded',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE id=$1 AND status='running' AND locked_by=$2 AND lease_expires_at>NOW()", id, workerID)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	// A repeated acknowledgement is safe once the job is already terminal.
	// Do not treat an expired running lease or a different terminal state as success.
	var status string
	if err := q.db.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1", id).Scan(&status); err != nil {
		return fmt.Errorf("complete job: ownership lost: %w", err)
	}
	if status == "succeeded" {
		return nil
	}
	return fmt.Errorf("complete job: ownership lost")
}

func (q *PostgresQueue) Fail(ctx context.Context, id, workerID string) error {
	tag, err := q.db.Exec(ctx, "UPDATE jobs SET status='failed',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE id=$1 AND status='running' AND locked_by=$2 AND lease_expires_at>NOW()", id, workerID)
	if err != nil {
		return fmt.Errorf("fail job: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("fail job: ownership lost")
	}
	return nil
}

func (q *PostgresQueue) Retry(ctx context.Context, id, workerID string, delay time.Duration) error {
	tx, err := q.db.Begin(ctx)
	if err != nil { return fmt.Errorf("begin retry transaction: %w", err) }
	defer tx.Rollback(ctx)

	var status, queueName, jobType string
	var payload []byte
	var attempts, maxAttempts int
	err = tx.QueryRow(ctx, "UPDATE jobs SET status=CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END, available_at=CASE WHEN attempts < max_attempts THEN NOW()+$3::interval ELSE available_at END, locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE id=$1 AND status='running' AND locked_by=$2 AND lease_expires_at>NOW() RETURNING status,queue_name,job_type,payload,attempts,max_attempts", id, workerID, delay.String()).Scan(&status, &queueName, &jobType, &payload, &attempts, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) { return fmt.Errorf("retry job: ownership lost") }
	if err != nil { return fmt.Errorf("retry job: %w", err) }

	if status == "failed" {
		_, err = tx.Exec(ctx, "INSERT INTO dead_letter_jobs (job_id,queue_name,job_type,payload,attempts,max_attempts,failure_reason,failed_at,replayed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW(),NULL) ON CONFLICT (job_id) DO UPDATE SET queue_name=EXCLUDED.queue_name,job_type=EXCLUDED.job_type,payload=EXCLUDED.payload,attempts=EXCLUDED.attempts,max_attempts=EXCLUDED.max_attempts,failure_reason=EXCLUDED.failure_reason,failed_at=NOW(),replayed_at=NULL", id, queueName, jobType, payload, attempts, maxAttempts, fmt.Sprintf("maximum attempts exhausted after %d attempts", attempts))
		if err != nil { return fmt.Errorf("record dead-letter job: %w", err) }
	}
	if err := tx.Commit(ctx); err != nil { return fmt.Errorf("commit job retry: %w", err) }
	return nil
}
