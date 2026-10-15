package queue

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type DeadLetter struct {
	JobID         string
	QueueName     string
	JobType       string
	Payload       []byte
	Attempts      int
	MaxAttempts   int
	FailureReason string
	FailedAt      string
}

// ListDeadLetters returns unreplayed dead letters for one queue, newest first.
func (q *PostgresQueue) ListDeadLetters(ctx context.Context, queueName string, limit int) ([]DeadLetter, error) {
	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf("list dead letters: limit must be between 1 and 100")
	}
	rows, err := q.db.Query(ctx, "SELECT job_id::text,queue_name,job_type,payload,attempts,max_attempts,failure_reason,failed_at::text FROM dead_letter_jobs WHERE queue_name=$1 AND replayed_at IS NULL ORDER BY failed_at DESC LIMIT $2", queueName, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()

	items := make([]DeadLetter, 0)
	for rows.Next() {
		var item DeadLetter
		if err := rows.Scan(&item.JobID, &item.QueueName, &item.JobType, &item.Payload, &item.Attempts, &item.MaxAttempts, &item.FailureReason, &item.FailedAt); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letters: %w", err)
	}
	return items, nil
}

// ReplayDeadLetter requeues an exhausted job once and retains its dead-letter
// record for audit. A later exhaustion refreshes that record and makes it active.
func (q *PostgresQueue) ReplayDeadLetter(ctx context.Context, jobID string) error {
	tx, err := q.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin dead-letter replay: %w", err)
	}
	defer tx.Rollback(ctx)

	var storedID string
	err = tx.QueryRow(ctx, "SELECT job_id::text FROM dead_letter_jobs WHERE job_id=$1 AND replayed_at IS NULL FOR UPDATE", jobID).Scan(&storedID)
	if err == pgx.ErrNoRows {
		return fmt.Errorf("replay dead letter: active dead-letter record not found")
	}
	if err != nil {
		return fmt.Errorf("read dead-letter record: %w", err)
	}

	tag, err := tx.Exec(ctx, "UPDATE jobs SET status='pending',attempts=0,available_at=NOW(),locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE id=$1 AND status='failed'", jobID)
	if err != nil {
		return fmt.Errorf("requeue dead-letter job: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("replay dead letter: job is not in failed state")
	}
	if _, err := tx.Exec(ctx, "UPDATE dead_letter_jobs SET replayed_at=NOW() WHERE job_id=$1 AND replayed_at IS NULL", jobID); err != nil {
		return fmt.Errorf("mark dead letter replayed: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dead-letter replay: %w", err)
	}
	return nil
}
