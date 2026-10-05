package queue

import ("context";"errors";"fmt";"time";"github.com/jackc/pgx/v5";"github.com/jackc/pgx/v5/pgxpool";"github.com/nooby6/forgequeue/internal/job")

type PostgresQueue struct{db *pgxpool.Pool}
func NewPostgresQueue(db *pgxpool.Pool)*PostgresQueue{return &PostgresQueue{db:db}}
const jobColumns="id,idempotency_key,queue_name,job_type,payload,status,priority,attempts,available_at,created_at,updated_at"

func scanJob(row pgx.Row)(job.Job,error){var j job.Job;err:=row.Scan(&j.ID,&j.IdempotencyKey,&j.QueueName,&j.JobType,&j.Payload,&j.Status,&j.Priority,&j.Attempts,&j.AvailableAt,&j.CreatedAt,&j.UpdatedAt);return j,err}

func(q *PostgresQueue)ClaimNext(ctx context.Context,queueName,workerID string,lease time.Duration)(job.Job,error){
	tx,err:=q.db.Begin(ctx);if err!=nil{return job.Job{},fmt.Errorf("begin claim transaction: %w",err)};defer tx.Rollback(ctx)
	var id string
	err=tx.QueryRow(ctx,"SELECT id FROM jobs WHERE queue_name=$1 AND status='pending' AND available_at<=NOW() ORDER BY priority DESC,available_at ASC,created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1",queueName).Scan(&id)
	if errors.Is(err,pgx.ErrNoRows){return job.Job{},ErrNoJob};if err!=nil{return job.Job{},fmt.Errorf("select job to claim: %w",err)}
	j,err:=scanJob(tx.QueryRow(ctx,"UPDATE jobs SET status='running',attempts=attempts+1,locked_by=$2,locked_at=NOW(),lease_expires_at=NOW()+$3::interval,updated_at=NOW() WHERE id=$1 RETURNING "+jobColumns,id,workerID,lease.String()))
	if err!=nil{return job.Job{},fmt.Errorf("claim job: %w",err)}
	if err=tx.Commit(ctx);err!=nil{return job.Job{},fmt.Errorf("commit job claim: %w",err)};return j,nil
}
func(q *PostgresQueue)Complete(ctx context.Context,id,workerID string)error{tag,err:=q.db.Exec(ctx,"UPDATE jobs SET status='succeeded',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE id=$1 AND status='running' AND locked_by=$2",id,workerID);if err!=nil{return fmt.Errorf("complete job: %w",err)};if tag.RowsAffected()!=1{return fmt.Errorf("complete job: ownership lost")};return nil}
func(q *PostgresQueue)Fail(ctx context.Context,id,workerID string)error{tag,err:=q.db.Exec(ctx,"UPDATE jobs SET status='failed',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE id=$1 AND status='running' AND locked_by=$2",id,workerID);if err!=nil{return fmt.Errorf("fail job: %w",err)};if tag.RowsAffected()!=1{return fmt.Errorf("fail job: ownership lost")};return nil}
