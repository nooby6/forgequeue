package job

import (
 "context"
 "errors"
 "fmt"
 "github.com/jackc/pgx/v5"
 "github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("job not found")
var ErrNotCancellable = errors.New("job cannot be cancelled in its current state")
type PostgresRepository struct { db *pgxpool.Pool }
func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository { return &PostgresRepository{db:db} }
const jobColumns = "id,idempotency_key,queue_name,job_type,payload,status,priority,attempts,max_attempts,available_at,cancellation_requested_at,created_at,updated_at"
func scanJob(row pgx.Row) (Job,error) {
 var j Job
 err:=row.Scan(&j.ID,&j.IdempotencyKey,&j.QueueName,&j.JobType,&j.Payload,&j.Status,&j.Priority,&j.Attempts,&j.MaxAttempts,&j.AvailableAt,&j.CancellationRequestedAt,&j.CreatedAt,&j.UpdatedAt)
 return j,err
}
func (r *PostgresRepository) Create(ctx context.Context, req CreateRequest) (Job,error) {
 tx,err:=r.db.Begin(ctx); if err!=nil{return Job{},fmt.Errorf("begin create job: %w",err)}; defer tx.Rollback(ctx)
 j,err:=scanJob(tx.QueryRow(ctx,"INSERT INTO jobs(idempotency_key,queue_name,job_type,payload,status,priority,max_attempts,available_at) VALUES($1,$2,$3,$4,'pending',$5,$6,COALESCE($7,NOW())) ON CONFLICT(idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key RETURNING "+jobColumns,req.IdempotencyKey,req.QueueName,req.JobType,req.Payload,req.Priority,req.MaxAttempts,req.ScheduledAt))
 if err!=nil{return Job{},fmt.Errorf("insert job: %w",err)}
 var inserted bool
 if req.IdempotencyKey!="" { if err:=tx.QueryRow(ctx,"SELECT (xmax = 0) FROM jobs WHERE id=$1",j.ID).Scan(&inserted);err!=nil{return Job{},fmt.Errorf("inspect idempotent create: %w",err)} } else {inserted=true}
 if inserted { for _,dependencyID:=range req.DependsOn { if _,err:=tx.Exec(ctx,"INSERT INTO job_dependencies(job_id,depends_on_job_id) VALUES($1,$2)",j.ID,dependencyID);err!=nil{return Job{},fmt.Errorf("add job dependency %s: %w",dependencyID,err)} } }
 if err:=tx.Commit(ctx);err!=nil{return Job{},fmt.Errorf("commit create job: %w",err)}
 return j,nil
}
func (r *PostgresRepository) Cancel(ctx context.Context,id string)(Job,error) {
 cancelled,err:=scanJob(r.db.QueryRow(ctx,"UPDATE jobs SET status='cancelled',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE id=$1 AND status='pending' RETURNING "+jobColumns,id))
 if err==nil{return cancelled,nil};if !errors.Is(err,pgx.ErrNoRows){return Job{},fmt.Errorf("cancel job: %w",err)}
 requested,err:=scanJob(r.db.QueryRow(ctx,"UPDATE jobs SET cancellation_requested_at=COALESCE(cancellation_requested_at,NOW()),updated_at=NOW() WHERE id=$1 AND status='running' RETURNING "+jobColumns,id))
 if err==nil{return requested,nil};if !errors.Is(err,pgx.ErrNoRows){return Job{},fmt.Errorf("request job cancellation: %w",err)}
 current,err:=r.Get(ctx,id);if err!=nil{return Job{},err};if current.Status==StatusCancelled{return current,nil};return Job{},ErrNotCancellable
}
func (r *PostgresRepository) Get(ctx context.Context,id string)(Job,error) {
 j,err:=scanJob(r.db.QueryRow(ctx,"SELECT "+jobColumns+" FROM jobs WHERE id=$1",id))
 if errors.Is(err,pgx.ErrNoRows){return Job{},ErrNotFound};if err!=nil{return Job{},fmt.Errorf("get job: %w",err)};return j,nil
}