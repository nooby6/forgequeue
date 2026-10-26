package job
import("context";"errors";"fmt";"github.com/jackc/pgx/v5";"github.com/jackc/pgx/v5/pgxpool")
var ErrNotFound=errors.New("job not found")
var ErrNotCancellable=errors.New("job cannot be cancelled in its current state")
type PostgresRepository struct{db *pgxpool.Pool}
func NewPostgresRepository(db *pgxpool.Pool)*PostgresRepository{return &PostgresRepository{db:db}}
const jobColumns="id,idempotency_key,queue_name,job_type,payload,status,priority,attempts,max_attempts,available_at,created_at,updated_at"
func scanJob(row pgx.Row)(Job,error){var j Job;err:=row.Scan(&j.ID,&j.IdempotencyKey,&j.QueueName,&j.JobType,&j.Payload,&j.Status,&j.Priority,&j.Attempts,&j.MaxAttempts,&j.AvailableAt,&j.CreatedAt,&j.UpdatedAt);return j,err}
func(r *PostgresRepository)Create(ctx context.Context,req CreateRequest)(Job,error){q:="INSERT INTO jobs(idempotency_key,queue_name,job_type,payload,status,priority,max_attempts) VALUES($1,$2,$3,$4,'pending',$5,$6) ON CONFLICT(idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key RETURNING "+jobColumns;return scanJob(r.db.QueryRow(ctx,q,req.IdempotencyKey,req.QueueName,req.JobType,req.Payload,req.Priority,req.MaxAttempts))}
func (r *PostgresRepository) Cancel(ctx context.Context, id string) (Job, error) {
	cancelled, err := scanJob(r.db.QueryRow(ctx,
		"UPDATE jobs SET status='cancelled',locked_by=NULL,locked_at=NULL,lease_expires_at=NULL,updated_at=NOW() WHERE id=$1 AND status='pending' RETURNING "+jobColumns,
		id,
	))
	if err == nil {
		return cancelled, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("cancel job: %w", err)
	}

	current, err := r.Get(ctx, id)
	if err != nil {
		return Job{}, err
	}
	if current.Status == StatusCancelled {
		return current, nil
	}
	return Job{}, ErrNotCancellable
}

func(r *PostgresRepository)Get(ctx context.Context,id string)(Job,error){j,err:=scanJob(r.db.QueryRow(ctx,"SELECT "+jobColumns+" FROM jobs WHERE id=$1",id));if errors.Is(err,pgx.ErrNoRows){return Job{},ErrNotFound};if err!=nil{return Job{},fmt.Errorf("get job: %w",err)};return j,nil}
