package job
import("context";"errors";"fmt";"github.com/jackc/pgx/v5";"github.com/jackc/pgx/v5/pgxpool")
var ErrNotFound=errors.New("job not found")
type PostgresRepository struct{db *pgxpool.Pool}
func NewPostgresRepository(db *pgxpool.Pool)*PostgresRepository{return &PostgresRepository{db:db}}
const jobColumns="id,idempotency_key,queue_name,job_type,payload,status,priority,attempts,available_at,created_at,updated_at"
func scanJob(row pgx.Row)(Job,error){var j Job;err:=row.Scan(&j.ID,&j.IdempotencyKey,&j.QueueName,&j.JobType,&j.Payload,&j.Status,&j.Priority,&j.Attempts,&j.AvailableAt,&j.CreatedAt,&j.UpdatedAt);return j,err}
func(r *PostgresRepository)Create(ctx context.Context,req CreateRequest)(Job,error){q:="INSERT INTO jobs(idempotency_key,queue_name,job_type,payload,priority) VALUES($1,$2,$3,$4,$5) ON CONFLICT(idempotency_key) DO UPDATE SET idempotency_key=EXCLUDED.idempotency_key RETURNING "+jobColumns;return scanJob(r.db.QueryRow(ctx,q,req.IdempotencyKey,req.QueueName,req.JobType,req.Payload,req.Priority))}
func(r *PostgresRepository)Get(ctx context.Context,id string)(Job,error){j,err:=scanJob(r.db.QueryRow(ctx,"SELECT "+jobColumns+" FROM jobs WHERE id=$1",id));if errors.Is(err,pgx.ErrNoRows){return Job{},ErrNotFound};if err!=nil{return Job{},fmt.Errorf("get job: %w",err)};return j,nil}
