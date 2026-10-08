package worker

import ("context";"encoding/json";"log/slog";"sync";"time";"github.com/nooby6/forgequeue/internal/queue")

type Config struct{QueueName string;Concurrency int;PollInterval time.Duration;Lease time.Duration;RetryBackoff time.Duration;WorkerID string}
type Worker struct{queue queue.Queue;registry *Registry;logger *slog.Logger;config Config}

func New(q queue.Queue,r *Registry,l *slog.Logger,c Config)*Worker{if c.Concurrency<1{c.Concurrency=1};if c.PollInterval<=0{c.PollInterval=500*time.Millisecond};if c.Lease<=0{c.Lease=30*time.Second};if c.RetryBackoff<=0{c.RetryBackoff=time.Second};return &Worker{queue:q,registry:r,logger:l,config:c}}
func(w *Worker)Run(ctx context.Context){var wg sync.WaitGroup;wg.Add(w.config.Concurrency);for i:=0;i<w.config.Concurrency;i++{go func(slot int){defer wg.Done();w.loop(ctx,slot)}(i)};wg.Wait()}
func(w *Worker)loop(ctx context.Context,slot int){ticker:=time.NewTicker(w.config.PollInterval);defer ticker.Stop();for{err:=w.processOne(ctx);if err==queue.ErrNoJob{select{case<-ctx.Done():return;case<-ticker.C:};continue};if err!=nil{w.logger.Error("worker_iteration_failed","worker_id",w.config.WorkerID,"slot",slot,"error",err);select{case<-ctx.Done():return;case<-ticker.C:}};if ctx.Err()!=nil{return}}}
func(w *Worker)processOne(ctx context.Context)error{j,err:=w.queue.ClaimNext(ctx,w.config.QueueName,w.config.WorkerID,w.config.Lease);if err!=nil{return err};h,ok:=w.registry.Lookup(j.JobType);if !ok{if e:=w.queue.Fail(ctx,j.ID,w.config.WorkerID);e!=nil{return e};w.logger.Error("job_handler_missing","job_id",j.ID,"job_type",j.JobType);return nil};if err=h(ctx,json.RawMessage(j.Payload));err!=nil{if ctx.Err()!=nil{return ctx.Err()};delay:=w.retryDelay(j.Attempts);if e:=w.queue.Retry(ctx,j.ID,w.config.WorkerID,delay);e!=nil{return e};w.logger.Error("job_failed","job_id",j.ID,"job_type",j.JobType,"attempt",j.Attempts,"retry_delay",delay.String(),"error",err);return nil};if err=w.queue.Complete(ctx,j.ID,w.config.WorkerID);err!=nil{return err};w.logger.Info("job_completed","job_id",j.ID,"job_type",j.JobType,"worker_id",w.config.WorkerID);return nil}
func(w *Worker)retryDelay(attempt int)time.Duration{if attempt<1{return w.config.RetryBackoff};delay:=w.config.RetryBackoff;for i:=1;i<attempt;i++{if delay>24*time.Hour/2{return 24*time.Hour};delay*=2};return delay}
