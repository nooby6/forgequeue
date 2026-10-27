package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nooby6/forgequeue/internal/job"
	"github.com/nooby6/forgequeue/internal/queue"
)

type Config struct {
	QueueName string
	Concurrency int
	PollInterval time.Duration
	Lease time.Duration
	RetryBackoff time.Duration
	ShutdownTimeout time.Duration
	WorkerID string
}

type Worker struct {
	queue queue.Queue
	registry *Registry
	logger *slog.Logger
	config Config
	instanceID string
}

func New(q queue.Queue, r *Registry, l *slog.Logger, c Config) *Worker {
	if c.Concurrency < 1 { c.Concurrency = 1 }
	if c.PollInterval <= 0 { c.PollInterval = 500 * time.Millisecond }
	if c.Lease <= 0 { c.Lease = 30 * time.Second }
	if c.RetryBackoff <= 0 { c.RetryBackoff = time.Second }
	if c.ShutdownTimeout <= 0 { c.ShutdownTimeout = 10 * time.Second }
	if c.WorkerID == "" { c.WorkerID = "forgequeue-worker" }
	if l == nil { l = slog.Default() }
	return &Worker{queue:q, registry:r, logger:l, config:c, instanceID:newInstanceID()}
}

func newInstanceID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err == nil { return hex.EncodeToString(bytes[:]) }
	return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
}

func (w *Worker) workerIdentity(slot int) string {
	return fmt.Sprintf("%s/%s/%d", w.config.WorkerID, w.instanceID, slot)
}

// Run stops claiming jobs when ctx is canceled and gives active handlers up to
// ShutdownTimeout to finish. Handlers should honor their context.
func (w *Worker) Run(ctx context.Context) {
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	shutdownFinished := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			timer := time.NewTimer(w.config.ShutdownTimeout)
			defer timer.Stop()
			select {
			case <-timer.C: cancelWork()
			case <-shutdownFinished:
			}
		case <-shutdownFinished:
		}
	}()
	var wg sync.WaitGroup
	wg.Add(w.config.Concurrency)
	for i:=0;i<w.config.Concurrency;i++ {
		go func(slot int){ defer wg.Done(); w.loop(ctx,workCtx,slot) }(i)
	}
	wg.Wait()
	close(shutdownFinished)
}

func (w *Worker) loop(ctx, workCtx context.Context, slot int) {
	ticker:=time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()
	workerID:=w.workerIdentity(slot)
	for {
		if ctx.Err()!=nil { return }
		err:=w.processOne(ctx,workCtx,workerID)
		if err==queue.ErrNoJob {
			select { case <-ctx.Done(): return; case <-ticker.C: }
			continue
		}
		if err!=nil {
			if ctx.Err()==nil { w.logger.Error("worker_iteration_failed","worker_id",workerID,"slot",slot,"error",err) }
			select { case <-ctx.Done(): return; case <-ticker.C: }
		}
		if ctx.Err()!=nil { return }
	}
}

func (w *Worker) processOne(ctx, workCtx context.Context, workerID string) error {
	j,err:=w.queue.ClaimNext(ctx,w.config.QueueName,workerID,w.config.Lease)
	if err!=nil { return err }
	h,ok:=w.registry.Lookup(j.JobType)
	if !ok {
		if e:=w.queue.Fail(workCtx,j.ID,workerID); e!=nil { return e }
		w.logger.Error("job_handler_missing","job_id",j.ID,"job_type",j.JobType)
		return nil
	}

	handlerCtx,cancelHandler:=context.WithCancel(workCtx)
	leaseLost:=make(chan error,1)
	stopRenewal:=make(chan struct{})
	renewalDone:=make(chan struct{})
	go w.renewLeaseUntilStopped(j,workerID,cancelHandler,stopRenewal,renewalDone,leaseLost)
	handlerErr:=h(handlerCtx,json.RawMessage(j.Payload))
	close(stopRenewal)
	<-renewalDone
	cancelHandler()

	select {
	case renewErr:=<-leaseLost:
		if errors.Is(renewErr,queue.ErrCancellationRequested) {
			return w.acknowledgeCancellation(j.ID,workerID)
		}
		return fmt.Errorf("job %s lease renewal failed: %w",j.ID,renewErr)
	default:
	}

	if handlerErr!=nil {
		if ctx.Err()!=nil || workCtx.Err()!=nil { return handlerErr }
		delay:=w.retryDelay(j.Attempts)
		if e:=w.queue.Retry(workCtx,j.ID,workerID,delay); e!=nil {
			if errors.Is(e,queue.ErrCancellationRequested) { return w.acknowledgeCancellation(j.ID,workerID) }
			return e
		}
		w.logger.Error("job_failed","job_id",j.ID,"job_type",j.JobType,"attempt",j.Attempts,"retry_delay",delay.String(),"error",handlerErr)
		return nil
	}
	if err=w.queue.Complete(workCtx,j.ID,workerID); err!=nil {
		if errors.Is(err,queue.ErrCancellationRequested) { return w.acknowledgeCancellation(j.ID,workerID) }
		return err
	}
	w.logger.Info("job_completed","job_id",j.ID,"job_type",j.JobType,"worker_id",workerID)
	return nil
}

func (w *Worker) acknowledgeCancellation(id, workerID string) error {
	ctx,cancel:=context.WithTimeout(context.Background(),3*time.Second)
	defer cancel()
	if err:=w.queue.CancelRunning(ctx,id,workerID); err!=nil {
		return fmt.Errorf("acknowledge cancellation for job %s: %w",id,err)
	}
	w.logger.Info("job_cancelled","job_id",id,"worker_id",workerID)
	return nil
}

func (w *Worker) renewLeaseUntilStopped(j job.Job, workerID string, cancelHandler context.CancelFunc, stop <-chan struct{}, done chan<- struct{}, leaseLost chan<- error) {
	defer close(done)
	interval:=w.config.Lease/3
	if interval<=0 { interval=time.Millisecond }
	ticker:=time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop: return
		case <-ticker.C:
			renewCtx,cancel:=context.WithTimeout(context.Background(),interval)
			err:=w.queue.RenewLease(renewCtx,j.ID,workerID,w.config.Lease)
			cancel()
			if err!=nil {
				select { case leaseLost<-err: default: }
				cancelHandler()
				return
			}
		}
	}
}

func (w *Worker) retryDelay(attempt int) time.Duration {
	if attempt<1 { return w.config.RetryBackoff }
	delay:=w.config.RetryBackoff
	for i:=1;i<attempt;i++ {
		if delay>24*time.Hour/2 { return 24*time.Hour }
		delay*=2
	}
	return delay
}
