package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nooby6/forgequeue/internal/job"
	"github.com/nooby6/forgequeue/internal/queue"
)

type fakeQueue struct {
	mu sync.Mutex
	job job.Job
	claimed bool
	claims int
	completed int
	cancelled int
	failed int
	retried int
	renewals int
	claimWorkerIDs []string
	completeWorkerIDs []string
	retryDelay time.Duration
	renewErr error
	onRetry func()
	onRenew func()
}
func (f *fakeQueue) ClaimNext(_ context.Context,_ string,workerID string,_ time.Duration)(job.Job,error){
	f.mu.Lock();defer f.mu.Unlock();f.claims++;f.claimWorkerIDs=append(f.claimWorkerIDs,workerID)
	if f.claimed{return job.Job{},queue.ErrNoJob};f.claimed=true;return f.job,nil
}
func (f *fakeQueue) RenewLease(context.Context,string,string,time.Duration)error{
	f.mu.Lock();f.renewals++;onRenew:=f.onRenew;err:=f.renewErr;f.mu.Unlock();if onRenew!=nil{onRenew()};return err
}
func (f *fakeQueue) Complete(_ context.Context,_ string,workerID string)error{
	f.mu.Lock();defer f.mu.Unlock();f.completed++;f.completeWorkerIDs=append(f.completeWorkerIDs,workerID);return nil
}
func (f *fakeQueue) CancelRunning(context.Context,string,string)error{f.mu.Lock();defer f.mu.Unlock();f.cancelled++;return nil}
func (f *fakeQueue) Fail(context.Context,string,string)error{f.mu.Lock();defer f.mu.Unlock();f.failed++;return nil}
func (f *fakeQueue) Retry(_ context.Context,_ string,_ string,delay time.Duration)error{
	f.mu.Lock();f.retried++;f.retryDelay=delay;onRetry:=f.onRetry;f.mu.Unlock();if onRetry!=nil{onRetry()};return nil
}

func TestRegistryRejectsDuplicateHandlers(t *testing.T){
	r:=NewRegistry();h:=func(context.Context,json.RawMessage)error{return nil}
	if err:=r.Register("email",h);err!=nil{t.Fatal(err)}
	if err:=r.Register("email",h);err==nil{t.Fatal("expected duplicate registration to fail")}
}
func TestWorkerProcessesJob(t *testing.T){
	ctx,cancel:=context.WithCancel(context.Background());defer cancel()
	fq:=&fakeQueue{job:job.Job{ID:"job-1",JobType:"noop",Payload:json.RawMessage("{}")}}
	reg:=NewRegistry();if err:=reg.Register("noop",func(context.Context,json.RawMessage)error{cancel();return nil});err!=nil{t.Fatal(err)}
	w:=New(fq,reg,slog.Default(),Config{QueueName:"default",Concurrency:1,PollInterval:time.Millisecond,Lease:time.Second,RetryBackoff:time.Second,WorkerID:"test-worker"})
	w.Run(ctx);fq.mu.Lock();defer fq.mu.Unlock()
	if fq.completed!=1{t.Fatalf("expected 1 completed job, got %d",fq.completed)}
	if fq.failed!=0{t.Fatalf("expected 0 failed jobs, got %d",fq.failed)}
	if len(fq.claimWorkerIDs)!=1||len(fq.completeWorkerIDs)!=1{t.Fatalf("expected one claim and completion identity, got claims=%v completions=%v",fq.claimWorkerIDs,fq.completeWorkerIDs)}
	if fq.claimWorkerIDs[0]==""||fq.claimWorkerIDs[0]!=fq.completeWorkerIDs[0]{t.Fatalf("claim and completion must use same identity: claims=%v completions=%v",fq.claimWorkerIDs,fq.completeWorkerIDs)}
	if fq.claimWorkerIDs[0]=="test-worker"{t.Fatal("lease identity must include instance and slot")}
}
func TestWorkerRetriesHandlerFailure(t *testing.T){
	ctx,cancel:=context.WithCancel(context.Background());defer cancel()
	fq:=&fakeQueue{job:job.Job{ID:"job-2",JobType:"flaky",Payload:json.RawMessage("{}"),Attempts:2},onRetry:cancel}
	reg:=NewRegistry();if err:=reg.Register("flaky",func(context.Context,json.RawMessage)error{return errors.New("temporary failure")});err!=nil{t.Fatal(err)}
	w:=New(fq,reg,slog.Default(),Config{QueueName:"default",Concurrency:1,PollInterval:time.Millisecond,Lease:time.Second,RetryBackoff:2*time.Second,WorkerID:"test-worker"})
	w.Run(ctx);fq.mu.Lock();defer fq.mu.Unlock()
	if fq.retried!=1{t.Fatalf("expected 1 retry, got %d",fq.retried)}
	if fq.retryDelay!=4*time.Second{t.Fatalf("expected 4s delay, got %s",fq.retryDelay)}
}
func TestWorkerCooperativelyCancelsRunningHandler(t *testing.T){
	ctx,cancel:=context.WithCancel(context.Background());defer cancel();fq:=&fakeQueue{job:job.Job{ID:"job-cancel",JobType:"blocking",Payload:json.RawMessage("{}")},renewErr:queue.ErrCancellationRequested}
	reg:=NewRegistry();started:=make(chan struct{})
	if err:=reg.Register("blocking",func(ctx context.Context,_ json.RawMessage)error{close(started);<-ctx.Done();return ctx.Err()});err!=nil{t.Fatal(err)}
	w:=New(fq,reg,slog.New(slog.NewTextHandler(discardWriter{},nil)),Config{QueueName:"default",Concurrency:1,PollInterval:time.Millisecond,Lease:30*time.Millisecond,ShutdownTimeout:time.Second})
	done:=make(chan struct{});go func(){w.Run(ctx);close(done)}()
	select{case <-started:case <-time.After(time.Second):t.Fatal("handler did not start")};cancel()
	select{case <-done:case <-time.After(time.Second):t.Fatal("worker did not acknowledge cancellation")}
	fq.mu.Lock();defer fq.mu.Unlock()
	if fq.cancelled!=1{t.Fatalf("expected cancellation acknowledgement, got %d",fq.cancelled)}
	if fq.completed!=0||fq.retried!=0{t.Fatalf("cancelled job must not complete/retry: completed=%d retried=%d",fq.completed,fq.retried)}
}
func TestWorkerGracefulShutdownWaitsForInFlightJob(t *testing.T){
	ctx,cancel:=context.WithCancel(context.Background());fq:=&fakeQueue{job:job.Job{ID:"job-shutdown",JobType:"slow",Payload:json.RawMessage("{}")}}
	reg:=NewRegistry();started:=make(chan struct{});release:=make(chan struct{})
	if err:=reg.Register("slow",func(context.Context,json.RawMessage)error{close(started);<-release;return nil});err!=nil{t.Fatal(err)}
	w:=New(fq,reg,slog.New(slog.NewTextHandler(discardWriter{},nil)),Config{QueueName:"default",Concurrency:1,PollInterval:time.Millisecond,Lease:time.Second,ShutdownTimeout:time.Second,WorkerID:"test-worker"})
	done:=make(chan struct{});go func(){w.Run(ctx);close(done)}()
	select{case <-started:case <-time.After(time.Second):t.Fatal("handler did not start")};cancel()
	select{case <-done:t.Fatal("worker returned before in-flight job finished");case <-time.After(20*time.Millisecond):}
	close(release);select{case <-done:case <-time.After(time.Second):t.Fatal("worker did not stop after in-flight job finished")}
	fq.mu.Lock();defer fq.mu.Unlock();if fq.completed!=1{t.Fatalf("expected in-flight job to complete, got %d",fq.completed)};if fq.claims!=1{t.Fatalf("expected no further claims, got %d",fq.claims)}
}
func TestWorkerCancelsHandlerAfterShutdownTimeout(t *testing.T){
	ctx,cancel:=context.WithCancel(context.Background());defer cancel();fq:=&fakeQueue{job:job.Job{ID:"job-timeout",JobType:"blocked",Payload:json.RawMessage("{}")}}
	reg:=NewRegistry();started:=make(chan struct{})
	if err:=reg.Register("blocked",func(ctx context.Context,_ json.RawMessage)error{close(started);<-ctx.Done();return ctx.Err()});err!=nil{t.Fatal(err)}
	w:=New(fq,reg,slog.New(slog.NewTextHandler(discardWriter{},nil)),Config{QueueName:"default",Concurrency:1,PollInterval:time.Millisecond,Lease:time.Second,ShutdownTimeout:30*time.Millisecond,WorkerID:"test-worker"})
	done:=make(chan struct{});go func(){w.Run(ctx);close(done)}()
	select{case <-started:case <-time.After(time.Second):t.Fatal("handler did not start")};cancel()
	select{case <-done:case <-time.After(time.Second):t.Fatal("worker did not cancel handler")}
}
func TestWorkerRenewsLeaseForLongRunningHandler(t *testing.T){
	ctx,cancel:=context.WithCancel(context.Background());defer cancel();renewed:=make(chan struct{},4)
	fq:=&fakeQueue{job:job.Job{ID:"job-long",JobType:"long",Payload:json.RawMessage("{}")},onRenew:func(){select{case renewed<-struct{}{}:default:}}}
	reg:=NewRegistry();if err:=reg.Register("long",func(context.Context,json.RawMessage)error{for i:=0;i<2;i++{select{case <-renewed:case <-time.After(time.Second):return errors.New("timed out waiting for lease renewal")}};cancel();return nil});err!=nil{t.Fatal(err)}
	w:=New(fq,reg,slog.Default(),Config{QueueName:"default",Concurrency:1,PollInterval:time.Millisecond,Lease:90*time.Millisecond,ShutdownTimeout:time.Second,WorkerID:"test-worker"});w.Run(ctx)
	fq.mu.Lock();defer fq.mu.Unlock();if fq.renewals<2{t.Fatalf("expected at least 2 renewals, got %d",fq.renewals)};if fq.completed!=1{t.Fatalf("expected completion, got %d",fq.completed)}
}
func TestWorkerStopsHandlerWhenLeaseRenewalFails(t *testing.T){
	ctx,cancel:=context.WithCancel(context.Background());fq:=&fakeQueue{job:job.Job{ID:"job-lost",JobType:"long",Payload:json.RawMessage("{}")},renewErr:errors.New("ownership lost")}
	reg:=NewRegistry();started:=make(chan struct{})
	if err:=reg.Register("long",func(ctx context.Context,_ json.RawMessage)error{close(started);<-ctx.Done();cancel();return ctx.Err()});err!=nil{t.Fatal(err)}
	w:=New(fq,reg,slog.New(slog.NewTextHandler(discardWriter{},nil)),Config{QueueName:"default",Concurrency:1,PollInterval:time.Millisecond,Lease:30*time.Millisecond,ShutdownTimeout:time.Second,WorkerID:"test-worker"})
	done:=make(chan struct{});go func(){w.Run(ctx);close(done)}()
	select{case <-started:case <-time.After(time.Second):t.Fatal("handler did not start")};select{case <-done:case <-time.After(time.Second):t.Fatal("worker did not stop after lease renewal failed")}
	fq.mu.Lock();defer fq.mu.Unlock();if fq.completed!=0{t.Fatalf("job must not complete after lease loss, got %d",fq.completed)};if fq.retried!=0{t.Fatalf("job must not retry after lease loss, got %d",fq.retried)}
}
type discardWriter struct{}
func(discardWriter)Write(p []byte)(int,error){return len(p),nil}
func TestRetryDelayCapsAt24Hours(t *testing.T){w:=New(&fakeQueue{},NewRegistry(),slog.Default(),Config{RetryBackoff:2*time.Hour});if got:=w.retryDelay(20);got!=24*time.Hour{t.Fatalf("expected 24h cap, got %s",got)}}
func TestWorkerLeaseIdentitiesAreUniquePerSlotAndInstance(t *testing.T){
	newWorker:=func()*Worker{return New(&fakeQueue{},NewRegistry(),slog.Default(),Config{QueueName:"default",Concurrency:2,WorkerID:"shared-hostname"})}
	first,second:=newWorker(),newWorker();identities:=[]string{first.workerIdentity(0),first.workerIdentity(1),second.workerIdentity(0),second.workerIdentity(1)};seen:=make(map[string]struct{},len(identities))
	for _,identity:=range identities{if identity==""{t.Fatal("identity must not be empty")};if _,ok:=seen[identity];ok{t.Fatalf("identity %q reused",identity)};seen[identity]=struct{}{}}
}
