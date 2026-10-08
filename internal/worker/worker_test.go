package worker

import ("context";"encoding/json";"errors";"log/slog";"sync";"testing";"time";"github.com/nooby6/forgequeue/internal/job";"github.com/nooby6/forgequeue/internal/queue")

type fakeQueue struct{mu sync.Mutex;job job.Job;claimed bool;completed int;failed int;retried int;retryDelay time.Duration}
func(f *fakeQueue)ClaimNext(context.Context,string,string,time.Duration)(job.Job,error){f.mu.Lock();defer f.mu.Unlock();if f.claimed{return job.Job{},queue.ErrNoJob};f.claimed=true;return f.job,nil}
func(f *fakeQueue)Complete(context.Context,string,string)error{f.mu.Lock();defer f.mu.Unlock();f.completed++;return nil}
func(f *fakeQueue)Fail(context.Context,string,string)error{f.mu.Lock();defer f.mu.Unlock();f.failed++;return nil}
func(f *fakeQueue)Retry(_ context.Context,_ string,_ string,delay time.Duration)error{f.mu.Lock();defer f.mu.Unlock();f.retried++;f.retryDelay=delay;return nil}

func TestRegistryRejectsDuplicateHandlers(t *testing.T){r:=NewRegistry();h:=func(context.Context,json.RawMessage)error{return nil};if err:=r.Register("email",h);err!=nil{t.Fatal(err)};if err:=r.Register("email",h);err==nil{t.Fatal("expected duplicate registration to fail")}}

func TestWorkerProcessesJob(t *testing.T){ctx,cancel:=context.WithCancel(context.Background());defer cancel();fq:=&fakeQueue{job:job.Job{ID:"job-1",JobType:"noop",Payload:json.RawMessage("{}")}};reg:=NewRegistry();if err:=reg.Register("noop",func(context.Context,json.RawMessage)error{cancel();return nil});err!=nil{t.Fatal(err)};w:=New(fq,reg,slog.Default(),Config{QueueName:"default",Concurrency:1,PollInterval:time.Millisecond,Lease:time.Second,RetryBackoff:time.Second,WorkerID:"test-worker"});w.Run(ctx);fq.mu.Lock();defer fq.mu.Unlock();if fq.completed!=1{t.Fatalf("expected 1 completed job, got %d",fq.completed)};if fq.failed!=0{t.Fatalf("expected 0 failed jobs, got %d",fq.failed)}}

func TestWorkerRetriesHandlerFailure(t *testing.T){ctx,cancel:=context.WithCancel(context.Background());defer cancel();fq:=&fakeQueue{job:job.Job{ID:"job-2",JobType:"flaky",Payload:json.RawMessage("{}"),Attempts:2}};reg:=NewRegistry();if err:=reg.Register("flaky",func(context.Context,json.RawMessage)error{cancel();return errors.New("temporary failure")});err!=nil{t.Fatal(err)};w:=New(fq,reg,slog.Default(),Config{QueueName:"default",Concurrency:1,PollInterval:time.Millisecond,Lease:time.Second,RetryBackoff:2*time.Second,WorkerID:"test-worker"});w.Run(ctx);fq.mu.Lock();defer fq.mu.Unlock();if fq.retried!=1{t.Fatalf("expected 1 retry, got %d",fq.retried)};if fq.retryDelay!=4*time.Second{t.Fatalf("expected exponential retry delay of 4s, got %s",fq.retryDelay)}}

func TestRetryDelayCapsAt24Hours(t *testing.T){w:=New(&fakeQueue{},NewRegistry(),slog.Default(),Config{RetryBackoff:2*time.Hour});if got:=w.retryDelay(20);got!=24*time.Hour{t.Fatalf("expected 24h cap, got %s",got)}}
