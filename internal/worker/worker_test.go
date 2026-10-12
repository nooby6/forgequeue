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
	mu         sync.Mutex
	job        job.Job
	claimed    bool
	claims     int
	completed  int
	failed     int
	retried    int
	retryDelay time.Duration
	onRetry    func()
}

func (f *fakeQueue) ClaimNext(context.Context, string, string, time.Duration) (job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	if f.claimed {
		return job.Job{}, queue.ErrNoJob
	}
	f.claimed = true
	return f.job, nil
}

func (f *fakeQueue) Complete(context.Context, string, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completed++
	return nil
}

func (f *fakeQueue) Fail(context.Context, string, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed++
	return nil
}

func (f *fakeQueue) Retry(_ context.Context, _ string, _ string, delay time.Duration) error {
	f.mu.Lock()
	f.retried++
	f.retryDelay = delay
	onRetry := f.onRetry
	f.mu.Unlock()
	if onRetry != nil {
		onRetry()
	}
	return nil
}

func TestRegistryRejectsDuplicateHandlers(t *testing.T) {
	r := NewRegistry()
	h := func(context.Context, json.RawMessage) error { return nil }
	if err := r.Register("email", h); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("email", h); err == nil {
		t.Fatal("expected duplicate registration to fail")
	}
}

func TestWorkerProcessesJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fq := &fakeQueue{job: job.Job{ID: "job-1", JobType: "noop", Payload: json.RawMessage("{}")}}
	reg := NewRegistry()
	if err := reg.Register("noop", func(context.Context, json.RawMessage) error {
		cancel()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	w := New(fq, reg, slog.Default(), Config{QueueName: "default", Concurrency: 1, PollInterval: time.Millisecond, Lease: time.Second, RetryBackoff: time.Second, WorkerID: "test-worker"})
	w.Run(ctx)
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.completed != 1 {
		t.Fatalf("expected 1 completed job, got %d", fq.completed)
	}
	if fq.failed != 0 {
		t.Fatalf("expected 0 failed jobs, got %d", fq.failed)
	}
}

func TestWorkerRetriesHandlerFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fq := &fakeQueue{job: job.Job{ID: "job-2", JobType: "flaky", Payload: json.RawMessage("{}"), Attempts: 2}, onRetry: cancel}
	reg := NewRegistry()
	if err := reg.Register("flaky", func(context.Context, json.RawMessage) error { return errors.New("temporary failure") }); err != nil {
		t.Fatal(err)
	}
	w := New(fq, reg, slog.Default(), Config{QueueName: "default", Concurrency: 1, PollInterval: time.Millisecond, Lease: time.Second, RetryBackoff: 2 * time.Second, WorkerID: "test-worker"})
	w.Run(ctx)
	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.retried != 1 {
		t.Fatalf("expected 1 retry, got %d", fq.retried)
	}
	if fq.retryDelay != 4*time.Second {
		t.Fatalf("expected exponential retry delay of 4s, got %s", fq.retryDelay)
	}
}

func TestWorkerGracefulShutdownWaitsForInFlightJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fq := &fakeQueue{job: job.Job{ID: "job-shutdown", JobType: "slow", Payload: json.RawMessage("{}")}}
	reg := NewRegistry()
	started := make(chan struct{})
	release := make(chan struct{})
	if err := reg.Register("slow", func(context.Context, json.RawMessage) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	w := New(fq, reg, slog.New(slog.NewTextHandler(discardWriter{}, nil)), Config{
		QueueName: "default", Concurrency: 1, PollInterval: time.Millisecond,
		Lease: time.Second, ShutdownTimeout: time.Second, WorkerID: "test-worker",
	})
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()

	select {
	case <-done:
		t.Fatal("worker returned before the in-flight job finished")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after the in-flight job finished")
	}

	fq.mu.Lock()
	defer fq.mu.Unlock()
	if fq.completed != 1 {
		t.Fatalf("expected in-flight job to complete, got %d completions", fq.completed)
	}
	if fq.claims != 1 {
		t.Fatalf("expected no further claims during shutdown, got %d claim attempts", fq.claims)
	}
}

func TestWorkerCancelsHandlerAfterShutdownTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fq := &fakeQueue{job: job.Job{ID: "job-timeout", JobType: "blocked", Payload: json.RawMessage("{}")}}
	reg := NewRegistry()
	started := make(chan struct{})
	if err := reg.Register("blocked", func(ctx context.Context, _ json.RawMessage) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	w := New(fq, reg, slog.New(slog.NewTextHandler(discardWriter{}, nil)), Config{
		QueueName: "default", Concurrency: 1, PollInterval: time.Millisecond,
		Lease: time.Second, ShutdownTimeout: 30 * time.Millisecond, WorkerID: "test-worker",
	})
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not cancel handler after the shutdown timeout")
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestRetryDelayCapsAt24Hours(t *testing.T) {
	w := New(&fakeQueue{}, NewRegistry(), slog.Default(), Config{RetryBackoff: 2 * time.Hour})
	if got := w.retryDelay(20); got != 24*time.Hour {
		t.Fatalf("expected 24h cap, got %s", got)
	}
}
