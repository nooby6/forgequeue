package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestPostgresQueueRecoversJobAfterWorkerRestart exercises the durable queue
// boundary across two distinct worker instances: the first worker claims a job
// and disappears without acknowledging it, its lease expires, and a replacement
// instance reclaims and completes the job. This intentionally tests the real
// PostgreSQL lease/recovery path without relying on process scheduling in CI.
func TestPostgresQueueRecoversJobAfterWorkerRestart(t *testing.T) {
	pool := integrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("restart-recovery-%d", time.Now().UnixNano())
	id := insertIntegrationJob(t, pool, queueName, 0, 3, "pending", time.Time{})
	q := NewPostgresQueue(pool)

	const lease = 120 * time.Millisecond
	first, err := q.ClaimNext(ctx, queueName, "worker-instance-before-restart/slot-0", lease)
	if err != nil {
		t.Fatalf("first worker claim: %v", err)
	}
	if first.ID != id || first.Attempts != 1 {
		t.Fatalf("first claim = id %q attempts %d; want id %q attempts 1", first.ID, first.Attempts, id)
	}

	// A live lease must prevent a replacement instance from stealing the job.
	if _, err := q.ClaimNext(ctx, queueName, "worker-instance-after-restart/slot-0", lease); err != ErrNoJob {
		t.Fatalf("replacement claim before lease expiry returned %v; want ErrNoJob", err)
	}

	// Model an abrupt process exit: no renewal, completion, retry, or graceful
	// shutdown runs. Waiting on the stored lease makes recovery deterministic.
	time.Sleep(lease + 30*time.Millisecond)

	replacementID := "worker-instance-after-restart/slot-0"
	recovered, err := q.ClaimNext(ctx, queueName, replacementID, lease)
	if err != nil {
		t.Fatalf("replacement worker could not recover expired job: %v", err)
	}
	if recovered.ID != id {
		t.Fatalf("replacement claimed job %q; want %q", recovered.ID, id)
	}
	if recovered.Attempts != 2 {
		t.Fatalf("recovered attempts = %d; want 2", recovered.Attempts)
	}

	if err := q.Complete(ctx, id, firstWorkerIdentity(first.ID, "worker-instance-before-restart/slot-0")); err == nil {
		t.Fatal("stale worker completed a job after replacement claimed it")
	}
	if err := q.Complete(ctx, id, replacementID); err != nil {
		t.Fatalf("replacement worker completion: %v", err)
	}

	var status string
	var attempts int
	var lockedBy *string
	if err := pool.QueryRow(ctx, "SELECT status, attempts, locked_by FROM jobs WHERE id=$1", id).Scan(&status, &attempts, &lockedBy); err != nil {
		t.Fatalf("read final job state: %v", err)
	}
	if status != "succeeded" || attempts != 2 || lockedBy != nil {
		t.Fatalf("final state status=%q attempts=%d locked_by=%v; want succeeded, 2, NULL", status, attempts, lockedBy)
	}

	letters, err := q.ListDeadLetters(ctx, queueName, 10)
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	if len(letters) != 0 {
		t.Fatalf("dead letters = %d; want none for successfully recovered job", len(letters))
	}
}

// firstWorkerIdentity keeps the stale owner explicit in the assertion above.
// The job ID is accepted to make the call site read as a worker/job pair.
func firstWorkerIdentity(_ string, identity string) string { return identity }
