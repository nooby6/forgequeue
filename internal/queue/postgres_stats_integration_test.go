package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPostgresQueueStatsAggregateStatusesAndDeadLetters(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	queueName := fmt.Sprintf("stats-%d", time.Now().UnixNano())

	insertIntegrationJob(t, pool, queueName, 0, 3, "pending", time.Time{})
	insertIntegrationJob(t, pool, queueName, 1, 3, "running", time.Now().Add(time.Minute))
	insertIntegrationJob(t, pool, queueName, 1, 3, "succeeded", time.Time{})
	insertIntegrationJob(t, pool, queueName, 3, 3, "failed", time.Time{})
	insertIntegrationJob(t, pool, queueName, 0, 3, "cancelled", time.Time{})
	insertIntegrationJob(t, pool, queueName+"-other", 0, 3, "pending", time.Time{})

	_, err := pool.Exec(ctx, `
		INSERT INTO dead_letter_jobs
			(job_id, queue_name, job_type, payload, attempts, max_attempts, failure_reason)
		SELECT id, queue_name, job_type, payload, attempts, max_attempts, 'test failure'
		FROM jobs WHERE queue_name = $1 AND status = 'failed'
	`, queueName)
	if err != nil {
		t.Fatalf("insert dead-letter record: %v", err)
	}

	stats, err := NewPostgresQueue(pool).Stats(ctx, queueName)
	if err != nil {
		t.Fatalf("read queue stats: %v", err)
	}
	if stats.QueueName != queueName || stats.Pending != 1 || stats.Running != 1 ||
		stats.Succeeded != 1 || stats.Failed != 1 || stats.Cancelled != 1 ||
		stats.ActiveDeadLetters != 1 {
		t.Fatalf("unexpected queue stats: %+v", stats)
	}
	if stats.OldestPendingAt == nil {
		t.Fatal("expected oldest_pending_at for a queue with pending jobs")
	}

	empty, err := NewPostgresQueue(pool).Stats(ctx, queueName+"-empty")
	if err != nil {
		t.Fatalf("read empty queue stats: %v", err)
	}
	if empty.Pending != 0 || empty.OldestPendingAt != nil || empty.ActiveDeadLetters != 0 {
		t.Fatalf("unexpected empty queue stats: %+v", empty)
	}
}
