package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPostgresQueueAcknowledgesRunningCancellation(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	queueName := fmt.Sprintf("cancel-running-%d", time.Now().UnixNano())
	id := insertIntegrationJob(t, pool, queueName, 0, 3, "pending", time.Time{})
	q := NewPostgresQueue(pool)

	claimed, err := q.ClaimNext(ctx, queueName, "worker-cancel-test", time.Minute)
	if err != nil { t.Fatalf("claim job: %v", err) }
	if claimed.ID != id { t.Fatalf("claimed %s, want %s", claimed.ID, id) }

	if _, err := pool.Exec(ctx, "UPDATE jobs SET cancellation_requested_at=NOW() WHERE id=$1", id); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	if err := q.RenewLease(ctx, id, "worker-cancel-test", time.Minute); err != ErrCancellationRequested {
		t.Fatalf("renewal error = %v, want ErrCancellationRequested", err)
	}
	if err := q.CancelRunning(ctx, id, "worker-cancel-test"); err != nil {
		t.Fatalf("acknowledge cancellation: %v", err)
	}

	var status string
	var lockedBy *string
	var requestedAt *time.Time
	if err := pool.QueryRow(ctx, "SELECT status,locked_by,cancellation_requested_at FROM jobs WHERE id=$1", id).Scan(&status, &lockedBy, &requestedAt); err != nil {
		t.Fatalf("read final job: %v", err)
	}
	if status != "cancelled" || lockedBy != nil || requestedAt == nil {
		t.Fatalf("unexpected final cancellation state: status=%q locked_by=%v requested_at=%v", status, lockedBy, requestedAt)
	}
}

func TestPostgresQueueFinalizesAbandonedCancellationAfterLeaseExpiry(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	queueName := fmt.Sprintf("cancel-expired-%d", time.Now().UnixNano())
	cancelledID := insertIntegrationJob(t, pool, queueName, 1, 3, "running", time.Now().Add(-time.Minute))
	insertIntegrationJob(t, pool, queueName, 0, 3, "pending", time.Time{})
	if _, err := pool.Exec(ctx, "UPDATE jobs SET cancellation_requested_at=NOW() WHERE id=$1", cancelledID); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}

	claimed, err := NewPostgresQueue(pool).ClaimNext(ctx, queueName, "replacement-worker", time.Minute)
	if err != nil { t.Fatalf("claim next job: %v", err) }
	if claimed.ID == cancelledID { t.Fatal("cancelled running job was incorrectly reclaimed") }

	var status string
	if err := pool.QueryRow(ctx, "SELECT status FROM jobs WHERE id=$1", cancelledID).Scan(&status); err != nil {
		t.Fatalf("read abandoned job: %v", err)
	}
	if status != "cancelled" { t.Fatalf("abandoned cancellation status = %q, want cancelled", status) }
}
