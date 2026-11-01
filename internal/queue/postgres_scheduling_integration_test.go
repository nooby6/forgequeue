package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nooby6/forgequeue/internal/job"
)

func TestPostgresQueueDoesNotClaimScheduledJobBeforeAvailableAt(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	queueName := fmt.Sprintf("scheduled-%d", time.Now().UnixNano())
	availableAt := time.Now().UTC().Add(time.Hour)

	created, err := job.NewPostgresRepository(pool).Create(ctx, job.CreateRequest{
		QueueName:   queueName,
		JobType:     "scheduled-test",
		Payload:     []byte(`{}`),
		MaxAttempts: 3,
		AvailableAt: &availableAt,
	})
	if err != nil {
		t.Fatalf("create scheduled job: %v", err)
	}

	q := NewPostgresQueue(pool)
	if _, err := q.ClaimNext(ctx, queueName, "scheduled-worker", time.Minute); !errors.Is(err, ErrNoJob) {
		t.Fatalf("claim before available_at error = %v; want ErrNoJob", err)
	}

	if _, err := pool.Exec(ctx, "UPDATE jobs SET available_at = NOW() WHERE id = $1", created.ID); err != nil {
		t.Fatalf("make scheduled job available: %v", err)
	}
	claimed, err := q.ClaimNext(ctx, queueName, "scheduled-worker", time.Minute)
	if err != nil {
		t.Fatalf("claim job after available_at: %v", err)
	}
	if claimed.ID != created.ID {
		t.Fatalf("claimed job = %s; want %s", claimed.ID, created.ID)
	}
}
