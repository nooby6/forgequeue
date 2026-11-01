package job

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRepositoryCancelPendingJob(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	// Serialize extension initialization with the queue package's integration tests.
	const extensionLock int64 = 748392615
	if _, err := admin.Exec(ctx, "SELECT pg_advisory_lock($1)", extensionLock); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("lock pgcrypto initialization: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public"); err != nil {
		_, _ = admin.Exec(ctx, "SELECT pg_advisory_unlock($1)", extensionLock)
		_ = admin.Close(ctx)
		t.Fatalf("ensure pgcrypto extension: %v", err)
	}
	if _, err := admin.Exec(ctx, "SELECT pg_advisory_unlock($1)", extensionLock); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("unlock pgcrypto initialization: %v", err)
	}
	schema := fmt.Sprintf("forgequeue_job_cancel_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("create test schema: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin connection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		conn, err := pgx.Connect(cleanupCtx, dsn)
		if err != nil {
			t.Errorf("connect to clean up schema: %v", err)
			return
		}
		defer conn.Close(cleanupCtx)
		if _, err := conn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse database config: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	t.Cleanup(pool.Close)

	for _, path := range []string{
		"../../db/migrations/000001_initial_schema.up.sql",
		"../../db/migrations/000002_worker_leases.up.sql",
		"../../db/migrations/000003_retries.up.sql",
		"../../db/migrations/000004_dead_letter_jobs.up.sql",
		"../../db/migrations/000005_running_job_cancellation.up.sql",
	} {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", path, err)
		}
	}

	repo := NewPostgresRepository(pool)
	scheduledAt := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Microsecond)
	pending, err := repo.Create(ctx, CreateRequest{
		IdempotencyKey: fmt.Sprintf("cancel-pending-%d", time.Now().UnixNano()),
		QueueName:      "cancel-test",
		JobType:        "test",
		Payload:        []byte(`{}`),
		MaxAttempts:    3,
		AvailableAt:    &scheduledAt,
	})
	if err != nil {
		t.Fatalf("create pending job: %v", err)
	}

	cancelled, err := repo.Cancel(ctx, pending.ID)
	if err != nil {
		t.Fatalf("cancel pending job: %v", err)
	}
	if cancelled.Status != StatusCancelled {
		t.Fatalf("cancelled status = %q; want %q", cancelled.Status, StatusCancelled)
	}

	repeated, err := repo.Cancel(ctx, pending.ID)
	if err != nil {
		t.Fatalf("repeat cancellation should be idempotent: %v", err)
	}
	if repeated.Status != StatusCancelled {
		t.Fatalf("repeated cancellation status = %q; want %q", repeated.Status, StatusCancelled)
	}

	running, err := repo.Create(ctx, CreateRequest{
		IdempotencyKey: fmt.Sprintf("cancel-running-%d", time.Now().UnixNano()),
		QueueName:      "cancel-test",
		JobType:        "test",
		Payload:        []byte(`{}`),
		MaxAttempts:    3,
	})
	if err != nil {
		t.Fatalf("create second job: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET status='running',locked_by='worker-1',locked_at=NOW(),lease_expires_at=NOW()+INTERVAL '1 minute' WHERE id=$1", running.ID); err != nil {
		t.Fatalf("mark job running: %v", err)
	}
	requested, err := repo.Cancel(ctx, running.ID)
	if err != nil {
		t.Fatalf("request running-job cancellation: %v", err)
	}
	if requested.Status != StatusRunning || requested.CancellationRequestedAt == nil {
		t.Fatalf("running cancellation should remain running with a request timestamp: %+v", requested)
	}
	repeatedRequest, err := repo.Cancel(ctx, running.ID)
	if err != nil {
		t.Fatalf("repeated running cancellation should be idempotent: %v", err)
	}
	if repeatedRequest.CancellationRequestedAt == nil || !repeatedRequest.CancellationRequestedAt.Equal(*requested.CancellationRequestedAt) {
		t.Fatalf("repeated cancellation should preserve the original request timestamp: first=%v second=%v", requested.CancellationRequestedAt, repeatedRequest.CancellationRequestedAt)
	}

	if _, err := repo.Cancel(ctx, "00000000-0000-0000-0000-000000000000"); err != ErrNotFound {
		t.Fatalf("cancel missing job error = %v; want ErrNotFound", err)
	}
}
