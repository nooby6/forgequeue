package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nooby6/forgequeue/internal/queue"
)

// TestWorkerProcessHelper is re-executed in a child test process by
// TestWorkerRecoversJobAfterAbruptProcessExit. It is inert during normal runs.
func TestWorkerProcessHelper(t *testing.T) {
	phase := os.Getenv("FORGEQUEUE_PROCESS_TEST_PHASE")
	if phase == "" {
		return
	}

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is required in child process")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	defer pool.Close()

	registry := NewRegistry()
	marker := os.Getenv("FORGEQUEUE_PROCESS_TEST_MARKER")
	if err := registry.Register("process-recovery-test", func(handlerCtx context.Context, _ []byte) error {
		if phase == "first" {
			if err := os.WriteFile(marker, []byte("claimed"), 0600); err != nil {
				return err
			}
			<-handlerCtx.Done()
			return handlerCtx.Err()
		}
		if err := os.WriteFile(marker, []byte("completed"), 0600); err != nil {
			return err
		}
		cancel()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	w := New(queue.NewPostgresQueue(pool), registry, slog.New(slog.NewTextHandler(discardWriter{}, nil)), Config{
		QueueName: os.Getenv("FORGEQUEUE_PROCESS_TEST_QUEUE"),
		Concurrency: 1,
		PollInterval: 10 * time.Millisecond,
		Lease: 450 * time.Millisecond,
		ShutdownTimeout: 100 * time.Millisecond,
		WorkerID: "process-recovery-test",
	})
	w.Run(ctx)
}

func TestWorkerRecoversJobAfterAbruptProcessExit(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer setupCancel()
	admin, err := pgx.Connect(setupCtx, dsn)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	schema := fmt.Sprintf("worker_process_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(setupCtx, "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public"); err != nil {
		admin.Close(setupCtx)
		t.Fatalf("ensure pgcrypto extension: %v", err)
	}
	if _, err := admin.Exec(setupCtx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close(setupCtx)
		t.Fatalf("create isolated schema: %v", err)
	}
	if err := admin.Close(setupCtx); err != nil {
		t.Fatalf("close schema setup connection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		conn, err := pgx.Connect(cleanupCtx, dsn)
		if err != nil {
			t.Errorf("connect to drop schema: %v", err)
			return
		}
		defer conn.Close(cleanupCtx)
		if _, err := conn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(setupCtx, cfg)
	if err != nil {
		t.Fatalf("create isolated pool: %v", err)
	}
	defer pool.Close()

	for _, migration := range []string{
		"../../db/migrations/000001_initial_schema.up.sql",
		"../../db/migrations/000002_worker_leases.up.sql",
		"../../db/migrations/000003_retries.up.sql",
		"../../db/migrations/000004_dead_letter_jobs.up.sql",
	} {
		sql, err := os.ReadFile(migration)
		if err != nil {
			t.Fatalf("read migration %s: %v", migration, err)
		}
		if _, err := pool.Exec(setupCtx, string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", migration, err)
		}
	}

	queueName := fmt.Sprintf("process-recovery-%d", time.Now().UnixNano())
	var jobID string
	if err := pool.QueryRow(setupCtx, `
		INSERT INTO jobs (idempotency_key, queue_name, job_type, payload, status, max_attempts, available_at)
		VALUES (gen_random_uuid()::text, $1, 'process-recovery-test', '{}', 'pending', 3, NOW())
		RETURNING id::text
	`, queueName).Scan(&jobID); err != nil {
		t.Fatalf("insert test job: %v", err)
	}

	dir := t.TempDir()
	firstMarker := filepath.Join(dir, "first-claimed")
	firstCmd := exec.Command(os.Args[0], "-test.run=^TestWorkerProcessHelper$")
	firstCmd.Env = append(os.Environ(),
		"FORGEQUEUE_PROCESS_TEST_PHASE=first",
		"FORGEQUEUE_PROCESS_TEST_QUEUE="+queueName,
		"FORGEQUEUE_PROCESS_TEST_MARKER="+firstMarker,
		"PGOPTIONS=-c search_path="+schema+",public",
	)
	if err := firstCmd.Start(); err != nil {
		t.Fatalf("start first worker process: %v", err)
	}
	t.Cleanup(func() {
		if firstCmd.Process != nil {
			_ = firstCmd.Process.Kill()
			_, _ = firstCmd.Process.Wait()
		}
	})

	waitForFile(t, firstMarker, 5*time.Second)
	firstCmd.Process.Kill()
	_ = firstCmd.Wait()

	var attempts int
	var status string
	if err := pool.QueryRow(setupCtx, "SELECT attempts,status FROM jobs WHERE id=$1", jobID).Scan(&attempts, &status); err != nil {
		t.Fatalf("read job after worker termination: %v", err)
	}
	if status != "running" || attempts != 1 {
		t.Fatalf("after first process exit status=%q attempts=%d; want running/1", status, attempts)
	}

	// The killed process cannot renew the lease. Wait until PostgreSQL considers
	// it expired before starting the replacement worker.
	time.Sleep(650 * time.Millisecond)
	replacementMarker := filepath.Join(dir, "replacement-completed")
	replacementCmd := exec.Command(os.Args[0], "-test.run=^TestWorkerProcessHelper$")
	replacementCmd.Env = append(os.Environ(),
		"FORGEQUEUE_PROCESS_TEST_PHASE=replacement",
		"FORGEQUEUE_PROCESS_TEST_QUEUE="+queueName,
		"FORGEQUEUE_PROCESS_TEST_MARKER="+replacementMarker,
		"PGOPTIONS=-c search_path="+schema+",public",
	)
	output, err := replacementCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("replacement worker process failed: %v; output: %s", err, output)
	}
	waitForFile(t, replacementMarker, 5*time.Second)

	var lockedBy *string
	if err := pool.QueryRow(setupCtx, "SELECT attempts,status,locked_by FROM jobs WHERE id=$1", jobID).Scan(&attempts, &status, &lockedBy); err != nil {
		t.Fatalf("read final job state: %v", err)
	}
	if status != "succeeded" || attempts != 2 || lockedBy != nil {
		t.Fatalf("final state status=%q attempts=%d locked_by=%v; want succeeded/2/NULL", status, attempts, lockedBy)
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for marker file %s", path)
}
