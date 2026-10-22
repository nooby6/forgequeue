package worker

import (
	"context"
	"encoding/json"
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

func TestWorkerProcessHelper(t *testing.T) {
	phase := os.Getenv("FORGEQUEUE_PROCESS_TEST_PHASE")
	if phase == "" { return }
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" { t.Fatal("TEST_DATABASE_URL is required") }
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil { t.Fatal(err) }
	cfg.ConnConfig.RuntimeParams["search_path"] = os.Getenv("FORGEQUEUE_PROCESS_TEST_SCHEMA") + ",public"
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil { t.Fatal(err) }
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewRegistry()
	marker := os.Getenv("FORGEQUEUE_PROCESS_TEST_MARKER")
	if err := registry.Register("process-recovery-test", func(handlerCtx context.Context, _ json.RawMessage) error {
		if phase == "first" {
			if err := os.WriteFile(marker, []byte("claimed"), 0600); err != nil { return err }
			<-handlerCtx.Done()
			return handlerCtx.Err()
		}
		if err := os.WriteFile(marker, []byte("completed"), 0600); err != nil { return err }
		cancel()
		return nil
	}); err != nil { t.Fatal(err) }
	New(queue.NewPostgresQueue(pool), registry, slog.New(slog.NewTextHandler(discardWriter{}, nil)), Config{
		QueueName: os.Getenv("FORGEQUEUE_PROCESS_TEST_QUEUE"), Concurrency: 1,
		PollInterval: 10*time.Millisecond, Lease: 450*time.Millisecond,
		ShutdownTimeout: 100*time.Millisecond, WorkerID: "process-recovery-test",
	}).Run(ctx)
}

func TestWorkerRecoversJobAfterAbruptProcessExit(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" { t.Skip("TEST_DATABASE_URL is not set") }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil { t.Fatal(err) }
	schema := fmt.Sprintf("worker_process_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public"); err != nil { t.Fatal(err) }
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil { t.Fatal(err) }
	if err := admin.Close(ctx); err != nil { t.Fatal(err) }
	t.Cleanup(func() {
		c, cc := context.WithTimeout(context.Background(), 10*time.Second); defer cc()
		conn, e := pgx.Connect(c, dsn); if e != nil { t.Errorf("connect cleanup: %v", e); return }
		defer conn.Close(c)
		if _, e = conn.Exec(c, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); e != nil { t.Errorf("drop schema: %v", e) }
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil { t.Fatal(err) }
	cfg.ConnConfig.RuntimeParams["search_path"] = schema+",public"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil { t.Fatal(err) }
	defer pool.Close()
	for _, path := range []string{"../../db/migrations/000001_initial_schema.up.sql","../../db/migrations/000002_worker_leases.up.sql","../../db/migrations/000003_retries.up.sql","../../db/migrations/000004_dead_letter_jobs.up.sql"} {
		sql, e := os.ReadFile(path); if e != nil { t.Fatal(e) }
		if _, e = pool.Exec(ctx, string(sql)); e != nil { t.Fatalf("migration %s: %v", path, e) }
	}
	queueName := fmt.Sprintf("process-recovery-%d", time.Now().UnixNano())
	var id string
	if err := pool.QueryRow(ctx, "INSERT INTO jobs (idempotency_key,queue_name,job_type,payload,status,max_attempts,available_at) VALUES (gen_random_uuid()::text,$1,'process-recovery-test','{}','pending',3,NOW()) RETURNING id::text", queueName).Scan(&id); err != nil { t.Fatal(err) }
	dir := t.TempDir()
	firstMarker := filepath.Join(dir,"first")
	first := exec.Command(os.Args[0], "-test.run=^TestWorkerProcessHelper$")
	first.Env = append(os.Environ(),"FORGEQUEUE_PROCESS_TEST_PHASE=first","FORGEQUEUE_PROCESS_TEST_SCHEMA="+schema,"FORGEQUEUE_PROCESS_TEST_QUEUE="+queueName,"FORGEQUEUE_PROCESS_TEST_MARKER="+firstMarker)
	if err := first.Start(); err != nil { t.Fatal(err) }
	waitForFile(t, firstMarker, 5*time.Second)
	_ = first.Process.Kill()
	_ = first.Wait()
	var attempts int
	var status string
	if err := pool.QueryRow(ctx,"SELECT attempts,status FROM jobs WHERE id=$1",id).Scan(&attempts,&status); err != nil { t.Fatal(err) }
	if status!="running" || attempts!=1 { t.Fatalf("after termination status=%q attempts=%d; want running/1",status,attempts) }
	time.Sleep(650*time.Millisecond)
	marker := filepath.Join(dir,"replacement")
	replacement := exec.Command(os.Args[0], "-test.run=^TestWorkerProcessHelper$")
	replacement.Env = append(os.Environ(),"FORGEQUEUE_PROCESS_TEST_PHASE=replacement","FORGEQUEUE_PROCESS_TEST_SCHEMA="+schema,"FORGEQUEUE_PROCESS_TEST_QUEUE="+queueName,"FORGEQUEUE_PROCESS_TEST_MARKER="+marker)
	out, err := replacement.CombinedOutput()
	if err != nil { t.Fatalf("replacement worker: %v: %s",err,out) }
	waitForFile(t,marker,5*time.Second)
	var lockedBy *string
	if err := pool.QueryRow(ctx,"SELECT attempts,status,locked_by FROM jobs WHERE id=$1",id).Scan(&attempts,&status,&lockedBy); err != nil { t.Fatal(err) }
	if status!="succeeded" || attempts!=2 || lockedBy!=nil { t.Fatalf("final state status=%q attempts=%d locked_by=%v; want succeeded/2/NULL",status,attempts,lockedBy) }
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline:=time.Now().Add(timeout)
	for time.Now().Before(deadline) { if _,err:=os.Stat(path); err==nil { return }; time.Sleep(10*time.Millisecond) }
	t.Fatalf("timed out waiting for %s",path)
}
