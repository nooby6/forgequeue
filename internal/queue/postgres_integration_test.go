package queue

import (
    "context"
    "fmt"
    "os"
    "sync"
    "testing"
    "time"

    "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	schema := fmt.Sprintf("forgequeue_test_%d", time.Now().UnixNano())
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to database for test schema: %v", err)
	}
	// Serialize extension initialization across package-level test processes.
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
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close(ctx)
		t.Fatalf("create isolated schema: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close setup connection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanupConn, err := pgx.Connect(cleanupCtx, dsn)
		if err != nil {
			t.Errorf("connect to drop test schema %s: %v", schema, err)
			return
		}
		defer cleanupConn.Close(cleanupCtx)
		if _, err := cleanupConn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("drop test schema %s: %v", schema, err)
		}
	})

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse database config: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping database: %v", err)
	}
	t.Cleanup(pool.Close)

	for _, path := range []string{
		"../../db/migrations/000001_initial_schema.up.sql",
		"../../db/migrations/000002_worker_leases.up.sql",
		"../../db/migrations/000003_retries.up.sql",
		"../../db/migrations/000004_dead_letter_jobs.up.sql",
		"../../db/migrations/000005_running_job_cancellation.up.sql",
		"../../db/migrations/000006_job_dependencies.up.sql",
	} {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read migration %s: %v", path, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply migration %s: %v", path, err)
		}
	}
	return pool
}

func insertIntegrationJob(t testing.TB, pool *pgxpool.Pool, queueName string, attempts, maxAttempts int, status string, lease time.Time) string {
    t.Helper()
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    var id string
    err := pool.QueryRow(ctx, `
        INSERT INTO jobs (idempotency_key, queue_name, job_type, payload, attempts, max_attempts, status, available_at, locked_by, locked_at, lease_expires_at)
        VALUES (gen_random_uuid()::text, $1, 'test', '{}', $2, $3, $4, NOW(), CASE WHEN $4 = 'running' THEN 'old-worker' ELSE NULL END, CASE WHEN $4 = 'running' THEN NOW() ELSE NULL END, $5)
        RETURNING id
    `, queueName, attempts, maxAttempts, status, lease).Scan(&id)
    if err != nil {
        t.Fatalf("insert job: %v", err)
    }
    return id
}

func TestPostgresQueueConcurrentClaim(t *testing.T) {
    pool := integrationPool(t)
    ctx := context.Background()
    queueName := fmt.Sprintf("integration-%d", time.Now().UnixNano())
    for i := 0; i < 8; i++ {
        insertIntegrationJob(t, pool, queueName, 0, 3, "pending", time.Time{})
    }

    q := NewPostgresQueue(pool)
    const workers = 8
    claimed := make(chan string, workers)
    var wg sync.WaitGroup
    wg.Add(workers)
    for i := 0; i < workers; i++ {
        workerID := fmt.Sprintf("worker-%d", i)
        go func() {
            defer wg.Done()
            j, err := q.ClaimNext(ctx, queueName, workerID, time.Minute)
            if err == ErrNoJob {
                return
            }
            if err != nil {
                t.Errorf("claim: %v", err)
                return
            }
            claimed <- j.ID
        }()
    }
    wg.Wait()
    close(claimed)

    seen := map[string]bool{}
    count := 0
    for id := range claimed {
        if seen[id] {
            t.Fatalf("job %s was claimed more than once", id)
        }
        seen[id] = true
        count++
    }
    if count != workers {
        t.Fatalf("expected %d claims, got %d", workers, count)
    }
}

func TestPostgresQueueRecoversExpiredLease(t *testing.T) {
    pool := integrationPool(t)
    queueName := fmt.Sprintf("lease-%d", time.Now().UnixNano())
    id := insertIntegrationJob(t, pool, queueName, 1, 3, "running", time.Now().Add(-time.Minute))

    q := NewPostgresQueue(pool)
    j, err := q.ClaimNext(context.Background(), queueName, "new-worker", time.Minute)
    if err != nil {
        t.Fatalf("claim recovered job: %v", err)
    }
    if j.ID != id {
        t.Fatalf("claimed %s, expected recovered job %s", j.ID, id)
    }
    if j.Attempts != 2 {
        t.Fatalf("attempts = %d, expected 2", j.Attempts)
    }
    if err := q.Complete(context.Background(), id, "old-worker"); err == nil {
        t.Fatal("old worker was allowed to complete recovered job")
    }
}

func TestPostgresQueueRetryExhaustion(t *testing.T) {
    pool := integrationPool(t)
    queueName := fmt.Sprintf("retry-%d", time.Now().UnixNano())
    id := insertIntegrationJob(t, pool, queueName, 3, 3, "running", time.Now().Add(time.Minute))

    q := NewPostgresQueue(pool)
    if err := q.Retry(context.Background(), id, "old-worker", time.Second); err != nil {
        t.Fatalf("retry exhausted job: %v", err)
    }
    var status string
    var lockedBy *string
    if err := pool.QueryRow(context.Background(), "SELECT status, locked_by FROM jobs WHERE id=$1", id).Scan(&status, &lockedBy); err != nil {
        t.Fatalf("read retried job: %v", err)
    }
    if status != "failed" {
        t.Fatalf("status = %s, expected failed", status)
    }
    if lockedBy != nil {
        t.Fatalf("locked_by = %q, expected NULL", *lockedBy)
    }
}


func TestPostgresQueueRenewsLeaseOnlyForCurrentOwner(t *testing.T) {
	pool := integrationPool(t)
	queueName := fmt.Sprintf("renew-%d", time.Now().UnixNano())
	insertIntegrationJob(t, pool, queueName, 0, 3, "pending", time.Time{})

	q := NewPostgresQueue(pool)
	j, err := q.ClaimNext(context.Background(), queueName, "current-worker", time.Minute)
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	if err := q.RenewLease(context.Background(), j.ID, "stale-worker", time.Minute); err == nil {
		t.Fatal("stale worker was allowed to renew the lease")
	}
	if err := q.RenewLease(context.Background(), j.ID, "current-worker", time.Minute); err != nil {
		t.Fatalf("current owner could not renew lease: %v", err)
	}

	var leaseExpiresAt time.Time
	if err := pool.QueryRow(context.Background(), "SELECT lease_expires_at FROM jobs WHERE id=$1", j.ID).Scan(&leaseExpiresAt); err != nil {
		t.Fatalf("read renewed lease: %v", err)
	}
	if time.Until(leaseExpiresAt) < 50*time.Second {
		t.Fatalf("renewed lease expires too soon: %s remaining", time.Until(leaseExpiresAt))
	}
}

func TestPostgresQueueCannotRenewExpiredLease(t *testing.T) {
	pool := integrationPool(t)
	queueName := fmt.Sprintf("expired-renew-%d", time.Now().UnixNano())
	id := insertIntegrationJob(t, pool, queueName, 1, 3, "running", time.Now().Add(-time.Minute))

	q := NewPostgresQueue(pool)
	if err := q.RenewLease(context.Background(), id, "old-worker", time.Minute); err == nil {
		t.Fatal("worker was allowed to revive an expired lease")
	}
}


func TestPostgresQueueRejectsTransitionsAfterLeaseExpiry(t *testing.T) {
	pool := integrationPool(t)
	q := NewPostgresQueue(pool)

	for _, operation := range []struct {
		name string
		run  func(string) error
	}{
		{name: "complete", run: func(id string) error { return q.Complete(context.Background(), id, "old-worker") }},
		{name: "fail", run: func(id string) error { return q.Fail(context.Background(), id, "old-worker") }},
		{name: "retry", run: func(id string) error { return q.Retry(context.Background(), id, "old-worker", time.Second) }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			queueName := fmt.Sprintf("expired-transition-%s-%d", operation.name, time.Now().UnixNano())
			id := insertIntegrationJob(t, pool, queueName, 1, 3, "running", time.Now().Add(-time.Minute))

			if err := operation.run(id); err == nil {
				t.Fatalf("%s succeeded after the lease expired", operation.name)
			}

			var status string
			if err := pool.QueryRow(context.Background(), "SELECT status FROM jobs WHERE id=$1", id).Scan(&status); err != nil {
				t.Fatalf("read job status: %v", err)
			}
			if status != "running" {
				t.Fatalf("status = %q, expected expired job to remain running until recovery", status)
			}
		})
	}
}

func TestPostgresQueueCompleteIsIdempotent(t *testing.T) {
	pool := integrationPool(t)
	queueName := fmt.Sprintf("idempotent-complete-%d", time.Now().UnixNano())
	insertIntegrationJob(t, pool, queueName, 0, 3, "pending", time.Time{})

	q := NewPostgresQueue(pool)
	j, err := q.ClaimNext(context.Background(), queueName, "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if err := q.Complete(context.Background(), j.ID, "worker-a"); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if err := q.Complete(context.Background(), j.ID, "worker-a"); err != nil {
		t.Fatalf("repeated completion should be idempotent: %v", err)
	}
}


func TestPostgresQueueDeadLetterCanBeInspectedAndReplayed(t *testing.T) {
	pool := integrationPool(t)
	queueName := fmt.Sprintf("dead-letter-%d", time.Now().UnixNano())
	id := insertIntegrationJob(t, pool, queueName, 3, 3, "running", time.Now().Add(time.Minute))
	q := NewPostgresQueue(pool)

	if err := q.Retry(context.Background(), id, "old-worker", time.Second); err != nil {
		t.Fatalf("exhausted retry: %v", err)
	}
	letters, err := q.ListDeadLetters(context.Background(), queueName, 10)
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	if len(letters) != 1 {
		t.Fatalf("dead letters = %d, expected 1", len(letters))
	}
	if letters[0].JobID != id || letters[0].Attempts != 3 {
		t.Fatalf("unexpected dead letter: %+v", letters[0])
	}
	if letters[0].FailureReason != "maximum attempts exhausted after 3 attempts" {
		t.Fatalf("unexpected failure reason: %q", letters[0].FailureReason)
	}

	if err := q.ReplayDeadLetter(context.Background(), id); err != nil {
		t.Fatalf("replay dead letter: %v", err)
	}
	var status string
	var attempts int
	if err := pool.QueryRow(context.Background(), "SELECT status,attempts FROM jobs WHERE id=$1", id).Scan(&status, &attempts); err != nil {
		t.Fatalf("read replayed job: %v", err)
	}
	if status != "pending" || attempts != 0 {
		t.Fatalf("replayed job status=%q attempts=%d, expected pending/0", status, attempts)
	}
	letters, err = q.ListDeadLetters(context.Background(), queueName, 10)
	if err != nil {
		t.Fatalf("list active dead letters after replay: %v", err)
	}
	if len(letters) != 0 {
		t.Fatalf("active dead letters after replay = %d, expected 0", len(letters))
	}
	if err := q.ReplayDeadLetter(context.Background(), id); err == nil {
		t.Fatal("replayed the same dead-letter record twice")
	}
}


func TestPostgresQueueDeadLettersExpiredLeaseAfterMaximumAttempts(t *testing.T) {
	pool := integrationPool(t)
	queueName := fmt.Sprintf("expired-exhausted-%d", time.Now().UnixNano())
	id := insertIntegrationJob(t, pool, queueName, 3, 3, "running", time.Now().Add(-time.Minute))
	q := NewPostgresQueue(pool)

	if _, err := q.ClaimNext(context.Background(), queueName, "replacement-worker", time.Minute); err != ErrNoJob {
		t.Fatalf("claim result = %v, expected no claim for exhausted expired job", err)
	}
	var status string
	if err := pool.QueryRow(context.Background(), "SELECT status FROM jobs WHERE id=$1", id).Scan(&status); err != nil {
		t.Fatalf("read exhausted job status: %v", err)
	}
	if status != "failed" {
		t.Fatalf("status = %q, expected failed", status)
	}
	letters, err := q.ListDeadLetters(context.Background(), queueName, 10)
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	if len(letters) != 1 || letters[0].JobID != id {
		t.Fatalf("dead letters = %+v, expected one record for %s", letters, id)
	}
	if letters[0].FailureReason != "lease expired after maximum attempts" {
		t.Fatalf("failure reason = %q", letters[0].FailureReason)
	}
}


func installDeadLetterFailureTrigger(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), "CREATE FUNCTION reject_dead_letter_insert() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''injected dead-letter storage failure''; RETURN NEW; END;'")
	if err != nil {
		t.Fatalf("create dead-letter failure function: %v", err)
	}
	_, err = pool.Exec(context.Background(), `CREATE TRIGGER reject_dead_letter_insert BEFORE INSERT ON dead_letter_jobs FOR EACH ROW EXECUTE FUNCTION reject_dead_letter_insert()`)
	if err != nil {
		t.Fatalf("create dead-letter failure trigger: %v", err)
	}
}

func TestPostgresQueueRetryRollsBackWhenDeadLetterWriteFails(t *testing.T) {
	pool := integrationPool(t)
	queueName := fmt.Sprintf("retry-dead-letter-failure-%d", time.Now().UnixNano())
	id := insertIntegrationJob(t, pool, queueName, 3, 3, "running", time.Now().Add(time.Minute))
	installDeadLetterFailureTrigger(t, pool)

	q := NewPostgresQueue(pool)
	if err := q.Retry(context.Background(), id, "old-worker", time.Second); err == nil {
		t.Fatal("expected injected dead-letter insert failure")
	}

	var status string
	var lockedBy *string
	if err := pool.QueryRow(
		context.Background(),
		"SELECT status, locked_by FROM jobs WHERE id=$1",
		id,
	).Scan(&status, &lockedBy); err != nil {
		t.Fatalf("read job after injected failure: %v", err)
	}
	if status != "running" {
		t.Fatalf("status = %q after rollback, expected running", status)
	}
	if lockedBy == nil || *lockedBy != "old-worker" {
		t.Fatalf("locked_by = %v after rollback, expected old-worker", lockedBy)
	}

	letters, err := q.ListDeadLetters(context.Background(), queueName, 10)
	if err != nil {
		t.Fatalf("list dead letters after rollback: %v", err)
	}
	if len(letters) != 0 {
		t.Fatalf("dead letters after rollback = %d, expected 0", len(letters))
	}
}

func TestPostgresQueueClaimRollsBackExpiredLeaseRecoveryWhenDeadLetterWriteFails(t *testing.T) {
	pool := integrationPool(t)
	queueName := fmt.Sprintf("claim-dead-letter-failure-%d", time.Now().UnixNano())
	id := insertIntegrationJob(t, pool, queueName, 3, 3, "running", time.Now().Add(-time.Minute))
	installDeadLetterFailureTrigger(t, pool)

	q := NewPostgresQueue(pool)
	if _, err := q.ClaimNext(context.Background(), queueName, "replacement-worker", time.Minute); err == nil {
		t.Fatal("expected injected dead-letter insert failure")
	}

	var status string
	var lockedBy *string
	if err := pool.QueryRow(
		context.Background(),
		"SELECT status, locked_by FROM jobs WHERE id=$1",
		id,
	).Scan(&status, &lockedBy); err != nil {
		t.Fatalf("read expired job after injected failure: %v", err)
	}
	if status != "running" {
		t.Fatalf("status = %q after rollback, expected running", status)
	}
	if lockedBy == nil || *lockedBy != "old-worker" {
		t.Fatalf("locked_by = %v after rollback, expected old-worker", lockedBy)
	}

	letters, err := q.ListDeadLetters(context.Background(), queueName, 10)
	if err != nil {
		t.Fatalf("list dead letters after recovery rollback: %v", err)
	}
	if len(letters) != 0 {
		t.Fatalf("dead letters after recovery rollback = %d, expected 0", len(letters))
	}
}


func TestPostgresQueueDoesNotClaimScheduledJobEarly(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	queueName := fmt.Sprintf("scheduled-%d", time.Now().UnixNano())
	runAt := time.Now().Add(2 * time.Minute).UTC()
	var id string
	err := pool.QueryRow(ctx, `
		INSERT INTO jobs (idempotency_key, queue_name, job_type, payload, status, priority, max_attempts, available_at)
		VALUES (gen_random_uuid()::text, $1, 'scheduled-test', '{}', 'pending', 0, 3, $2)
		RETURNING id
	`, queueName, runAt).Scan(&id)
	if err != nil {
		t.Fatalf("insert scheduled job: %v", err)
	}

	q := NewPostgresQueue(pool)
	if _, err := q.ClaimNext(ctx, queueName, "early-worker", time.Minute); err != ErrNoJob {
		t.Fatalf("claim before scheduled time returned %v; want ErrNoJob", err)
	}

	if _, err := pool.Exec(ctx, "UPDATE jobs SET available_at=NOW() WHERE id=$1", id); err != nil {
		t.Fatalf("make scheduled job ready: %v", err)
	}
	claimed, err := q.ClaimNext(ctx, queueName, "ready-worker", time.Minute)
	if err != nil {
		t.Fatalf("claim job after it becomes available: %v", err)
	}
	if claimed.ID != id {
		t.Fatalf("claimed job %s; want %s", claimed.ID, id)
	}
}


func TestPostgresQueueWaitsForDependencies(t *testing.T) {
 pool:=integrationPool(t);ctx:=context.Background();queueName:=fmt.Sprintf("deps-%d",time.Now().UnixNano())
 prerequisite:=insertIntegrationJob(t,pool,queueName,0,3,"pending",time.Time{})
 dependent:=insertIntegrationJob(t,pool,queueName,0,3,"pending",time.Time{})
 if _,err:=pool.Exec(ctx,"INSERT INTO job_dependencies(job_id,depends_on_job_id) VALUES($1,$2)",dependent,prerequisite);err!=nil{t.Fatal(err)}
 q:=NewPostgresQueue(pool);claimed,err:=q.ClaimNext(ctx,queueName,"worker",time.Minute)
 if err!=nil||claimed.ID!=prerequisite{t.Fatalf("first claim = %v, %v; want prerequisite %s",claimed,err,prerequisite)}
 if _,err:=q.ClaimNext(ctx,queueName,"worker-2",time.Minute);err!=ErrNoJob{t.Fatalf("claim with unresolved prerequisite = %v; want ErrNoJob",err)}
 if err:=q.Complete(ctx,prerequisite,"worker");err!=nil{t.Fatal(err)}
 claimed,err=q.ClaimNext(ctx,queueName,"worker-2",time.Minute)
 if err!=nil||claimed.ID!=dependent{t.Fatalf("claim after prerequisite = %v, %v; want dependent %s",claimed,err,dependent)}
}

func TestPostgresQueueCancelsJobsWithFailedDependencies(t *testing.T) {
 pool:=integrationPool(t);ctx:=context.Background();queueName:=fmt.Sprintf("failed-deps-%d",time.Now().UnixNano())
 prerequisite:=insertIntegrationJob(t,pool,queueName,0,3,"failed",time.Time{})
 dependent:=insertIntegrationJob(t,pool,queueName,0,3,"pending",time.Time{})
 if _,err:=pool.Exec(ctx,"INSERT INTO job_dependencies(job_id,depends_on_job_id) VALUES($1,$2)",dependent,prerequisite);err!=nil{t.Fatal(err)}
 q:=NewPostgresQueue(pool);if _,err:=q.ClaimNext(ctx,queueName,"worker",time.Minute);err!=ErrNoJob{t.Fatalf("claim = %v; want ErrNoJob",err)}
 var status string;if err:=pool.QueryRow(ctx,"SELECT status FROM jobs WHERE id=$1",dependent).Scan(&status);err!=nil{t.Fatal(err)}
 if status!="cancelled"{t.Fatalf("dependent status = %q; want cancelled",status)}
}
