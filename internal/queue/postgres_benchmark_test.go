package queue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkPostgresQueueClaimNext measures PostgreSQL-backed job claim throughput.
// Set TEST_DATABASE_URL to a disposable PostgreSQL database. Use -cpu=1,4,16
// to compare parallel claimers at different GOMAXPROCS values, for example:
//
// TEST_DATABASE_URL=postgres://... go test ./internal/queue -run '^$' \
//   -bench '^BenchmarkPostgresQueueClaimNext$' -benchmem -benchtime=5s -cpu=1,4,16
//
// The benchmark preloads one pending job per operation, so setup inserts are
// excluded from the timed interval. Results depend on the database and host.
func BenchmarkPostgresQueueClaimNext(b *testing.B) {
	pool := integrationPool(b)
	ctx := context.Background()
	queueName := fmt.Sprintf("benchmark-%d", time.Now().UnixNano())

	_, err := pool.Exec(ctx, `
		INSERT INTO jobs (idempotency_key, queue_name, job_type, payload, status, priority, attempts, max_attempts, available_at)
		SELECT gen_random_uuid()::text, $1, 'benchmark', '{}'::jsonb, 'pending', 0, 0, 3, NOW()
		FROM generate_series(1, $2)
	`, queueName, b.N)
	if err != nil {
		b.Fatalf("seed benchmark jobs: %v", err)
	}

	q := NewPostgresQueue(pool)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.ClaimNext(ctx, queueName, "benchmark-worker", time.Minute); err != nil {
				b.Errorf("claim job: %v", err)
				return
			}
		}
	})
}
