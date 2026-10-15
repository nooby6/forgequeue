# ForgeQueue

ForgeQueue is a Go and PostgreSQL job queue built to explore durable job execution, concurrent workers, lease ownership, retries, and failure recovery.

## Current status

**Milestone 4 — Reliability and Recovery (in progress)**

Implemented:
- Durable job submission and lookup API
- PostgreSQL persistence and idempotency keys
- Transactional job claiming with `FOR UPDATE SKIP LOCKED`
- Concurrent-claim integration test proving each job is claimed at most once
- Worker identity and execution leases
- Expired-lease recovery and attempt-count increments
- Exponential retry backoff and maximum-attempt exhaustion
- Graceful worker shutdown
- Lease renewal for long-running handlers
- Active-lease guards for completion, failure, and retry
- Idempotent repeated successful completion
- PostgreSQL integration tests for lease ownership and expiry
- CI against PostgreSQL

Remaining in Milestone 4:
- Explicit cancellation semantics
- Failure-injection tests for database outages and worker restarts

## Architecture

Client → HTTP API → Job service → PostgreSQL → Queue runtime → Worker pool → Handler

Workers claim jobs in a database transaction. PostgreSQL row locking prevents two workers from claiming the same pending row. Leases provide a time-bounded ownership window; a worker cannot renew or acknowledge a job after that lease expires.

## Queue behaviour

- Jobs are selected by queue, readiness time, and priority.
- Each claim increments the attempt count.
- Expired running jobs are recovered when a worker next claims from that queue; if the expired job has exhausted its attempts, it is marked failed and recorded as a dead letter instead of being run again.
- Failed handlers are retried with capped exponential backoff until `max_attempts` is reached.
- Exhausted jobs become `failed` and are recorded in `dead_letter_jobs` in the same transaction.
- `ListDeadLetters` lists unreplayed failures; `ReplayDeadLetter` requeues one job, resets its attempt counter, and retains the audit record. If it exhausts attempts again, the existing record is refreshed.

## Run locally

Start PostgreSQL:

```sh
docker compose up -d postgres
export DATABASE_URL='postgres://forgequeue:forgequeue@localhost:5432/forgequeue?sslmode=disable'
go run ./cmd/api
```

Health checks:

```sh
curl http://localhost:8080/health/live
curl http://localhost:8080/health/ready
```

Submit a job:

```sh
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: noop-001' \
  -d '{"queue_name":"default","job_type":"noop","payload":{"message":"hello ForgeQueue"},"priority":10}'
```

## Tests

```sh
go test ./...
```

The GitHub Actions workflow runs the tests against PostgreSQL 16.

## Benchmarking

A PostgreSQL-backed benchmark measures job-claim throughput. Use a disposable database and run:

```sh
TEST_DATABASE_URL='postgres://forgequeue:forgequeue@localhost:5432/forgequeue' \
  go test ./internal/queue -run '^$' \
  -bench '^BenchmarkPostgresQueueClaimNext$' \
  -benchmem -benchtime=3s -cpu=1,4,16
```

The `-cpu` values compare parallel claimers under different `GOMAXPROCS` settings. Throughput depends on the host and PostgreSQL configuration. Publish numbers only alongside the environment and exact command used.

## Project structure

- `cmd/api/` — HTTP API and worker process
- `internal/config/` — environment configuration
- `internal/database/` — PostgreSQL connection pool
- `internal/health/` — liveness and readiness
- `internal/httpserver/` — HTTP routing and middleware
- `internal/job/` — job domain, service, repository, and REST API
- `internal/queue/` — PostgreSQL queue, dead-letter inspection/replay, integration tests, and benchmark
- `internal/worker/` — worker runtime and handler registry
- `db/` — database schema and migrations
- `docs/` — architecture decisions and code-review notes

## Engineering principles

1. Correct state transitions before throughput optimisation.
2. PostgreSQL enforces durable ownership and concurrent claim semantics.
3. A worker must prove it owns an active lease before acknowledging a job.
4. Concurrency and recovery behaviour must be testable.
5. Failure handling is part of the design, not an afterthought.
6. Operational behaviour belongs in the repository: CI, configuration, deployment guidance, and runbooks.

## Roadmap

- **Milestone 1 — Foundation:** complete
- **Milestone 2 — Durable Job API:** complete
- **Milestone 3 — Queue + Worker Runtime:** complete
- **Milestone 4 — Reliability + Recovery:** in progress
- **Milestone 5 — Scheduling + Dependencies:** planned
- **Milestone 6 — Observability + Load Testing:** planned
- **Milestone 7 — Infrastructure + Production Operations:** planned
