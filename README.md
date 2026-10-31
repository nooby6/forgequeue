# ForgeQueue

ForgeQueue is a Go and PostgreSQL job queue focused on durable execution, concurrent workers, lease ownership, retries, scheduling, cancellation, and failure recovery.

## Current status

**Milestone 5 — Scheduling + Dependencies (in progress)**

Implemented:
- Durable job submission and lookup API with idempotency keys
- One-time scheduled jobs using an RFC 3339 `scheduled_at` timestamp
- Durable job dependencies: workers wait for all prerequisites to succeed; dependents are cancelled if a prerequisite fails or is cancelled
- PostgreSQL-backed readiness times; scheduled jobs remain pending and cannot be claimed before their availability time
- Transactional job claiming with `FOR UPDATE SKIP LOCKED`
- Worker identity, execution leases, and lease renewal
- Expired-lease recovery and attempt-count increments
- Exponential retry backoff and maximum-attempt exhaustion
- Dead-letter inspection and replay
- Graceful worker shutdown
- Safe cancellation of pending jobs and cooperative cancellation of running jobs
- PostgreSQL integration tests for concurrency, lease ownership, cancellation, scheduling, and recovery
- CI against PostgreSQL

## Architecture

Client → HTTP API → Job service → PostgreSQL → Queue runtime → Worker pool → Handler

Workers claim jobs in a database transaction. PostgreSQL row locking prevents two workers from claiming the same pending row. Leases provide time-bounded ownership; a worker cannot renew or acknowledge a job after that lease expires. Lease identities include a random process-instance token and a worker-slot number so restarted processes and concurrent slots do not reuse an owner identity.

## Job lifecycle and scheduling

- Jobs are selected by queue, readiness time, and priority.
- Jobs submitted without `scheduled_at` become available immediately.
- A future `scheduled_at` timestamp is persisted as `available_at`; the worker's existing database claim query ignores the job until that timestamp has passed. No in-memory timer or separate scheduler process is required.
- Timestamps must be RFC 3339, for example `2027-02-03T12:30:00Z`. A timestamp in the past is treated as immediately eligible.
- Each claim increments the attempt count.
- Failed handlers are retried with capped exponential backoff until `max_attempts` is reached.
- Exhausted jobs become `failed` and are recorded as dead letters.
- Expired running jobs are recovered when a worker next claims from that queue.
- Pending jobs can be cancelled immediately.
- A running job receives a durable `cancellation_requested_at` timestamp. Its handler context is cancelled on the next lease-renewal check.
- The worker marks a running job `cancelled` only after the handler returns and the worker acknowledges cancellation. If the worker dies first, the queue finalizes the cancellation after the lease expires.
- Cancellation is cooperative: handlers must observe `ctx.Done()` and stop their work. ForgeQueue does not forcibly terminate goroutines or roll back external side effects already performed by a handler.

## HTTP API

### Submit an immediate job

```sh
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: noop-001' \
  -d '{"queue_name":"default","job_type":"noop","payload":{"message":"hello ForgeQueue"},"priority":10}'
```

### Schedule a job

Use `scheduled_at` as an RFC 3339 timestamp. The job is persisted immediately but cannot be claimed by a worker before its availability time.

```sh
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: daily-report-2027-02-03' \
  -d '{"queue_name":"default","job_type":"noop","payload":{"message":"scheduled report"},"scheduled_at":"2027-02-03T12:30:00Z"}'
```

The response's `available_at` field confirms the persisted time. Omitting `scheduled_at` keeps the previous immediate-execution behaviour. Reusing an idempotency key returns the existing job and does not reschedule it.

### Submit a job with dependencies\n\nPass existing job IDs in `depends_on`. The job is persisted immediately, but workers will not claim it until every prerequisite has succeeded. Dependencies are committed atomically with the job. Reusing an idempotency key does not change the original dependency graph. If any prerequisite fails or is cancelled, the dependent is marked cancelled during the next claim attempt for its queue.\n\n```sh\ncurl -X POST http://localhost:8080/jobs \\\n  -H 'Content-Type: application/json' \\\n  -d '{"queue_name":"default","job_type":"publish-report","payload":{"report_id":"rpt-42"},"depends_on":["PREPARATION_JOB_ID","EXPORT_JOB_ID"]}'\n```\n\n### Retrieve a job

```sh
curl http://localhost:8080/jobs/JOB_ID
```

### Cancel a job

```sh
curl -X DELETE http://localhost:8080/jobs/JOB_ID
```

For a pending job, the response has `status: cancelled`. For a running job, the response remains `status: running` and includes `cancellation_requested_at` while the handler responds to cancellation. Repeated requests are safe. A missing job returns HTTP 404; a terminal non-cancelled job returns HTTP 409.

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

Publish benchmark numbers only alongside the environment and exact command used.

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
4. Cancellation requests must not falsely report that active work has stopped.
5. Concurrency and recovery behaviour must be testable.
6. Failure handling is part of the design, not an afterthought.

## Roadmap

- **Milestone 1 — Foundation:** complete
- **Milestone 2 — Durable Job API:** complete
- **Milestone 3 — Queue + Worker Runtime:** complete
- **Milestone 4 — Reliability + Recovery:** complete
- **Milestone 5 — Scheduling + Dependencies:** in progress; one-time scheduling and dependency gating implemented; recurring schedules remain planned
- **Milestone 6 — Observability + Load Testing:** planned
- **Milestone 7 — Infrastructure + Production Operations:** planned
