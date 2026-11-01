# ForgeQueue

ForgeQueue is a Go and PostgreSQL job queue focused on durable execution, concurrent workers, lease ownership, retries, cancellation, and failure recovery.

## Current status

**Milestone 4 — Reliability and Recovery (verification in progress)**

Implemented:
- Durable job submission and lookup API with idempotency keys
- Scheduled job availability using an optional `available_at` timestamp
- Transactional job claiming with `FOR UPDATE SKIP LOCKED`
- Worker identity, execution leases, and lease renewal
- Expired-lease recovery and attempt-count increments
- Exponential retry backoff and maximum-attempt exhaustion
- Dead-letter inspection and replay
- Graceful worker shutdown
- Lease renewal for long-running handlers
- Active-lease guards for completion, failure, and retry
- Idempotent repeated successful completion
- Safe cancellation of pending jobs
- Cooperative cancellation requests for running jobs
- PostgreSQL integration tests for concurrency, lease ownership, cancellation, and recovery
- Queue statistics endpoint for operational visibility
- CI against PostgreSQL

## Architecture

Client → HTTP API → Job service → PostgreSQL → Queue runtime → Worker pool → Handler

Workers claim jobs in a database transaction. PostgreSQL row locking prevents two workers from claiming the same pending row. Leases provide time-bounded ownership; a worker cannot renew or acknowledge a job after its lease expires. Lease identities include a random process-instance token and a worker-slot number so restarted processes and concurrent slots do not reuse an owner identity.

## Job lifecycle and cancellation

- Jobs are selected by queue, readiness time, and priority.
- Each claim increments the attempt count.
- Failed handlers are retried with capped exponential backoff until `max_attempts` is reached.
- Exhausted jobs become `failed` and are recorded in `dead_letter_jobs`.
- Expired running jobs are recovered when a worker next claims from that queue.
- Pending jobs can be cancelled immediately.
- A running job receives a durable `cancellation_requested_at` timestamp. Its handler context is cancelled on the next lease-renewal check.
- The worker marks a running job `cancelled` only after the handler returns and the worker acknowledges cancellation. If the worker dies first, the queue finalizes the cancellation after the lease expires.
- Cancellation is cooperative: handlers must observe `ctx.Done()` and stop their work. ForgeQueue does not forcibly terminate goroutines or roll back external side effects already performed by a handler.
- `ListDeadLetters` lists unreplayed failures; `ReplayDeadLetter` requeues one job, resets its attempt counter, and retains the audit record. If it exhausts attempts again, the existing record is refreshed.
- Queue statistics are computed by PostgreSQL aggregates and scoped to a single queue; the endpoint does not load individual job rows.

## HTTP API

### Submit a job

```sh
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: noop-001' \
  -d '{"queue_name":"default","job_type":"noop","payload":{"message":"hello ForgeQueue"},"priority":10}'
```

### Schedule a job

Include `available_at` when submitting a job to make it ineligible for workers until that timestamp. The value must be an RFC 3339 timestamp; omit it to make the job available immediately. The worker claim query enforces this timestamp in PostgreSQL.

```sh
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue_name":"default","job_type":"report","payload":{},"available_at":"2026-11-02T09:00:00Z"}'
```

### Retrieve a job

```sh
curl http://localhost:8080/jobs/JOB_ID
```

### Cancel a job

```sh
curl -X DELETE http://localhost:8080/jobs/JOB_ID
```

For a pending job, the response has `status: cancelled`. For a running job, the response remains `status: running` and includes `cancellation_requested_at` while the handler responds to cancellation. Repeated requests are safe. A missing job returns HTTP 404; a terminal non-cancelled job returns HTTP 409.

### Queue statistics

`GET /queues/{queueName}/stats` returns counts by job state, the count of unreplayed dead letters, and the creation time of the oldest pending job.

Example:

```sh
curl http://localhost:8080/queues/default/stats
```

Example response:

```json
{
  "queue_name": "default",
  "pending": 4,
  "running": 2,
  "succeeded": 108,
  "failed": 3,
  "cancelled": 0,
  "active_dead_letters": 3,
  "oldest_pending_at": "2026-10-26T08:00:00Z"
}
```

`oldest_pending_at` is `null` when no pending jobs exist. Counts are a point-in-time operational snapshot, not a transactional guarantee for subsequent requests.

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
- `internal/queue/` — PostgreSQL queue, queue statistics, dead-letter inspection/replay, integration tests, and benchmark
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
- **Milestone 4 — Reliability + Recovery:** final CI verification in progress
- **Milestone 5 — Scheduling + Dependencies:** scheduled job availability implemented; job dependencies planned
- **Milestone 6 — Observability + Load Testing:** initial queue statistics endpoint added; broader metrics and load testing planned
- **Milestone 7 — Infrastructure + Production Operations:** planned
