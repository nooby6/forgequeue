# ForgeQueue

**A distributed job and workflow engine built from first principles in Go.**

ForgeQueue is a backend systems project focused on the engineering behind reliable job infrastructure: durable state, transactional queueing, concurrent workers, ownership, leases, retries, scheduling, observability and failure recovery.

> **Current status: Milestone 3 — Queue + Worker Runtime**

## What this project is trying to prove

This is deliberately not another CRUD portfolio API. The system is being built incrementally so the difficult backend problems remain visible and testable:
- safe concurrent job claiming
- durable job state
- worker ownership
- execution leases
- failure recovery
- retries and backoff
- scheduling and dependencies
- observability and load testing
- production operations

## Stack

| Layer | Technology |
|---|---|
| Language | Go 1.23 |
| API | net/http |
| Database | PostgreSQL 16 |
| Driver | pgx/v5 |
| Queue | PostgreSQL-backed queue |
| Concurrency | Goroutines + worker pool |
| Containers | Docker / Docker Compose |
| CI | GitHub Actions |
| Logging | log/slog structured JSON |
| Future observability | Prometheus + OpenTelemetry |
| Future orchestration | Kubernetes |

## Architecture

Client → HTTP API → Job Service → PostgreSQL → Queue Runtime → Worker Pool → Handler Registry

Workers claim durable jobs from PostgreSQL. Queue delivery is separated from job execution so the queue answers **which job should run**, while the handler registry answers **how that job type executes**.

## Milestone 1 — Foundation ✓

- Go module and repository structure
- configuration
- PostgreSQL connection pool
- initial schema
- liveness/readiness endpoints
- structured logging
- graceful shutdown
- Docker Compose
- Dockerfile
- GitHub Actions CI
- initial architecture decision record

## Milestone 2 — Durable Job API ✓

- durable PostgreSQL job records
- POST /jobs
- GET /jobs/{id}
- JSON payload validation
- strict JSON decoding
- request-size protection
- priority
- Idempotency-Key support
- repository/service/HTTP separation

## Milestone 3 — Queue + Worker Runtime ✓

### PostgreSQL queue

The queue is behind an interface with ClaimNext, Complete and Fail operations. PostgreSQL is the first implementation so queue semantics remain explicit rather than hidden behind a job framework.

### Atomic claiming

Pending jobs are selected by queue, availability and priority using PostgreSQL row locking with FOR UPDATE SKIP LOCKED. The claim transitions the job to running, increments attempts, records the worker identity and creates a lease.

### Worker pool

Worker concurrency, queue name, polling interval and lease duration are configurable. Workers poll, claim, resolve a handler, execute it and acknowledge success or failure.

### Handler registry

Job types are mapped to handlers. A dependency-free noop handler is included so the full submission → claim → execution → completion path can be exercised locally.

### Ownership and leases

Running jobs now have locked_by, locked_at and lease_expires_at fields. Completion and failure require the worker that claimed the job.

Automatic recovery of expired leases is intentionally reserved for Milestone 4.

## Current job lifecycle

pending → running → succeeded

pending → running → failed

Cancellation exists in the domain model, but cancellation semantics are not yet implemented.

## Configuration

APP_ADDR=:8080

DATABASE_URL=postgres://forgequeue:forgequeue@localhost:5432/forgequeue?sslmode=disable

WORKER_QUEUE=default

WORKER_CONCURRENCY=4

WORKER_POLL_INTERVAL=500ms

WORKER_LEASE=30s

## Run locally

Start PostgreSQL:

    docker compose up -d postgres

Set the database connection:

    export DATABASE_URL='postgres://forgequeue:forgequeue@localhost:5432/forgequeue?sslmode=disable'

Start the API and workers:

    go run ./cmd/api

Health:

    curl http://localhost:8080/health/live
    curl http://localhost:8080/health/ready

Submit a job:

    curl -X POST http://localhost:8080/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: noop-001' -d '{"queue_name":"default","job_type":"noop","payload":{"message":"hello ForgeQueue"},"priority":10}'

Then retrieve it with GET /jobs/{job-id}. The worker should move it from pending to running and finally succeeded.

## Testing

Run:

    go test ./...

The GitHub Actions workflow provisions PostgreSQL and runs the Go test suite.

Tests currently cover configuration, health behaviour, job validation/HTTP handling, handler registration and worker execution. Database-level concurrency tests become a major focus in the reliability milestones.

## Project structure

forgequeue/
├── cmd/api/                  HTTP API and worker process
├── internal/config/          environment configuration
├── internal/database/        PostgreSQL connection pool
├── internal/health/          liveness/readiness
├── internal/httpserver/      HTTP routing and middleware
├── internal/job/             domain model, repository, service and REST API
├── internal/queue/           queue abstraction + PostgreSQL implementation
├── internal/worker/          worker runtime + handler registry
├── db/init/                  local schema
├── db/migrations/            versioned database migrations
├── docs/adr/                 architecture decisions
├── .github/workflows/        CI
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── go.mod
└── README.md

## Database migrations

Versioned migrations currently include:
- 000001_initial_schema — durable jobs and queue indexes
- 000002_worker_leases — worker ownership and lease fields

The local db/init schema contains the equivalent current development schema.

## Engineering decisions

### PostgreSQL first

Redis or a dedicated broker can be introduced later, but PostgreSQL is intentionally first. This forces the project to establish durability, transactional claiming, ordering, ownership and state transitions before adding another distributed component.

### SKIP LOCKED

An in-memory queue would hide the hardest persistence and concurrency problems. PostgreSQL row locking makes the delivery semantics explicit and allows multiple workers to consume the same queue without waiting on rows already claimed by another worker.

### Separate queue from execution

The queue decides which job runs. The handler registry decides how it runs. This boundary is important for later retries, scheduling, multiple worker types and testing.

## Roadmap

✓ Milestone 1 — Foundation

✓ Milestone 2 — Durable Job API

✓ Milestone 3 — Queue + Worker Runtime

→ Milestone 4 — Reliability + Failure Recovery

Milestone 5 — Scheduling + Dependencies

Milestone 6 — Observability + Load Testing

Milestone 7 — Kubernetes + Production Operations

### Milestone 4

- lease expiry detection
- automatic requeue
- retry policies
- exponential backoff
- maximum attempts
- dead-letter queues
- cancellation
- idempotent completion
- worker crash recovery
- failure-injection tests

### Milestone 5

- delayed jobs
- scheduled/recurring jobs
- dependency graphs
- dependency failure propagation
- workflow execution

### Milestone 6

- Prometheus metrics
- OpenTelemetry traces
- queue depth
- job latency
- worker utilisation
- throughput benchmarks
- structured execution events
- load testing
- failure injection

### Milestone 7

- container hardening
- Kubernetes deployment
- horizontal worker scaling
- health/readiness integration
- secrets management
- resource limits
- deployment strategy
- operational runbooks

## Engineering principles

1. **Correctness before throughput.** Establish reliable state transitions before optimising.
2. **Concurrency should be explicit.** Worker coordination must be understandable from code and database semantics.
3. **Durability matters.** A process restart should not erase queued work.
4. **Ownership matters.** A worker should only acknowledge work it owns.
5. **Failure is normal.** Retries, leases, dead letters and recovery are core functionality.
6. **Infrastructure is part of the application.** Docker, CI, migrations, configuration and runbooks belong in the repository.
7. **Measure before optimising.** Performance claims will be backed by benchmarks and load tests.

## Project goal

The finished ForgeQueue system should demonstrate practical backend engineering across Go concurrency, PostgreSQL transactions, distributed coordination, queue semantics, worker systems, failure recovery, idempotency, scheduling, observability, containers, Kubernetes and performance testing.

The objective is not to build the largest job queue. The objective is to demonstrate that the engineer building it understands why distributed backend systems behave the way they do under concurrency, failure and load.