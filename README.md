# ForgeQueue

ForgeQueue is a production-oriented distributed job and workflow engine built in Go.

This is deliberately more than a CRUD API. The project is being built from first principles around durable state, concurrent delivery, worker coordination, failure recovery, observability and operational engineering.

## Current status

**Milestone 3 — Queue + Worker Runtime**

### Completed
- Go 1.23 backend
- PostgreSQL persistence
- Durable job state machine
- REST job submission and lookup
- JSON payload validation and request limits
- Idempotency-Key support
- Liveness/readiness endpoints
- Structured JSON logging
- Graceful HTTP shutdown
- Docker Compose development environment
- GitHub Actions CI
- PostgreSQL queue abstraction
- Transactional job claiming with FOR UPDATE SKIP LOCKED
- Priority-aware job selection
- Configurable worker concurrency
- Worker identity and execution leases
- Handler registry by job type
- Explicit succeeded/failed transitions
- Worker runtime tests

### Next milestones

**Milestone 4 — Reliability**
- lease expiry recovery
- automatic retries
- exponential backoff
- maximum attempt policies
- dead-letter queues
- cancellation semantics
- idempotent completion

**Milestone 5 — Scheduling**
- delayed jobs
- recurring schedules
- job dependencies
- dependency failure propagation

**Milestone 6 — Production Observability**
- Prometheus metrics
- OpenTelemetry tracing
- queue depth and worker utilisation
- latency/error measurements
- load testing and failure injection

**Milestone 7 — Infrastructure**
- container hardening
- Kubernetes deployment
- horizontal worker scaling
- configuration/secrets management
- operational runbooks

## Architecture

Client → HTTP API → Job Service → PostgreSQL → Queue Runtime → Worker Pool → Job Handler

Multiple worker loops can consume the same queue concurrently. PostgreSQL row locking prevents workers from claiming the same pending job.

## Queue semantics

Jobs are selected by queue, readiness time and priority. A worker claims a job inside a transaction using PostgreSQL SKIP LOCKED, transitions it to running, increments its attempt count, records its worker identity and establishes a lease.

This makes queue delivery an explicit part of the system rather than an abstraction hidden behind a third-party job library.

## Worker runtime

Each ForgeQueue process can run multiple workers.

Configuration:

WORKER_QUEUE=default
WORKER_CONCURRENCY=4
WORKER_POLL_INTERVAL=500ms
WORKER_LEASE=30s

The runtime currently includes a small noop handler so the system can be exercised without an external service.

The handler registry is intentionally separate from queue delivery: the queue decides which job should run, while the registry decides how that job type executes.

## Job lifecycle

pending → running → succeeded
                 ↘ failed

A running job is owned by a worker through locked_by and protected by a lease timestamp. Expired-lease recovery is deliberately reserved for Milestone 4 so the failure semantics can be implemented and tested properly.

## Run locally

Start PostgreSQL:

docker compose up -d postgres

Set the database connection:

export DATABASE_URL='postgres://forgequeue:forgequeue@localhost:5432/forgequeue?sslmode=disable'

Start the API and workers:

go run ./cmd/api

Health checks:

curl http://localhost:8080/health/live
curl http://localhost:8080/health/ready

Submit a job:

curl -X POST http://localhost:8080/jobs -H 'Content-Type: application/json' -H 'Idempotency-Key: noop-001' -d '{"queue_name":"default","job_type":"noop","payload":{"message":"hello ForgeQueue"},"priority":10}'

The worker should claim the job and move it from pending to running and then to succeeded.

## Testing

Run:

go test ./...

The GitHub Actions workflow also runs the test suite against PostgreSQL.

## Project structure

cmd/api/                 HTTP API and worker process
internal/config/         environment configuration
internal/database/       PostgreSQL connection pool
internal/health/         liveness/readiness
internal/httpserver/     HTTP routing and middleware
internal/job/            domain model, service and REST API
internal/queue/          queue abstraction and PostgreSQL implementation
internal/worker/         worker runtime and handler registry
db/init/                 local database schema
docs/adr/                architecture decisions

## Engineering principles

1. Durability before throughput — establish correct state transitions before optimising delivery.
2. Database semantics should be explicit — queue behaviour uses PostgreSQL primitives rather than hiding delivery behind a framework.
3. Workers must have ownership — a worker must prove that it owns a running job before acknowledging it.
4. Concurrency should be testable — queue and execution behaviour are isolated behind interfaces.
5. Failure is part of the design — retries, leases, dead letters and recovery are core system behaviour.
6. Operational behaviour belongs in the repository — configuration, CI, Docker, documentation and future runbooks are part of the system.

## Roadmap

Milestone 1  Foundation
Milestone 2  Durable Job API
Milestone 3  Queue + Worker Runtime  ← current
Milestone 4  Reliability + Recovery
Milestone 5  Scheduling + Dependencies
Milestone 6  Observability + Load Testing
Milestone 7  Kubernetes + Production Operations

ForgeQueue is intended to demonstrate real backend engineering: concurrency, transactions, distributed coordination, failure handling, API design, infrastructure and operational thinking.