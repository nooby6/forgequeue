# ForgeQueue

ForgeQueue is a production-oriented distributed job and workflow engine built in Go.

## Current status

**Milestone 2 — Durable Job API**

Implemented:
- Go 1.23 service layout
- PostgreSQL persistence
- Durable job model and state machine
- REST job submission and lookup
- JSON payload validation
- Idempotency-Key support
- Liveness/readiness endpoints
- Structured JSON logging
- Graceful HTTP shutdown
- Docker Compose development environment
- GitHub Actions CI

Next: transactional queue claiming, concurrent workers, retries, leases, scheduling, observability and Kubernetes deployment.

## Architecture

```text
Client -> HTTP API -> Job Service -> PostgreSQL -> Queue / Worker Runtime
```

## Run locally

```bash
docker compose up -d postgres
export DATABASE_URL='postgres://forgequeue:forgequeue@localhost:5432/forgequeue?sslmode=disable'
go run ./cmd/api
```

Submit a job:

```bash
curl -X POST http://localhost:8080/jobs \\
  -H 'Content-Type: application/json' \\
  -H 'Idempotency-Key: email-001' \\
  -d '{"queue_name":"default","job_type":"send_email","payload":{"to":"user@example.com"},"priority":10}'
```

## Job lifecycle

```text
pending -> running -> succeeded
                 -> failed
                 -> cancelled
```

Worker delivery is the next milestone; Milestone 2 persists jobs in the pending state.
