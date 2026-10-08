# ForgeQueue Code Review Notes

This document records an engineering review of the queue and worker runtime, with emphasis on correctness, concurrency, failure handling, testability, and operational behaviour.

## Review scope

- `internal/queue/postgres.go`
- `internal/worker/worker.go`
- PostgreSQL job-claiming and worker ownership model
- Worker lifecycle and error handling

## What is already strong

### Atomic job claiming

The queue uses a transaction with:

- `FOR UPDATE SKIP LOCKED`
- deterministic priority ordering
- a state transition from `pending` to `running`
- attempt counting
- worker ownership
- lease metadata

This is a good foundation for concurrent workers because workers do not need a global in-process lock to coordinate job ownership.

### Ownership checks

Completion and failure updates require both:

- `status = 'running'`
- `locked_by = workerID`

This prevents an unrelated worker from completing a job it does not own.

### Bounded worker concurrency

The worker runtime creates a fixed number of processing loops from configuration instead of creating an unbounded goroutine for every poll or job.

### Explicit failure paths

Missing handlers and handler errors are surfaced and transition the job out of the running state instead of silently leaving it stuck.

## Review findings

### 1. Lease expiry needs recovery semantics

**Risk:** a worker can claim a job and then disappear before completing it. The database records `lease_expires_at`, but the current claim query only considers `status = 'pending'`.

That means an expired `running` job cannot currently be reclaimed by another worker.

**Recommended change:** make lease recovery an explicit part of the queue contract. A recovery operation should atomically identify expired running jobs and return them to a claimable state, or reclaim them directly under worker ownership.

**Tests to add:**

- expired lease becomes claimable
- active lease is not reclaimed
- two workers cannot reclaim the same job
- a late completion from the previous owner is rejected

### 2. Retry policy is not yet separated from failure handling

The worker currently marks a handler error as `failed` immediately.

For a production queue, transient failures should normally be distinguishable from terminal failures.

**Recommended change:** introduce retry policy independently from the worker loop:

- maximum attempts
- retryable vs terminal errors
- exponential backoff
- next `available_at`
- dead-letter state after retry exhaustion

This keeps the worker runtime simple while making retry behaviour explicit and testable.

### 3. Handler execution needs lease-awareness

A lease only protects ownership in the database; it does not stop a handler that runs longer than the lease.

A long-running handler can therefore continue executing after its ownership has expired.

**Recommended change:** define lease renewal/heartbeat semantics for long-running jobs and make handler execution cancellation-aware.

**Tests to add:**

- heartbeat extends an active lease
- expired ownership prevents completion
- cancellation stops a handler cleanly

### 4. Queue correctness should have integration coverage

The current unit tests exercise worker behaviour, but the most important concurrency guarantees live in PostgreSQL.

**Recommended integration tests:**

- two workers claiming the same queue concurrently
- priority ordering
- lease ownership
- expired lease recovery
- concurrent completion
- idempotent job submission
- database constraint behaviour

Testcontainers is a strong fit because these guarantees depend on real PostgreSQL locking and transaction semantics.

## Review checklist

For future ForgeQueue changes, review against:

- **Correctness:** can the state machine reach an invalid state?
- **Concurrency:** what happens with two workers or requests at the same time?
- **Transactions:** which operations must be atomic?
- **Database:** are indexes, locks, constraints, and query plans appropriate?
- **Failure:** what happens if the process, network, database, or handler fails?
- **Retries:** is the operation safe to execute again?
- **API contract:** are validation and error semantics explicit?
- **Testing:** does the test reproduce the failure mode rather than only the happy path?
- **Performance:** where is the work happening, and can it become a bottleneck?
- **Operations:** can the behaviour be observed and debugged in production?

## Review principle

A good code review should not only ask whether the code works today.

It should ask what happens under concurrency, partial failure, retries, increasing load, malformed input, slow dependencies, and future maintenance.

That is the standard used for ForgeQueue as it moves toward its reliability and production-engineering milestones.
