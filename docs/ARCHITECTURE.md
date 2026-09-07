# RelayQ architecture

RelayQ currently has three deliberately small components:

```text
producer --> HTTP control plane --> SQLite job store <-- worker pool
                 |                      |
                 +---- read APIs -------+-- durable leases and retries
```

## Delivery model

RelayQ provides **at-least-once delivery**. Claiming a job increments its attempt
counter and grants a time-limited lease. A worker must complete or fail the job
while it owns that lease. If the worker disappears, the next claim operation
recovers the expired lease and requeues the job. A job is permanently failed
when its attempt budget is exhausted.

At-least-once delivery means a worker can finish an external side effect and
crash before recording completion. Production handlers therefore need to be
idempotent, usually by storing the job ID with the side effect.

## Concurrency

Claims use a short `BEGIN IMMEDIATE` transaction. SQLite permits many readers
but serializes writers, so two control-plane processes cannot lease the same
job. WAL mode keeps reads available while a writer commits. This is appropriate
for the single-host MVP; a later PostgreSQL adapter will use `FOR UPDATE SKIP
LOCKED` for horizontal control-plane scaling.

## Queue ordering

Eligible jobs are ordered by descending integer priority, then FIFO creation
time. `available_at` supports delayed jobs and retry backoff without a separate
timer service.

## Trust boundaries

The MVP is intended for a trusted local network. Authentication, TLS, tenant
isolation, payload schemas, rate limiting, and audit logging are planned before
an internet-facing deployment.
