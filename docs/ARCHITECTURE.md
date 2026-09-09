# RelayQ architecture

RelayQ currently has three deliberately small components:

```text
producer --> HTTP control plane --> SQLite job store <-- worker pool
                 |          |           |
            SSE dashboard   +-- read APIs +-- durable leases and retries
```

## Delivery model

RelayQ provides **at-least-once delivery**. Claiming a job increments its attempt
counter and grants a time-limited lease. A worker must complete or fail the job
while it owns that lease. Long-running handlers renew their leases periodically.
If the worker disappears, the next claim operation recovers the expired lease
and requeues the job. A job is moved to the failed-job view when its attempt
budget is exhausted; an operator can requeue it with a fresh attempt budget.

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

## Retry scheduling

Retry policy is durable job metadata rather than worker configuration. After a
failed attempt, the store computes capped exponential backoff and applies
bounded random jitter before updating `available_at`. The state change and its
chosen delay are written to the event log atomically, making retry decisions
observable and reproducible in tests with an injected random source. See the
[retry policy guide](RETRY_POLICY.md) for the formula and defaults.

## Dead-letter recovery

Permanently failed work remains in the main jobs table so its payload, error,
attempt count, and timestamps stay available for diagnosis. The `/dead-letter`
view isolates those records operationally. Requeueing is an explicit action that
resets attempts and clears the old error, preventing accidental infinite retry
loops while preserving the job identity for auditability.

## Event log and live views

Every state mutation appends a structured row to `job_events` in the same
SQLite transaction. Monotonic event IDs make the log suitable for cursor-based
queries and reconnectable streaming. The dashboard uses Server-Sent Events
rather than WebSockets because updates flow in one direction and the browser's
native client automatically reconnects with its last received event ID.

The HTTP process polls for newly committed event IDs at 500 ms intervals. This
keeps the MVP dependency-free and works across multiple processes sharing the
database. A production adapter could replace polling with PostgreSQL `LISTEN /
NOTIFY` or a dedicated event broker without changing the public protocol.

## Trust boundaries

The MVP is intended for a trusted local network. An optional bearer token uses
constant-time comparison to protect all mutations. Read APIs remain open for
the dashboard and health probes. TLS, tenant isolation, scoped identities, rate
limiting, and administrative audit logging are still required before an
internet-facing deployment. See [the security model](SECURITY.md).

## Administrative control

Pause state lives in SQLite rather than process memory, so it survives restarts
and is observed consistently by every API process. A pause blocks new claims
but does not discard queued work or prevent current workers from acknowledging
their leases. Expired leases are still recovered while paused, ensuring work is
ready when an operator resumes dispatch.
