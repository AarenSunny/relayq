# RelayQ

RelayQ is a durable distributed job-processing platform built from first
principles with Node.js and SQLite. Producers submit jobs over HTTP, workers
lease them, and the queue safely retries work after explicit failures or worker
crashes.

This is a working portfolio project, not a wrapper around an existing queue.
The scheduling, lease ownership, retry state machine, persistence, worker
protocol, and HTTP control plane are implemented in this repository.

## Current features

- Durable priority queue with FIFO ordering inside each priority
- Delayed jobs and configurable attempt budgets
- Atomic worker leases that prevent double claims
- Automatic recovery after worker crashes
- Renewable leases for handlers that outlive their initial claim
- Explicit success, failure, cancellation, and retry transitions
- Dead-letter inspection and operator-controlled redrive
- HTTP endpoints for producers, workers, inspection, and queue statistics
- Example worker with `sum`, `uppercase`, and `sleep` task handlers
- Unit and end-to-end HTTP tests with no third-party runtime dependencies

## Quick start

RelayQ requires Node.js 24 or newer.

```bash
npm test
npm start
```

In another terminal, start a worker:

```bash
npm run worker
```

Submit a job:

```bash
curl -s http://localhost:8080/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"sum","payload":[20,22],"priority":5}'
```

Inspect the queue:

```bash
curl -s http://localhost:8080/jobs
curl -s http://localhost:8080/stats
```

The default database is `relayq.db`. Set `RELAYQ_DB`, `PORT`, `RELAYQ_URL`,
`WORKER_ID`, `POLL_MS`, or `LEASE_MS` to change runtime settings.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Enqueue a job |
| `GET` | `/jobs?status=queued&limit=100` | List jobs |
| `GET` | `/jobs/:id` | Inspect one job |
| `POST` | `/jobs/:id/cancel` | Cancel non-terminal work |
| `POST` | `/workers/:workerId/claim` | Lease the next eligible job |
| `POST` | `/jobs/:id/complete` | Record a leased job's result |
| `POST` | `/jobs/:id/fail` | Retry or permanently fail a job |
| `POST` | `/jobs/:id/heartbeat` | Renew a worker-owned lease |
| `GET` | `/dead-letter` | Inspect jobs that exhausted retries |
| `POST` | `/jobs/:id/requeue` | Redrive a failed job with fresh attempts |
| `GET` | `/stats` | Return counts by state |
| `GET` | `/health` | Liveness check |

See [the architecture notes](docs/ARCHITECTURE.md) for the delivery guarantees,
concurrency strategy, tradeoffs, and trust boundaries.

## Roadmap

- Exponential backoff with configurable jitter
- Bulk dead-letter redrive and retention policies
- Server-sent events dashboard
- PostgreSQL storage adapter and multi-node control plane
- Worker authentication, rate limiting, and structured audit events
- Docker Compose demo and load/failure benchmark

## Status

The durable queue MVP, renewable worker leases, and dead-letter recovery are
operational. The next milestone is structured event history plus an observable
web dashboard.

## License

MIT
