# RelayQ

[![CI](https://github.com/AarenSunny/relayq/actions/workflows/ci.yml/badge.svg)](https://github.com/AarenSunny/relayq/actions/workflows/ci.yml)

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
- Per-job exponential retry backoff with caps and jitter
- Persistent pause/resume controls for maintenance windows
- Bounded bulk dead-letter redrive and terminal-job retention cleanup
- Atomic worker leases that prevent double claims
- Automatic recovery after worker crashes
- Renewable leases for handlers that outlive their initial claim
- Explicit success, failure, cancellation, and retry transitions
- Dead-letter inspection and operator-controlled redrive
- Durable, ordered event history for every job transition
- Live browser dashboard powered by Server-Sent Events
- HTTP endpoints for producers, workers, inspection, and queue statistics
- Example worker with `sum`, `uppercase`, and `sleep` task handlers
- Unit and end-to-end HTTP tests with no third-party runtime dependencies
- Non-root container image and scalable multi-worker Compose demo
- Reproducible durability benchmark and deterministic crash demonstration
- Optional bearer-token protection and hardened HTTP request handling
- Dependency-free producer and operator CLI

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

Open `http://localhost:8080` to watch queue counts, jobs, and lifecycle events
update live. The dashboard can also enqueue work for the example worker.

Or launch a persistent API with three workers in containers:

```bash
docker compose up --build --scale worker=3
```

Submit a job:

```bash
curl -s http://localhost:8080/jobs \
  -H 'content-type: application/json' \
  -d '{"type":"sum","payload":[20,22],"priority":5,"maxAttempts":4}'
```

Inspect the queue:

```bash
curl -s http://localhost:8080/jobs
curl -s http://localhost:8080/stats
```

The same workflows are available through the CLI:

```bash
npm run cli -- submit sum '[20,22]' --priority 5
npm run cli -- jobs --status succeeded
npm run cli -- stats
```

The default database is `relayq.db`. Set `RELAYQ_DB`, `PORT`, `RELAYQ_URL`,
`WORKER_ID`, `POLL_MS`, or `LEASE_MS` to change runtime settings.
Set `RELAYQ_API_KEY` on both the API and workers to protect mutations.

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
| `GET` | `/events` | Query durable lifecycle events by cursor or job |
| `GET` | `/events/stream` | Subscribe to live events with SSE |
| `GET` | `/stats` | Return counts by state |
| `GET` | `/health` | Liveness check |
| `GET` | `/admin/state` | Inspect persistent pause state |
| `POST` | `/admin/pause` | Stop dispatching new work |
| `POST` | `/admin/resume` | Resume dispatch |
| `POST` | `/admin/redrive` | Redrive a bounded batch of failed jobs |
| `POST` | `/admin/purge` | Remove eligible terminal jobs by age |

See [the architecture notes](docs/ARCHITECTURE.md) for the delivery guarantees,
concurrency strategy, tradeoffs, and trust boundaries. The
[operations guide](docs/OPERATIONS.md) covers containers, benchmarking, and a
reproducible worker-crash demonstration. [Retry policy documentation](docs/RETRY_POLICY.md)
explains the backoff formula, jitter, overrides, and migration behavior.
The [security model](docs/SECURITY.md) documents authentication guarantees and
the remaining requirements for an internet-facing deployment.
The [administration guide](docs/ADMINISTRATION.md) provides safe redrive,
retention, and maintenance procedures.
For a concise project presentation, follow the
[three-minute portfolio demo](docs/DEMO.md).

## Roadmap

- Scheduled retention and dead-letter policy automation
- Dashboard filtering and per-job timeline views
- PostgreSQL storage adapter and multi-node control plane
- Scoped worker identities, rate limiting, and administrative audit events
- Multi-host benchmark and automated chaos scenarios

## Status

The portfolio release includes renewable leases, failed-job recovery,
transactional event history, a live control-room dashboard, container packaging,
and repeatable performance and failure demonstrations. Retry scheduling now uses
per-job capped exponential backoff with jitter. Persistent maintenance controls,
optional mutation authentication, and defensive request handling are also
operational. Bounded bulk redrive, retention cleanup, and the operator CLI
complete the planned core workflow; remaining items are documented extensions.

## License

MIT
