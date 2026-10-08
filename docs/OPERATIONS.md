# Running and evaluating RelayQ

## Multi-worker demo

Start the API and three independent workers:

```bash
docker compose up --build --scale worker=3
```

To protect state-changing endpoints, pass the same API key to the API and
workers through Compose:

```bash
RELAYQ_API_KEY='replace-with-a-random-secret' docker compose up --build --scale worker=3
```

Open <http://localhost:8080>, submit several `sum`, `uppercase`, or `sleep`
jobs, and watch separate worker IDs claim work in the live activity feed. Queue
state is stored in the `relayq-data` volume and survives container replacement.

The container runs as an unprivileged user, exposes an HTTP health check, and
persists only the control plane's SQLite database. Workers are stateless and
communicate exclusively through the HTTP lease protocol.

## Measured multi-process load

Run the bounded experiment to start one local API and independent Node.js
worker processes, fill the queue with timed jobs, and verify that every worker
participates and every job reaches `succeeded` exactly once:

```bash
npm run multi-worker
npm run multi-worker -- --jobs 200 --workers 8 --task-ms 25 --json
```

The JSON report includes wall-clock throughput, completion counts per worker,
final queue statistics, explicit invariants, and limitations. Runs use an
isolated temporary database, accept at most 500 jobs and 32 workers, and remove
their state on exit.

Reference run on October 8, 2026: Node.js 24.16.0 on an Apple M4 running macOS
27.0 completed 100 25-ms jobs with four active worker processes in 0.778
seconds (128.5 jobs/second), with 25 completions per worker. This is a local
coordination measurement, not a multi-host scalability claim. One API process
owns SQLite, traffic stays on loopback, and the timed handler does not model
CPU-bound execution. Compare results only with the same inputs and environment.

The design deliberately targets a cloud/tools role signal—process orchestration,
HTTP worker coordination, and measurable load—while fitting RelayQ's existing
lease protocol. Its smallest credible scope is one API plus real worker
processes, its evidence is the machine-readable report and invariants, and its
honest boundary is single-host SQLite rather than distributed storage.

## Local durability benchmark

The benchmark exercises the real SQLite state machine: each job is inserted,
claimed, completed, and accompanied by three durable event writes.

```bash
npm run benchmark
npm run benchmark -- --jobs 10000 --workers 16 --json
```

Results depend heavily on storage and should be reported with hardware context.
`--workers` labels logical consumers; it does not claim parallel CPU scaling in
this single-process benchmark. The goal is a reproducible baseline for future
PostgreSQL and multi-process implementations.

## Deterministic crash-recovery demo

```bash
npm run failure-demo
```

The demo leases work to a worker that disappears, advances a controlled clock
past the lease deadline, and shows the same job being recovered and completed
by a replacement. Its event table makes the at-least-once state transitions
easy to explain in an interview or screen recording.

## Controlled reliability experiment

Run a repeatable batch in which every tenth logical worker disappears after
claiming a job. The experiment advances a controlled clock past the lease,
reclaims the abandoned work, and verifies that both the stale worker and a
duplicate acknowledgement are rejected.

```bash
npm run reliability
npm run reliability -- --jobs 200 --workers 8 --crash-every 10 --json
```

The JSON report captures simulated crashes, recovered leases, retry attempts,
rejection counts, event totals, final queue state, and wall-clock throughput.
It is intentionally a state-machine experiment: logical workers execute
sequentially in one process, the clock jump is deterministic, and it does not
model network partitions or multi-host availability. Use the Docker demo for
separate worker processes and treat the report as a reproducible correctness
baseline rather than a distributed throughput claim.

Runs are capped at 1,000 jobs and cannot declare more logical workers than
jobs. Each run uses an isolated temporary database and removes it on exit, so
the experiment cannot mutate the development or demo queue.

## Operational notes

- Back up the `/data` volume rather than copying an open database file.
- A readiness probe should use `/health`; queue depth is available at `/stats`.
- Pause dispatch during maintenance with `POST /admin/pause`, then restore it
  with `POST /admin/resume`; current leases can still finish while paused.
- Redrive and retention calls are bounded. Follow the
  [administration runbook](ADMINISTRATION.md) for safe maintenance sequences.
- Scale only the stateless `worker` service in the SQLite configuration.
- Bearer authentication is optional; without `RELAYQ_API_KEY`, run only in a
  trusted demo environment. TLS, scoped identities, and rate limiting remain
  deployment responsibilities.
