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

The container runs as an unprivileged user, exposes separate liveness and
SQLite-backed readiness checks, and persists only the control plane's SQLite
database. Workers are stateless and communicate exclusively through the HTTP
lease protocol.

## Measured multi-process load

Run the bounded experiment to start one local API and independent Node.js
worker processes, fill the queue with timed jobs, and verify that every worker
participates and every job reaches `succeeded` exactly once:

```bash
npm run multi-worker
npm run multi-worker -- --jobs 200 --workers 8 --task-ms 25 --json
```

The JSON report includes wall-clock throughput, completion counts per worker,
enqueue-to-completion p50/p95/max latency, final queue statistics, explicit
invariants, and limitations. Latency is derived from durable lifecycle-event
timestamps, and the report verifies that every completion has a matching
enqueue sample. Runs use an isolated temporary database, accept at most 500
jobs and 32 workers, and remove their state on exit.

Reference run on October 17, 2026: Node.js 24.16.0 on an Apple M4 running macOS
27.0 completed 100 25-ms jobs with four active worker processes in 0.794
seconds (126.0 jobs/second), with 25 completions per worker. Enqueue-to-
completion latency was 408 ms at p50, 756 ms at p95, and 784 ms maximum across
all 100 durable event pairs. This is a local coordination measurement, not a
multi-host scalability claim. One API process owns SQLite, traffic stays on
loopback, and the timed handler does not model CPU-bound execution. Latency uses
millisecond wall-clock timestamps on that one host. Compare results only with
the same inputs and environment.

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

## Worker-process crash experiment

Run a real process failure in an isolated environment:

```bash
npm run process-crash
npm run process-crash -- --lease-ms 1000 --task-ms 250 --json
```

The experiment starts an API and worker, waits until the worker owns a timed
job, kills that process with `SIGKILL`, and starts a replacement. The report
verifies the expired lease, two claim attempts, one recorded completion, the
replacement's identity, and an empty final queue.

Reference run on October 11, 2026: Node.js 24.16.0 on an Apple M4 running macOS
27.0 recovered and completed the job 1,256 ms after the crash with a 1,000-ms
lease and 250-ms task. This is evidence of the HTTP lease protocol surviving a
worker-process failure, not evidence of multi-host availability. The API and
SQLite process stay healthy, traffic uses loopback, and the side-effect-free
probe cannot prove exactly-once external effects.

The role signal is failure investigation and recovery automation for a
cloud/tools system. The change fits RelayQ's existing lease state machine; its
smallest credible scope is one killed owner and one replacement; its evidence
is the event sequence, recovery time, and checked invariants; and its honest
boundary is one host with a healthy control plane.

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
- Use `/health` for liveness and `/ready` for readiness. The readiness check is
  a lightweight read of RelayQ's control row; it returns `503` when SQLite is
  unavailable. The image's built-in Docker health check remains a liveness
  probe; configure `/ready` as the traffic readiness gate in an orchestrator.
  Queue depth is available separately at `/stats`.
- Pause dispatch during maintenance with `POST /admin/pause`, then restore it
  with `POST /admin/resume`; current leases can still finish while paused.
- Workers log completion only after the API accepts the lease-owned transition.
  A rejected completion is reported as a control-plane error and is not turned
  into a handler failure: the worker may no longer own the lease, and the queue
  will recover it through normal lease expiry. Actual handler errors still use
  the lease-owned failure endpoint and durable retry policy.
- Workers request renewal at roughly one-third of the configured lease, including
  the minimum one-second lease. Each worker keeps at most one renewal request in
  flight per job, preventing a slow control plane from accumulating overlapping
  heartbeats. A worker also awaits that request before acknowledging completion
  or failure, preventing its own lease mutations from overlapping. Renewal timers
  are best effort and requests have no timeout: event-loop stalls or network
  delays can still expire a lease or delay acknowledgement, so handlers remain
  idempotent.
- Redrive and retention calls are bounded. Follow the
  [administration runbook](ADMINISTRATION.md) for safe maintenance sequences.
- Scale only the stateless `worker` service in the SQLite configuration.
- Bearer authentication is optional; without `RELAYQ_API_KEY`, run only in a
  trusted demo environment. TLS, scoped identities, and rate limiting remain
  deployment responsibilities.
