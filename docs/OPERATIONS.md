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

## Operational notes

- Back up the `/data` volume rather than copying an open database file.
- A readiness probe should use `/health`; queue depth is available at `/stats`.
- Pause dispatch during maintenance with `POST /admin/pause`, then restore it
  with `POST /admin/resume`; current leases can still finish while paused.
- Scale only the stateless `worker` service in the SQLite configuration.
- The API is unauthenticated and intended for a trusted demo environment.
