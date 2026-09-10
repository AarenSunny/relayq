# Three-minute portfolio demo

This walkthrough demonstrates RelayQ's user experience and its underlying
distributed-systems decisions without relying on hidden setup.

## 1. Start the control plane and worker

```bash
npm ci
npm start
```

In a second terminal:

```bash
npm run worker
```

Open <http://localhost:8080>. The dashboard should show a live connection and
zeroed queue counters.

## 2. Submit and inspect work

```bash
npm run cli -- submit sum '[20,22]' --priority 5 --max-attempts 4
npm run cli -- submit uppercase '"distributed systems"'
npm run cli -- jobs
npm run cli -- stats
```

Point out the priority/FIFO ordering, worker identity, attempt budget, durable
result, and lifecycle events as they update in the browser.

## 3. Demonstrate operational control

```bash
npm run cli -- pause "maintenance demo"
npm run cli -- submit sum '[1,2,3]'
npm run cli -- jobs --status queued
npm run cli -- resume
```

The queued job remains durable during the pause and is claimed immediately
after resume. Existing leases can still finish, which avoids corrupting active
work during maintenance.

## 4. Show failure recovery and measurements

```bash
npm run failure-demo
npm run benchmark -- --jobs 1000 --workers 8
```

The failure demo shows one worker disappearing and another completing the same
job ID after lease expiry. The benchmark exercises real SQLite writes for job
creation, claims, results, and event history.

## Interview talking points

- RelayQ guarantees at-least-once delivery; handlers should be idempotent.
- `BEGIN IMMEDIATE` serializes claims so two workers cannot receive one job.
- Renewable leases distinguish slow workers from crashed workers.
- Exponential backoff with jitter reduces synchronized retry storms.
- SQLite is an intentional single-host MVP; PostgreSQL with `SKIP LOCKED` is the
  documented path to a horizontally scaled control plane.
- Every important limitation—authentication scope, benchmark concurrency, and
  deployment boundary—is stated plainly rather than hidden behind a demo.
