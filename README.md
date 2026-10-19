# RelayQ

A job queue I built with Node.js and SQLite. Producers submit jobs over HTTP, workers claim and run them, and if a worker crashes, the job goes back into the queue instead of disappearing. I started this as independent work alongside Stanford's CS 106B class because I wanted to explore queues beyond the classroom. I picked a problem that seemed challenging but also useful in real systems.

## What it does

- Priority queue (FIFO within the same priority), delayed jobs, and retry limits
- Workers take a **lease** on a job. If they don't finish or renew it in time, the job is released for someone else
- Late or duplicate "I'm done" messages from a worker whose lease already expired get rejected
- Failed jobs retry with exponential backoff and jitter, then land in a dead-letter queue you can inspect and replay
- A live dashboard in the browser that updates through Server-Sent Events

## Try it

```sh
npm test
npm start          # API + dashboard on http://localhost:8080
npm run worker     # in another terminal
```

Or run `docker compose up --build --scale worker=3` to start several workers.

## The interesting part

The hard bug was two workers both thinking they owned the same job. Leases fixed the claiming side, but I also had to reject completions from a worker whose lease had already run out. There's a crash test that kills workers on purpose. One hundred jobs still finish correctly through 10 simulated crashes.

## Limits

Everything runs on one SQLite file on one machine, so it isn't distributed. Postgres support and multi-node operation are ideas, not features. More detail is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
