# Queue administration

RelayQ keeps administrative operations bounded and explicit so maintenance does
not accidentally turn into an unbounded database transaction.

## Bulk dead-letter redrive

```bash
curl -X POST http://localhost:8080/admin/redrive \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $RELAYQ_API_KEY" \
  -d '{"limit":100,"delayMs":5000}'
```

The oldest failed jobs are selected first. Each selected job keeps its ID and
retry policy, resets its attempt counter, clears its error, and receives its own
`redriven` lifecycle event with `bulk: true`. The hard limit of 1,000 jobs per
request prevents a redrive from monopolizing the SQLite writer.

## Retention cleanup

```bash
curl -X POST http://localhost:8080/admin/purge \
  -H 'content-type: application/json' \
  -H "authorization: Bearer $RELAYQ_API_KEY" \
  -d '{"olderThanMs":604800000,"limit":1000,"includeFailed":false}'
```

By default, cleanup removes only succeeded and cancelled jobs older than seven
days. Failed jobs are preserved for diagnosis unless `includeFailed` is exactly
`true`. Queued and running work is never eligible. Deleting a job also removes
its lifecycle events through a foreign-key cascade.

The response lists deleted job IDs so an operator can record what happened.
Each request is capped at 10,000 records; repeat the call for larger retention
backlogs.

## Safe maintenance sequence

1. Pause dispatch with `POST /admin/pause` and a reason.
2. Allow current worker leases to complete or expire.
3. Inspect `/stats` and `/dead-letter`.
4. Redrive or purge bounded batches as appropriate.
5. Resume dispatch with `POST /admin/resume`.

When `RELAYQ_API_KEY` is configured, all these state-changing endpoints require
the bearer token.
