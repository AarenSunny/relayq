# Retry policy

RelayQ stores retry behavior with each job so workers remain stateless and every
attempt follows the policy selected by the producer.

## Formula

For attempt number `n`, the uncapped delay is:

```text
base delay × 2^(n - 1)
```

RelayQ caps that value at `backoffMaxMs`, then applies uniform jitter from
`-backoffJitter` to `+backoffJitter`. With the defaults, retries are centered on
1, 2, 4, 8, 16, 32, and 60 seconds, with ±20% jitter after the cap is applied.
Jitter prevents many failed jobs from retrying simultaneously when a shared
dependency recovers.

## Per-job configuration

```json
{
  "type": "send-report",
  "payload": { "reportId": "demo" },
  "maxAttempts": 5,
  "backoffBaseMs": 500,
  "backoffMaxMs": 30000,
  "backoffJitter": 0.15
}
```

`backoffJitter` is a fraction from `0` through `1`. Set it to `0` for exact,
deterministic delays. A worker or operator may supply `retryDelayMs` when failing
a job to override the computed delay for that single transition.

All configured and operator-supplied delays use non-negative safe-integer
milliseconds. RelayQ also rejects a delay when adding it to the current clock
would exceed JavaScript's exactly representable integer range, before changing
queue state. This protects SQLite scheduling timestamps from fractional or
rounded values without imposing a shorter deployment-specific delay policy.

Jobs move to the dead-letter view after the final failed attempt. Redriving one
resets its attempt counter but preserves its configured retry policy.

## Compatibility

On startup, RelayQ detects databases created before retry policies existed and
adds the three columns with safe defaults. The migration is covered by an
integration test using a legacy schema fixture.
