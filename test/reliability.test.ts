import assert from "node:assert/strict";
import { test } from "node:test";
import { runReliabilityExperiment } from "../tools/reliability-experiment.ts";

test("the reliability experiment recovers expired work and rejects stale acknowledgements", () => {
  const report = runReliabilityExperiment({
    jobs: 20,
    logicalWorkers: 4,
    crashEvery: 5,
    leaseMs: 1_000,
  });

  assert.equal(report.simulatedCrashes, 4);
  assert.equal(report.recoveredLeases, 4);
  assert.equal(report.retryAttempts, 4);
  assert.equal(report.staleCompletionRejections, 4);
  assert.equal(report.duplicateCompletionRejections, 4);
  assert.equal(report.eventCounts.enqueued, 20);
  assert.equal(report.eventCounts.claimed, 24);
  assert.equal(report.eventCounts.lease_expired, 4);
  assert.equal(report.eventCounts.completed, 20);
  assert.deepEqual(report.finalStats, {
    queued: 0, running: 0, succeeded: 20, failed: 0, cancelled: 0, total: 20,
  });
  assert.ok(Object.values(report.invariants).every(Boolean));
});

test("the reliability experiment rejects scenarios with no simulated crash", () => {
  assert.throws(() => runReliabilityExperiment({
    jobs: 4,
    logicalWorkers: 2,
    crashEvery: 5,
    leaseMs: 1_000,
  }), /crashEvery must be a positive integer no greater than 4/);
});
