import assert from "node:assert/strict";
import { test } from "node:test";
import { runMultiWorkerExperiment } from "../tools/multi-worker-experiment.ts";

test("separate worker processes drain the queue exactly once", async () => {
  const report = await runMultiWorkerExperiment({
    jobs: 12,
    workers: 3,
    taskMs: 20,
    timeoutMs: 10_000,
  });

  assert.equal(report.model, "separate-http-workers-single-api");
  assert.equal(report.activeWorkers, 3);
  assert.equal(Object.values(report.completionsByWorker).reduce((sum, count) => sum + count, 0), 12);
  assert.deepEqual(report.finalStats, {
    queued: 0, running: 0, succeeded: 12, failed: 0, cancelled: 0, total: 12,
  });
  assert.ok(Object.values(report.invariants).every(Boolean));
});

test("multi-worker experiment rejects unbounded scenarios", async () => {
  await assert.rejects(
    runMultiWorkerExperiment({ jobs: 501, workers: 2, taskMs: 1, timeoutMs: 1_000 }),
    /jobs must be an integer between 1 and 500/,
  );
  await assert.rejects(
    runMultiWorkerExperiment({ jobs: 2, workers: 3, taskMs: 1, timeoutMs: 1_000 }),
    /workers must be an integer between 1 and 2/,
  );
});
