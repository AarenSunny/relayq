import assert from "node:assert/strict";
import { test } from "node:test";
import { JobStore } from "../src/store.ts";
import { readJobEvents, runMultiWorkerExperiment } from "../tools/multi-worker-experiment.ts";

test("separate worker processes drain the queue exactly once", async () => {
  const report = await runMultiWorkerExperiment({
    jobs: 12,
    workers: 3,
    taskMs: 20,
    timeoutMs: 10_000,
  });

  assert.equal(report.model, "separate-http-workers-single-api");
  assert.equal(report.environment.nodeVersion, process.version);
  assert.ok(report.environment.platform.length > 0);
  assert.ok(report.environment.operatingSystemRelease.length > 0);
  assert.ok(report.environment.architecture.length > 0);
  assert.ok(report.environment.availableParallelism > 0);
  assert.equal(report.activeWorkers, 3);
  assert.equal(Object.values(report.completionsByWorker).reduce((sum, count) => sum + count, 0), 12);
  assert.deepEqual(report.finalStats, {
    queued: 0, running: 0, succeeded: 12, failed: 0, cancelled: 0, total: 12,
  });
  assert.equal(report.enqueueToCompletionMs.samples, 12);
  assert.ok(report.enqueueToCompletionMs.p50 >= 0);
  assert.ok(report.enqueueToCompletionMs.p50 <= report.enqueueToCompletionMs.p95);
  assert.ok(report.enqueueToCompletionMs.p95 <= report.enqueueToCompletionMs.max);
  assert.ok(Object.values(report.invariants).every(Boolean));
});

test("experiment evidence includes jobs older than the global event window", () => {
  const store = new JobStore(":memory:");
  try {
    const jobIds: string[] = [];
    for (let index = 0; index < 334; index += 1) {
      const job = store.enqueue({ type: "evidence-window" });
      jobIds.push(job.id);
      store.claim("worker");
      store.complete(job.id, "worker");
    }

    assert.equal(store.listEvents(undefined, 0, 1_000).length, 1_000);
    const events = readJobEvents(store, jobIds);
    assert.equal(events.length, 1_002);
    assert.equal(events.filter((event) => event.type === "enqueued").length, 334);
    assert.equal(events.filter((event) => event.type === "completed").length, 334);
  } finally {
    store.close();
  }
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
