import assert from "node:assert/strict";
import { test } from "node:test";
import { runProcessCrashExperiment } from "../tools/process-crash-experiment.ts";

test("a replacement process recovers work after its owner is killed", async () => {
  const report = await runProcessCrashExperiment({
    leaseMs: 1_000,
    taskMs: 100,
    timeoutMs: 5_000,
  });

  assert.equal(report.environment.nodeVersion, process.version);
  assert.ok(report.environment.platform.length > 0);
  assert.ok(report.environment.operatingSystemRelease.length > 0);
  assert.ok(report.environment.architecture.length > 0);
  assert.ok(report.environment.availableParallelism > 0);
  assert.equal(report.crashedWorkerSignal, "SIGKILL");
  assert.equal(report.attempts, 2);
  assert.deepEqual(report.eventSequence, [
    "enqueued:queue",
    "claimed:crash-experiment-worker",
    "lease_expired:crash-experiment-worker",
    "claimed:crash-experiment-replacement",
    "completed:crash-experiment-replacement",
  ]);
  assert.deepEqual(report.finalStats, {
    queued: 0, running: 0, succeeded: 1, failed: 0, cancelled: 0, total: 1,
  });
  assert.ok(report.recoveryAfterCrashMs >= 1_000);
  assert.ok(Object.values(report.invariants).every(Boolean));
});

test("process-crash experiment rejects a timeout shorter than its lease", async () => {
  await assert.rejects(
    runProcessCrashExperiment({ leaseMs: 2_000, taskMs: 100, timeoutMs: 2_000 }),
    /timeoutMs must be an integer greater than leaseMs/,
  );
});
