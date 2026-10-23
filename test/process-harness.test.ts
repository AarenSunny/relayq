import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";
import { stopWorkerProcess } from "../tools/process-harness.ts";

test("forced worker shutdown waits for the child to exit", async () => {
  const worker = spawn(process.execPath, [
    "-e",
    "process.on('SIGTERM', () => {}); console.log('ready'); setInterval(() => {}, 1000);",
  ]);
  await once(worker.stdout, "data");

  try {
    await Promise.race([
      stopWorkerProcess(worker, "SIGTERM", 25),
      delay(2_000, undefined, { ref: false }).then(() => {
        throw new Error("worker shutdown timed out");
      }),
    ]);
    assert.equal(worker.signalCode, "SIGKILL");
    assert.notEqual(worker.exitCode ?? worker.signalCode, null);
  } finally {
    if (worker.exitCode === null && worker.signalCode === null) {
      worker.kill("SIGKILL");
      await once(worker, "exit");
    }
  }
});
