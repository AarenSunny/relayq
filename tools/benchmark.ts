import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { JobStore } from "../src/store.ts";

function integerArg(name: string, fallback: number): number {
  const index = process.argv.indexOf(name);
  const value = index === -1 ? fallback : Number(process.argv[index + 1]);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

const jobCount = integerArg("--jobs", 1_000);
const workerCount = integerArg("--workers", 8);
const jsonOutput = process.argv.includes("--json");
const directory = mkdtempSync(join(tmpdir(), "relayq-benchmark-"));
const store = new JobStore(join(directory, "queue.db"));

try {
  const started = performance.now();
  for (let index = 0; index < jobCount; index += 1) {
    store.enqueue({ type: "benchmark", payload: { index }, priority: index % 5 });
  }
  const enqueued = performance.now();

  let completed = 0;
  while (completed < jobCount) {
    const workerId = `benchmark-worker-${completed % workerCount}`;
    const job = store.claim(workerId);
    if (!job) throw new Error(`queue became empty after ${completed} completions`);
    store.complete(job.id, workerId, { accepted: true });
    completed += 1;
  }

  const finished = performance.now();
  const enqueueSeconds = (enqueued - started) / 1_000;
  const processSeconds = (finished - enqueued) / 1_000;
  const totalSeconds = (finished - started) / 1_000;
  const sampledEvents = store.listEvents(undefined, 0, 1_000).length;
  const report = {
    jobs: jobCount,
    logicalWorkers: workerCount,
    enqueueJobsPerSecond: Math.round(jobCount / enqueueSeconds),
    processJobsPerSecond: Math.round(jobCount / processSeconds),
    endToEndJobsPerSecond: Math.round(jobCount / totalSeconds),
    elapsedSeconds: Number(totalSeconds.toFixed(3)),
    persistedEvents: sampledEvents === 1_000 ? "1000+" : sampledEvents,
    finalStats: store.stats(),
  };

  if (jsonOutput) console.log(JSON.stringify(report, null, 2));
  else {
    console.log("RelayQ local durability benchmark");
    console.table(report);
  }
} finally {
  store.close();
  rmSync(directory, { recursive: true, force: true });
}
