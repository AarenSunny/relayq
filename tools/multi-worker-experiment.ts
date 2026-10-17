import type { ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { createRelayServer } from "../src/server.ts";
import { JobStore } from "../src/store.ts";
import type { JobEvent, QueueStats } from "../src/types.ts";
import {
  closeHttpServer,
  listenOnLoopback,
  startWorkerProcess,
  stopWorkerProcess,
} from "./process-harness.ts";

export interface MultiWorkerOptions {
  jobs: number;
  workers: number;
  taskMs: number;
  timeoutMs: number;
}

export interface MultiWorkerReport {
  model: "separate-http-workers-single-api";
  jobs: number;
  workerProcesses: number;
  activeWorkers: number;
  completionsByWorker: Record<string, number>;
  taskMs: number;
  jobsPerSecond: number;
  elapsedSeconds: number;
  enqueueToCompletionMs: {
    samples: number;
    p50: number;
    p95: number;
    max: number;
  };
  finalStats: QueueStats;
  invariants: {
    allJobsSucceeded: boolean;
    everyWorkerParticipated: boolean;
    eachJobCompletedOnce: boolean;
    everyCompletionMeasured: boolean;
  };
  limitations: string[];
}

export const DEFAULT_MULTI_WORKER_OPTIONS: MultiWorkerOptions = {
  jobs: 100,
  workers: 4,
  taskMs: 25,
  timeoutMs: 30_000,
};

function positiveInteger(name: string, value: number, maximum: number): void {
  if (!Number.isInteger(value) || value < 1 || value > maximum) {
    throw new Error(`${name} must be an integer between 1 and ${maximum}`);
  }
}

function validateOptions(options: MultiWorkerOptions): void {
  positiveInteger("jobs", options.jobs, 500);
  positiveInteger("workers", options.workers, Math.min(options.jobs, 32));
  positiveInteger("taskMs", options.taskMs, 5_000);
  positiveInteger("timeoutMs", options.timeoutMs, 120_000);
}

function readAllEvents(store: JobStore): JobEvent[] {
  const events: JobEvent[] = [];
  let cursor = 0;
  while (true) {
    const batch = store.listEvents(undefined, cursor, 1_000);
    events.push(...batch);
    if (batch.length < 1_000) return events;
    cursor = batch.at(-1)!.id;
  }
}

function summarizeDurations(values: number[]): MultiWorkerReport["enqueueToCompletionMs"] {
  const sorted = values.toSorted((left, right) => left - right);
  const percentile = (value: number) => sorted[Math.ceil((value / 100) * sorted.length) - 1];
  return {
    samples: sorted.length,
    p50: percentile(50),
    p95: percentile(95),
    max: sorted.at(-1)!,
  };
}

export async function runMultiWorkerExperiment(
  options: MultiWorkerOptions,
): Promise<MultiWorkerReport> {
  validateOptions(options);
  const directory = mkdtempSync(join(tmpdir(), "relayq-multi-worker-"));
  const store = new JobStore(join(directory, "queue.db"));
  const server = createRelayServer(store);
  const workers: ChildProcess[] = [];
  const workerErrors: string[][] = [];

  try {
    const baseUrl = await listenOnLoopback(server);
    const readyWorkers: Promise<void>[] = [];
    for (let index = 0; index < options.workers; index += 1) {
      const startedWorker = startWorkerProcess({
        baseUrl,
        workerId: `experiment-worker-${index + 1}`,
      });
      const worker = startedWorker.process;
      workerErrors.push(startedWorker.errors);
      workers.push(worker);
      readyWorkers.push(startedWorker.ready);
    }
    await Promise.race([
      Promise.all(readyWorkers),
      delay(5_000, undefined, { ref: false }).then(() => { throw new Error("worker startup timed out"); }),
    ]);
    for (let index = 0; index < options.jobs; index += 1) {
      store.enqueue({ type: "sleep", payload: options.taskMs });
    }
    const started = performance.now();

    const deadline = started + options.timeoutMs;
    let finalStats = store.stats();
    while (finalStats.succeeded < options.jobs && performance.now() < deadline) {
      const exited = workers.find((worker) => worker.exitCode !== null || worker.signalCode !== null);
      if (exited) {
        throw new Error(`worker exited before the queue drained: ${workerErrors.flat().join("; ")}`);
      }
      await delay(10);
      finalStats = store.stats();
    }
    if (finalStats.succeeded !== options.jobs) {
      throw new Error(`timed out with ${finalStats.succeeded}/${options.jobs} jobs completed`);
    }
    const finished = performance.now();

    const completionsByWorker: Record<string, number> = {};
    const events = readAllEvents(store);
    const enqueuedAt = new Map<string, number>();
    const completionLatencies: number[] = [];
    for (const event of events) {
      if (event.type === "enqueued") enqueuedAt.set(event.jobId, event.createdAt);
      if (event.type === "completed" && event.workerId) {
        completionsByWorker[event.workerId] = (completionsByWorker[event.workerId] ?? 0) + 1;
        const start = enqueuedAt.get(event.jobId);
        if (start !== undefined) completionLatencies.push(event.createdAt - start);
      }
    }
    const activeWorkers = Object.keys(completionsByWorker).length;
    const completionCount = Object.values(completionsByWorker).reduce((sum, count) => sum + count, 0);
    const invariants = {
      allJobsSucceeded: finalStats.succeeded === options.jobs && finalStats.total === options.jobs,
      everyWorkerParticipated: activeWorkers === options.workers,
      eachJobCompletedOnce: completionCount === options.jobs,
      everyCompletionMeasured: completionLatencies.length === options.jobs,
    };
    if (Object.values(invariants).some((value) => !value)) {
      throw new Error(`multi-worker invariant failed: ${JSON.stringify(invariants)}`);
    }

    const elapsedSeconds = (finished - started) / 1_000;
    return {
      model: "separate-http-workers-single-api",
      jobs: options.jobs,
      workerProcesses: options.workers,
      activeWorkers,
      completionsByWorker,
      taskMs: options.taskMs,
      jobsPerSecond: Number((options.jobs / elapsedSeconds).toFixed(1)),
      elapsedSeconds: Number(elapsedSeconds.toFixed(3)),
      enqueueToCompletionMs: summarizeDurations(completionLatencies),
      finalStats,
      invariants,
      limitations: [
        "Workers are separate Node.js processes, but one API process owns the SQLite database.",
        "Traffic stays on the local loopback interface and does not model network faults.",
        "The sleep handler measures queue coordination rather than CPU-bound execution.",
        "Latency uses millisecond wall-clock event timestamps from one host.",
        "Throughput depends on the host and should only be compared under matching conditions.",
      ],
    };
  } finally {
    await Promise.all(workers.map((worker) => stopWorkerProcess(worker)));
    if (server.listening) await closeHttpServer(server);
    store.close();
    rmSync(directory, { recursive: true, force: true });
  }
}

function integerArg(name: string, fallback: number): number {
  const index = process.argv.indexOf(name);
  const value = index === -1 ? fallback : Number(process.argv[index + 1]);
  if (!Number.isInteger(value)) throw new Error(`${name} must be an integer`);
  return value;
}

async function main(): Promise<void> {
  const report = await runMultiWorkerExperiment({
    jobs: integerArg("--jobs", DEFAULT_MULTI_WORKER_OPTIONS.jobs),
    workers: integerArg("--workers", DEFAULT_MULTI_WORKER_OPTIONS.workers),
    taskMs: integerArg("--task-ms", DEFAULT_MULTI_WORKER_OPTIONS.taskMs),
    timeoutMs: integerArg("--timeout-ms", DEFAULT_MULTI_WORKER_OPTIONS.timeoutMs),
  });
  if (process.argv.includes("--json")) {
    console.log(JSON.stringify(report, null, 2));
    return;
  }
  console.log("RelayQ multi-process worker experiment");
  console.table({
    jobs: report.jobs,
    workerProcesses: report.workerProcesses,
    activeWorkers: report.activeWorkers,
    taskMs: report.taskMs,
    jobsPerSecond: report.jobsPerSecond,
    elapsedSeconds: report.elapsedSeconds,
    completionP50Ms: report.enqueueToCompletionMs.p50,
    completionP95Ms: report.enqueueToCompletionMs.p95,
    completionMaxMs: report.enqueueToCompletionMs.max,
  });
  console.log("Completions by worker:");
  console.table(report.completionsByWorker);
  console.log("Limitations:");
  for (const limitation of report.limitations) console.log(`- ${limitation}`);
}

const isEntryPoint = process.argv[1]
  ? resolve(process.argv[1]) === fileURLToPath(import.meta.url)
  : false;
if (isEntryPoint) await main();
