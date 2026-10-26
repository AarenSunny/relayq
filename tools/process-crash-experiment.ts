import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { createRelayServer } from "../src/server.ts";
import { JobStore } from "../src/store.ts";
import type { JobEvent, QueueStats } from "../src/types.ts";
import {
  captureExperimentEnvironment,
  type ExperimentEnvironment,
} from "./experiment-environment.ts";
import {
  closeHttpServer,
  listenOnLoopback,
  startWorkerProcess,
  stopWorkerProcess,
  type WorkerProcessHandle,
} from "./process-harness.ts";

export interface ProcessCrashOptions {
  leaseMs: number;
  taskMs: number;
  timeoutMs: number;
}

export interface ProcessCrashReport {
  model: "killed-worker-process-single-api";
  environment: ExperimentEnvironment;
  leaseMs: number;
  taskMs: number;
  recoveryAfterCrashMs: number;
  crashedWorkerSignal: NodeJS.Signals | null;
  attempts: number;
  eventSequence: string[];
  finalStats: QueueStats;
  invariants: {
    crashedProcessTerminated: boolean;
    expiredLeaseRecovered: boolean;
    replacementCompleted: boolean;
    exactlyTwoAttempts: boolean;
    exactlyOneCompletion: boolean;
    queueSettled: boolean;
  };
  limitations: string[];
}

export const DEFAULT_PROCESS_CRASH_OPTIONS: ProcessCrashOptions = {
  leaseMs: 1_000,
  taskMs: 250,
  timeoutMs: 10_000,
};

function validateOptions(options: ProcessCrashOptions): void {
  if (!Number.isInteger(options.leaseMs) || options.leaseMs < 1_000 || options.leaseMs > 30_000) {
    throw new Error("leaseMs must be an integer between 1000 and 30000");
  }
  if (!Number.isInteger(options.taskMs) || options.taskMs < 50 || options.taskMs > 5_000) {
    throw new Error("taskMs must be an integer between 50 and 5000");
  }
  if (!Number.isInteger(options.timeoutMs) || options.timeoutMs <= options.leaseMs ||
      options.timeoutMs > 120_000) {
    throw new Error("timeoutMs must be an integer greater than leaseMs and no greater than 120000");
  }
}

async function waitUntil(predicate: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (!predicate() && performance.now() < deadline) await delay(5);
  if (!predicate()) throw new Error(message);
}

async function waitForWorker(handle: WorkerProcessHandle, timeoutMs: number): Promise<void> {
  await Promise.race([
    handle.ready,
    delay(timeoutMs, undefined, { ref: false }).then(() => {
      throw new Error(`worker startup timed out: ${handle.errors.join("; ")}`);
    }),
  ]);
}

function eventSequence(events: JobEvent[]): string[] {
  return events.map((event) => `${event.type}:${event.workerId ?? "queue"}`);
}

export async function runProcessCrashExperiment(
  options: ProcessCrashOptions,
): Promise<ProcessCrashReport> {
  validateOptions(options);
  const environment = captureExperimentEnvironment();
  const directory = mkdtempSync(join(tmpdir(), "relayq-process-crash-"));
  const store = new JobStore(join(directory, "queue.db"));
  const server = createRelayServer(store);
  const handles: WorkerProcessHandle[] = [];

  try {
    const baseUrl = await listenOnLoopback(server);
    const crashedWorkerId = "crash-experiment-worker";
    const replacementWorkerId = "crash-experiment-replacement";
    const crashed = startWorkerProcess({
      baseUrl,
      workerId: crashedWorkerId,
      pollMs: 10,
      leaseMs: options.leaseMs,
    });
    handles.push(crashed);
    await waitForWorker(crashed, options.timeoutMs);

    const job = store.enqueue({ type: "sleep", payload: options.taskMs, maxAttempts: 2 });
    await waitUntil(
      () => store.get(job.id)?.workerId === crashedWorkerId,
      options.timeoutMs,
      "crash worker did not claim the probe job",
    );

    const crashedAt = performance.now();
    await stopWorkerProcess(crashed.process, "SIGKILL");
    const replacement = startWorkerProcess({
      baseUrl,
      workerId: replacementWorkerId,
      pollMs: 10,
      leaseMs: options.leaseMs,
    });
    handles.push(replacement);
    await waitForWorker(replacement, options.timeoutMs);
    await waitUntil(
      () => store.get(job.id)?.status === "succeeded",
      options.timeoutMs,
      "replacement worker did not recover and complete the probe job",
    );
    const recoveredAt = performance.now();

    const completedJob = store.get(job.id)!;
    const events = store.listEvents(job.id, 0, 100);
    const completionEvents = events.filter((event) => event.type === "completed");
    const expiredEvents = events.filter((event) => event.type === "lease_expired");
    const finalStats = store.stats();
    const invariants = {
      crashedProcessTerminated: crashed.process.signalCode === "SIGKILL",
      expiredLeaseRecovered: expiredEvents.length === 1 && expiredEvents[0].workerId === crashedWorkerId,
      replacementCompleted: completedJob.status === "succeeded" &&
        completionEvents[0]?.workerId === replacementWorkerId,
      exactlyTwoAttempts: completedJob.attempts === 2,
      exactlyOneCompletion: completionEvents.length === 1,
      queueSettled: finalStats.succeeded === 1 && finalStats.total === 1 &&
        finalStats.queued === 0 && finalStats.running === 0,
    };
    if (Object.values(invariants).some((value) => !value)) {
      throw new Error(`process-crash invariant failed: ${JSON.stringify(invariants)}`);
    }

    return {
      model: "killed-worker-process-single-api",
      environment,
      leaseMs: options.leaseMs,
      taskMs: options.taskMs,
      recoveryAfterCrashMs: Math.round(recoveredAt - crashedAt),
      crashedWorkerSignal: crashed.process.signalCode,
      attempts: completedJob.attempts,
      eventSequence: eventSequence(events),
      finalStats,
      invariants,
      limitations: [
        "The worker is killed on one host while the API and SQLite process remain healthy.",
        "Recovery is triggered by replacement polling after a time-based lease expires.",
        "Loopback HTTP does not model network partitions or multi-host availability.",
        "The side-effect-free sleep task does not prove exactly-once external effects.",
        "Measured recovery includes the configured lease and task duration and varies by host scheduling.",
      ],
    };
  } finally {
    await Promise.all(handles.map((handle) => stopWorkerProcess(handle.process)));
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
  const report = await runProcessCrashExperiment({
    leaseMs: integerArg("--lease-ms", DEFAULT_PROCESS_CRASH_OPTIONS.leaseMs),
    taskMs: integerArg("--task-ms", DEFAULT_PROCESS_CRASH_OPTIONS.taskMs),
    timeoutMs: integerArg("--timeout-ms", DEFAULT_PROCESS_CRASH_OPTIONS.timeoutMs),
  });
  if (process.argv.includes("--json")) {
    console.log(JSON.stringify(report, null, 2));
    return;
  }
  console.log("RelayQ worker-process crash experiment");
  console.table({
    leaseMs: report.leaseMs,
    taskMs: report.taskMs,
    recoveryAfterCrashMs: report.recoveryAfterCrashMs,
    crashedWorkerSignal: report.crashedWorkerSignal,
    attempts: report.attempts,
  });
  console.log(`Event sequence: ${report.eventSequence.join(" -> ")}`);
  console.log("Limitations:");
  for (const limitation of report.limitations) console.log(`- ${limitation}`);
}

const isEntryPoint = process.argv[1]
  ? resolve(process.argv[1]) === fileURLToPath(import.meta.url)
  : false;
if (isEntryPoint) await main();
