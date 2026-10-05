import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { JobStore } from "../src/store.ts";
import type { JobEventType, QueueStats } from "../src/types.ts";

export interface ReliabilityOptions {
  jobs: number;
  logicalWorkers: number;
  crashEvery: number;
  leaseMs: number;
}

export interface ReliabilityReport {
  model: "single-process controlled-clock";
  jobs: number;
  logicalWorkers: number;
  simulatedCrashes: number;
  recoveredLeases: number;
  retryAttempts: number;
  staleCompletionRejections: number;
  duplicateCompletionRejections: number;
  reclaimAfterLeaseExpiryMs: number;
  endToEndJobsPerSecond: number;
  elapsedSeconds: number;
  eventCounts: Partial<Record<JobEventType, number>>;
  finalStats: QueueStats;
  invariants: {
    allJobsSucceeded: boolean;
    everyCrashRecovered: boolean;
    noStaleCompletionAccepted: boolean;
    noDuplicateCompletionAccepted: boolean;
  };
  limitations: string[];
}

export const DEFAULT_RELIABILITY_OPTIONS: ReliabilityOptions = {
  jobs: 100,
  logicalWorkers: 8,
  crashEvery: 10,
  leaseMs: 1_000,
};

function positiveInteger(name: string, value: number, maximum?: number): void {
  if (!Number.isInteger(value) || value < 1 || (maximum !== undefined && value > maximum)) {
    const suffix = maximum === undefined ? "" : ` no greater than ${maximum}`;
    throw new Error(`${name} must be a positive integer${suffix}`);
  }
}

function validateOptions(options: ReliabilityOptions): void {
  positiveInteger("jobs", options.jobs, 1_000);
  positiveInteger("logicalWorkers", options.logicalWorkers, options.jobs);
  positiveInteger("crashEvery", options.crashEvery, options.jobs);
  if (!Number.isInteger(options.leaseMs) || options.leaseMs < 1_000) {
    throw new Error("leaseMs must be an integer of at least 1000");
  }
}

export function runReliabilityExperiment(options: ReliabilityOptions): ReliabilityReport {
  validateOptions(options);
  const directory = mkdtempSync(join(tmpdir(), "relayq-reliability-"));
  let now = 1_000_000;
  const store = new JobStore(join(directory, "queue.db"), () => now, () => 0.5);
  const crashedOwners = new Map<string, string>();
  let staleCompletionRejections = 0;
  let duplicateCompletionRejections = 0;

  try {
    const started = performance.now();
    for (let index = 0; index < options.jobs; index += 1) {
      store.enqueue({
        type: "reliability-probe",
        payload: { index },
        maxAttempts: 2,
        backoffJitter: 0,
      });
    }

    for (let index = 0; index < options.jobs; index += 1) {
      const workerId = `worker-${index % options.logicalWorkers}`;
      const job = store.claim(workerId, options.leaseMs);
      if (!job) throw new Error(`queue became empty after ${index} initial claims`);
      if ((index + 1) % options.crashEvery === 0) {
        crashedOwners.set(job.id, workerId);
      } else {
        store.complete(job.id, workerId, { phase: "initial" });
      }
    }

    now += options.leaseMs + 1;
    for (let index = 0; index < crashedOwners.size; index += 1) {
      const replacementId = `replacement-${index % options.logicalWorkers}`;
      const recovered = store.claim(replacementId, options.leaseMs);
      if (!recovered) throw new Error(`missing recovered job after ${index} replacement claims`);
      const staleOwner = crashedOwners.get(recovered.id);
      if (!staleOwner) throw new Error(`unexpected recovery of job ${recovered.id}`);

      try {
        store.complete(recovered.id, staleOwner, { shouldNotBeAccepted: true });
      } catch (error) {
        if (!(error instanceof Error) || !/not running for this worker/.test(error.message)) throw error;
        staleCompletionRejections += 1;
      }

      store.complete(recovered.id, replacementId, { phase: "recovered" });
      try {
        store.complete(recovered.id, replacementId, { shouldNotBeAccepted: true });
      } catch (error) {
        if (!(error instanceof Error) || !/not running for this worker/.test(error.message)) throw error;
        duplicateCompletionRejections += 1;
      }
    }

    const finished = performance.now();
    const jobs = store.list(undefined, options.jobs);
    const events = jobs.flatMap((job) => store.listEvents(job.id, 0, 10));
    const eventCounts: Partial<Record<JobEventType, number>> = {};
    for (const event of events) eventCounts[event.type] = (eventCounts[event.type] ?? 0) + 1;
    const retryAttempts = jobs.reduce((total, job) => total + job.attempts, 0) - options.jobs;
    const recoveredLeases = eventCounts.lease_expired ?? 0;
    const finalStats = store.stats();
    const elapsedSeconds = (finished - started) / 1_000;
    const simulatedCrashes = crashedOwners.size;
    const invariants = {
      allJobsSucceeded: finalStats.succeeded === options.jobs && finalStats.total === options.jobs,
      everyCrashRecovered: recoveredLeases === simulatedCrashes && retryAttempts === simulatedCrashes,
      noStaleCompletionAccepted: staleCompletionRejections === simulatedCrashes,
      noDuplicateCompletionAccepted: duplicateCompletionRejections === simulatedCrashes,
    };
    if (Object.values(invariants).some((value) => !value)) {
      throw new Error(`reliability invariant failed: ${JSON.stringify(invariants)}`);
    }

    return {
      model: "single-process controlled-clock",
      jobs: options.jobs,
      logicalWorkers: options.logicalWorkers,
      simulatedCrashes,
      recoveredLeases,
      retryAttempts,
      staleCompletionRejections,
      duplicateCompletionRejections,
      reclaimAfterLeaseExpiryMs: 1,
      endToEndJobsPerSecond: Math.round(options.jobs / elapsedSeconds),
      elapsedSeconds: Number(elapsedSeconds.toFixed(3)),
      eventCounts,
      finalStats,
      invariants,
      limitations: [
        "Logical worker IDs run sequentially in one Node.js process.",
        "A controlled clock advances one millisecond past a shared lease deadline.",
        "This validates the SQLite state machine, not multi-host availability or network partitions.",
      ],
    };
  } finally {
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

function main(): void {
  const report = runReliabilityExperiment({
    jobs: integerArg("--jobs", DEFAULT_RELIABILITY_OPTIONS.jobs),
    logicalWorkers: integerArg("--workers", DEFAULT_RELIABILITY_OPTIONS.logicalWorkers),
    crashEvery: integerArg("--crash-every", DEFAULT_RELIABILITY_OPTIONS.crashEvery),
    leaseMs: integerArg("--lease-ms", DEFAULT_RELIABILITY_OPTIONS.leaseMs),
  });
  if (process.argv.includes("--json")) console.log(JSON.stringify(report, null, 2));
  else {
    console.log("RelayQ controlled reliability experiment");
    console.table({
      jobs: report.jobs,
      logicalWorkers: report.logicalWorkers,
      simulatedCrashes: report.simulatedCrashes,
      recoveredLeases: report.recoveredLeases,
      retryAttempts: report.retryAttempts,
      staleCompletionRejections: report.staleCompletionRejections,
      duplicateCompletionRejections: report.duplicateCompletionRejections,
      reclaimAfterLeaseExpiryMs: report.reclaimAfterLeaseExpiryMs,
      endToEndJobsPerSecond: report.endToEndJobsPerSecond,
      elapsedSeconds: report.elapsedSeconds,
    });
    console.log("Limitations:");
    for (const limitation of report.limitations) console.log(`- ${limitation}`);
  }
}

const isEntryPoint = process.argv[1]
  ? resolve(process.argv[1]) === fileURLToPath(import.meta.url)
  : false;
if (isEntryPoint) main();
