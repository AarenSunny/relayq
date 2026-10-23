import { spawn, type ChildProcess } from "node:child_process";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

export interface WorkerProcessOptions {
  baseUrl: string;
  workerId: string;
  pollMs?: number;
  leaseMs?: number;
}

export interface WorkerProcessHandle {
  process: ChildProcess;
  ready: Promise<void>;
  errors: string[];
}

const repositoryRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const workerEntry = fileURLToPath(new URL("../src/worker.ts", import.meta.url));

export async function listenOnLoopback(server: Server): Promise<string> {
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address() as AddressInfo;
  return `http://127.0.0.1:${address.port}`;
}

export function startWorkerProcess(options: WorkerProcessOptions): WorkerProcessHandle {
  const worker = spawn(process.execPath, [workerEntry], {
    cwd: repositoryRoot,
    env: {
      ...process.env,
      RELAYQ_URL: options.baseUrl,
      WORKER_ID: options.workerId,
      POLL_MS: String(options.pollMs ?? 10),
      LEASE_MS: String(options.leaseMs ?? 5_000),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const errors: string[] = [];
  worker.stdout?.setEncoding("utf8");
  worker.stderr?.setEncoding("utf8");
  worker.stderr?.on("data", (chunk: string) => errors.push(chunk.trim()));
  const ready = new Promise<void>((resolveReady, rejectReady) => {
    worker.once("error", rejectReady);
    worker.stdout?.once("data", () => resolveReady());
  });
  return { process: worker, ready, errors };
}

export async function stopWorkerProcess(
  worker: ChildProcess,
  signal: NodeJS.Signals = "SIGTERM",
  graceMs = 2_000,
): Promise<void> {
  if (worker.exitCode !== null || worker.signalCode !== null) return;
  const exited = new Promise<void>((resolveExit) => worker.once("exit", () => resolveExit()));
  worker.kill(signal);
  await Promise.race([
    exited,
    delay(graceMs, undefined, { ref: false }).then(() => {
      if (worker.exitCode === null && worker.signalCode === null) worker.kill("SIGKILL");
    }),
  ]);
  if (worker.exitCode === null && worker.signalCode === null) await exited;
}

export async function closeHttpServer(server: Server): Promise<void> {
  server.closeAllConnections();
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => error ? reject(error) : resolveClose());
  });
}
