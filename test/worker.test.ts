import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";

async function waitUntil(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) await delay(10);
  if (!predicate()) throw new Error("timed out waiting for worker requests");
}

async function stopWorker(worker: ChildProcessWithoutNullStreams): Promise<void> {
  if (worker.exitCode !== null || worker.signalCode !== null) return;
  worker.kill("SIGTERM");
  await Promise.race([
    once(worker, "exit"),
    delay(2_000).then(() => { throw new Error("worker did not stop"); }),
  ]);
}

test("worker does not report completion when the control plane rejects it", async () => {
  let claims = 0;
  let completions = 0;
  let failures = 0;
  const server = createServer((request, response) => {
    if (request.url?.includes("/claim")) {
      claims += 1;
      if (claims === 1) {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ id: "job-1", type: "uppercase", payload: "hello" }));
        return;
      }
      response.writeHead(204).end();
      return;
    }
    if (request.url === "/jobs/job-1/complete") {
      completions += 1;
    } else if (request.url === "/jobs/job-1/fail") {
      failures += 1;
    }
    response.writeHead(409, { "content-type": "application/json" });
    response.end(JSON.stringify({ error: "lease lost" }));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as AddressInfo;
  const worker = spawn(process.execPath, ["src/worker.ts"], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      RELAYQ_URL: `http://127.0.0.1:${address.port}`,
      WORKER_ID: "rejection-test-worker",
      POLL_MS: "10",
    },
  });
  let stdout = "";
  let stderr = "";
  worker.stdout.setEncoding("utf8").on("data", (chunk: string) => { stdout += chunk; });
  worker.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });

  try {
    await waitUntil(() => completions === 1 && failures === 1, 5_000);
  } finally {
    try {
      await stopWorker(worker);
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => error ? reject(error) : resolve()));
    }
  }

  assert.equal(completions, 1);
  assert.equal(failures, 1);
  assert.doesNotMatch(stdout, /completed job-1/);
  assert.match(stderr, /failure acknowledgement for job-1 returned 409: lease lost/);
});

test("worker renews the shortest supported lease before it expires", async () => {
  let claims = 0;
  let heartbeats = 0;
  let completions = 0;
  let failures = 0;
  const server = createServer((request, response) => {
    if (request.url?.includes("/claim")) {
      claims += 1;
      if (claims === 1) {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify({ id: "job-short-lease", type: "sleep", payload: 1_200 }));
        return;
      }
      response.writeHead(204).end();
      return;
    }
    if (request.url === "/jobs/job-short-lease/heartbeat") {
      heartbeats += 1;
      response.writeHead(200, { "content-type": "application/json" });
      response.end("{}");
      return;
    }
    if (request.url === "/jobs/job-short-lease/complete") {
      completions += 1;
      response.writeHead(200, { "content-type": "application/json" });
      response.end("{}");
      return;
    }
    if (request.url === "/jobs/job-short-lease/fail") failures += 1;
    response.writeHead(500).end();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as AddressInfo;
  const worker = spawn(process.execPath, ["src/worker.ts"], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      RELAYQ_URL: `http://127.0.0.1:${address.port}`,
      WORKER_ID: "short-lease-worker",
      POLL_MS: "10",
      LEASE_MS: "1000",
    },
  });

  try {
    await waitUntil(() => completions === 1, 5_000);
  } finally {
    try {
      await stopWorker(worker);
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => error ? reject(error) : resolve()));
    }
  }

  assert.ok(heartbeats >= 2, `expected at least two early renewals, received ${heartbeats}`);
  assert.equal(completions, 1);
  assert.equal(failures, 0);
});
