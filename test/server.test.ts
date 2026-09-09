import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import type { AddressInfo } from "node:net";
import { createRelayServer } from "../src/server.ts";
import { JobStore } from "../src/store.ts";

const store = new JobStore(":memory:");
const server = createRelayServer(store);
let baseUrl = "";

before(async () => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${address.port}`;
});

after(async () => {
  server.closeAllConnections();
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  store.close();
});

test("job lifecycle is available over HTTP", async () => {
  const createdResponse = await fetch(`${baseUrl}/jobs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ type: "sum", payload: [10, 20], priority: 4 }),
  });
  assert.equal(createdResponse.status, 201);
  const created = await createdResponse.json() as { id: string; status: string };
  assert.equal(created.status, "queued");

  const claimResponse = await fetch(`${baseUrl}/workers/test-worker/claim`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ leaseMs: 5_000 }),
  });
  const claimed = await claimResponse.json() as { id: string; attempts: number };
  assert.equal(claimed.id, created.id);
  assert.equal(claimed.attempts, 1);

  const completeResponse = await fetch(`${baseUrl}/jobs/${created.id}/complete`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workerId: "test-worker", result: 30 }),
  });
  assert.equal(completeResponse.status, 200);

  const fetched = await fetch(`${baseUrl}/jobs/${created.id}`).then((response) => response.json()) as
    { status: string; result: number };
  assert.equal(fetched.status, "succeeded");
  assert.equal(fetched.result, 30);

  const stats = await fetch(`${baseUrl}/stats`).then((response) => response.json()) as
    { succeeded: number; total: number };
  assert.equal(stats.succeeded, 1);
  assert.equal(stats.total, 1);
});

test("invalid jobs return a useful client error", async () => {
  const response = await fetch(`${baseUrl}/jobs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ payload: "missing type" }),
  });
  assert.equal(response.status, 409);
  assert.match((await response.json() as { error: string }).error, /type is required/);
});

test("workers renew leases and operators recover dead-lettered jobs", async () => {
  const created = await fetch(`${baseUrl}/jobs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ type: "flaky", maxAttempts: 1 }),
  }).then((response) => response.json()) as { id: string };

  await fetch(`${baseUrl}/workers/recovery-worker/claim`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ leaseMs: 5_000 }),
  });
  const renewed = await fetch(`${baseUrl}/jobs/${created.id}/heartbeat`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workerId: "recovery-worker", leaseMs: 10_000 }),
  }).then((response) => response.json()) as { leaseExpiresAt: number };
  assert.ok(renewed.leaseExpiresAt > Date.now());

  await fetch(`${baseUrl}/jobs/${created.id}/fail`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workerId: "recovery-worker", error: "planned test failure" }),
  });
  const deadLetter = await fetch(`${baseUrl}/dead-letter`).then((response) => response.json()) as
    { jobs: Array<{ id: string }> };
  assert.ok(deadLetter.jobs.some((job) => job.id === created.id));

  const requeued = await fetch(`${baseUrl}/jobs/${created.id}/requeue`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ delayMs: 100 }),
  }).then((response) => response.json()) as { status: string; attempts: number };
  assert.equal(requeued.status, "queued");
  assert.equal(requeued.attempts, 0);
});

test("dashboard and durable event history are exposed", async () => {
  const dashboard = await fetch(`${baseUrl}/`);
  assert.equal(dashboard.status, 200);
  assert.match(await dashboard.text(), /RelayQ Control Room/);

  const response = await fetch(`${baseUrl}/events?limit=100`);
  assert.equal(response.status, 200);
  const body = await response.json() as { events: Array<{ id: number; type: string; jobId: string }> };
  assert.ok(body.events.length >= 6);
  assert.ok(body.events.some((event) => event.type === "completed"));
  assert.ok(body.events.every((event, index) => index === 0 || body.events[index - 1].id < event.id));
});

test("HTTP workers inherit the producer's retry policy", async () => {
  const created = await fetch(`${baseUrl}/jobs`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      type: "network-call",
      priority: 99,
      maxAttempts: 3,
      backoffBaseMs: 500,
      backoffMaxMs: 500,
      backoffJitter: 0,
    }),
  }).then((response) => response.json()) as { id: string; backoffBaseMs: number };
  assert.equal(created.backoffBaseMs, 500);

  const claimed = await fetch(`${baseUrl}/workers/retry-worker/claim`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ leaseMs: 5_000 }),
  }).then((response) => response.json()) as { id: string };
  assert.equal(claimed.id, created.id);

  const failedAt = Date.now();
  const retried = await fetch(`${baseUrl}/jobs/${created.id}/fail`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ workerId: "retry-worker", error: "upstream unavailable" }),
  }).then((response) => response.json()) as { status: string; availableAt: number };
  assert.equal(retried.status, "queued");
  assert.ok(retried.availableAt >= failedAt + 500);
});
