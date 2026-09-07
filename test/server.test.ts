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
