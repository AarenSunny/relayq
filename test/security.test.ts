import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { createRelayServer } from "../src/server.ts";
import { JobStore } from "../src/store.ts";

const store = new JobStore(":memory:");
const server = createRelayServer(store, { apiKey: "test-secret" });
let baseUrl = "";

before(async () => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

after(async () => {
  server.closeAllConnections();
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  store.close();
});

test("configured API keys protect mutations", async () => {
  const request = (authorization?: string) => fetch(`${baseUrl}/jobs`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(authorization ? { authorization } : {}),
    },
    body: JSON.stringify({ type: "secured" }),
  });

  const missing = await request();
  assert.equal(missing.status, 401);
  assert.equal(missing.headers.get("www-authenticate"), "Bearer");
  assert.equal((await request("Bearer incorrect")).status, 401);
  assert.equal((await request("Bearer test-secret")).status, 201);
});

test("request bodies require JSON and responses include defensive headers", async () => {
  const invalidType = await fetch(`${baseUrl}/jobs`, {
    method: "POST",
    headers: { authorization: "Bearer test-secret", "content-type": "text/plain" },
    body: "not json",
  });
  assert.equal(invalidType.status, 415);

  const health = await fetch(`${baseUrl}/health`);
  assert.equal(health.headers.get("x-content-type-options"), "nosniff");
  assert.equal(health.headers.get("x-frame-options"), "DENY");
  assert.equal(health.headers.get("referrer-policy"), "no-referrer");
});
