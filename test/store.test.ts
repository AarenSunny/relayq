import assert from "node:assert/strict";
import { test } from "node:test";
import { JobStore } from "../src/store.ts";

test("claims by priority and completes with a result", () => {
  let now = 1_000;
  const store = new JobStore(":memory:", () => now);
  const low = store.enqueue({ type: "sum", payload: [1, 2], priority: 1 });
  const high = store.enqueue({ type: "sum", payload: [3, 4], priority: 10 });

  const claimed = store.claim("worker-a", 5_000);
  assert.equal(claimed?.id, high.id);
  assert.equal(claimed?.attempts, 1);
  const completed = store.complete(high.id, "worker-a", 7);
  assert.equal(completed.status, "succeeded");
  assert.equal(completed.result, 7);
  assert.equal(store.claim("worker-a")?.id, low.id);
  store.close();
});

test("an expired lease makes work available to another worker", () => {
  let now = 5_000;
  const store = new JobStore(":memory:", () => now);
  const job = store.enqueue({ type: "uppercase", payload: "hello", maxAttempts: 2 });
  assert.equal(store.claim("crashed-worker", 1_000)?.id, job.id);

  now += 1_001;
  const reclaimed = store.claim("healthy-worker", 1_000);
  assert.equal(reclaimed?.id, job.id);
  assert.equal(reclaimed?.attempts, 2);
  assert.equal(reclaimed?.error, "worker lease expired");

  now += 1_001;
  assert.equal(store.claim("third-worker"), null);
  assert.equal(store.get(job.id)?.status, "failed");
  store.close();
});

test("failure retries until maxAttempts is exhausted", () => {
  const store = new JobStore(":memory:");
  const job = store.enqueue({ type: "bad", maxAttempts: 2 });
  store.claim("worker-a");
  assert.equal(store.fail(job.id, "worker-a", "first failure").status, "queued");
  store.claim("worker-b");
  const failed = store.fail(job.id, "worker-b", "second failure");
  assert.equal(failed.status, "failed");
  assert.equal(failed.attempts, 2);
  assert.equal(store.stats().failed, 1);
  store.close();
});

test("only the lease owner can complete a job", () => {
  const store = new JobStore(":memory:");
  const job = store.enqueue({ type: "sum" });
  store.claim("worker-a");
  assert.throws(() => store.complete(job.id, "worker-b"), /not running for this worker/);
  assert.equal(store.get(job.id)?.status, "running");
  store.close();
});
