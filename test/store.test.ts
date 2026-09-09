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
  const events = store.listEvents(high.id);
  assert.deepEqual(events.map((event) => event.type), ["enqueued", "claimed", "completed"]);
  assert.deepEqual(events.map((event) => event.toStatus), ["queued", "running", "succeeded"]);
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
  assert.deepEqual(store.listEvents(job.id).map((event) => event.type), [
    "enqueued", "claimed", "lease_expired", "claimed", "lease_expired",
  ]);
  store.close();
});

test("failure retries until maxAttempts is exhausted", () => {
  const store = new JobStore(":memory:");
  const job = store.enqueue({ type: "bad", maxAttempts: 2 });
  store.claim("worker-a");
  assert.equal(store.fail(job.id, "worker-a", "first failure", 0).status, "queued");
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

test("a worker can renew its lease but another worker cannot", () => {
  let now = 20_000;
  const store = new JobStore(":memory:", () => now);
  const job = store.enqueue({ type: "sleep" });
  store.claim("worker-a", 2_000);

  now += 1_000;
  const renewed = store.renewLease(job.id, "worker-a", 4_000);
  assert.equal(renewed.leaseExpiresAt, 25_000);
  assert.throws(() => store.renewLease(job.id, "worker-b"), /owned by another worker/);

  now = 24_500;
  assert.equal(store.claim("worker-b"), null);
  store.complete(job.id, "worker-a");
  store.close();
});

test("an operator can requeue a dead-lettered job with a fresh attempt budget", () => {
  let now = 30_000;
  const store = new JobStore(":memory:", () => now);
  const job = store.enqueue({ type: "flaky", maxAttempts: 1 });
  store.claim("worker-a");
  const failed = store.fail(job.id, "worker-a", "dependency unavailable");
  assert.equal(failed.status, "failed");
  assert.equal(store.list("failed")[0].id, job.id);

  const requeued = store.requeueFailed(job.id, 500);
  assert.equal(requeued.status, "queued");
  assert.equal(requeued.attempts, 0);
  assert.equal(requeued.error, null);
  now += 499;
  assert.equal(store.claim("worker-b"), null);
  now += 1;
  assert.equal(store.claim("worker-b")?.id, job.id);
  store.close();
});

test("event cursors return only newer state transitions", () => {
  const store = new JobStore(":memory:");
  const job = store.enqueue({ type: "sum" });
  const firstEvent = store.listEvents(job.id)[0];
  store.claim("worker-a");
  store.complete(job.id, "worker-a", 0);
  const newer = store.listEvents(job.id, firstEvent.id);
  assert.deepEqual(newer.map((event) => event.type), ["claimed", "completed"]);
  assert.ok(newer.every((event) => event.id > firstEvent.id));
  store.close();
});

test("automatic retries use exponential backoff and respect the cap", () => {
  let now = 50_000;
  const store = new JobStore(":memory:", () => now, () => 0.5);
  const job = store.enqueue({
    type: "flaky",
    maxAttempts: 4,
    backoffBaseMs: 100,
    backoffMaxMs: 250,
    backoffJitter: 0,
  });

  store.claim("worker-a");
  assert.equal(store.fail(job.id, "worker-a", "attempt one").availableAt, 50_100);
  now = 50_100;
  store.claim("worker-a");
  assert.equal(store.fail(job.id, "worker-a", "attempt two").availableAt, 50_300);
  now = 50_300;
  store.claim("worker-a");
  const thirdRetry = store.fail(job.id, "worker-a", "attempt three");
  assert.equal(thirdRetry.availableAt, 50_550);
  assert.equal(store.listEvents(job.id).at(-1)?.detail.retryDelayMs, 250);
  store.close();
});

test("retry jitter is deterministic when a random source is supplied", () => {
  let now = 60_000;
  const low = new JobStore(":memory:", () => now, () => 0);
  const lowJob = low.enqueue({ type: "flaky", backoffBaseMs: 1_000, backoffJitter: 0.25 });
  low.claim("worker");
  assert.equal(low.fail(lowJob.id, "worker", "low jitter").availableAt, 60_750);
  low.close();

  const high = new JobStore(":memory:", () => now, () => 1);
  const highJob = high.enqueue({ type: "flaky", backoffBaseMs: 1_000, backoffJitter: 0.25 });
  high.claim("worker");
  assert.equal(high.fail(highJob.id, "worker", "high jitter").availableAt, 61_250);
  high.close();
});
