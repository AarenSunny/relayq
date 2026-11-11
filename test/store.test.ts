import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { JobStore } from "../src/store.ts";

type TransactionRunner = <T>(operation: () => T) => T;

test("readiness reflects SQLite availability", () => {
  const store = new JobStore(":memory:");
  assert.equal(store.isReady(), true);
  store.close();
  assert.equal(store.isReady(), false);
});

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

test("corrupt persisted attempt budgets fail before a queued job is claimed", () => {
  const directory = mkdtempSync(join(tmpdir(), "relayq-attempt-corruption-"));
  const filename = join(directory, "queue.db");
  try {
    const writer = new JobStore(filename);
    const job = writer.enqueue({ type: "corrupt-attempt-budget", maxAttempts: 2 });
    writer.close();

    const sabotage = new DatabaseSync(filename);
    sabotage.prepare("UPDATE jobs SET attempts = max_attempts WHERE id = ?").run(job.id);
    sabotage.close();

    const reader = new JobStore(filename);
    assert.throws(() => reader.get(job.id), /corrupt job attempt budget/);
    assert.throws(() => reader.claim("worker-a"), /corrupt job attempt budget/);
    reader.close();

    const inspected = new DatabaseSync(filename);
    const persisted = inspected.prepare(
      "SELECT status, attempts, max_attempts AS maxAttempts FROM jobs WHERE id = ?",
    ).get(job.id) as { status: string; attempts: number; maxAttempts: number };
    inspected.close();
    assert.deepEqual({ ...persisted }, { status: "queued", attempts: 2, maxAttempts: 2 });
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("corrupt persisted retry policies fail before a queued job is claimed", () => {
  const directory = mkdtempSync(join(tmpdir(), "relayq-retry-policy-corruption-"));
  const filename = join(directory, "queue.db");
  try {
    const writer = new JobStore(filename);
    const job = writer.enqueue({
      type: "corrupt-retry-policy",
      backoffBaseMs: 1_000,
      backoffMaxMs: 2_000,
    });
    writer.close();

    const sabotage = new DatabaseSync(filename);
    sabotage.prepare("UPDATE jobs SET backoff_max_ms = 500 WHERE id = ?").run(job.id);
    sabotage.close();

    const reader = new JobStore(filename);
    assert.throws(() => reader.get(job.id), /corrupt job retry policy/);
    assert.throws(() => reader.claim("worker-a"), /corrupt job retry policy/);
    reader.close();

    const inspected = new DatabaseSync(filename);
    const persisted = inspected.prepare(
      "SELECT status, attempts, worker_id AS workerId FROM jobs WHERE id = ?",
    ).get(job.id) as { status: string; attempts: number; workerId: string | null };
    const events = inspected.prepare(
      "SELECT event_type AS type FROM job_events WHERE job_id = ? ORDER BY id",
    ).all(job.id) as Array<{ type: string }>;
    inspected.close();
    assert.deepEqual(
      { ...persisted },
      { status: "queued", attempts: 0, workerId: null },
    );
    assert.deepEqual(events.map((event) => event.type), ["enqueued"]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("corrupt running attempt budgets fail before lease recovery", () => {
  const directory = mkdtempSync(join(tmpdir(), "relayq-recovery-corruption-"));
  const filename = join(directory, "queue.db");
  try {
    const writer = new JobStore(filename, () => 10_000);
    const job = writer.enqueue({ type: "corrupt-recovery-budget", maxAttempts: 2 });
    writer.claim("worker-that-stopped", 1_000);
    writer.close();

    const sabotage = new DatabaseSync(filename);
    sabotage.prepare("UPDATE jobs SET attempts = 0 WHERE id = ?").run(job.id);
    sabotage.close();

    const reader = new JobStore(filename, () => 20_000);
    assert.throws(() => reader.claim("replacement-worker"), /corrupt job attempt budget/);
    reader.close();

    const inspected = new DatabaseSync(filename);
    const persisted = inspected.prepare(
      "SELECT status, attempts, worker_id AS workerId FROM jobs WHERE id = ?",
    ).get(job.id) as { status: string; attempts: number; workerId: string };
    const events = inspected.prepare(
      "SELECT event_type AS type FROM job_events WHERE job_id = ? ORDER BY id",
    ).all(job.id) as Array<{ type: string }>;
    inspected.close();
    assert.deepEqual(
      { ...persisted },
      { status: "running", attempts: 0, workerId: "worker-that-stopped" },
    );
    assert.deepEqual(events.map((event) => event.type), ["enqueued", "claimed"]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("corrupt running lease metadata fails before recovery", () => {
  const directory = mkdtempSync(join(tmpdir(), "relayq-lease-state-corruption-"));
  const filename = join(directory, "queue.db");
  try {
    const writer = new JobStore(filename, () => 10_000);
    const job = writer.enqueue({ type: "corrupt-lease-state", maxAttempts: 2 });
    writer.claim("worker-that-stopped", 1_000);
    writer.close();

    const sabotage = new DatabaseSync(filename);
    sabotage.prepare("UPDATE jobs SET lease_expires_at = NULL WHERE id = ?").run(job.id);
    sabotage.close();

    const reader = new JobStore(filename, () => 20_000);
    assert.throws(() => reader.get(job.id), /corrupt job lease state/);
    assert.throws(() => reader.claim("replacement-worker"), /corrupt job lease state/);
    reader.close();

    const inspected = new DatabaseSync(filename);
    const persisted = inspected.prepare(
      "SELECT status, attempts, worker_id AS workerId, lease_expires_at AS leaseExpiresAt " +
      "FROM jobs WHERE id = ?",
    ).get(job.id) as {
      status: string;
      attempts: number;
      workerId: string;
      leaseExpiresAt: number | null;
    };
    const events = inspected.prepare(
      "SELECT event_type AS type FROM job_events WHERE job_id = ? ORDER BY id",
    ).all(job.id) as Array<{ type: string }>;
    inspected.close();
    assert.deepEqual(
      { ...persisted },
      {
        status: "running",
        attempts: 1,
        workerId: "worker-that-stopped",
        leaseExpiresAt: null,
      },
    );
    assert.deepEqual(events.map((event) => event.type), ["enqueued", "claimed"]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("only the lease owner can complete a job", () => {
  const store = new JobStore(":memory:");
  const job = store.enqueue({ type: "sum" });
  store.claim("worker-a");
  assert.throws(
    () => store.complete(job.id, "worker-b"),
    /lease is missing, expired, or owned by another worker/,
  );
  assert.equal(store.get(job.id)?.status, "running");
  store.close();
});

test("expired workers cannot complete or fail jobs before lease recovery", () => {
  let now = 15_000;
  const store = new JobStore(":memory:", () => now);
  const completed = store.enqueue({ type: "late-completion", maxAttempts: 2 });
  store.claim("worker-a", 1_000);

  now += 1_001;
  assert.throws(
    () => store.complete(completed.id, "worker-a", "stale result"),
    /lease is missing, expired, or owned by another worker/,
  );
  assert.equal(store.get(completed.id)?.status, "running");
  assert.equal(store.claim("worker-b", 1_000)?.id, completed.id);
  assert.equal(store.complete(completed.id, "worker-b", "fresh result").result, "fresh result");

  const failed = store.enqueue({ type: "late-failure", maxAttempts: 2 });
  store.claim("worker-a", 1_000);
  now += 1_001;
  assert.throws(
    () => store.fail(failed.id, "worker-a", "stale failure"),
    /lease is missing, expired, or owned by another worker/,
  );
  assert.equal(store.claim("worker-b", 1_000)?.id, failed.id);
  assert.equal(store.fail(failed.id, "worker-b", "fresh failure").status, "failed");
  assert.deepEqual(store.listEvents(failed.id).map((event) => event.type), [
    "enqueued", "claimed", "lease_expired", "claimed", "dead_lettered",
  ]);
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

test("lease renewal cannot shorten an existing ownership deadline", () => {
  let now = 30_000;
  const store = new JobStore(":memory:", () => now);
  const job = store.enqueue({ type: "monotonic-lease" });
  const claimed = store.claim("worker-a", 10_000)!;
  assert.equal(claimed.leaseExpiresAt, 40_000);

  now = 31_000;
  const renewed = store.renewLease(job.id, "worker-a", 1_000);
  assert.equal(renewed.leaseExpiresAt, 40_000);

  now = 32_001;
  assert.equal(store.claim("worker-b", 1_000), null);
  assert.equal(store.get(job.id)?.workerId, "worker-a");

  now = 40_000;
  assert.equal(store.claim("worker-b", 1_000)?.id, job.id);
  store.close();
});

test("worker identity normalization is consistent across lease mutations", () => {
  const store = new JobStore(":memory:");
  const completed = store.enqueue({ type: "normalized-completion" });
  assert.equal(store.claim("  worker-a  ")?.workerId, "worker-a");
  assert.equal(store.renewLease(completed.id, " worker-a ").workerId, "worker-a");
  assert.equal(store.complete(completed.id, " worker-a ").status, "succeeded");
  assert.deepEqual(
    store.listEvents(completed.id).map((event) => event.workerId),
    [null, "worker-a", "worker-a", "worker-a"],
  );

  const failed = store.enqueue({ type: "normalized-failure", maxAttempts: 1 });
  assert.equal(store.claim(" worker-b ")?.workerId, "worker-b");
  assert.equal(store.fail(failed.id, " worker-b ", "planned failure").status, "failed");
  assert.deepEqual(
    store.listEvents(failed.id).map((event) => event.workerId),
    [null, "worker-b", "worker-b"],
  );
  assert.throws(() => store.claim("   "), /workerId is required/);
  store.close();
});

test("worker identity size is bounded before lease state changes", () => {
  const store = new JobStore(":memory:");
  const job = store.enqueue({ type: "bounded-worker-identity" });
  const oversized = "é".repeat(257);

  assert.throws(
    () => store.claim(oversized),
    /workerId must be at most 512 UTF-8 bytes/,
  );
  assert.deepEqual(
    { status: store.get(job.id)?.status, attempts: store.get(job.id)?.attempts },
    { status: "queued", attempts: 0 },
  );
  assert.deepEqual(store.listEvents(job.id).map((event) => event.type), ["enqueued"]);

  const maximum = "w".repeat(512);
  assert.equal(store.claim(maximum)?.workerId, maximum);
  assert.equal(store.complete(job.id, maximum).status, "succeeded");
  store.close();
});

test("invalid lease durations are rejected before queue state changes", () => {
  let now = 25_000;
  const store = new JobStore(":memory:", () => now);
  const job = store.enqueue({ type: "bounded-lease" });
  const invalidDurations = [999, 1_000.5, Number.MAX_SAFE_INTEGER];

  for (const leaseMs of invalidDurations) {
    assert.throws(
      () => store.claim("worker-a", leaseMs),
      /leaseMs must be an integer between 1000 and 2147483647/,
    );
    assert.deepEqual(
      { status: store.get(job.id)?.status, attempts: store.get(job.id)?.attempts },
      { status: "queued", attempts: 0 },
    );
  }

  const claimed = store.claim("worker-a", 1_000)!;
  assert.equal(claimed.leaseExpiresAt, 26_000);
  now += 100;
  assert.throws(
    () => store.renewLease(job.id, "worker-a", Number.MAX_SAFE_INTEGER),
    /leaseMs must be an integer between 1000 and 2147483647/,
  );
  assert.equal(store.get(job.id)?.leaseExpiresAt, 26_000);
  store.close();
});

test("lease expirations must remain exactly representable timestamps", () => {
  let now = Number.MAX_SAFE_INTEGER - 500;
  const claimStore = new JobStore(":memory:", () => now);
  const queued = claimStore.enqueue({ type: "bounded-lease-timestamp" });
  assert.throws(
    () => claimStore.claim("worker-a", 1_000),
    /leaseMs exceeds the safe timestamp range/,
  );
  assert.deepEqual(
    { status: claimStore.get(queued.id)?.status, attempts: claimStore.get(queued.id)?.attempts },
    { status: "queued", attempts: 0 },
  );
  claimStore.close();

  now = 30_000;
  const renewalStore = new JobStore(":memory:", () => now);
  const running = renewalStore.enqueue({ type: "bounded-renewal-timestamp" });
  const claimed = renewalStore.claim("worker-a", 1_000)!;
  now = Number.MAX_SAFE_INTEGER - 500;
  assert.throws(
    () => renewalStore.renewLease(running.id, "worker-a", 1_000),
    /leaseMs exceeds the safe timestamp range/,
  );
  assert.equal(renewalStore.get(running.id)?.leaseExpiresAt, claimed.leaseExpiresAt);
  assert.equal(renewalStore.listEvents(running.id).at(-1)?.type, "claimed");
  renewalStore.close();
});

test("scheduling delays require exactly representable whole milliseconds", () => {
  let now = 27_000;
  const store = new JobStore(":memory:", () => now);
  for (const delayMs of [0.5, Number.MAX_VALUE, Number.MAX_SAFE_INTEGER]) {
    assert.throws(
      () => store.enqueue({ type: "invalid-delay", delayMs }),
      /delayMs must be a non-negative safe integer|delayMs exceeds the safe timestamp range/,
    );
  }
  assert.equal(store.stats().total, 0);
  assert.throws(
    () => store.enqueue({ type: "invalid-backoff", backoffBaseMs: 1.5 }),
    /backoffBaseMs must be a non-negative safe integer/,
  );
  assert.throws(
    () => store.enqueue({ type: "invalid-backoff", backoffMaxMs: Number.MAX_VALUE }),
    /backoffMaxMs must be a safe integer/,
  );

  const job = store.enqueue({ type: "bounded-retry", maxAttempts: 2 });
  store.claim("worker-a");
  assert.throws(
    () => store.fail(job.id, "worker-a", "retry later", 0.5),
    /retryDelayMs must be a non-negative safe integer/,
  );
  assert.equal(store.get(job.id)?.status, "running");
  assert.equal(store.listEvents(job.id).at(-1)?.type, "claimed");

  store.fail(job.id, "worker-a", "retry now", 0);
  store.claim("worker-b");
  store.fail(job.id, "worker-b", "permanent failure");
  assert.throws(
    () => store.requeueFailed(job.id, 0.5),
    /delayMs must be a non-negative safe integer/,
  );
  assert.equal(store.get(job.id)?.status, "failed");
  assert.throws(
    () => store.requeueFailedMany(1, Number.MAX_SAFE_INTEGER),
    /delayMs exceeds the safe timestamp range/,
  );
  assert.equal(store.get(job.id)?.status, "failed");
  store.close();
});

test("enqueue bounds durable priority values and retry budgets", () => {
  const store = new JobStore(":memory:");
  for (const priority of [0.5, Number.MAX_VALUE, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(
      () => store.enqueue({ type: "invalid-priority", priority }),
      /priority must be a safe integer/,
    );
  }
  for (const maxAttempts of [0, 1.5, 1_001, Number.MAX_VALUE]) {
    assert.throws(
      () => store.enqueue({ type: "invalid-budget", maxAttempts }),
      /maxAttempts must be an integer between 1 and 1000/,
    );
  }
  assert.equal(store.stats().total, 0);
  assert.equal(store.listEvents().length, 0);

  const bounded = store.enqueue({
    type: "bounded-input",
    priority: Number.MAX_SAFE_INTEGER,
    maxAttempts: 1_000,
  });
  assert.equal(bounded.priority, Number.MAX_SAFE_INTEGER);
  assert.equal(bounded.maxAttempts, 1_000);
  store.close();
});

test("enqueue bounds the maximum jittered retry delay", () => {
  const store = new JobStore(":memory:", () => 0, () => 1);
  const maximumBeforeJitter = Math.floor(Number.MAX_SAFE_INTEGER / 2);
  assert.throws(
    () => store.enqueue({
      type: "unsafe-jitter",
      backoffBaseMs: maximumBeforeJitter + 1,
      backoffMaxMs: maximumBeforeJitter + 1,
      backoffJitter: 1,
    }),
    /backoffMaxMs with jitter exceeds the safe integer range/,
  );
  assert.equal(store.stats().total, 0);
  assert.equal(store.listEvents().length, 0);

  const job = store.enqueue({
    type: "bounded-jitter",
    backoffBaseMs: maximumBeforeJitter,
    backoffMaxMs: maximumBeforeJitter,
    backoffJitter: 1,
  });
  store.claim("worker");
  const retried = store.fail(job.id, "worker", "retry at the exact boundary");
  assert.equal(retried.availableAt, maximumBeforeJitter * 2);
  assert.equal(
    store.listEvents(job.id).at(-1)?.detail.retryDelayMs,
    maximumBeforeJitter * 2,
  );
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

test("concurrent single-job redrives produce one transition and preserve its delay", () => {
  const directory = mkdtempSync(join(tmpdir(), "relayq-redrive-race-"));
  const filename = join(directory, "queue.db");
  const operator = new JobStore(filename, () => 10_000);
  const competingOperator = new JobStore(filename, () => 20_000);
  try {
    const job = operator.enqueue({ type: "redrive-race", maxAttempts: 1 });
    operator.claim("worker-a");
    operator.fail(job.id, "worker-a", "planned failure");

    const internals = operator as unknown as { transaction: TransactionRunner };
    const originalTransaction = internals.transaction.bind(operator) as TransactionRunner;
    internals.transaction = <T>(operation: () => T): T => {
      competingOperator.requeueFailed(job.id, 5_000);
      internals.transaction = originalTransaction;
      return originalTransaction(operation);
    };

    assert.throws(() => operator.requeueFailed(job.id), /only failed jobs can be requeued/);
    assert.equal(operator.get(job.id)?.availableAt, 25_000);
    assert.deepEqual(operator.listEvents(job.id).map((event) => event.type), [
      "enqueued", "claimed", "dead_lettered", "redriven",
    ]);
  } finally {
    operator.close();
    competingOperator.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("cancellation cannot overwrite a concurrent completion", () => {
  const directory = mkdtempSync(join(tmpdir(), "relayq-cancel-race-"));
  const filename = join(directory, "queue.db");
  const operator = new JobStore(filename);
  const worker = new JobStore(filename);
  try {
    const job = operator.enqueue({ type: "terminal-race" });
    operator.claim("worker-a");

    const internals = operator as unknown as { transaction: TransactionRunner };
    const originalTransaction = internals.transaction.bind(operator) as TransactionRunner;
    internals.transaction = <T>(operation: () => T): T => {
      worker.complete(job.id, "worker-a", "finished");
      internals.transaction = originalTransaction;
      return originalTransaction(operation);
    };

    assert.throws(() => operator.cancel(job.id), /job is already terminal/);
    assert.equal(operator.get(job.id)?.status, "succeeded");
    assert.deepEqual(operator.listEvents(job.id).map((event) => event.type), [
      "enqueued", "claimed", "completed",
    ]);
  } finally {
    operator.close();
    worker.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("event cursors return only newer state transitions", () => {
  const store = new JobStore(":memory:");
  for (const cursor of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, Number.MAX_VALUE]) {
    assert.throws(
      () => store.listEvents(undefined, cursor),
      /afterId must be a non-negative safe integer/,
    );
  }
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

test("pausing prevents new claims while preserving queued work", () => {
  let now = 70_000;
  const store = new JobStore(":memory:", () => now);
  const job = store.enqueue({ type: "maintenance-safe" });
  const paused = store.pause("database maintenance");
  assert.equal(paused.paused, true);
  assert.equal(paused.reason, "database maintenance");
  assert.equal(store.claim("worker"), null);
  assert.equal(store.get(job.id)?.status, "queued");

  now += 1_000;
  const resumed = store.resume();
  assert.equal(resumed.paused, false);
  assert.equal(resumed.updatedAt, now);
  assert.equal(store.claim("worker")?.id, job.id);
  store.close();
});

test("bulk redrive is bounded and records each recovery", () => {
  let now = 80_000;
  const store = new JobStore(":memory:", () => now);
  const failedIds: string[] = [];
  for (let index = 0; index < 3; index += 1) {
    const job = store.enqueue({ type: `failed-${index}`, maxAttempts: 1 });
    failedIds.push(job.id);
    store.claim("worker");
    store.fail(job.id, "worker", "permanent failure");
  }

  const redriven = store.requeueFailedMany(2, 500);
  assert.equal(redriven.length, 2);
  assert.ok(redriven.every((job) => job.status === "queued" && job.availableAt === now + 500));
  assert.deepEqual(store.stats(), {
    queued: 2, running: 0, succeeded: 0, failed: 1, cancelled: 0, total: 3,
  });
  const event = store.listEvents(redriven[0].id).at(-1)!;
  assert.equal(event.type, "redriven");
  assert.equal(event.detail.bulk, true);
  assert.ok(failedIds.includes(store.list("failed")[0].id));
  store.close();
});

test("retention removes only eligible terminal jobs and cascades events", () => {
  let now = 90_000;
  const store = new JobStore(":memory:", () => now);
  const succeeded = store.enqueue({ type: "old-success" });
  store.claim("worker");
  store.complete(succeeded.id, "worker");
  const failed = store.enqueue({ type: "old-failure", maxAttempts: 1 });
  store.claim("worker");
  store.fail(failed.id, "worker", "keep for diagnosis");
  const queued = store.enqueue({ type: "still-pending" });

  now += 10_000;
  const recent = store.enqueue({ type: "recent-success", priority: 10 });
  store.claim("worker");
  store.complete(recent.id, "worker");

  const firstPurge = store.purgeTerminal(5_000);
  assert.deepEqual(firstPurge.jobIds, [succeeded.id]);
  assert.equal(store.get(succeeded.id), null);
  assert.deepEqual(store.listEvents(succeeded.id), []);
  assert.equal(store.get(failed.id)?.status, "failed");
  assert.equal(store.get(queued.id)?.status, "queued");
  assert.equal(store.get(recent.id)?.status, "succeeded");

  const includingFailed = store.purgeTerminal(0, 10, true);
  assert.ok(includingFailed.jobIds.includes(failed.id));
  assert.ok(includingFailed.jobIds.includes(recent.id));
  assert.equal(store.get(queued.id)?.status, "queued");
  store.close();
});

test("retention rejects imprecise ages before deleting terminal jobs", () => {
  const store = new JobStore(":memory:", () => 100_000);
  const job = store.enqueue({ type: "retain-on-invalid-cutoff" });
  store.claim("worker");
  store.complete(job.id, "worker");

  for (const olderThanMs of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, Number.MAX_VALUE]) {
    assert.throws(
      () => store.purgeTerminal(olderThanMs),
      /olderThanMs must be a non-negative safe integer/,
    );
    assert.equal(store.get(job.id)?.status, "succeeded");
    assert.equal(store.listEvents(job.id).at(-1)?.type, "completed");
  }
  store.close();

  const boundaryStore = new JobStore(":memory:", () => -Number.MAX_SAFE_INTEGER);
  assert.throws(
    () => boundaryStore.purgeTerminal(1),
    /olderThanMs exceeds the safe timestamp range/,
  );
  boundaryStore.close();
});
