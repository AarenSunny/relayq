import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import type { Job, JobEvent, JobEventType, JobStatus, NewJob, QueueControl, QueueStats } from "./types.ts";

type Clock = () => number;
type Random = () => number;
type JobRow = Record<string, unknown>;

const TERMINAL_STATUSES = new Set<JobStatus>(["succeeded", "failed", "cancelled"]);
const MIN_LEASE_MS = 1_000;
const MAX_LEASE_MS = 2_147_483_647;
const MAX_ATTEMPTS = 1_000;
const MAX_WORKER_ID_BYTES = 512;

function decodeRetryPolicy(row: JobRow): {
  backoffBaseMs: number;
  backoffMaxMs: number;
  backoffJitter: number;
} {
  const backoffBaseMs = row.backoff_base_ms;
  const backoffMaxMs = row.backoff_max_ms;
  const backoffJitter = row.backoff_jitter;
  const invalidBase = typeof backoffBaseMs !== "number" ||
    !Number.isSafeInteger(backoffBaseMs) || backoffBaseMs < 0;
  const invalidMaximum = typeof backoffMaxMs !== "number" ||
    !Number.isSafeInteger(backoffMaxMs) ||
    typeof backoffBaseMs !== "number" || backoffMaxMs < backoffBaseMs;
  const invalidJitter = typeof backoffJitter !== "number" ||
    !Number.isFinite(backoffJitter) || backoffJitter < 0 || backoffJitter > 1;
  const unsafeJitteredMaximum = !invalidMaximum && !invalidJitter &&
    backoffMaxMs > Number.MAX_SAFE_INTEGER / (1 + backoffJitter);
  if (invalidBase || invalidMaximum || invalidJitter || unsafeJitteredMaximum) {
    throw new Error("corrupt job retry policy");
  }
  return { backoffBaseMs, backoffMaxMs, backoffJitter };
}

function decodeAttemptBudget(row: JobRow): { maxAttempts: number; attempts: number } {
  const maxAttempts = Number(row.max_attempts);
  const attempts = Number(row.attempts);
  const status = String(row.status);
  const invalidCount = !Number.isSafeInteger(maxAttempts) || maxAttempts < 1 ||
    !Number.isSafeInteger(attempts) || attempts < 0 || attempts > maxAttempts;
  const invalidState = (status === "queued" && attempts >= maxAttempts) ||
    (["running", "succeeded", "failed"].includes(status) && attempts === 0);
  if (invalidCount || invalidState) throw new Error("corrupt job attempt budget");
  return { maxAttempts, attempts };
}

function normalizeWorkerId(workerId: string): string {
  const normalized = workerId?.trim();
  if (!normalized) throw new Error("workerId is required");
  if (Buffer.byteLength(normalized, "utf8") > MAX_WORKER_ID_BYTES) {
    throw new Error(`workerId must be at most ${MAX_WORKER_ID_BYTES} UTF-8 bytes`);
  }
  return normalized;
}

function decodeLeaseState(row: JobRow): {
  leaseExpiresAt: number | null;
  workerId: string | null;
} {
  const running = String(row.status) === "running";
  const hasLease = row.lease_expires_at !== null;
  const hasWorker = row.worker_id !== null;
  if (!running) {
    if (hasLease || hasWorker) throw new Error("corrupt job lease state");
    return { leaseExpiresAt: null, workerId: null };
  }
  if (!hasLease || !hasWorker) throw new Error("corrupt job lease state");

  const leaseExpiresAt = Number(row.lease_expires_at);
  const workerId = String(row.worker_id);
  if (!Number.isSafeInteger(leaseExpiresAt)) throw new Error("corrupt job lease state");
  let normalizedWorkerId: string;
  try {
    normalizedWorkerId = normalizeWorkerId(workerId);
  } catch {
    throw new Error("corrupt job lease state");
  }
  if (normalizedWorkerId !== workerId) throw new Error("corrupt job lease state");
  return { leaseExpiresAt, workerId };
}

function decodeQueueControl(row: JobRow | undefined): QueueControl {
  if (!row) throw new Error("corrupt queue control state");
  const paused = row.paused;
  const reason = row.reason;
  const updatedAt = row.updated_at;
  if (paused !== 0 && paused !== 1) throw new Error("corrupt queue control state");
  if (reason !== null && typeof reason !== "string") throw new Error("corrupt queue control state");
  if (paused === 0 && reason !== null) throw new Error("corrupt queue control state");
  if (typeof updatedAt !== "number" || !Number.isSafeInteger(updatedAt)) {
    throw new Error("corrupt queue control state");
  }
  return { paused: paused === 1, reason, updatedAt };
}

function validateLeaseMs(leaseMs: number): void {
  if (!Number.isSafeInteger(leaseMs) || leaseMs < MIN_LEASE_MS || leaseMs > MAX_LEASE_MS) {
    throw new Error(`leaseMs must be an integer between ${MIN_LEASE_MS} and ${MAX_LEASE_MS}`);
  }
}

function validateDelayMs(delayMs: number, label: string): void {
  if (!Number.isSafeInteger(delayMs) || delayMs < 0) {
    throw new Error(`${label} must be a non-negative safe integer`);
  }
}

function scheduledAt(now: number, delayMs: number, label: string): number {
  validateDelayMs(delayMs, label);
  const timestamp = now + delayMs;
  if (!Number.isSafeInteger(timestamp)) {
    throw new Error(`${label} exceeds the safe timestamp range`);
  }
  return timestamp;
}

function retentionCutoff(now: number, olderThanMs: number): number {
  validateDelayMs(olderThanMs, "olderThanMs");
  const timestamp = now - olderThanMs;
  if (!Number.isSafeInteger(timestamp)) {
    throw new Error("olderThanMs exceeds the safe timestamp range");
  }
  return timestamp;
}

export class JobStore {
  private readonly db: DatabaseSync;
  private readonly clock: Clock;
  private readonly random: Random;

  constructor(filename = "relayq.db", clock: Clock = Date.now, random: Random = Math.random) {
    this.db = new DatabaseSync(filename);
    this.clock = clock;
    this.random = random;
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('queued','running','succeeded','failed','cancelled')),
        priority INTEGER NOT NULL,
        max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
        backoff_base_ms INTEGER NOT NULL DEFAULT 1000,
        backoff_max_ms INTEGER NOT NULL DEFAULT 60000,
        backoff_jitter REAL NOT NULL DEFAULT 0.2,
        attempts INTEGER NOT NULL DEFAULT 0,
        available_at INTEGER NOT NULL,
        lease_expires_at INTEGER,
        worker_id TEXT,
        result TEXT,
        error TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS jobs_queue_order
        ON jobs(status, available_at, priority DESC, created_at ASC);
      CREATE INDEX IF NOT EXISTS jobs_worker ON jobs(worker_id, status);
      CREATE TABLE IF NOT EXISTS job_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
        event_type TEXT NOT NULL,
        from_status TEXT,
        to_status TEXT NOT NULL,
        worker_id TEXT,
        detail TEXT NOT NULL DEFAULT '{}',
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS job_events_job_id ON job_events(job_id, id);
      CREATE TABLE IF NOT EXISTS queue_control (
        id INTEGER PRIMARY KEY CHECK(id = 1),
        paused INTEGER NOT NULL DEFAULT 0,
        reason TEXT,
        updated_at INTEGER NOT NULL
      );
    `);
    this.db.prepare(`
      INSERT OR IGNORE INTO queue_control (id, paused, reason, updated_at)
      VALUES (1, 0, NULL, ?)
    `).run(this.clock());
    this.ensureColumn("backoff_base_ms", "INTEGER NOT NULL DEFAULT 1000");
    this.ensureColumn("backoff_max_ms", "INTEGER NOT NULL DEFAULT 60000");
    this.ensureColumn("backoff_jitter", "REAL NOT NULL DEFAULT 0.2");
  }

  close(): void {
    this.db.close();
  }

  isReady(): boolean {
    try {
      const row = this.db.prepare(
        "SELECT paused, reason, updated_at FROM queue_control WHERE id = 1",
      ).get() as JobRow | undefined;
      decodeQueueControl(row);
      return true;
    } catch {
      return false;
    }
  }

  enqueue(input: NewJob): Job {
    if (!input.type?.trim()) throw new Error("job type is required");
    const priority = input.priority ?? 0;
    const maxAttempts = input.maxAttempts ?? 3;
    const delayMs = input.delayMs ?? 0;
    const backoffBaseMs = input.backoffBaseMs ?? 1_000;
    const backoffMaxMs = input.backoffMaxMs ?? 60_000;
    const backoffJitter = input.backoffJitter ?? 0.2;
    if (!Number.isSafeInteger(priority)) throw new Error("priority must be a safe integer");
    if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > MAX_ATTEMPTS) {
      throw new Error(`maxAttempts must be an integer between 1 and ${MAX_ATTEMPTS}`);
    }
    validateDelayMs(delayMs, "delayMs");
    if (!Number.isSafeInteger(backoffBaseMs) || backoffBaseMs < 0) {
      throw new Error("backoffBaseMs must be a non-negative safe integer");
    }
    if (!Number.isSafeInteger(backoffMaxMs) || backoffMaxMs < backoffBaseMs) {
      throw new Error("backoffMaxMs must be a safe integer at least as large as backoffBaseMs");
    }
    if (!Number.isFinite(backoffJitter) || backoffJitter < 0 || backoffJitter > 1) {
      throw new Error("backoffJitter must be between 0 and 1");
    }
    if (backoffMaxMs > Number.MAX_SAFE_INTEGER / (1 + backoffJitter)) {
      throw new Error("backoffMaxMs with jitter exceeds the safe integer range");
    }

    const now = this.clock();
    const availableAt = scheduledAt(now, delayMs, "delayMs");
    const id = randomUUID();
    this.transaction(() => {
      this.db.prepare(`
        INSERT INTO jobs (
          id, type, payload, status, priority, max_attempts,
          backoff_base_ms, backoff_max_ms, backoff_jitter, attempts,
          available_at, created_at, updated_at
        ) VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, 0, ?, ?, ?)
      `).run(id, input.type.trim(), JSON.stringify(input.payload ?? null), priority,
        maxAttempts, backoffBaseMs, backoffMaxMs, backoffJitter, availableAt, now, now);
      this.recordEvent(id, "enqueued", null, "queued", null, {
        priority, delayMs, maxAttempts, backoffBaseMs, backoffMaxMs, backoffJitter,
      }, now);
    });
    return this.get(id)!;
  }

  get(id: string): Job | null {
    const row = this.db.prepare("SELECT * FROM jobs WHERE id = ?").get(id) as JobRow | undefined;
    return row ? this.toJob(row) : null;
  }

  list(status?: JobStatus, limit = 100): Job[] {
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error("limit must be between 1 and 1000");
    }
    const rows = status
      ? this.db.prepare("SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT ?").all(status, limit)
      : this.db.prepare("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?").all(limit);
    return (rows as JobRow[]).map((row) => this.toJob(row));
  }

  listEvents(jobId?: string, afterId = 0, limit = 100): JobEvent[] {
    if (!Number.isSafeInteger(afterId) || afterId < 0) {
      throw new Error("afterId must be a non-negative safe integer");
    }
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) {
      throw new Error("limit must be between 1 and 1000");
    }
    let rows: JobRow[];
    if (jobId && afterId > 0) {
      rows = this.db.prepare(`
        SELECT * FROM job_events WHERE job_id = ? AND id > ? ORDER BY id ASC LIMIT ?
      `).all(jobId, afterId, limit) as JobRow[];
    } else if (jobId) {
      rows = this.db.prepare(`
        SELECT * FROM (
          SELECT * FROM job_events WHERE job_id = ? ORDER BY id DESC LIMIT ?
        ) ORDER BY id ASC
      `).all(jobId, limit) as JobRow[];
    } else if (afterId > 0) {
      rows = this.db.prepare(`
        SELECT * FROM job_events WHERE id > ? ORDER BY id ASC LIMIT ?
      `).all(afterId, limit) as JobRow[];
    } else {
      rows = this.db.prepare(`
        SELECT * FROM (
          SELECT * FROM job_events ORDER BY id DESC LIMIT ?
        ) ORDER BY id ASC
      `).all(limit) as JobRow[];
    }
    return rows.map((row) => this.toEvent(row));
  }

  control(): QueueControl {
    const row = this.db.prepare(
      "SELECT paused, reason, updated_at FROM queue_control WHERE id = 1",
    ).get() as JobRow | undefined;
    return decodeQueueControl(row);
  }

  pause(reason?: string): QueueControl {
    const now = this.clock();
    const normalizedReason = reason?.trim() || null;
    this.db.prepare(`
      UPDATE queue_control SET paused = 1, reason = ?, updated_at = ? WHERE id = 1
    `).run(normalizedReason, now);
    return this.control();
  }

  resume(): QueueControl {
    const now = this.clock();
    this.db.prepare(`
      UPDATE queue_control SET paused = 0, reason = NULL, updated_at = ? WHERE id = 1
    `).run(now);
    return this.control();
  }

  claim(workerId: string, leaseMs = 30_000): Job | null {
    const owner = normalizeWorkerId(workerId);
    validateLeaseMs(leaseMs);

    const now = this.clock();
    const leaseExpiresAt = scheduledAt(now, leaseMs, "leaseMs");
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.recoverExpiredLeases(now);
      if (this.control().paused) {
        this.db.exec("COMMIT");
        return null;
      }
      const candidateRow = this.db.prepare(`
        SELECT * FROM jobs
        WHERE status = 'queued' AND available_at <= ?
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
      `).get(now) as JobRow | undefined;

      if (!candidateRow) {
        this.db.exec("COMMIT");
        return null;
      }
      const candidate = this.toJob(candidateRow);

      this.db.prepare(`
        UPDATE jobs
        SET status = 'running', worker_id = ?, lease_expires_at = ?,
            attempts = attempts + 1, updated_at = ?
        WHERE id = ? AND status = 'queued'
      `).run(owner, leaseExpiresAt, now, candidate.id);
      this.recordEvent(candidate.id, "claimed", "queued", "running", owner, { leaseMs }, now);
      this.db.exec("COMMIT");
      return this.get(candidate.id);
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  complete(id: string, workerId: string, result: unknown = null): Job {
    const owner = normalizeWorkerId(workerId);
    const now = this.clock();
    return this.transaction(() => {
      const change = this.db.prepare(`
        UPDATE jobs
        SET status = 'succeeded', result = ?, error = NULL, worker_id = NULL,
            lease_expires_at = NULL, updated_at = ?
        WHERE id = ? AND status = 'running' AND worker_id = ? AND lease_expires_at > ?
      `).run(JSON.stringify(result), now, id, owner, now);
      if (change.changes !== 1) {
        throw new Error("job lease is missing, expired, or owned by another worker");
      }
      this.recordEvent(id, "completed", "running", "succeeded", owner, {}, now);
      return this.get(id)!;
    });
  }

  renewLease(id: string, workerId: string, leaseMs = 30_000): Job {
    const owner = normalizeWorkerId(workerId);
    validateLeaseMs(leaseMs);
    const now = this.clock();
    const leaseExpiresAt = scheduledAt(now, leaseMs, "leaseMs");
    return this.transaction(() => {
      const change = this.db.prepare(`
        UPDATE jobs
        SET lease_expires_at = MAX(lease_expires_at, ?), updated_at = ?
        WHERE id = ? AND status = 'running' AND worker_id = ? AND lease_expires_at > ?
      `).run(leaseExpiresAt, now, id, owner, now);
      if (change.changes !== 1) throw new Error("job lease is missing, expired, or owned by another worker");
      this.recordEvent(id, "lease_renewed", "running", "running", owner, { leaseMs }, now);
      return this.get(id)!;
    });
  }

  fail(id: string, workerId: string, error: string, retryDelayMs?: number): Job {
    if (!error?.trim()) throw new Error("error is required");
    const owner = normalizeWorkerId(workerId);
    if (retryDelayMs !== undefined) validateDelayMs(retryDelayMs, "retryDelayMs");
    const now = this.clock();
    return this.transaction(() => {
      const job = this.get(id);
      if (!job || job.status !== "running" || job.workerId !== owner ||
          job.leaseExpiresAt === null || job.leaseExpiresAt <= now) {
        throw new Error("job lease is missing, expired, or owned by another worker");
      }
      const exhausted = job.attempts >= job.maxAttempts;
      const delayMs = exhausted ? 0 : retryDelayMs ?? this.retryDelay(job);
      const availableAt = scheduledAt(now, delayMs, "retryDelayMs");
      const toStatus = exhausted ? "failed" : "queued";
      const change = this.db.prepare(`
        UPDATE jobs
        SET status = ?, error = ?, worker_id = NULL, lease_expires_at = NULL,
            available_at = ?, updated_at = ?
        WHERE id = ? AND status = 'running' AND worker_id = ? AND lease_expires_at > ?
      `).run(toStatus, error.trim(), availableAt, now, id, owner, now);
      if (change.changes !== 1) {
        throw new Error("job lease is missing, expired, or owned by another worker");
      }
      this.recordEvent(id, exhausted ? "dead_lettered" : "retry_scheduled", "running", toStatus,
        owner, { error: error.trim(), retryDelayMs: delayMs, attempt: job.attempts }, now);
      return this.get(id)!;
    });
  }

  cancel(id: string): Job {
    return this.transaction(() => {
      const job = this.get(id);
      if (!job) throw new Error("job not found");
      if (TERMINAL_STATUSES.has(job.status)) throw new Error("job is already terminal");
      const now = this.clock();
      const change = this.db.prepare(`
        UPDATE jobs
        SET status = 'cancelled', worker_id = NULL, lease_expires_at = NULL, updated_at = ?
        WHERE id = ? AND status IN ('queued', 'running')
      `).run(now, id);
      if (change.changes !== 1) throw new Error("job is already terminal");
      this.recordEvent(id, "cancelled", job.status, "cancelled", job.workerId, {}, now);
      return this.get(id)!;
    });
  }

  requeueFailed(id: string, delayMs = 0): Job {
    validateDelayMs(delayMs, "delayMs");
    return this.transaction(() => {
      const job = this.get(id);
      if (!job) throw new Error("job not found");
      if (job.status !== "failed") throw new Error("only failed jobs can be requeued");
      const now = this.clock();
      const availableAt = scheduledAt(now, delayMs, "delayMs");
      const change = this.db.prepare(`
        UPDATE jobs
        SET status = 'queued', attempts = 0, available_at = ?, worker_id = NULL,
            lease_expires_at = NULL, result = NULL, error = NULL, updated_at = ?
        WHERE id = ? AND status = 'failed'
      `).run(availableAt, now, id);
      if (change.changes !== 1) throw new Error("only failed jobs can be requeued");
      this.recordEvent(id, "redriven", "failed", "queued", null, { delayMs }, now);
      return this.get(id)!;
    });
  }

  requeueFailedMany(limit = 100, delayMs = 0): Job[] {
    this.validateBatch(limit, delayMs);
    const now = this.clock();
    const availableAt = scheduledAt(now, delayMs, "delayMs");
    return this.transaction(() => {
      const rows = this.db.prepare(`
        SELECT id FROM jobs WHERE status = 'failed' ORDER BY updated_at ASC, id ASC LIMIT ?
      `).all(limit) as Array<{ id: string }>;
      for (const row of rows) {
        this.db.prepare(`
          UPDATE jobs
          SET status = 'queued', attempts = 0, available_at = ?, worker_id = NULL,
              lease_expires_at = NULL, result = NULL, error = NULL, updated_at = ?
          WHERE id = ? AND status = 'failed'
        `).run(availableAt, now, row.id);
        this.recordEvent(row.id, "redriven", "failed", "queued", null, { delayMs, bulk: true }, now);
      }
      return rows.map((row) => this.get(row.id)!);
    });
  }

  purgeTerminal(olderThanMs: number, limit = 1_000, includeFailed = false): { deleted: number; jobIds: string[] } {
    if (!Number.isInteger(limit) || limit < 1 || limit > 10_000) {
      throw new Error("limit must be between 1 and 10000");
    }
    const cutoff = retentionCutoff(this.clock(), olderThanMs);
    return this.transaction(() => {
      const statusSql = includeFailed
        ? "status IN ('succeeded', 'cancelled', 'failed')"
        : "status IN ('succeeded', 'cancelled')";
      const rows = this.db.prepare(`
        SELECT id FROM jobs WHERE ${statusSql} AND updated_at <= ?
        ORDER BY updated_at ASC, id ASC LIMIT ?
      `).all(cutoff, limit) as Array<{ id: string }>;
      const remove = this.db.prepare("DELETE FROM jobs WHERE id = ?");
      for (const row of rows) remove.run(row.id);
      return { deleted: rows.length, jobIds: rows.map((row) => row.id) };
    });
  }

  stats(): QueueStats {
    const stats: QueueStats = {
      queued: 0, running: 0, succeeded: 0, failed: 0, cancelled: 0, total: 0,
    };
    const rows = this.db.prepare("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status").all() as
      Array<{ status: JobStatus; count: number }>;
    for (const row of rows) {
      stats[row.status] = Number(row.count);
      stats.total += Number(row.count);
    }
    return stats;
  }

  private recoverExpiredLeases(now: number): void {
    const expiredRows = this.db.prepare(`
      SELECT * FROM jobs
      WHERE status = 'running' AND (
        lease_expires_at IS NULL OR worker_id IS NULL OR
        typeof(lease_expires_at) != 'integer' OR lease_expires_at <= ?
      )
    `).all(now) as JobRow[];

    for (const row of expiredRows) {
      const job = this.toJob(row);
      const exhausted = job.attempts >= job.maxAttempts;
      const toStatus: JobStatus = exhausted ? "failed" : "queued";
      const error = exhausted ? "lease expired after final attempt" : "worker lease expired";
      this.db.prepare(`
        UPDATE jobs
        SET status = ?, error = ?, worker_id = NULL, lease_expires_at = NULL,
            available_at = ?, updated_at = ?
        WHERE id = ? AND status = 'running' AND lease_expires_at <= ?
      `).run(toStatus, error, now, now, job.id, now);
      this.recordEvent(job.id, "lease_expired", "running", toStatus, job.workerId,
        { exhausted, attempt: job.attempts }, now);
    }
  }

  private retryDelay(job: Job): number {
    const exponent = Math.max(0, job.attempts - 1);
    const uncapped = job.backoffBaseMs * (2 ** exponent);
    const capped = Math.min(uncapped, job.backoffMaxMs);
    const jitterMultiplier = 1 + ((this.random() * 2 - 1) * job.backoffJitter);
    return Math.max(0, Math.round(capped * jitterMultiplier));
  }

  private validateBatch(limit: number, delayMs: number): void {
    if (!Number.isInteger(limit) || limit < 1 || limit > 1_000) {
      throw new Error("limit must be between 1 and 1000");
    }
    validateDelayMs(delayMs, "delayMs");
  }

  private ensureColumn(name: string, definition: string): void {
    const columns = this.db.prepare("PRAGMA table_info(jobs)").all() as Array<{ name: string }>;
    if (!columns.some((column) => column.name === name)) {
      this.db.exec(`ALTER TABLE jobs ADD COLUMN ${name} ${definition}`);
    }
  }

  private transaction<T>(operation: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = operation();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  private recordEvent(
    jobId: string,
    type: JobEventType,
    fromStatus: JobStatus | null,
    toStatus: JobStatus,
    workerId: string | null,
    detail: Record<string, unknown>,
    createdAt: number,
  ): void {
    this.db.prepare(`
      INSERT INTO job_events (
        job_id, event_type, from_status, to_status, worker_id, detail, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(jobId, type, fromStatus, toStatus, workerId, JSON.stringify(detail), createdAt);
  }

  private toJob(row: JobRow): Job {
    const { maxAttempts, attempts } = decodeAttemptBudget(row);
    const { leaseExpiresAt, workerId } = decodeLeaseState(row);
    const { backoffBaseMs, backoffMaxMs, backoffJitter } = decodeRetryPolicy(row);
    return {
      id: String(row.id),
      type: String(row.type),
      payload: JSON.parse(String(row.payload)),
      status: row.status as JobStatus,
      priority: Number(row.priority),
      maxAttempts,
      backoffBaseMs,
      backoffMaxMs,
      backoffJitter,
      attempts,
      availableAt: Number(row.available_at),
      leaseExpiresAt,
      workerId,
      result: row.result === null ? null : JSON.parse(String(row.result)),
      error: row.error === null ? null : String(row.error),
      createdAt: Number(row.created_at),
      updatedAt: Number(row.updated_at),
    };
  }

  private toEvent(row: JobRow): JobEvent {
    return {
      id: Number(row.id),
      jobId: String(row.job_id),
      type: row.event_type as JobEventType,
      fromStatus: row.from_status === null ? null : row.from_status as JobStatus,
      toStatus: row.to_status as JobStatus,
      workerId: row.worker_id === null ? null : String(row.worker_id),
      detail: JSON.parse(String(row.detail)) as Record<string, unknown>,
      createdAt: Number(row.created_at),
    };
  }
}
