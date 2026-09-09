import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import type { Job, JobEvent, JobEventType, JobStatus, NewJob, QueueStats } from "./types.ts";

type Clock = () => number;
type Random = () => number;
type JobRow = Record<string, unknown>;

const TERMINAL_STATUSES = new Set<JobStatus>(["succeeded", "failed", "cancelled"]);

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
    `);
    this.ensureColumn("backoff_base_ms", "INTEGER NOT NULL DEFAULT 1000");
    this.ensureColumn("backoff_max_ms", "INTEGER NOT NULL DEFAULT 60000");
    this.ensureColumn("backoff_jitter", "REAL NOT NULL DEFAULT 0.2");
  }

  close(): void {
    this.db.close();
  }

  enqueue(input: NewJob): Job {
    if (!input.type?.trim()) throw new Error("job type is required");
    const priority = input.priority ?? 0;
    const maxAttempts = input.maxAttempts ?? 3;
    const delayMs = input.delayMs ?? 0;
    const backoffBaseMs = input.backoffBaseMs ?? 1_000;
    const backoffMaxMs = input.backoffMaxMs ?? 60_000;
    const backoffJitter = input.backoffJitter ?? 0.2;
    if (!Number.isInteger(priority)) throw new Error("priority must be an integer");
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new Error("maxAttempts must be a positive integer");
    }
    if (!Number.isFinite(delayMs) || delayMs < 0) throw new Error("delayMs must be non-negative");
    if (!Number.isInteger(backoffBaseMs) || backoffBaseMs < 0) {
      throw new Error("backoffBaseMs must be a non-negative integer");
    }
    if (!Number.isInteger(backoffMaxMs) || backoffMaxMs < backoffBaseMs) {
      throw new Error("backoffMaxMs must be an integer at least as large as backoffBaseMs");
    }
    if (!Number.isFinite(backoffJitter) || backoffJitter < 0 || backoffJitter > 1) {
      throw new Error("backoffJitter must be between 0 and 1");
    }

    const now = this.clock();
    const id = randomUUID();
    this.transaction(() => {
      this.db.prepare(`
        INSERT INTO jobs (
          id, type, payload, status, priority, max_attempts,
          backoff_base_ms, backoff_max_ms, backoff_jitter, attempts,
          available_at, created_at, updated_at
        ) VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, 0, ?, ?, ?)
      `).run(id, input.type.trim(), JSON.stringify(input.payload ?? null), priority,
        maxAttempts, backoffBaseMs, backoffMaxMs, backoffJitter, now + delayMs, now, now);
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
    if (!Number.isInteger(afterId) || afterId < 0) throw new Error("afterId must be non-negative");
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

  claim(workerId: string, leaseMs = 30_000): Job | null {
    if (!workerId?.trim()) throw new Error("workerId is required");
    if (!Number.isFinite(leaseMs) || leaseMs < 1_000) throw new Error("leaseMs must be at least 1000");

    const now = this.clock();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.recoverExpiredLeases(now);
      const candidate = this.db.prepare(`
        SELECT id FROM jobs
        WHERE status = 'queued' AND available_at <= ?
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
      `).get(now) as { id: string } | undefined;

      if (!candidate) {
        this.db.exec("COMMIT");
        return null;
      }

      this.db.prepare(`
        UPDATE jobs
        SET status = 'running', worker_id = ?, lease_expires_at = ?,
            attempts = attempts + 1, updated_at = ?
        WHERE id = ? AND status = 'queued'
      `).run(workerId.trim(), now + leaseMs, now, candidate.id);
      this.recordEvent(candidate.id, "claimed", "queued", "running", workerId.trim(), { leaseMs }, now);
      this.db.exec("COMMIT");
      return this.get(candidate.id);
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  complete(id: string, workerId: string, result: unknown = null): Job {
    const now = this.clock();
    return this.transaction(() => {
      const change = this.db.prepare(`
        UPDATE jobs
        SET status = 'succeeded', result = ?, error = NULL, worker_id = NULL,
            lease_expires_at = NULL, updated_at = ?
        WHERE id = ? AND status = 'running' AND worker_id = ?
      `).run(JSON.stringify(result), now, id, workerId);
      if (change.changes !== 1) throw new Error("job is not running for this worker");
      this.recordEvent(id, "completed", "running", "succeeded", workerId, {}, now);
      return this.get(id)!;
    });
  }

  renewLease(id: string, workerId: string, leaseMs = 30_000): Job {
    if (!workerId?.trim()) throw new Error("workerId is required");
    if (!Number.isFinite(leaseMs) || leaseMs < 1_000) throw new Error("leaseMs must be at least 1000");
    const now = this.clock();
    return this.transaction(() => {
      const change = this.db.prepare(`
        UPDATE jobs
        SET lease_expires_at = ?, updated_at = ?
        WHERE id = ? AND status = 'running' AND worker_id = ? AND lease_expires_at > ?
      `).run(now + leaseMs, now, id, workerId.trim(), now);
      if (change.changes !== 1) throw new Error("job lease is missing, expired, or owned by another worker");
      this.recordEvent(id, "lease_renewed", "running", "running", workerId.trim(), { leaseMs }, now);
      return this.get(id)!;
    });
  }

  fail(id: string, workerId: string, error: string, retryDelayMs?: number): Job {
    if (!error?.trim()) throw new Error("error is required");
    if (retryDelayMs !== undefined && (!Number.isFinite(retryDelayMs) || retryDelayMs < 0)) {
      throw new Error("retryDelayMs must be non-negative");
    }
    const job = this.get(id);
    if (!job || job.status !== "running" || job.workerId !== workerId) {
      throw new Error("job is not running for this worker");
    }

    const now = this.clock();
    const exhausted = job.attempts >= job.maxAttempts;
    const delayMs = exhausted ? 0 : retryDelayMs ?? this.retryDelay(job);
    return this.transaction(() => {
      const toStatus = exhausted ? "failed" : "queued";
      this.db.prepare(`
        UPDATE jobs
        SET status = ?, error = ?, worker_id = NULL, lease_expires_at = NULL,
            available_at = ?, updated_at = ?
        WHERE id = ?
      `).run(toStatus, error.trim(), now + delayMs, now, id);
      this.recordEvent(id, exhausted ? "dead_lettered" : "retry_scheduled", "running", toStatus,
        workerId, { error: error.trim(), retryDelayMs: delayMs, attempt: job.attempts }, now);
      return this.get(id)!;
    });
  }

  cancel(id: string): Job {
    const job = this.get(id);
    if (!job) throw new Error("job not found");
    if (TERMINAL_STATUSES.has(job.status)) throw new Error("job is already terminal");
    const now = this.clock();
    return this.transaction(() => {
      this.db.prepare(`
        UPDATE jobs
        SET status = 'cancelled', worker_id = NULL, lease_expires_at = NULL, updated_at = ?
        WHERE id = ?
      `).run(now, id);
      this.recordEvent(id, "cancelled", job.status, "cancelled", job.workerId, {}, now);
      return this.get(id)!;
    });
  }

  requeueFailed(id: string, delayMs = 0): Job {
    if (!Number.isFinite(delayMs) || delayMs < 0) throw new Error("delayMs must be non-negative");
    const job = this.get(id);
    if (!job) throw new Error("job not found");
    if (job.status !== "failed") throw new Error("only failed jobs can be requeued");
    const now = this.clock();
    return this.transaction(() => {
      this.db.prepare(`
        UPDATE jobs
        SET status = 'queued', attempts = 0, available_at = ?, worker_id = NULL,
            lease_expires_at = NULL, result = NULL, error = NULL, updated_at = ?
        WHERE id = ?
      `).run(now + delayMs, now, id);
      this.recordEvent(id, "redriven", "failed", "queued", null, { delayMs }, now);
      return this.get(id)!;
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
    const expired = this.db.prepare(`
      SELECT id, attempts, max_attempts, worker_id FROM jobs
      WHERE status = 'running' AND lease_expires_at <= ?
    `).all(now) as Array<{ id: string; attempts: number; max_attempts: number; worker_id: string | null }>;

    for (const job of expired) {
      const exhausted = job.attempts >= job.max_attempts;
      const toStatus: JobStatus = exhausted ? "failed" : "queued";
      const error = exhausted ? "lease expired after final attempt" : "worker lease expired";
      this.db.prepare(`
        UPDATE jobs
        SET status = ?, error = ?, worker_id = NULL, lease_expires_at = NULL,
            available_at = ?, updated_at = ?
        WHERE id = ? AND status = 'running' AND lease_expires_at <= ?
      `).run(toStatus, error, now, now, job.id, now);
      this.recordEvent(job.id, "lease_expired", "running", toStatus, job.worker_id,
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
    return {
      id: String(row.id),
      type: String(row.type),
      payload: JSON.parse(String(row.payload)),
      status: row.status as JobStatus,
      priority: Number(row.priority),
      maxAttempts: Number(row.max_attempts),
      backoffBaseMs: Number(row.backoff_base_ms),
      backoffMaxMs: Number(row.backoff_max_ms),
      backoffJitter: Number(row.backoff_jitter),
      attempts: Number(row.attempts),
      availableAt: Number(row.available_at),
      leaseExpiresAt: row.lease_expires_at === null ? null : Number(row.lease_expires_at),
      workerId: row.worker_id === null ? null : String(row.worker_id),
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
