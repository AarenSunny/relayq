import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import type { Job, JobStatus, NewJob, QueueStats } from "./types.ts";

type Clock = () => number;
type JobRow = Record<string, unknown>;

const TERMINAL_STATUSES = new Set<JobStatus>(["succeeded", "failed", "cancelled"]);

export class JobStore {
  private readonly db: DatabaseSync;
  private readonly clock: Clock;

  constructor(filename = "relayq.db", clock: Clock = Date.now) {
    this.db = new DatabaseSync(filename);
    this.clock = clock;
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('queued','running','succeeded','failed','cancelled')),
        priority INTEGER NOT NULL,
        max_attempts INTEGER NOT NULL CHECK(max_attempts > 0),
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
    `);
  }

  close(): void {
    this.db.close();
  }

  enqueue(input: NewJob): Job {
    if (!input.type?.trim()) throw new Error("job type is required");
    const priority = input.priority ?? 0;
    const maxAttempts = input.maxAttempts ?? 3;
    const delayMs = input.delayMs ?? 0;
    if (!Number.isInteger(priority)) throw new Error("priority must be an integer");
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new Error("maxAttempts must be a positive integer");
    }
    if (!Number.isFinite(delayMs) || delayMs < 0) throw new Error("delayMs must be non-negative");

    const now = this.clock();
    const id = randomUUID();
    this.db.prepare(`
      INSERT INTO jobs (
        id, type, payload, status, priority, max_attempts, attempts,
        available_at, created_at, updated_at
      ) VALUES (?, ?, ?, 'queued', ?, ?, 0, ?, ?, ?)
    `).run(id, input.type.trim(), JSON.stringify(input.payload ?? null), priority,
      maxAttempts, now + delayMs, now, now);
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
      this.db.exec("COMMIT");
      return this.get(candidate.id);
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  complete(id: string, workerId: string, result: unknown = null): Job {
    const now = this.clock();
    const change = this.db.prepare(`
      UPDATE jobs
      SET status = 'succeeded', result = ?, error = NULL, worker_id = NULL,
          lease_expires_at = NULL, updated_at = ?
      WHERE id = ? AND status = 'running' AND worker_id = ?
    `).run(JSON.stringify(result), now, id, workerId);
    if (change.changes !== 1) throw new Error("job is not running for this worker");
    return this.get(id)!;
  }

  renewLease(id: string, workerId: string, leaseMs = 30_000): Job {
    if (!workerId?.trim()) throw new Error("workerId is required");
    if (!Number.isFinite(leaseMs) || leaseMs < 1_000) throw new Error("leaseMs must be at least 1000");
    const now = this.clock();
    const change = this.db.prepare(`
      UPDATE jobs
      SET lease_expires_at = ?, updated_at = ?
      WHERE id = ? AND status = 'running' AND worker_id = ? AND lease_expires_at > ?
    `).run(now + leaseMs, now, id, workerId.trim(), now);
    if (change.changes !== 1) throw new Error("job lease is missing, expired, or owned by another worker");
    return this.get(id)!;
  }

  fail(id: string, workerId: string, error: string, retryDelayMs = 0): Job {
    if (!error?.trim()) throw new Error("error is required");
    if (!Number.isFinite(retryDelayMs) || retryDelayMs < 0) {
      throw new Error("retryDelayMs must be non-negative");
    }
    const job = this.get(id);
    if (!job || job.status !== "running" || job.workerId !== workerId) {
      throw new Error("job is not running for this worker");
    }

    const now = this.clock();
    const exhausted = job.attempts >= job.maxAttempts;
    this.db.prepare(`
      UPDATE jobs
      SET status = ?, error = ?, worker_id = NULL, lease_expires_at = NULL,
          available_at = ?, updated_at = ?
      WHERE id = ?
    `).run(exhausted ? "failed" : "queued", error.trim(), now + retryDelayMs, now, id);
    return this.get(id)!;
  }

  cancel(id: string): Job {
    const job = this.get(id);
    if (!job) throw new Error("job not found");
    if (TERMINAL_STATUSES.has(job.status)) throw new Error("job is already terminal");
    const now = this.clock();
    this.db.prepare(`
      UPDATE jobs
      SET status = 'cancelled', worker_id = NULL, lease_expires_at = NULL, updated_at = ?
      WHERE id = ?
    `).run(now, id);
    return this.get(id)!;
  }

  requeueFailed(id: string, delayMs = 0): Job {
    if (!Number.isFinite(delayMs) || delayMs < 0) throw new Error("delayMs must be non-negative");
    const job = this.get(id);
    if (!job) throw new Error("job not found");
    if (job.status !== "failed") throw new Error("only failed jobs can be requeued");
    const now = this.clock();
    this.db.prepare(`
      UPDATE jobs
      SET status = 'queued', attempts = 0, available_at = ?, worker_id = NULL,
          lease_expires_at = NULL, result = NULL, error = NULL, updated_at = ?
      WHERE id = ?
    `).run(now + delayMs, now, id);
    return this.get(id)!;
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
    this.db.prepare(`
      UPDATE jobs
      SET status = 'failed', error = 'lease expired after final attempt',
          worker_id = NULL, lease_expires_at = NULL, updated_at = ?
      WHERE status = 'running' AND lease_expires_at <= ? AND attempts >= max_attempts
    `).run(now, now);
    this.db.prepare(`
      UPDATE jobs
      SET status = 'queued', error = 'worker lease expired',
          worker_id = NULL, lease_expires_at = NULL, available_at = ?, updated_at = ?
      WHERE status = 'running' AND lease_expires_at <= ? AND attempts < max_attempts
    `).run(now, now, now);
  }

  private toJob(row: JobRow): Job {
    return {
      id: String(row.id),
      type: String(row.type),
      payload: JSON.parse(String(row.payload)),
      status: row.status as JobStatus,
      priority: Number(row.priority),
      maxAttempts: Number(row.max_attempts),
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
}
