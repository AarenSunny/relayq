export type JobStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";

export interface Job {
  id: string;
  type: string;
  payload: unknown;
  status: JobStatus;
  priority: number;
  maxAttempts: number;
  attempts: number;
  availableAt: number;
  leaseExpiresAt: number | null;
  workerId: string | null;
  result: unknown | null;
  error: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface NewJob {
  type: string;
  payload?: unknown;
  priority?: number;
  maxAttempts?: number;
  delayMs?: number;
}

export interface QueueStats {
  queued: number;
  running: number;
  succeeded: number;
  failed: number;
  cancelled: number;
  total: number;
}
