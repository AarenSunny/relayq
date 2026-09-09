export type JobStatus = "queued" | "running" | "succeeded" | "failed" | "cancelled";

export interface Job {
  id: string;
  type: string;
  payload: unknown;
  status: JobStatus;
  priority: number;
  maxAttempts: number;
  attempts: number;
  backoffBaseMs: number;
  backoffMaxMs: number;
  backoffJitter: number;
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
  backoffBaseMs?: number;
  backoffMaxMs?: number;
  backoffJitter?: number;
}

export interface QueueStats {
  queued: number;
  running: number;
  succeeded: number;
  failed: number;
  cancelled: number;
  total: number;
}

export type JobEventType =
  | "enqueued"
  | "claimed"
  | "lease_renewed"
  | "lease_expired"
  | "completed"
  | "retry_scheduled"
  | "dead_lettered"
  | "cancelled"
  | "redriven";

export interface JobEvent {
  id: number;
  jobId: string;
  type: JobEventType;
  fromStatus: JobStatus | null;
  toStatus: JobStatus;
  workerId: string | null;
  detail: Record<string, unknown>;
  createdAt: number;
}
