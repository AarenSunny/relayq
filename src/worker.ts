import { setTimeout as delay } from "node:timers/promises";

const baseUrl = process.env.RELAYQ_URL ?? "http://localhost:8080";
const workerId = process.env.WORKER_ID ?? `worker-${process.pid}`;
const pollMs = Number(process.env.POLL_MS ?? 500);

const handlers: Record<string, (payload: unknown) => Promise<unknown>> = {
  sum: async (payload) => {
    if (!Array.isArray(payload) || !payload.every((value) => typeof value === "number")) {
      throw new Error("sum expects an array of numbers");
    }
    return payload.reduce((total, value) => total + value, 0);
  },
  uppercase: async (payload) => String(payload).toUpperCase(),
  sleep: async (payload) => {
    const milliseconds = Number(payload);
    if (!Number.isFinite(milliseconds) || milliseconds < 0 || milliseconds > 20_000) {
      throw new Error("sleep expects milliseconds between 0 and 20000");
    }
    await delay(milliseconds);
    return { sleptMs: milliseconds };
  },
};

async function request(path: string, init?: RequestInit): Promise<Response> {
  return fetch(`${baseUrl}${path}`, {
    ...init,
    headers: { "content-type": "application/json", ...init?.headers },
  });
}

async function run(): Promise<void> {
  console.log(`${workerId} polling ${baseUrl}`);
  while (true) {
    try {
      const response = await request(`/workers/${encodeURIComponent(workerId)}/claim`, {
        method: "POST", body: JSON.stringify({ leaseMs: 30_000 }),
      });
      if (response.status === 204) {
        await delay(pollMs);
        continue;
      }
      if (!response.ok) throw new Error(`claim failed with ${response.status}`);
      const job = await response.json() as { id: string; type: string; payload: unknown };
      const handler = handlers[job.type];
      try {
        if (!handler) throw new Error(`unknown job type: ${job.type}`);
        const result = await handler(job.payload);
        await request(`/jobs/${job.id}/complete`, {
          method: "POST", body: JSON.stringify({ workerId, result }),
        });
        console.log(`completed ${job.id} (${job.type})`);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        await request(`/jobs/${job.id}/fail`, {
          method: "POST",
          body: JSON.stringify({ workerId, error: message, retryDelayMs: 1_000 }),
        });
        console.error(`failed ${job.id}: ${message}`);
      }
    } catch (error) {
      console.error(error instanceof Error ? error.message : error);
      await delay(Math.max(pollMs, 1_000));
    }
  }
}

await run();
