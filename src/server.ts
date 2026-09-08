import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { fileURLToPath } from "node:url";
import { dashboardHtml } from "./dashboard.ts";
import { JobStore } from "./store.ts";
import type { JobStatus } from "./types.ts";

const VALID_STATUSES = new Set<JobStatus>(["queued", "running", "succeeded", "failed", "cancelled"]);

function json(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(body));
}

function streamEvents(request: IncomingMessage, response: ServerResponse, store: JobStore, url: URL): void {
  const headerCursor = Array.isArray(request.headers["last-event-id"])
    ? request.headers["last-event-id"][0]
    : request.headers["last-event-id"];
  let cursor = Number(headerCursor ?? url.searchParams.get("after") ?? 0);
  if (!Number.isInteger(cursor) || cursor < 0) cursor = 0;
  response.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
  });
  response.write("retry: 1000\n\n");

  const publish = () => {
    for (const event of store.listEvents(undefined, cursor, 100)) {
      cursor = event.id;
      response.write(`id: ${event.id}\ndata: ${JSON.stringify(event)}\n\n`);
    }
  };
  publish();
  const poller = setInterval(publish, 500);
  const keepAlive = setInterval(() => response.write(": keep-alive\n\n"), 15_000);
  request.on("close", () => { clearInterval(poller); clearInterval(keepAlive); });
}

async function readJson(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > 1_000_000) throw new Error("request body exceeds 1 MB");
    chunks.push(chunk);
  }
  if (chunks.length === 0) return {};
  const value = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  if (!value || Array.isArray(value) || typeof value !== "object") {
    throw new Error("request body must be a JSON object");
  }
  return value;
}

export function createRelayServer(store: JobStore): Server {
  return createServer(async (request, response) => {
    const url = new URL(request.url ?? "/", "http://relayq.local");
    const method = request.method ?? "GET";

    try {
      if (method === "GET" && url.pathname === "/") {
        response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        return response.end(dashboardHtml);
      }
      if (method === "GET" && url.pathname === "/health") {
        return json(response, 200, { status: "ok" });
      }
      if (method === "GET" && url.pathname === "/stats") {
        return json(response, 200, store.stats());
      }
      if (method === "GET" && url.pathname === "/events") {
        const jobId = url.searchParams.get("jobId") ?? undefined;
        const after = Number(url.searchParams.get("after") ?? 0);
        const limit = Number(url.searchParams.get("limit") ?? 100);
        return json(response, 200, { events: store.listEvents(jobId, after, limit) });
      }
      if (method === "GET" && url.pathname === "/events/stream") {
        return streamEvents(request, response, store, url);
      }
      if (method === "POST" && url.pathname === "/jobs") {
        const body = await readJson(request);
        const job = store.enqueue({
          type: String(body.type ?? ""),
          payload: body.payload,
          priority: body.priority === undefined ? undefined : Number(body.priority),
          maxAttempts: body.maxAttempts === undefined ? undefined : Number(body.maxAttempts),
          delayMs: body.delayMs === undefined ? undefined : Number(body.delayMs),
        });
        return json(response, 201, job);
      }
      if (method === "GET" && url.pathname === "/jobs") {
        const rawStatus = url.searchParams.get("status");
        if (rawStatus && !VALID_STATUSES.has(rawStatus as JobStatus)) {
          return json(response, 400, { error: "invalid status filter" });
        }
        const limit = Number(url.searchParams.get("limit") ?? 100);
        return json(response, 200, { jobs: store.list(rawStatus as JobStatus | undefined, limit) });
      }
      if (method === "GET" && url.pathname === "/dead-letter") {
        const limit = Number(url.searchParams.get("limit") ?? 100);
        return json(response, 200, { jobs: store.list("failed", limit) });
      }

      const jobMatch = url.pathname.match(/^\/jobs\/([^/]+)$/);
      if (method === "GET" && jobMatch) {
        const job = store.get(jobMatch[1]);
        return job ? json(response, 200, job) : json(response, 404, { error: "job not found" });
      }

      const cancelMatch = url.pathname.match(/^\/jobs\/([^/]+)\/cancel$/);
      if (method === "POST" && cancelMatch) {
        return json(response, 200, store.cancel(cancelMatch[1]));
      }

      const requeueMatch = url.pathname.match(/^\/jobs\/([^/]+)\/requeue$/);
      if (method === "POST" && requeueMatch) {
        const body = await readJson(request);
        const delayMs = body.delayMs === undefined ? 0 : Number(body.delayMs);
        return json(response, 200, store.requeueFailed(requeueMatch[1], delayMs));
      }

      const claimMatch = url.pathname.match(/^\/workers\/([^/]+)\/claim$/);
      if (method === "POST" && claimMatch) {
        const body = await readJson(request);
        const leaseMs = body.leaseMs === undefined ? 30_000 : Number(body.leaseMs);
        const job = store.claim(decodeURIComponent(claimMatch[1]), leaseMs);
        return job ? json(response, 200, job) : json(response, 204, null);
      }

      const completeMatch = url.pathname.match(/^\/jobs\/([^/]+)\/complete$/);
      if (method === "POST" && completeMatch) {
        const body = await readJson(request);
        return json(response, 200, store.complete(completeMatch[1], String(body.workerId ?? ""), body.result));
      }

      const heartbeatMatch = url.pathname.match(/^\/jobs\/([^/]+)\/heartbeat$/);
      if (method === "POST" && heartbeatMatch) {
        const body = await readJson(request);
        const leaseMs = body.leaseMs === undefined ? 30_000 : Number(body.leaseMs);
        return json(response, 200, store.renewLease(
          heartbeatMatch[1], String(body.workerId ?? ""), leaseMs,
        ));
      }

      const failMatch = url.pathname.match(/^\/jobs\/([^/]+)\/fail$/);
      if (method === "POST" && failMatch) {
        const body = await readJson(request);
        return json(response, 200, store.fail(
          failMatch[1],
          String(body.workerId ?? ""),
          String(body.error ?? ""),
          body.retryDelayMs === undefined ? 0 : Number(body.retryDelayMs),
        ));
      }

      return json(response, 404, { error: "route not found" });
    } catch (error) {
      const message = error instanceof Error ? error.message : "unexpected error";
      const status = error instanceof SyntaxError ? 400 : message.includes("not found") ? 404 : 409;
      return json(response, status, { error: message });
    }
  });
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  const store = new JobStore(process.env.RELAYQ_DB ?? "relayq.db");
  const server = createRelayServer(store);
  const port = Number(process.env.PORT ?? 8080);
  server.listen(port, () => console.log(`RelayQ listening on http://localhost:${port}`));
  const shutdown = () => server.close(() => { store.close(); process.exit(0); });
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}
