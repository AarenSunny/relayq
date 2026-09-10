import { fileURLToPath } from "node:url";

export interface CliPlan {
  method: "GET" | "POST";
  path: string;
  body?: Record<string, unknown>;
}

export const usage = `RelayQ CLI

Usage:
  npm run cli -- submit <type> <json-payload> [--priority N] [--max-attempts N]
  npm run cli -- jobs [--status STATUS] [--limit N]
  npm run cli -- stats
  npm run cli -- dead-letter [--limit N]
  npm run cli -- pause [reason]
  npm run cli -- resume
  npm run cli -- redrive [--limit N] [--delay-ms N]
  npm run cli -- purge [--older-than-ms N] [--limit N] [--include-failed]

Environment:
  RELAYQ_URL       API base URL (default: http://localhost:8080)
  RELAYQ_API_KEY   Optional bearer token for mutations`;

function option(args: string[], name: string): string | undefined {
  const index = args.indexOf(name);
  if (index === -1) return undefined;
  const value = args[index + 1];
  if (value === undefined || value.startsWith("--")) throw new Error(`${name} requires a value`);
  return value;
}

function numberOption(args: string[], name: string): number | undefined {
  const raw = option(args, name);
  if (raw === undefined) return undefined;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) throw new Error(`${name} must be non-negative`);
  return value;
}

function query(path: string, values: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams();
  for (const [name, value] of Object.entries(values)) {
    if (value !== undefined) params.set(name, String(value));
  }
  const suffix = params.toString();
  return suffix ? `${path}?${suffix}` : path;
}

export function parseCli(args: string[]): CliPlan {
  const [command] = args;
  switch (command) {
    case "submit": {
      const type = args[1];
      if (!type) throw new Error("submit requires a job type");
      if (args[2] === undefined) throw new Error("submit requires a JSON payload");
      let payload: unknown;
      try {
        payload = JSON.parse(args[2]);
      } catch {
        throw new Error("submit payload must be valid JSON");
      }
      return {
        method: "POST",
        path: "/jobs",
        body: {
          type,
          payload,
          priority: numberOption(args, "--priority"),
          maxAttempts: numberOption(args, "--max-attempts"),
        },
      };
    }
    case "jobs":
      return { method: "GET", path: query("/jobs", {
        status: option(args, "--status"), limit: numberOption(args, "--limit"),
      }) };
    case "stats":
      return { method: "GET", path: "/stats" };
    case "dead-letter":
      return { method: "GET", path: query("/dead-letter", { limit: numberOption(args, "--limit") }) };
    case "pause":
      return { method: "POST", path: "/admin/pause", body: { reason: args[1] } };
    case "resume":
      return { method: "POST", path: "/admin/resume" };
    case "redrive":
      return { method: "POST", path: "/admin/redrive", body: {
        limit: numberOption(args, "--limit"), delayMs: numberOption(args, "--delay-ms"),
      } };
    case "purge":
      return { method: "POST", path: "/admin/purge", body: {
        olderThanMs: numberOption(args, "--older-than-ms"),
        limit: numberOption(args, "--limit"),
        includeFailed: args.includes("--include-failed"),
      } };
    default:
      throw new Error(command ? `unknown command: ${command}` : "a command is required");
  }
}

export async function executeCli(
  plan: CliPlan,
  options: { baseUrl?: string; apiKey?: string; fetcher?: typeof fetch } = {},
): Promise<unknown> {
  const baseUrl = options.baseUrl ?? "http://localhost:8080";
  const headers: Record<string, string> = { accept: "application/json" };
  if (options.apiKey) headers.authorization = `Bearer ${options.apiKey}`;
  if (plan.body) headers["content-type"] = "application/json";
  const response = await (options.fetcher ?? fetch)(new URL(plan.path, baseUrl), {
    method: plan.method,
    headers,
    body: plan.body ? JSON.stringify(plan.body) : undefined,
  });
  if (response.status === 204) return { status: "no content" };
  const text = await response.text();
  const result = text ? JSON.parse(text) : {};
  if (!response.ok) throw new Error(result.error ?? `request failed with ${response.status}`);
  return result;
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  try {
    const plan = parseCli(process.argv.slice(2));
    const result = await executeCli(plan, {
      baseUrl: process.env.RELAYQ_URL,
      apiKey: process.env.RELAYQ_API_KEY,
    });
    console.log(JSON.stringify(result, null, 2));
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    console.error(`\n${usage}`);
    process.exitCode = 1;
  }
}
