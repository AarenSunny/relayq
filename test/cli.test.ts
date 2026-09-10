import assert from "node:assert/strict";
import { test } from "node:test";
import { executeCli, parseCli } from "../src/cli.ts";

test("submit parses payload and scheduling options", () => {
  assert.deepEqual(parseCli([
    "submit", "sum", "[20,22]", "--priority", "5", "--max-attempts", "4",
  ]), {
    method: "POST",
    path: "/jobs",
    body: { type: "sum", payload: [20, 22], priority: 5, maxAttempts: 4 },
  });
});

test("read and administration commands map to API routes", () => {
  assert.deepEqual(parseCli(["jobs", "--status", "failed", "--limit", "25"]), {
    method: "GET", path: "/jobs?status=failed&limit=25",
  });
  assert.deepEqual(parseCli(["redrive", "--limit", "10", "--delay-ms", "500"]), {
    method: "POST", path: "/admin/redrive", body: { limit: 10, delayMs: 500 },
  });
  assert.deepEqual(parseCli(["purge", "--older-than-ms", "1000", "--include-failed"]), {
    method: "POST",
    path: "/admin/purge",
    body: { olderThanMs: 1_000, limit: undefined, includeFailed: true },
  });
});

test("invalid commands and payloads fail before a request", () => {
  assert.throws(() => parseCli([]), /command is required/);
  assert.throws(() => parseCli(["submit", "sum", "not-json"]), /valid JSON/);
  assert.throws(() => parseCli(["jobs", "--limit", "-1"]), /non-negative/);
});

test("execution applies the base URL, JSON body, and bearer token", async () => {
  let capturedUrl = "";
  let capturedInit: RequestInit | undefined;
  const fetcher = async (input: string | URL | Request, init?: RequestInit) => {
    capturedUrl = String(input);
    capturedInit = init;
    return new Response(JSON.stringify({ accepted: true }), {
      status: 201, headers: { "content-type": "application/json" },
    });
  };

  const result = await executeCli(parseCli(["submit", "sum", "[1,2]"]), {
    baseUrl: "https://relayq.example/api/",
    apiKey: "secret",
    fetcher: fetcher as typeof fetch,
  });
  assert.deepEqual(result, { accepted: true });
  assert.equal(capturedUrl, "https://relayq.example/jobs");
  assert.equal(capturedInit?.method, "POST");
  assert.equal((capturedInit?.headers as Record<string, string>).authorization, "Bearer secret");
  assert.match(String(capturedInit?.body), /"type":"sum"/);
});
