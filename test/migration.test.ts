import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { JobStore } from "../src/store.ts";

test("existing queue databases gain retry-policy columns", () => {
  const directory = mkdtempSync(join(tmpdir(), "relayq-migration-"));
  const filename = join(directory, "queue.db");
  const legacy = new DatabaseSync(filename);
  legacy.exec(`
    CREATE TABLE jobs (
      id TEXT PRIMARY KEY, type TEXT NOT NULL, payload TEXT NOT NULL,
      status TEXT NOT NULL, priority INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0, available_at INTEGER NOT NULL,
      lease_expires_at INTEGER, worker_id TEXT, result TEXT, error TEXT,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    )
  `);
  legacy.close();

  const store = new JobStore(filename);
  const job = store.enqueue({ type: "migrated", backoffBaseMs: 250, backoffMaxMs: 2_000 });
  assert.equal(job.backoffBaseMs, 250);
  assert.equal(job.backoffMaxMs, 2_000);
  assert.equal(job.backoffJitter, 0.2);
  store.close();
  rmSync(directory, { recursive: true, force: true });
});
