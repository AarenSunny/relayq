import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import type { IncomingMessage, ServerResponse } from "node:http";
import { test } from "node:test";
import { streamEvents } from "../src/server.ts";
import type { JobStore } from "../src/store.ts";
import type { JobEvent } from "../src/types.ts";

const events: JobEvent[] = [1, 2].map((id) => ({
  id,
  jobId: "job-1",
  type: id === 1 ? "enqueued" : "claimed",
  fromStatus: id === 1 ? null : "queued",
  toStatus: id === 1 ? "queued" : "running",
  workerId: id === 1 ? null : "worker-1",
  detail: {},
  createdAt: id,
}));

test("event streams pause at backpressure and resume from the durable cursor", () => {
  const request = new EventEmitter() as EventEmitter & { headers: Record<string, string> };
  request.headers = {};
  const response = new EventEmitter() as EventEmitter & {
    writes: string[];
    writeHead: (status: number, headers: Record<string, string>) => void;
    write: (chunk: string) => boolean;
  };
  response.writes = [];
  response.writeHead = () => {};
  response.write = (chunk) => {
    response.writes.push(chunk);
    return response.writes.length !== 2 && response.writes.length !== 3;
  };

  const cursors: number[] = [];
  const store = {
    listEvents: (_jobId: string | undefined, afterId: number) => {
      cursors.push(afterId);
      return events.filter((event) => event.id > afterId);
    },
  } as unknown as JobStore;

  streamEvents(
    request as IncomingMessage,
    response as unknown as ServerResponse,
    store,
    new URL("http://relayq.local/events/stream"),
  );

  assert.deepEqual(cursors, [0]);
  assert.equal(response.writes.length, 2);
  assert.match(response.writes[1], /^id: 1\n/);

  response.emit("drain");
  assert.deepEqual(cursors, [0, 1]);
  assert.equal(response.writes.length, 3);
  assert.match(response.writes[2], /^id: 2\n/);

  request.emit("close");
  response.emit("drain");
  assert.deepEqual(cursors, [0, 1]);
  assert.equal(response.writes.length, 3);
});
