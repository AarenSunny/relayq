import { JobStore } from "../src/store.ts";

let now = Date.now();
const store = new JobStore(":memory:", () => now);

try {
  const job = store.enqueue({ type: "critical-report", payload: { account: "demo" }, maxAttempts: 3 });
  const firstAttempt = store.claim("worker-that-crashes", 1_000)!;
  console.log(`1. ${firstAttempt.workerId} leased ${job.id.slice(0, 8)} (attempt ${firstAttempt.attempts})`);
  console.log("2. The worker disappears without acknowledging the job");

  now += 1_001;
  const recovered = store.claim("replacement-worker", 5_000)!;
  console.log(`3. RelayQ recovered the expired lease and assigned attempt ${recovered.attempts}`);
  store.complete(job.id, "replacement-worker", { reportUrl: "/reports/demo.pdf" });
  console.log("4. The replacement worker completed the original job ID\n");

  console.table(store.listEvents(job.id).map((event) => ({
    event: event.type,
    transition: `${event.fromStatus ?? "—"} → ${event.toStatus}`,
    worker: event.workerId ?? "—",
  })));
} finally {
  store.close();
}
