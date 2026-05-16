# Client-Reactive Throttling in OJP — Deep Analysis

## The Idea

Instead of waiting for the server to explicitly tell the client to slow down,
the JDBC driver could observe server-side rejection exceptions on its own and
self-throttle proactively:

1. Driver receives a semaphore-reject signal from the server
   (`RESOURCE_EXHAUSTED` from `ConcurrencyThrottleInterceptor`,
   or a timeout-translated `ServerOverloadException` propagated as `SQLException`).
2. Driver activates a local concurrency limiter for the affected datasource.
3. Subsequent calls queue locally inside the driver, up to a bounded depth.
4. If the local queue is already full, new requests fail immediately with a clear exception,
   without wasting a round-trip to the server.
5. When server responses improve, driver gradually deactivates the limiter.

This is a purely client-reactive model — no new server-side protocol changes needed.

---

## What the Server Already Sends

| Signal source | What the driver receives today |
|---|---|
| `ConcurrencyThrottleInterceptor` | gRPC `RESOURCE_EXHAUSTED` with message `"Server overloaded: too many concurrent requests"` |
| `SlotManager.acquireSlowSlot` / `acquireFastSlot` returning `false` | `ServerOverloadException` thrown server-side, propagated to driver as `SQLException` |
| `SlotManager.canWaitForSlot` returning `false` (queue depth exceeded) | Same `ServerOverloadException` path |
| Slot acquisition timeout | Same `ServerOverloadException` path |

The driver today does not act on these differently from other `SQLException`s.
Everything needed to trigger client-reactive throttling is already there.

---

## Pros

### 1. Zero server-side changes required
The trigger signal already exists in both forms (`RESOURCE_EXHAUSTED` status code and
`ServerOverloadException`). No protocol change, no new gRPC messages, no server rebuild.

### 2. Earlier protection for the calling application
With pure server-side rejection, each refused request still consumed:
- a gRPC channel slot,
- network round-trip latency (even for fast-fail paths),
- application thread time waiting for the exception.

A local queue absorbs bursts silently and prevents those costs from piling up.

### 3. Reduces server-side queue pressure during recovery
When the server is recovering from overload, rejected clients that immediately retry
can re-flood the server. Client-side queuing creates a natural back-pressure buffer,
giving the server room to drain its own semaphore queues.

### 4. Cleaner application error semantics
Without this, application threads see `RESOURCE_EXHAUSTED` / `ServerOverloadException`
directly. With client-side queuing, most requests simply wait a controlled amount of time,
and only fail if the local queue is exhausted — a shorter, more deterministic wait.

### 5. Complements, not replaces, existing server controls
The server admission control (`SlotManager`, `ConcurrencyThrottleInterceptor`) remains
the authoritative gate. Client throttling is a second shield that reduces unnecessary
server hits. Defense in depth.

### 6. Works without server coordination
For multinode setups where different server nodes may be under different loads,
each driver instance reacts to the node it is currently hitting. No need for cross-node
policy agreement at this stage.

---

## Cons

### 1. Client has an incomplete view of server state
The driver knows it was rejected, but it does not know:
- whether the server is still overloaded or already recovered,
- whether other clients are also backing off (leading to under-utilization),
- whether the overload was transient (e.g., a GC pause) or structural.

This can lead to overly cautious throttling that outlasts the actual problem.

### 2. Risk of over-throttling good clients
If one datasource is under load and another is idle, a naive global driver-side
limiter would throttle all datasources equally. Granularity must be per-datasource
(per `connHash`) or the throttling will be too broad.

### 3. Queue memory footprint in the driver
Each queued request holds a thread (or at minimum a waiting object + monitor lock).
With N app threads and M datasources, worst-case queue memory can be significant.
A hard bounded queue depth mitigates this, but sizing it incorrectly causes its own
problems (too small → fast fail even under light load; too large → out-of-memory risk).

### 4. Throttle deactivation is harder than activation
Activation is easy: see a rejection, enable throttling.
Deactivation requires evidence that the server has recovered — but the driver only
gets that evidence by sending requests through. This creates a "cold restart" problem:
the driver may stay throttled long after the server has recovered.

### 5. Latency amplification in the success path
Once throttling is active, every request incurs queue wait overhead even if it
would have succeeded on the server. This raises p99 latency for all users of that
datasource, not just those that would have been rejected.

### 6. Interactions with session stickiness and XA
OJP session stickiness requires the same driver to reuse the same physical server-side
session. If the driver throttles and queues a request that belongs to an open
transaction, and the transaction times out server-side while the request is sitting in
the driver queue, the client will eventually get a stale-session error when it finally
sends the request. The driver queue timeout must be shorter than the server-side
session/transaction timeout.

### 7. Multinode asymmetry
If the driver is in multinode mode and one server node rejects while another is
available, the right answer is to route to the healthy node, not to queue locally.
Client-reactive throttling must be aware of multinode routing and only activate
per-node, not globally.

---

## Concerns

### C1 — What exception types should be the trigger?

The driver currently receives two distinct signals:
- gRPC `RESOURCE_EXHAUSTED` (ConcurrencyThrottleInterceptor)
- `SQLException` wrapping `ServerOverloadException`

These may not be easy to distinguish from SQL-level errors that happen to use
similar gRPC status codes. The driver needs a clear, reliable classification.
**Concern: Is the current exception taxonomy precise enough to act on safely?**

### C2 — Activation threshold tuning

A single rejection → throttle activation is too aggressive.
A hundred rejections before activation is too slow.
The right number depends on query frequency and server capacity.
**Concern: A fixed default threshold will be wrong for many deployments.
This needs to be configurable, and the default requires careful thought.**

### C3 — Flapping between throttled and unthrottled state

Without hysteresis, the driver will oscillate:
1. Receives rejection → activates throttle.
2. First few requests pass through → deactivates throttle.
3. Server is still stressed → rejects → activates again.
...repeat.

This flapping adds noise to metrics and can cause latency spikes.
**Concern: Hysteresis logic is easy to get wrong and hard to test.
It needs dedicated test coverage with simulated server stress patterns.**

### C4 — Queue depth vs wait timeout — which is the primary control?

Both are needed, but they interact:
- A deep queue with a long timeout can hold threads for minutes.
- A shallow queue with a short timeout fails fast but may not protect the server.

**Concern: The right combination is workload-specific. Operators need guidance on
how to tune these together, not just individual knobs.**

### C5 — Impact on connection pool metrics and health checks

If the application uses connection pool health checks (e.g., Spring Boot actuator),
those checks also go through the driver. A throttled driver that queues health check
pings will make a healthy server appear unhealthy.
**Concern: Health check paths should bypass the driver-side throttle queue,
or at minimum use a dedicated permit bucket.**

### C6 — Exception message clarity

When the driver rejects a call due to local queue saturation, the exception message
must clearly distinguish this from a server rejection. The application and operators
need to know whether to blame the client configuration or the server load.
**Concern: "Connection refused" or a raw SQL exception code is not enough.
The exception must name the source: client-side throttle queue exhausted.**

### C7 — Testing difficulty

Server-side admission control can be tested by starting an OJP server and overwhelming
it. Client-reactive throttling is harder to test deterministically because the trigger
depends on receiving specific exceptions from the server, which requires fine-grained
control of the test server's semaphore state.
**Concern: Without dedicated test infrastructure (e.g., a mock gRPC server that injects
RESOURCE_EXHAUSTED on demand), this feature will be hard to test reliably in CI.**

---

## Suggestions

### S1 — Trigger on `RESOURCE_EXHAUSTED` + classify on consecutive count

Use a sliding window of N consecutive `RESOURCE_EXHAUSTED` / `ServerOverloadException`
responses (not just one) to activate throttling. Start with N=3 as default.
This avoids triggering on transient single-request failures.

### S2 — Scope throttle per `connHash` (datasource), not globally

Each OJP driver instance may manage multiple datasources via different `connHash` values.
The local semaphore/queue should be keyed by `connHash` so a loaded datasource does not
block requests to an idle one.

### S3 — Use a probe request for deactivation

Rather than waiting for N successes under throttle, periodically send one "probe" request
through the full path (bypassing local queue) to test whether the server has recovered.
If the probe succeeds, step up concurrency. This is the AIMD (additive increase,
multiplicative decrease) recovery pattern.

### S4 — Short queue, short timeout

Default queue depth: `max(2, threads_per_datasource / 2)`.
Default queue wait: 2–5 seconds.
These are deliberately conservative: fail fast is better than masking an outage.
Make both configurable.

### S5 — Bypass throttle for in-flight transactions

If a JDBC `Connection` is mid-transaction (`autoCommit = false`), its subsequent
statements must not be queued separately — they must be allowed through immediately
to avoid transaction timeout. Track per-connection transaction state in the driver
and exempt in-transaction calls from the queue.

### S6 — Add a metric + log for every throttle state transition

- Log `WARN` when throttle activates, with: datasource, rejection count, active threads.
- Log `INFO` when throttle deactivates, with: datasource, recovered after N seconds.
- Expose via JMX/OpenTelemetry: throttle-active flag, queue depth, rejected request count.

### S7 — Keep the feature off by default

Introduce behind an opt-in property, e.g. `ojp.jdbc.clientThrottle.enabled=false`.
Operators who want the behavior enable it explicitly. Allows gradual rollout.

---

## Questions

**Q1 — Should the local queue block the calling thread or use a callback/future model?**
Blocking the calling thread is JDBC-natural and the simplest implementation.
But it consumes a thread per queued request. If the calling application uses virtual
threads (Java 21+) this is fine. For platform threads, thread starvation is a real risk.

**Q2 — Should client throttling be per `clientUUID` (JVM) or per `connHash` (datasource)?**
Per `clientUUID` would be simpler, but all datasources on one JVM would share a limit.
Per `connHash` is more precise but requires more state. Both?

**Q3 — Should the driver tell the server it is throttling?**
If the driver sends a hint ("I am backing off"), the server can relax its own queue
limit for this client temporarily. This turns the reactive model into a cooperative
one but requires protocol changes.

**Q4 — How does this interact with the circuit breaker?**
OJP already has a circuit breaker timeout (default 60 seconds). If the server is down,
the circuit opens and requests fail fast. Client-reactive throttling targets a different
scenario: server is up but overloaded. These two features must not conflict —
circuit-open should bypass the client throttle queue entirely.

**Q5 — What happens if multiple threads hit the same throttle at the same time?**
The local queue semaphore must be thread-safe. `java.util.concurrent.Semaphore` with fair
mode is the obvious choice. But fair mode adds latency under contention. Is fairness
important here, or is FIFO within the queue sufficient?

**Q6 — How long should the throttle stay active after the last rejection?**
There needs to be a minimum active window (e.g., 10 seconds) even if no more rejections
arrive, to avoid deactivating too early while the server is still under pressure.
What should the default minimum window be?

**Q7 — Should read-only queries be throttled less aggressively than writes?**
Reads are typically idempotent and retryable. Writes may have side effects and
different latency SLAs. Should there be separate queue limits for reads vs writes?

---

## Opinions

**On the overall idea — Confidence: High (80%)**

This is the right first step for a client-side protection layer. It requires no
server changes, leverages existing exception signals, and provides real value by
absorbing burst traffic before it hits the server repeatedly.
The main risk is incorrect deactivation timing — but that is a tuning problem,
not a fundamental flaw.

**On activation threshold — N=3 consecutive rejections is probably right for a default.**
It is fast enough to react to real overload and robust enough to ignore transient
single-request failures (GC pauses, restart hiccups).

**On queue depth — Err small, not large.**
The entire point of fail-fast is to surface the problem to the application quickly.
A large queue hides overload and makes root-cause analysis harder. Start with a
shallow default (e.g., queue depth = active threads / 2) and let operators increase it.

**On blocking threads — This is the JDBC model and should be embraced.**
JDBC is synchronous by contract. Blocking the calling thread is the expected behavior.
A non-blocking/async implementation would require a fundamentally different API that
most applications are not ready for.

**On deactivation — The probe approach is better than time-based deactivation.**
A fixed cooldown window is fragile. If the server recovers in 5 seconds but the cooldown
is 30 seconds, you are throttling for no reason. Probe-based recovery (AIMD) adapts
to actual server state.

**On scope: per-`connHash` is the right granularity.**
OJP is primarily a proxy for multiple datasources. A global driver throttle would be
the wrong abstraction. The connection hash already distinguishes datasources and is
the right key for the throttle state.

**On the interaction with multinode mode — This is a real gap that needs more thought.**
In multinode mode, a rejection from one node should trigger routing to another node
before activating local throttling. If all nodes reject, then activate throttling.
Implementing client throttling without multinode awareness risks masking node failures
that should trigger failover instead of local queuing.

---

## Relationship to Existing Analysis

This document is a deep dive into one specific variant described in Option Set 2 / Option 3
("Implicit only via status codes") and Option Set 4 ("JDBC Queue and Early-Fail Policies")
of [`JDBC_SERVER_TRIGGERED_THROTTLING_OPTIONS.md`](./JDBC_SERVER_TRIGGERED_THROTTLING_OPTIONS.md).

The key difference from the other options in that document is that this approach is
**fully client-driven** — the server does not need to send explicit throttle instructions.
The client infers the throttle signal from existing rejection exceptions.

---

## Summary

| Dimension | Assessment |
|---|---|
| Implementation effort | Medium (client-only change, but state management is non-trivial) |
| Server changes required | None |
| Risk level | Low–Medium (can be off by default; staged rollout is easy) |
| Main risk | Over-throttling after recovery; interaction with multinode failover |
| Recommended first step | Proof-of-concept per-`connHash` semaphore in the driver, triggered by N consecutive `RESOURCE_EXHAUSTED` signals, with a shallow bounded queue and short wait timeout |
| Needs more design | Deactivation probe logic; multinode awareness; in-transaction bypass |
