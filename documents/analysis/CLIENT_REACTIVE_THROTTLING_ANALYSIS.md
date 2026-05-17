# OJP Client-Side Throttling — Design Notes

## Current Design: Two Configurable Modes

Both modes use the same `AtomicInteger` fail-fast counter in the driver (no blocking,
no Semaphore) and AIMD step-limited increase. They differ only in where the limit comes from.

### Protocol additions to `SessionInfo`

```proto
int32 clientCount   = 9;   // distinct JVMs (clientUUID) connected to this connHash on this node
int32 maxAdmission  = 10;  // per-node HikariCP pool size (confirmed: SlotManager.totalSlots = actualPoolSize)
int32 observedPeak  = 11;  // adaptive effective capacity (0 = no failure observed yet)
```

`clusterHealth` already carries the UP/DOWN node list; `numOjpServers` is derived from it.

---

### Proactive Mode

Throttles from the very first `connect()` using the fair-share formula:

```java
int effectiveAdmission = (observedPeak > 0) ? observedPeak : maxAdmission;
int rawLimit = (int) Math.ceil((double) effectiveAdmission / clientCount) * numOjpServers;
int limit = Math.max(1, (int)(rawLimit * 0.9));  // 10% safety headroom
```

**Why ceiling division + 10% headroom:**
Floor division permanently wastes capacity (`floor(20/7)=2` leaves 6 slots idle).
Ceiling slightly over-allocates (`ceil(20/7)=3`, total=21 vs capacity=20), so the 10%
reduction absorbs one stale `clientCount` error.

**Why 10%, not more:** Absorbs exactly one missed client join/leave cycle at typical
client counts without meaningfully reducing steady-state throughput.

**Concurrency control — `AtomicInteger` counter:**

```java
AtomicInteger inFlight = new AtomicInteger(0);
volatile int limit;  // single volatile write on every SessionInfo response

// Acquire (non-blocking, fail-fast):
if (inFlight.incrementAndGet() > limit) {
    inFlight.decrementAndGet();
    throw new SQLException("Client throttle limit exceeded: " + connHash);
}
// Release (always in finally):
inFlight.decrementAndGet();
```

Zero overhead on the happy path. Resizing is one volatile write.

**AIMD step-limited increase:**
- Decrease: apply immediately (fast overload response).
- Increase: `limit = min(newLimit, currentLimit + 1)` per `SessionInfo` update.

Rationale: when 4 out of 8 clients disconnect simultaneously, every remaining client
would burst to the new higher limit at once. Step-limited increase prevents this spike.
Under normal query load, `SessionInfo` arrives every few ms; convergence takes seconds.

**In-transaction bypass:**
When `autoCommit == false`, subsequent statements on that connection skip the `inFlight`
check. Without this, a thread holding an open transaction can block waiting for a permit
while other threads consume all permits, causing the server's transaction timeout to fire
before the thread ever sends its next statement (deadlock-by-timeout).

**Cross-node `clientCount` caveat (v1 documented limitation):**
Each node counts only its own connected clients. In a 2-node cluster where App1 connects
to Node A and App2 to Node B, both nodes report `clientCount = 1`, so both clients
compute `(10/1)*2 = 20` permits — against a real cluster capacity of 20. The formula
over-allocates. The server's own `SlotManager` is the final safety gate. Fix deferred
to v2 (cross-node count sharing).

---

### Reactive Mode (`observedPeak`)

Instead of using the static configured pool size, the server tracks the actual peak
in-flight count just before an admission timeout occurred, and sends it as `observedPeak`.

**How it works (TCP CWND analogy — shrink on loss, grow slowly on clean delivery):**

Server changes in `SlotManager`:
```java
// On wait-timeout: snap observedPeak down to current active count, with 10% floor
int currentActive = activeFastOperations.get() + activeSlowOperations.get();
int floor = Math.max(1, (int)(totalSlots * 0.1));
observedPeak.updateAndGet(prev -> Math.max(floor, Math.min(prev, currentActive)));

// AIMD recovery: every totalSlots*2 successful releases, increment by 1
if (successCount.incrementAndGet() % (totalSlots * 2) == 0) {
    observedPeak.updateAndGet(prev -> Math.min(totalSlots, prev + 1));
}
```

Initialized to `totalSlots`. Sent as `observedPeak` in `SessionInfo`.
Driver uses it in the proactive formula in place of `maxAdmission`.

**Key risks and mitigations:**

| Risk | Mitigation |
|---|---|
| "False floor": single slow query fires timeout at low in-flight count → `observedPeak` collapses | 10% floor (`max(1, totalSlots * 0.1)`) |
| Recovery too slow → server underutilized | K configurable via `ojp.server.admissionControl.observedPeakRecoveryFactor`; default `totalSlots × 2` |
| Recovery too fast → burst risk | AIMD additive increase (+1 per cycle) is inherently slow |
| SQS interaction: slow-lane timeout ≠ total overload | Use `activeSlow + activeFast` total, not per-lane counts |
| Concurrent timeout races updating `observedPeak` | Pre-snapshot `currentActive` before CAS; `updateAndGet` handles the loop |
| Only semaphore wait-timeout is a clean signal | Queue-depth rejections should **not** update `observedPeak` (fires before wait, may reflect low `currentActive` unrelated to capacity) |

---

### Combined Mode (Recommended)

```java
int effectiveLimit = Math.min(proactiveLimit, reactiveLimit);
```

Proactive ensures fair share from day 1. Reactive tightens the limit when the DB
is actually struggling below its configured pool size. Neither alone is sufficient.

---

### Open Questions

**Q1 — Default configuration:** Should combined mode be on by default or off?
Recommended: off by default (new behavior), opt-in via `ojp.jdbc.clientThrottle.mode`.
Options: `off` (default), `proactive`, `reactive`, `combined`.

**Q2 — Proactive-only without `observedPeak`:** Is `clientCount` tracking worth the
server-side overhead if reactive mode is not used? Server needs a
`ConcurrentHashMap<connHash, Set<clientUUID>>` updated on every session create/terminate.
Acceptable overhead for the value it provides?

**Q3 — `observedPeak = 0` sentinel:** Means "no failure observed yet; use `maxAdmission`".
Should a separate boolean field be clearer? Or is `0 = uninitialised` sufficient?

**Q4 — Reactive-only (skip `clientCount`):** Simpler server-side implementation — no
client counting needed. Downside: no fairness guarantee between clients (one greedy
client can saturate the remaining `observedPeak` capacity). Is fairness required in v1?

---

## Summary

| Dimension | Proactive | Reactive (`observedPeak`) | Combined |
|---|---|---|---|
| Server changes | `clientCount` + `maxAdmission` in `SessionInfo` | `observedPeak` + server AIMD in `SlotManager` | Both |
| Activation | First `connect()` | After first admission timeout | First `connect()`, adapts on timeout |
| Limit source | Static config | Observed runtime capacity | `min(proactive, reactive)` |
| Adapts to DB degradation | No | Yes | Yes |
| Fairness between clients | Guaranteed | Not guaranteed | Guaranteed |
| Collapse risk | Low | Mitigated by 10% floor | Low |
| Implementation complexity | Medium | Medium | Medium–High |
| Recommended | Yes | Yes (adaptive environments) | Yes (best overall) |

---

## Dropped Approaches

### Purely client-reactive (no server changes)
Driver observes `RESOURCE_EXHAUSTED` / `ServerOverloadException` and activates a local
semaphore after N consecutive rejections.

**Why dropped:** Incomplete server state view, hard deactivation (requires probe logic or
fixed cooldown), flapping between throttled/unthrottled states, no fairness between clients.
Server already sends the signals but the driver cannot infer the right limit from them.
Superseded by the server-cooperative approach which sends the limit explicitly.

### `java.util.concurrent.Semaphore` for concurrency control
**Why dropped:** No `setPermits()` — resizing requires draining and re-injecting permits.
Complex, race-prone, high overhead. Replaced by `AtomicInteger` counter + `volatile int limit`.

### Floor division (`maxAdmission / clientCount`)
**Why dropped:** Permanently wastes capacity. `floor(20/7) = 2`, 6 slots idle even
at full load. Replaced by ceiling division + 10% safety headroom.
