# OJP JDBC-Side Throttling — Options Survey

> **Current design:** See [`CLIENT_REACTIVE_THROTTLING_ANALYSIS.md`](./CLIENT_REACTIVE_THROTTLING_ANALYSIS.md)
> for the chosen approach (proactive + reactive modes via `SessionInfo` fields).

---

## What the Server Already Provides

| Component | Capability |
|---|---|
| `ConcurrencyThrottleInterceptor` | Global gRPC gate; rejects with `RESOURCE_EXHAUSTED` |
| `SlotManager` / `AdmissionControlManager` | Per-datasource admission semaphores + queue depth caps |
| `ClientUUID` | Stable JVM identity sent in `ConnectionDetails` on connect |
| `clusterHealth` in `SessionInfo` | UP/DOWN node list already available to driver |

**Gap:** No explicit per-client throttle contract from server to driver today.

---

## Design Goals

1. Off by default; opt-in.
2. Server provides the limit; client enforces it.
3. Bounded memory and fail-fast on the JDBC side.
4. No oscillation; no penalising healthy clients.

---

## Options Considered

### Triggering throttle activation
- **Consecutive count (N rejections):** Simple, predictable; can flap.
- **Sliding-window error rate:** More stable; slightly more complex.
- **Hybrid (chosen direction):** Fast activation via consecutive count, sustained via rate threshold.

### Server → driver signaling
- **gRPC metadata/trailers (chosen):** Low overhead; fits naturally in `SessionInfo` response.
  Already implemented as new `int32` fields on `SessionInfo`.
- Dedicated control stream: near-real-time push, but adds lifecycle complexity. Not needed in v1.
- Implicit via status codes only: too coarse; no fair-share information.

### Per-client limit computation
- **Fair-share formula (chosen):** `ceil(maxAdmission / clientCount) * numOjpServers * 0.9`
  Deterministic, auditable, handles multinode. See design doc for ceiling/headroom rationale.
- **Adaptive `observedPeak` (chosen as reactive mode):** TCP CWND analogy; adapts to real DB load.
- Weighted/latency-aware: useful long-term; deferred.

### Queue scope
- **Per-`connHash` (chosen):** Isolates datasources; prevents one busy pool from blocking another.
- Per-JVM global: simpler but too broad.
- Per-session: highest isolation; highest complexity; not needed in v1.

### Early-fail policy
- **Depth + wait time cap (chosen):** `max queue depth` and `max queue wait` both required.
- Depth only or time only: incomplete; misses the other failure mode.

---

## Stability Controls (All Recommended)

- **Hysteresis:** Different thresholds to activate vs deactivate.
- **AIMD step-limited increase:** Cap increase to `currentLimit + 1` per `SessionInfo` update.
- **Safety floor:** Minimum concurrency (10% of `maxAdmission`) so clients are never fully starved.
- **In-transaction bypass:** Skip counter check when `autoCommit == false` to avoid deadlock-by-timeout.

---

## Rollout Phases

1. **Passive** — compute "would-throttle" decisions, log only, no enforcement.
2. **Static limits** — proactive mode only; `maxAdmission` + `clientCount` formula.
3. **Adaptive limits** — reactive mode (`observedPeak`) added.
4. **Cross-node `clientCount`** — cluster-aggregate counts via gossip (deferred to v2).
