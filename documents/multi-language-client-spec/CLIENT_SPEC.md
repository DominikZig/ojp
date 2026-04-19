# OJP Multi-Language Client Specification

> **Status:** Draft — April 2026  
> **Scope:** This document defines every aspect that a new OJP client library (in any language other than Java) must implement in order to be fully compatible with an OJP server. It is written language-agnostically; where Java-specific concepts appear they are labelled as the reference implementation only.  
> **Reference implementation:** `ojp-jdbc-driver` module.  
> **Protocol source of truth:** `ojp-grpc-commons/src/main/proto/StatementService.proto` and `echo.proto`.

---

## Table of Contents

1. [gRPC Interface Implementation](#1-grpc-interface-implementation)
2. [URL Parsing](#2-url-parsing)
3. [Client Identity](#3-client-identity)
4. [Connection Establishment and connHash Caching](#4-connection-establishment-and-connhash-caching)
5. [Session Management](#5-session-management)
6. [Session Stickiness](#6-session-stickiness)
7. [Load Balancing](#7-load-balancing)
8. [Failover](#8-failover)
9. [Health Checking](#9-health-checking)
10. [Connection Redistribution on Recovery](#10-connection-redistribution-on-recovery)
11. [Cluster Health Propagation](#11-cluster-health-propagation)
12. [Transaction Management (non-XA)](#12-transaction-management-non-xa)
13. [Savepoints](#13-savepoints)
14. [XA / Distributed Transactions](#14-xa--distributed-transactions)
15. [Statement Execution](#15-statement-execution)
16. [Parameter Type Mapping](#16-parameter-type-mapping)
17. [Temporal Type Handling](#17-temporal-type-handling)
18. [Result Set and Streaming](#18-result-set-and-streaming)
19. [LOB (Large Object) Handling](#19-lob-large-object-handling)
20. [CallResource Protocol](#20-callresource-protocol)
21. [Error and Exception Mapping](#21-error-and-exception-mapping)
22. [Configuration System](#22-configuration-system)
23. [Query Result Caching](#23-query-result-caching)
24. [Security / Transport](#24-security--transport)
25. [DataSource / Integration API](#25-datasource--integration-api)
26. [Testing Coverage](#26-testing-coverage)

---

## 1. gRPC Interface Implementation

### What to implement

The client must implement stubs for every RPC in `StatementService` and `EchoService`.

**`StatementService` RPCs:**

| RPC | Type | Purpose |
|---|---|---|
| `connect` | unary | Open a logical connection and receive `SessionInfo` |
| `executeUpdate` | unary | DML (INSERT / UPDATE / DELETE / DDL) |
| `executeQuery` | server-streaming | SELECT — returns a stream of `OpResult` blocks |
| `fetchNextRows` | unary | Pull the next page of rows for an open result set |
| `createLob` | client-streaming | Upload LOB data to the server in chunks |
| `readLob` | server-streaming | Download LOB data from the server |
| `terminateSession` | unary | Release server-side session state |
| `startTransaction` | unary | Begin an explicit transaction |
| `commitTransaction` | unary | Commit the active transaction |
| `rollbackTransaction` | unary | Roll back the active transaction |
| `callResource` | unary | Generic remote call for metadata, cursor navigation, savepoints |
| `xaStart` | unary | Begin an XA transaction branch |
| `xaEnd` | unary | End an XA transaction branch |
| `xaPrepare` | unary | Prepare an XA transaction branch |
| `xaCommit` | unary | Commit an XA transaction branch |
| `xaRollback` | unary | Roll back an XA transaction branch |
| `xaRecover` | unary | List XIDs of prepared transactions |
| `xaForget` | unary | Forget a heuristically completed transaction |
| `xaSetTransactionTimeout` | unary | Set XA timeout in seconds |
| `xaGetTransactionTimeout` | unary | Get current XA timeout |
| `xaIsSameRM` | unary | Check whether two sessions share a resource manager |

**`EchoService` RPC:**

| RPC | Type | Purpose |
|---|---|---|
| `Echo` | unary | Lightweight heartbeat / connectivity check |

### gRPC channel lifecycle

- One `ManagedChannel` (or equivalent) per server endpoint. Channels are long-lived and shared across all logical connections to that endpoint.
- Channels are created lazily on first connection to an endpoint, or eagerly during initialisation when endpoints are known upfront.
- Use DNS-prefixed targets (`dns:///host:port`) where the gRPC runtime supports it, to allow future SRV-based discovery.
- Blocking stubs are used for synchronous operations; async stubs are required for client-streaming (`createLob`) and server-streaming (`executeQuery`, `readLob`) RPCs.
- Channel shutdown must be graceful (allow in-flight calls to complete) and must be triggered on client shutdown.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`StatementService`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementService.java): the unified interface declaring all RPC methods (`connect`, `executeUpdate`, `executeQuery`, `fetchNextRows`, `createLob`, `readLob`, `terminateSession`, `startTransaction`, `commitTransaction`, `rollbackTransaction`, `callResource`, all XA operations).
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java): the single-node gRPC implementation of `StatementService`; contains the concrete gRPC stub calls and the `grpcChannelOpenAndStubsInitialized()` channel lifecycle method.
> - `ojp-jdbc-driver` — [`MultinodeStatementService`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeStatementService.java): the multinode façade that wraps `StatementServiceGrpcClient` per endpoint with routing, failover, and stickiness.
> - `ojp-grpc-commons` — [`GrpcChannelFactory`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/GrpcChannelFactory.java): `createChannel(host, port)` / `createChannel(target)` — builds `ManagedChannel` instances with plaintext or TLS; handles the `dns:///` prefix and max inbound message size.

---

## 2. URL Parsing

### URL format

```
jdbc:ojp[host:port(datasourceName),host2:port2(datasourceName2)]_actual-db-url
```

All three parts are mandatory:
- `jdbc:ojp` — fixed prefix that identifies the driver.
- `[...]` — bracket-enclosed, comma-separated list of OJP server endpoints. Each endpoint is `host:port`, optionally followed by a datasource name in parentheses: `host:port(dsName)`.
- `_` — separator between the OJP section and the actual database URL.
- `actual-db-url` — the full database URL that the server will use to connect to the real database (e.g., `postgresql://localhost:5432/mydb`).

### Parsing rules

1. **Extract the endpoint list** by capturing everything between `[` and `]`.
2. **Split by comma** to enumerate endpoints. Trim whitespace around each item.
3. **For each endpoint**, split on `:` to obtain host and port. If a `(dsName)` suffix is present, strip it and record the datasource name; default is `"default"`.
4. **Validate** that host is non-empty, port is an integer in `[1, 65535]`, and at least one endpoint is present.
5. **Extract the actual database URL** by removing everything up to and including the first `]_` pattern.
6. **Produce a single-endpoint URL** for use in `ConnectionDetails.url` by replacing `[host1:port1,host2:port2]` with `[chosen_host:chosen_port]` (the endpoint actually selected for the first connection). This stripped URL is forwarded to the server; the server never sees the multinode list.

### Examples

| Input | Endpoints | Datasource names | Actual DB URL |
|---|---|---|---|
| `jdbc:ojp[localhost:10591]_postgresql://db:5432/mydb` | `localhost:10591` | `default` | `postgresql://db:5432/mydb` |
| `jdbc:ojp[a:1059,b:1059]_h2:mem:test` | `a:1059`, `b:1059` | `default`, `default` | `h2:mem:test` |
| `jdbc:ojp[a:1059(web),b:1059(analytics)]_postgresql://db/mydb` | `a:1059`, `b:1059` | `web`, `analytics` | `postgresql://db/mydb` |

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeUrlParser`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeUrlParser.java): `parseServerEndpoints(url, dataSourceNames)` parses the bracket-enclosed endpoint list; `extractActualJdbcUrl(url)` strips the OJP prefix; `replaceBracketsWithSingleEndpoint(url, endpoint)` produces the single-endpoint URL forwarded to the server; `getOrCreateStatementService(url)` is the main entry point that ties parsing to channel creation.
> - `ojp-jdbc-driver` — [`UrlParser`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/UrlParser.java): `parseUrlWithDataSource(url)` handles single-node URL parsing and datasource name extraction.
> - `ojp-jdbc-driver` — [`Driver.connect(url, info)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Driver.java): the JDBC driver entry point that calls both parsers and dispatches to single-node or multinode paths.

---

## 3. Client Identity

### clientUUID

- Generate one random UUID (version 4) when the client library is first loaded or when the process starts. This UUID must remain stable for the entire lifetime of the process.
- Attach `clientUUID` to every `ConnectionDetails` message sent to the server.
- The server uses `clientUUID` to group all sessions from the same client process.
- Do not persist `clientUUID` across process restarts; each new process should generate a fresh UUID.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`ClientUUID`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/ClientUUID.java): `getUUID()` returns the static, process-scoped UUID that is generated once at class-loading time via `UUID.randomUUID()`.

---

## 4. Connection Establishment and connHash Caching

### First connection (cache miss)

1. Build a `ConnectionDetails` message:
   - `url` — the single-endpoint URL extracted during parsing (see §2).
   - `user`, `password` — credentials.
   - `clientUUID` — the stable process UUID (see §3).
   - `properties` — datasource-specific properties from configuration (see §22), including cache rules (see §23).
   - `serverEndpoints` — list of all known server endpoints as `host:port` strings, used by the server for cluster coordination.
   - `clusterHealth` — current cluster health string (see §11); empty on very first connect.
   - `isXA` — `true` for XA connections, `false` otherwise.
2. Call `connect(ConnectionDetails)` on the chosen server. Receive `SessionInfo`.
3. Cache the returned `connHash`, keyed on `url + "|" + user + "|" + password + "|" + datasourceName`. Also store the full `ConnectionDetails` so it can be replayed if the server restarts.
4. Return the received `SessionInfo` to the caller.

### Subsequent connections (cache hit, non-XA only)

When a subsequent connection uses the same credentials:
1. Look up `connHash` from the local cache by the connection key.
2. Build a `SessionInfo` locally without making any gRPC call:
   ```
   SessionInfo {
     connHash:   <cached value>
     clientUUID: <process UUID>
     isXA:       false
   }
   ```
3. Return this locally-built `SessionInfo`. No `sessionUUID` is set yet; it will be assigned by the server when the first SQL operation requires a session (e.g., on `startTransaction`).

**XA connections always call the server** — caching is disabled for XA because each XA connection must create a dedicated pool entry on a specific server.

### Cache invalidation (NOT_FOUND recovery)

When any gRPC call returns `Status.NOT_FOUND`, the server has lost its in-memory pool (e.g., after a restart). Recovery procedure:
1. Remove the cached `connHash → connection-key` entry (but keep the stored `ConnectionDetails`).
2. Re-issue a real `connect()` RPC using the stored `ConnectionDetails`.
3. Cache the new `connHash` returned.
4. Retry the original failed operation once with the new `SessionInfo`.
5. This retry is only safe if the original request had no active `sessionUUID` (no open transaction). If a session was in progress, surface the error to the caller — the transaction state is permanently lost.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.connect()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): orchestrates first-connect vs. cache-hit logic; calls `connectToAllServers()` for the real RPC path and `buildLocalSessionInfo()` for the cache-hit path.
> - `MultinodeConnectionManager.computeConnectionKey()`: builds the `url|user|password|datasourceName` cache key.
> - `MultinodeConnectionManager.invalidateConnHash()`: removes the stale key from `connHashByConnectionKey` on `NOT_FOUND`.
> - `MultinodeConnectionManager.reconnectForConnHash()`: re-issues the real `connect()` RPC using stored `ConnectionDetails` and updates the cache.
> - `MultinodeConnectionManager.buildLocalSessionInfo()`: constructs the in-memory `SessionInfo` for cache-hit connections without an RPC call.

---

## 5. Session Management

### SessionInfo fields

| Field | Type | Meaning |
|---|---|---|
| `connHash` | string | Server-side key identifying which connection pool to use |
| `clientUUID` | string | Client process identity (see §3) |
| `sessionUUID` | string | Server-side session handle; set once a session is established (on `startTransaction`, LOB creation, etc.) |
| `transactionInfo` | `TransactionInfo` | Contains `transactionUUID` and `transactionStatus` (`TRX_ACTIVE`, `TRX_COMMITED`, `TRX_ROLLBACK`) |
| `sessionStatus` | `SessionStatus` | `SESSION_ACTIVE` or `SESSION_TERMINATED` |
| `isXA` | bool | Whether this is an XA session |
| `targetServer` | string | `host:port` of the server this session is pinned to (set by the server, used by the client for stickiness) |
| `clusterHealth` | string | Current cluster health snapshot from the server's perspective |

### Lifecycle rules

- Always propagate the **latest** `SessionInfo` on every outgoing request. The server updates and returns it in every response; the client must replace its local copy with the one returned.
- When the response contains a `sessionUUID` that was absent in the request, register it immediately with the session-stickiness layer (see §6).
- On connection close: call `terminateSession(SessionInfo)`. This is mandatory for releasing server-side resources, especially in multinode deployments where multiple servers may hold pools.
- If `sessionStatus == SESSION_TERMINATED` is received, treat the connection as closed and do not make further calls on it.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`Connection`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Connection.java): holds the mutable `session` field (`SessionInfo`); `close()` calls `terminateSession(session)` and nulls the session; `checkValid()` guards every method against a closed or force-invalidated connection.
> - `ojp-jdbc-driver` — [`MultinodeStatementService.withClusterHealth()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeStatementService.java): enriches outgoing `SessionInfo` with the current cluster health string before each RPC.
> - `MultinodeStatementService.checkAndBindSession()`: updates the stickiness map whenever the server returns a new or changed `sessionUUID`.
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.terminateSession()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): forwards `terminateSession` to every server that received a `connect()` for this `connHash`.

---

## 6. Session Stickiness

### Rule

Once a `sessionUUID` is established, **every subsequent request for that session must go to the same server**. The server embeds `targetServer` (`host:port`) in the `SessionInfo` response; the client must record this binding and honour it.

### Enforcement

- Maintain a thread-safe map of `sessionUUID → host:port`.
- On each request: if `sessionUUID` is set in the local `SessionInfo`, look up the bound server. Route the request to that server only.
- If the bound server is currently marked unhealthy: **raise an error to the caller** — do not silently reroute to another server. The in-flight session state (open transaction, LOB handle, cursor) cannot be migrated and the caller must handle the failure.
- When a session is closed (`terminateSession`), remove the binding from the map and decrement the session count for that server in the load-balancing tracker (see §7).

### Session binding sources

A session binding is created or updated in these cases:
- A response contains a `sessionUUID` that was not present in the request (first assignment).
- The `targetServer` field in a response differs from the currently recorded binding (re-binding after a recovery; log a warning).

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.affinityServer(sessionKey)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): returns the bound server for a `sessionUUID`, or selects a new one via load balancing when no binding exists yet; throws `SQLException` if the bound server is unhealthy.
> - `MultinodeConnectionManager.bindSession(sessionUUID, targetServer)`: records the `sessionUUID → host:port` mapping in `sessionToServerMap`.
> - `MultinodeConnectionManager.getBoundTargetServer(sessionUUID)`: reads the current binding.
> - `MultinodeConnectionManager.unbindSession(sessionUUID)`: removes the binding on session close.
> - `ojp-jdbc-driver` — [`SessionTracker`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/SessionTracker.java): maintains per-server session counts used by the load-balancer and redistribution logic.

---

## 7. Load Balancing

### Server selection strategies

Two strategies must be supported, selectable via configuration (see §22, property `ojp.loadaware.selection.enabled`):

**Least-connections (default, `true`)**  
Select the healthy server with the lowest number of active sessions. Track session counts in a thread-safe counter per server endpoint. Use round-robin as a tie-breaker when all servers have equal counts.

**Round-robin (`false`)**  
Cycle through healthy servers in order using an atomic counter modulo the number of healthy servers.

### When selection runs

Server selection runs on every new connection attempt (non-XA, first `connect()`) and on every XA `connect()`. Once a session is assigned a server (via session stickiness), selection does not run again for that session.

### Healthy server filter

Only servers whose `isHealthy() == true` are eligible for selection. If no healthy servers exist, raise a connection error.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.selectHealthyServer()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): the entry point that dispatches to one of the two strategies based on config.
> - `MultinodeConnectionManager.selectByLeastConnections(healthyServers)`: picks the server with the lowest active-session count; falls back to round-robin on a tie.
> - `MultinodeConnectionManager.selectByRoundRobin(healthyServers)`: atomically increments `roundRobinCounter` and selects `servers[counter % size]`.
> - `ojp-jdbc-driver` — [`ServerEndpoint`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/ServerEndpoint.java): holds `isHealthy`, `lastFailureTime`, host, and port state for each endpoint.

---

## 8. Failover

### What triggers failover

Connection-level gRPC errors indicate that the server is unreachable. The following gRPC status codes are treated as connectivity failures:

| Status code | Trigger failover? |
|---|---|
| `UNAVAILABLE` | Yes |
| `DEADLINE_EXCEEDED` | Yes |
| `UNKNOWN` (with "connection" in message) | Yes |
| `INTERNAL` with SQL metadata trailers | **No** — this is a database-level error |
| `INTERNAL` without SQL metadata trailers | Yes — treated as a transport-level failure |
| `NOT_FOUND` | **No** — triggers reconnect (see §4), not failover |
| `RESOURCE_EXHAUSTED` (pool exhaustion) | **No** — surface to caller |
| `CANCELLED` | **No** — this is a client-initiated cancellation signal; must never mark a server unhealthy |
| Any `SQLException` from server | **No** |

### Failover procedure

1. When a connectivity error is detected on a server:
   a. Capture whether the server was previously healthy (`wasHealthy`).
   b. Mark the server unhealthy (`isHealthy = false`), recording the failure timestamp.
   c. Log the failure.
   d. If this is a genuine healthy → unhealthy transition (`wasHealthy == true`), submit `pushClusterHealthToAllHealthyServers()` asynchronously to the background scheduler so surviving servers resize their pools immediately. The push is submitted (not called inline) to avoid blocking the query thread.
   e. Shut down the gRPC channel for the failed server gracefully (allow in-flight calls to drain, then discard).
2. Select the next healthy server (using the configured strategy, excluding the failed server and any already attempted in this retry cycle).
3. Retry the operation on the new server.
4. If all servers have been attempted and all failed, raise a connection error to the caller.
5. Retry attempts and delay between retries are configurable (see §22, properties `ojp.multinode.retry.attempts` and `ojp.multinode.retry.delay`).

### What must NOT trigger failover

- Database errors (bad SQL, constraint violations, auth failures) — surface directly to caller.
- Pool exhaustion — surface directly to caller.
- Session-invalidation errors (session lost after server failure) — surface directly to caller; the caller must re-establish the session.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`GrpcExceptionHandler.isConnectionLevelError()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/GrpcExceptionHandler.java): classifies a `StatusRuntimeException` as a connectivity failure vs. a SQL/business error. `CANCELLED` is explicitly **excluded** (it is a client-side signal, not a server failure).
> - `GrpcExceptionHandler.isPoolNotFoundException()`: returns `true` for `NOT_FOUND`, triggering reconnect rather than failover.
> - `GrpcExceptionHandler.isSessionInvalidationError()`: returns `true` when the server indicates the session is gone.
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.handleServerFailure(endpoint, exception)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): marks the server unhealthy, timestamps the failure, shuts down the gRPC channel gracefully, and — only on a genuine healthy→unhealthy transition (`wasHealthy == true`) — submits `pushClusterHealthToAllHealthyServers()` to the background `healthCheckScheduler` so the cluster health push does not block the query thread.
> - `MultinodeStatementService.executeOpResultWithSessionStickinessAndBinding()`: the retry loop that catches `StatusRuntimeException`, calls `isConnectionLevelError`, drives the server-selection retry cycle, and calls `handleServerFailure` on each failed attempt.

---

## 9. Health Checking

### Background task

Run a periodic background task that checks server health. The task must:
- Run at a configurable fixed interval (property `ojp.health.check.interval`, default 5 000 ms).
- Not block the main execution thread.
- Be a daemon task so it does not prevent process shutdown.

### Two-phase check

**Phase 1 — probe healthy servers (detect newly failed servers)**  
Run when there are active XA sessions (`sessionToServerMap` is non-empty) **or** cached non-XA connection details (`connectionDetailsByConnHash` is non-empty). This dual guard ensures both XA and non-XA workloads trigger early failure detection. The guard prevents spurious "no healthy servers" errors before any connection has been established. For each currently healthy server that passes the guard, send a probe call. If the call fails, mark the server unhealthy and call the server-failure handler (see §8 and §11).

**Phase 2 — probe unhealthy servers (detect recovery)**  
For each currently unhealthy server, check if enough time has passed since the last failure (property `ojp.health.check.threshold`, default 5 000 ms). If so, probe the server. If the probe succeeds, run recovery (see §10).

### Health probe modes

| Mode | How to probe | When to use |
|---|---|---|
| Heartbeat (lightweight) | Send `connect()` with empty `url`, `user`, `password` — any response means transport is up | Default |
| Full validation | Send `connect()` with real credentials; on success, call `terminateSession` on the returned session | When heartbeat is insufficient |

### Configurable properties (see §22)

| Property | Default | Meaning |
|---|---|---|
| `ojp.health.check.interval` | 5000 ms | How often the check runs |
| `ojp.health.check.threshold` | 5000 ms | How long to wait before re-probing an unhealthy server |
| `ojp.health.check.timeout` | 5000 ms | Maximum time for a single probe call |
| `ojp.redistribution.enabled` | `true` | Whether to run the periodic health checker at all |

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.performHealthCheck()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): the scheduled task body; implements the two-phase check. Phase 1 fires when `!sessionToServerMap.isEmpty() || !connectionDetailsByConnHash.isEmpty()` (XA sessions OR non-XA cached connections). Phase 1 failure calls `pushClusterHealthToAllHealthyServers()` inline on the health-check thread. Phase 2 calls `reinitializePoolOnRecoveredServer()` before `markHealthy()`, then pushes cluster health.
> - `ojp-jdbc-driver` — [`HealthCheckValidator.validateServer(endpoint)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/HealthCheckValidator.java): performs a single lightweight probe; `validateServer(endpoint, connectionDetails)` performs the full-validation probe with real credentials followed by `terminateSession`.
> - `ojp-jdbc-driver` — [`HealthCheckConfig`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/HealthCheckConfig.java): POJO holding `healthCheckIntervalMs`, `healthCheckThresholdMs`, `healthCheckTimeoutMs`, and `redistributionEnabled`.
> - `MultinodeConnectionManager` constructor: schedules `performHealthCheck` on a `ScheduledExecutorService` at the configured interval.

---

## 10. Connection Redistribution on Recovery

### Goal

When a failed server comes back online, rebalance client-side connections so that the recovered server receives its fair share of traffic again. This avoids all load remaining on the servers that survived the outage.

### Procedure on recovery

1. **Reinitialize pools on the recovered server first** (before marking healthy). Check whether any non-XA connections have been cached (`connectionDetailsByConnHash` is non-empty). If so, for every cached `connHash`/`ConnectionDetails` pair, call `connect()` on the recovered server so it creates the HikariCP pool immediately. This closes the NOT_FOUND window that would otherwise exist between marking the server healthy and the first SQL call reaching it. Only after all pools are pre-created, proceed to step 2.
2. Mark the server healthy (`endpoint.markHealthy()`).
3. Push the updated cluster health string to all healthy servers (see §11) so they can resize their pools.
4. If redistribution is enabled (`ojp.redistribution.enabled = true`), begin rebalancing:
   - Determine the ideal share: `totalConnections / numberOfHealthyServers`.
   - Identify over-loaded servers (connections > ideal share).
   - Close a fraction of idle connections on over-loaded servers so they are returned to the pool, then re-opened — the client's load-balancing layer will route the re-opens to the least-loaded server (including the recovered one).
   - Honour the configurable fraction (`ojp.redistribution.idleRebalanceFraction`, default 1.0) and max-close-per-cycle limit (`ojp.redistribution.maxClosePerRecovery`, default 100).

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.reinitializePoolOnRecoveredServer(recoveredServer)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): runs only when `!connectionDetailsByConnHash.isEmpty()`; iterates the map and calls `connect()` on the recovered server for each stored `ConnectionDetails`; always called **before** `endpoint.markHealthy()` to eliminate the NOT_FOUND window.
> - `ojp-jdbc-driver` — [`ConnectionRedistributor.rebalance(recoveredServers, allHealthyServers)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/ConnectionRedistributor.java): closes a fraction of idle connections on over-loaded servers for non-XA mode.
> - `ojp-jdbc-driver` — [`XAConnectionRedistributor.rebalance(recoveredServers, allHealthyServers)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/XAConnectionRedistributor.java): equivalent redistribution for XA connections.
> - `ojp-jdbc-driver` — [`ConnectionTracker`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/ConnectionTracker.java): maintains the per-server `Connection` list consulted by `ConnectionRedistributor`.

---

## 11. Cluster Health Propagation

### Cluster health string format

```
host1:port1(UP);host2:port2(DOWN);host3:port3(UP)
```

Each semicolon-separated segment is `host:port(STATUS)` where status is `UP` or `DOWN`.

### Client responsibilities

- **Build** the cluster health string from local server endpoint health state before every `connect()` call and before every operation that carries a `SessionInfo` (by populating `SessionInfo.clusterHealth`).
- **Consume** the cluster health string returned in `SessionInfo.clusterHealth` on every response. Update local endpoint health states accordingly: mark endpoints `DOWN` as unhealthy and endpoints `UP` as healthy (if they were previously failed).
- **Proactively push** the updated cluster health to all currently healthy servers whenever the topology changes (a server fails or recovers). This push happens via two independent triggers — both are necessary:

  **Trigger 1 — health-check thread**: When `performHealthCheck()` detects a newly failed server or a recovered server, it calls `pushClusterHealthToAllHealthyServers()` inline on the health-check thread. This covers the case when no SQL traffic is active at the moment of the topology change.

  **Trigger 2 — query thread**: When a SQL query thread detects server failure via `handleServerFailure()`, it submits `pushClusterHealthToAllHealthyServers()` to the background scheduler. This covers the race where the query thread marks the server unhealthy before the health checker runs (the health checker's Phase 1 loop would then skip the already-unhealthy server and never push). The push is submitted asynchronously to avoid blocking the query thread.

  The push is done by calling `connect()` on each healthy server with a `ConnectionDetails` whose `clusterHealth` field contains the new topology string. The server uses this to resize its pool immediately, regardless of whether any SQL is in flight.

### Generation

```
generate_cluster_health(endpoints):
    return ";".join(
        f"{ep.host}:{ep.port}({'UP' if ep.is_healthy else 'DOWN'})"
        for ep in endpoints
    )
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.generateClusterHealth()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): builds the semicolon-delimited health string from `serverEndpoints`.
> - `MultinodeConnectionManager.pushClusterHealthToAllHealthyServers()`: calls `connect()` on every healthy server with the new cluster health embedded in `ConnectionDetails`; only runs when `!connectionDetailsByConnHash.isEmpty()` (no-op until the first real connection is established).
> - `MultinodeConnectionManager.handleServerFailure()` (Trigger 2): submits `pushClusterHealthToAllHealthyServers()` to `healthCheckScheduler` on a genuine healthy→unhealthy transition so query threads are never blocked by the push.
> - `MultinodeConnectionManager.performHealthCheck()` (Trigger 1): calls `pushClusterHealthToAllHealthyServers()` directly (inline on health-check thread) after marking a server DOWN or after a recovered server is marked healthy.
> - `MultinodeStatementService.withClusterHealth(sessionInfo)`: attaches the current health string to an outgoing `SessionInfo` before each RPC (reactive secondary path).

---

## 12. Transaction Management (non-XA)

### autoCommit semantics

- Default state is `autoCommit = true`.
- When `autoCommit` is switched **off** (`false`), immediately call `startTransaction(SessionInfo)`. Store the returned `SessionInfo` (which now contains a `transactionUUID` and `TRX_ACTIVE` status).
- When `autoCommit` is switched **on** (`true`) while a transaction is active (`TRX_ACTIVE`), immediately call `commitTransaction(SessionInfo)` to commit the pending work.
- In `autoCommit = false` mode, no `startTransaction` call is needed before each SQL statement — the server tracks the open transaction via `sessionUUID`.

### Commit and rollback

| Client call | gRPC call | Condition |
|---|---|---|
| `commit()` | `commitTransaction(SessionInfo)` | Only when `autoCommit == false` |
| `rollback()` | `rollbackTransaction(SessionInfo)` | Only when `autoCommit == false` |

Always replace the local `SessionInfo` with the one returned by these calls.

### Transaction isolation

- Set isolation level via `callResource` with `CallType.CALL_SET`, resource name `"TransactionIsolation"`, and the integer isolation level as parameter.
- Get isolation level via `callResource` with `CallType.CALL_GET`, resource name `"TransactionIsolation"`.
- The isolation level must be reset to the default after each logical connection is returned to a pool (if the client integrates with a connection pool).

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`Connection.setAutoCommit(boolean)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Connection.java): calls `commitTransaction` when switching on and `startTransaction` when switching off; updates the local `session` field from each response.
> - `Connection.commit()` / `Connection.rollback()`: delegate to `statementService.commitTransaction(session)` / `rollbackTransaction(session)` when `autoCommit == false`.
> - `Connection.close()`: calls `terminateSession(session)` unconditionally.
> - `Connection.setTransactionIsolation(level)` / `getTransactionIsolation()`: forwarded via `callProxy(CallType.CALL_SET/GET, "TransactionIsolation", ...)`.
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient.startTransaction()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java) / `commitTransaction()` / `rollbackTransaction()`: the single-node gRPC calls.

---

## 13. Savepoints

Savepoints are implemented through the `callResource` protocol using `ResourceType.RES_SAVEPOINT`.

### Creating a savepoint

Call `callResource` with:
- `resourceType = RES_SAVEPOINT`
- `target.callType = CALL_SET` (or `CALL_INSERT` for named savepoints, depending on server version)
- `target.resourceName = "Savepoint"`
- `target.params = [savepointName]` if named; empty for anonymous savepoints.

The response contains the savepoint UUID in `CallResourceResponse.resourceUUID`.

### Rolling back to a savepoint

Call `callResource` with:
- `resourceType = RES_SAVEPOINT`
- `resourceUUID = <savepoint UUID from creation>`
- `target.callType = CALL_ROLLBACK`

### Releasing a savepoint

Call `callResource` with:
- `resourceType = RES_SAVEPOINT`
- `resourceUUID = <savepoint UUID>`
- `target.callType = CALL_RELEASE`

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`Connection.setSavepoint()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Connection.java) / `setSavepoint(name)`: calls `callProxy` with `CALL_SET`, `"Savepoint"`, and the optional name; wraps the returned resource UUID in a [`Savepoint`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Savepoint.java) object.
> - `Connection.rollback(Savepoint)`: calls `callProxy` with `CALL_ROLLBACK`, `"Savepoint"`, and the savepoint's resource UUID.
> - `Connection.releaseSavepoint(Savepoint)`: calls `callProxy` with `CALL_RELEASE`.

---

## 14. XA / Distributed Transactions

### Overview

XA support maps the standard XA resource manager protocol to gRPC RPCs. XA connections are always pinned to a single server (§6).

### XA transaction lifecycle

```
xaStart(XaStartRequest)       -- Begin branch; safe to retry on connection error
xaEnd(XaEndRequest)           -- End branch; NEVER retry after this point
xaPrepare(XaPrepareRequest)   -- Two-phase prepare; returns XA_OK or XA_RDONLY
xaCommit(XaCommitRequest)     -- Commit (onePhase=true for one-phase optimisation)
xaRollback(XaRollbackRequest) -- Roll back the branch
xaRecover(XaRecoverRequest)   -- List in-doubt XIDs (for recovery after crash)
xaForget(XaForgetRequest)     -- Forget a heuristically completed branch
```

### Xid encoding (XidProto)

| Field | Type | Meaning |
|---|---|---|
| `formatId` | int32 | Transaction format ID |
| `globalTransactionId` | bytes | Global transaction ID (up to 64 bytes) |
| `branchQualifier` | bytes | Branch qualifier (up to 64 bytes) |

### Retry policy

- **`xaStart`** only: retry on connection-level errors (see §8). No transaction state exists yet so retrying is safe.
- **All other XA operations**: do not retry automatically. Surface failures to the caller's transaction manager.

### XA session binding

On the response to `xaStart`, record the `sessionUUID → targetServer` binding (§6). All subsequent XA operations for this branch must go to the same server. If that server is unavailable, raise `XAException(XAER_RMFAIL)`.

### Timeout

- `xaSetTransactionTimeout(seconds)` and `xaGetTransactionTimeout()` are straightforward pass-throughs to the server.
- `xaIsSameRM` checks whether two `SessionInfo` objects originate from the same resource manager (same server).

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`OjpXAResource`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/xa/OjpXAResource.java): implements `XAResource`; all 10 lifecycle methods (`start`, `end`, `prepare`, `commit`, `rollback`, `recover`, `forget`, `setTransactionTimeout`, `getTransactionTimeout`, `isSameRM`); contains the `xaStart` retry loop and the `toXidProto` / `fromXidProto` conversion helpers.
> - `ojp-jdbc-driver` — [`OjpXAConnection`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/xa/OjpXAConnection.java): creates the XA-mode `StatementService` connection (always calling the server, never cache-hit) and vends `OjpXAResource`.
> - `ojp-jdbc-driver` — [`OjpXADataSource`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/xa/OjpXADataSource.java): entry point for XA; calls `MultinodeConnectionManager.connectXA()` to pin the session to a single server.
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient.xaStart()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java) … `xaIsSameRM()`: the 10 single-node gRPC stub wrappers.

---

## 15. Statement Execution

### Three statement types

**Plain Statement**  
Execute arbitrary SQL strings without parameters. Maps to `executeUpdate` or `executeQuery` with an empty `parameters` list.

**Prepared Statement**  
Pre-compiled SQL with positional parameters (`?` placeholders). Parameters are accumulated locally and sent with the SQL in a single `StatementRequest`. Assign and track a `statementUUID` (a random UUID per prepared statement instance) for server-side resource management.

**Callable Statement**  
Stored-procedure calls with IN, OUT, and INOUT parameters. The stored-procedure call string is prepared on the server via `callResource` with `CallType.CALL_PREPARE` first. The returned `resourceUUID` becomes the Callable Statement handle. Parameters are registered by index and type, and OUT/INOUT values are retrieved from `CallResourceResponse.values` after execution.

### StatementRequest structure

```
StatementRequest {
    session:       SessionInfo   // current session
    sql:           string        // the SQL (or call string)
    parameters:    ParameterProto[]  // indexed parameters
    statementUUID: string        // UUID for this statement (for resource tracking)
    properties:    PropertyEntry[]   // optional per-statement properties
}
```

### Execution routing

- Use `executeUpdate` for INSERT / UPDATE / DELETE / DDL — returns `OpResult` with `type = INTEGER` containing affected row count.
- Use `executeQuery` for SELECT — returns a server-streaming response. Consume the first `OpResult` to get the initial batch; call `fetchNextRows` for subsequent pages (see §18).
- After any execution, update the local `SessionInfo` from the `OpResult.session` field.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`Statement`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Statement.java): `executeQuery(sql)` → `statementService.executeQuery(...)`; `executeUpdate(sql)` → `statementService.executeUpdate(...)`; holds `statementUUID` (assigned lazily); `execute(sql)` handles the dual-result case.
> - `ojp-jdbc-driver` — [`PreparedStatement`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/PreparedStatement.java): accumulates parameters in a `SortedMap<Integer, Parameter>`; `executeQuery()` and `executeUpdate()` pass the full param map to `statementService`; all 28 `setXxx(index, value)` methods map to the corresponding `ParameterType` (see §16).
> - `ojp-jdbc-driver` — [`CallableStatement`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/CallableStatement.java): issues `callResource(CALL_PREPARE)` on construction; retrieves OUT/INOUT values via `callResource(CALL_EXECUTE)` after execution.

---

## 16. Parameter Type Mapping

### ParameterProto

Each parameter is represented as:
```
ParameterProto {
    index:  int32              // 1-based parameter position
    type:   ParameterTypeProto // one of the 28 type codes
    values: ParameterValue[]   // one value for normal params; multiple for array params
}
```

### ParameterTypeProto values and their ParameterValue encoding

| Proto enum value | Wire field in `ParameterValue` | Notes |
|---|---|---|
| `PT_NULL` | `is_null = true` | Explicit null |
| `PT_BOOLEAN` | `bool_value` | |
| `PT_BYTE` | `int_value` | Clamp to byte range |
| `PT_SHORT` | `int_value` | Clamp to short range |
| `PT_INT` | `int_value` | |
| `PT_LONG` | `long_value` | |
| `PT_FLOAT` | `float_value` | |
| `PT_DOUBLE` | `double_value` | |
| `PT_BIG_DECIMAL` | `string_value` | Encode as `"<unscaledInteger> <scale>"` — see §16.1 |
| `PT_STRING` | `string_value` | |
| `PT_BYTES` | `bytes_value` | Raw bytes |
| `PT_DATE` | `date_value` | `google.type.Date` (year/month/day, no timezone) |
| `PT_TIME` | `time_value` | `google.type.TimeOfDay` (hours/minutes/seconds/nanos) |
| `PT_TIMESTAMP` | `timestamp_value` | `TimestampWithZone` — see §17 |
| `PT_ASCII_STREAM` | `bytes_value` | ASCII bytes |
| `PT_UNICODE_STREAM` | `bytes_value` | Unicode bytes |
| `PT_BINARY_STREAM` | `bytes_value` | Binary bytes |
| `PT_OBJECT` | varies | Best-effort mapping to one of the concrete value types |
| `PT_CHARACTER_READER` | `string_value` | Contents of the character stream |
| `PT_REF` | `string_value` | REF value as string |
| `PT_BLOB` | (LOB reference UUID) | Create LOB first (§19); then pass UUID as `string_value` |
| `PT_CLOB` | (LOB reference UUID) | Same as BLOB |
| `PT_ARRAY` | `int_array_value` / `long_array_value` / `string_array_value` | Use the typed array message matching element type |
| `PT_URL` | `url_value` (StringValue) | `URL.toExternalForm()` — presence-aware; unset = null |
| `PT_ROW_ID` | `rowid_value` (StringValue) | Base64-encoded bytes of the RowId — presence-aware |
| `PT_N_STRING` | `string_value` | Same wire format as PT_STRING |
| `PT_N_CHARACTER_STREAM` | `string_value` | Contents of the NCharacter stream |
| `PT_N_CLOB` | (LOB reference UUID) | Same as CLOB |
| `PT_SQL_XML` | `string_value` | XML content as string |

#### 16.1 BigDecimal encoding

BigDecimal is serialised as a space-separated string: `"<unscaledInteger> <scale>"`.

- `unscaledInteger`: the decimal string representation of the unscaled value (may be negative), e.g. `"-12345"`.
- `scale`: integer scale (number of decimal places), e.g. `2`.
- Full value = `unscaledInteger × 10^(-scale)`.

Example: `BigDecimal("123.45")` → `"12345 2"`.

> **Note:** A separate binary wire format is documented in `documents/protocol/BIGDECIMAL_WIRE_FORMAT.md` for contexts where binary efficiency is needed.

#### 16.2 Presence-aware fields

`url_value`, `rowid_value`, `uuid_value`, `biginteger_value`, `rowidlifetime_value` are all `google.protobuf.StringValue` (a wrapper message). An absent (unset) wrapper means SQL NULL. An empty string inside the wrapper is a valid non-null value.

> **Reference implementation:**
> - `ojp-grpc-commons` — [`ProtoConverter.toProto(Parameter)`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/ProtoConverter.java): converts a host-language `Parameter` object to `ParameterProto`; `fromProto(ParameterProto)` is the inverse.
> - `ProtoConverter.toParameterValue(Object value)`: the central dispatcher that routes each Java type to the correct `ParameterValue` oneof field.
> - `ProtoConverter.fromParameterValue(ParameterValue, ParameterType)`: decodes a wire value back to a Java object using both the value and the declared type as hints.
> - `ojp-grpc-commons` — [`ProtoTypeConverters`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/ProtoTypeConverters.java): `uuidToProto(UUID)` / `uuidFromProto(StringValue)`, `urlToProto(URL)` / `urlFromProto(StringValue)`, `rowIdToProto(RowId)` / `rowIdBytesFromProto(StringValue)` — handles the presence-aware `StringValue` wrappers for UUID, URL, and RowId.
> - `ojp-grpc-commons` — [`BigDecimalWire`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/BigDecimalWire.java): `writeBigDecimal` / `readBigDecimal` — binary wire encoding for BigDecimal (also see `documents/protocol/BIGDECIMAL_WIRE_FORMAT.md`).

---

## 17. Temporal Type Handling

### TimestampWithZone

Timestamps are transmitted as:

```
TimestampWithZone {
    instant:       google.protobuf.Timestamp  // seconds + nanos since Unix epoch (UTC)
    timezone:      string                      // IANA zone ID or UTC offset (e.g., "Europe/Rome", "+02:00")
    original_type: TemporalType               // preserves the caller's original type
}
```

### TemporalType enum

| Value | Original type |
|---|---|
| `TEMPORAL_TYPE_UNSPECIFIED` | Default / unknown |
| `TEMPORAL_TYPE_TIMESTAMP` | `java.sql.Timestamp` |
| `TEMPORAL_TYPE_CALENDAR` | `java.util.Calendar` |
| `TEMPORAL_TYPE_OFFSET_DATE_TIME` | `java.time.OffsetDateTime` |
| `TEMPORAL_TYPE_LOCAL_DATE_TIME` | `java.time.LocalDateTime` |
| `TEMPORAL_TYPE_INSTANT` | `java.time.Instant` |
| `TEMPORAL_TYPE_LOCAL_DATE` | `java.time.LocalDate` |
| `TEMPORAL_TYPE_LOCAL_TIME` | `java.time.LocalTime` |
| `TEMPORAL_TYPE_OFFSET_TIME` | `java.time.OffsetTime` |

### Encoding rules

1. Convert the host-language datetime value to an absolute UTC instant (seconds + nanoseconds since the Unix epoch).
2. Record the IANA timezone or UTC offset string.
3. Set `original_type` to the closest matching `TemporalType` enum value.

### Decoding rules

On the receiving side, use `original_type` to reconstruct the correct host-language type:
- `TEMPORAL_TYPE_LOCAL_DATE_TIME` / `TEMPORAL_TYPE_TIMESTAMP` → local datetime in the client's timezone.
- `TEMPORAL_TYPE_OFFSET_DATE_TIME` → datetime with offset reconstructed from the `timezone` string.
- `TEMPORAL_TYPE_INSTANT` → UTC instant.
- `TEMPORAL_TYPE_LOCAL_DATE` → date only (no time component).
- `TEMPORAL_TYPE_LOCAL_TIME` / `TEMPORAL_TYPE_OFFSET_TIME` → time-only value with or without offset.

**Date-only values** use `google.type.Date` (year, month, day — no timezone).  
**Time-only values** use `google.type.TimeOfDay` (hours, minutes, seconds, nanos — no timezone).

### Timezone requirement

The OJP server must always run with `user.timezone=UTC`. Client libraries should also normalise to UTC when encoding timestamps, using the `timezone` field to carry the original zone for faithful reconstruction.

> **Reference implementation:**
> - `ojp-grpc-commons` — [`TemporalConverter`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/TemporalConverter.java): the definitive encoding/decoding reference for all temporal types:
>   - `toTimestampWithZone(java.sql.Timestamp, ZoneId)` / `fromTimestampWithZone(TimestampWithZone)` — `Timestamp` ↔ `TimestampWithZone`.
>   - `calendarToTimestampWithZone(Calendar)` / `timestampWithZoneToCalendar(TimestampWithZone)` — `Calendar`.
>   - `offsetDateTimeToTimestampWithZone` / `timestampWithZoneToOffsetDateTime` — `OffsetDateTime`.
>   - `localDateTimeToTimestampWithZone` / `timestampWithZoneToLocalDateTime` — `LocalDateTime`.
>   - `instantToTimestampWithZone` / `timestampWithZoneToInstant` — `Instant`.
>   - `localDateToProtoDate(LocalDate)` / `protoDateToLocalDate(Date)` — `LocalDate` ↔ `google.type.Date`.
>   - `localTimeToProtoTimeOfDay(LocalTime)` / `protoTimeOfDayToLocalTime(TimeOfDay)` — `LocalTime` ↔ `google.type.TimeOfDay`.
>   - `offsetTimeToTimestampWithZone` / `timestampWithZoneToOffsetTime` — `OffsetTime`.
>   - `fromTimestampWithZoneToObject(TimestampWithZone)`: the unified decoder that uses `TemporalType` to reconstruct the original type.

---

## 18. Result Set and Streaming

### Consuming executeQuery

`executeQuery` is a server-streaming RPC. The response stream contains one or more `OpResult` messages:

1. **First `OpResult`**: always contains the initial data batch in `query_result`:
   - `resultSetUUID` — server-side handle for this result set.
   - `labels` — ordered list of column names.
   - `rows` — first batch of `ResultRow` objects, each containing a `ParameterValue` per column.
   - `flag` — if `"ROW_BY_ROW"`, the server sends one row per stream message (row-by-row mode); otherwise the initial batch may contain multiple rows.

2. **Subsequent `OpResult` messages** (only in non-row-by-row streaming mode): additional batches until the stream closes.

3. **`fetchNextRows`**: After the initial stream closes, call `fetchNextRows(ResultSetFetchRequest)` with `resultSetUUID` and a page size to fetch additional rows. Repeat until the response contains an empty `rows` list or the result set is exhausted.

### Column value decoding

Map each `ParameterValue` oneof to the host language's equivalent type following the inverse of the encoding table in §16. Pay attention to `is_null = true` for SQL NULL values.

### Cursor navigation

Scrollable result sets support cursor positioning through `callResource` with `ResourceType.RES_RESULT_SET` and the appropriate `CallType`:

| Cursor operation | CallType |
|---|---|
| `next()` | `CALL_NEXT` |
| `first()` | `CALL_FIRST` |
| `last()` | `CALL_LAST` |
| `beforeFirst()` | `CALL_BEFORE` |
| `afterLast()` | `CALL_AFTER` |
| `absolute(row)` | `CALL_ABSOLUTE` |
| `relative(rows)` | `CALL_RELATIVE` |
| `previous()` | `CALL_PREVIOUS` |
| `close()` | `CALL_CLOSE` |

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`ResultSet`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/ResultSet.java): `next()` drives the multi-block iteration; `setNextOpResult()` loads a new batch from the iterator; `nextWithSessionUpdate()` updates the session from each block. All `getXxx(columnIndex)` methods call `ProtoConverter.fromParameterValue()` on the column's `ParameterValue`.
> - `ojp-jdbc-driver` — [`RemoteProxyResultSet`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/RemoteProxyResultSet.java): base class holding `resultSetUUID` and `statementService`; all scrollable-cursor operations issue `callResource(RES_RESULT_SET, CALL_FIRST/LAST/ABSOLUTE/…)`.
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient.fetchNextRows(sessionInfo, resultSetUUID, size)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java): the RPC that fetches the next page.
> - `ojp-grpc-commons` — [`ProtoConverter.fromProto(OpQueryResultProto)`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/ProtoConverter.java): deserialises the initial `OpQueryResult` (labels + rows + resultSetUUID).

---

## 19. LOB (Large Object) Handling

### LOB types

| LobType enum | Meaning |
|---|---|
| `LT_BLOB` | Binary large object |
| `LT_CLOB` | Character large object |
| `LT_BINARY_STREAM` | Binary stream (column-streaming variant) |
| `LT_ASCII_STREAM` | ASCII character stream |
| `LT_UNICODE_STREAM` | Unicode character stream |
| `LT_CHARACTER_STREAM` | Generic character stream |

### Writing a LOB (createLob)

1. Open a client-streaming call to `createLob`.
2. Send one or more `LobDataBlock` messages:
   ```
   LobDataBlock {
       session:  SessionInfo
       position: int64   // byte offset of this chunk
       data:     bytes   // chunk content (recommended chunk size: 32–64 KB)
       lobType:  LobType
       metadata: PropertyEntry[]  // used for binary streams to carry prepared statement info
   }
   ```
3. Close the stream. The server responds with a `LobReference` stream (typically one message):
   ```
   LobReference {
       session:      SessionInfo
       uuid:         string    // LOB handle
       bytesWritten: int32
       lobType:      LobType
   }
   ```
4. Store the `LobReference.uuid`. This UUID is what gets passed as a parameter value (§16) when binding the LOB to a SQL statement.

### Reading a LOB (readLob)

Call `readLob(ReadLobRequest)`:
```
ReadLobRequest {
    lobReference: LobReference  // uuid + session info
    position:     int64          // start byte (1-based for JDBC compatibility)
    length:       int32          // max bytes to return
}
```
Receive a server-streaming response of `LobDataBlock` messages. Concatenate the `data` fields in order to reconstruct the content.

### LOB and session stickiness

LOB handles are server-side objects. A connection that has an open LOB must remain bound to the same server (§6). Do not reroute such connections during failover; instead surface the error to the caller.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`LobServiceImpl`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/LobServiceImpl.java): `sendBytes(lobType, pos, inputStream)` opens the client-streaming `createLob` call, chunks the data into `LobDataBlock` messages, and returns the `LobReference`. `parseReceivedBlocks(Iterator<LobDataBlock>)` reassembles chunks from a `readLob` stream into an `InputStream`.
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient.createLob(connection, iterator)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java): the client-streaming gRPC call; uses an async stub and a `CountDownLatch` to bridge the streaming API back to a synchronous return value.
> - `StatementServiceGrpcClient.readLob(lobReference, pos, length)`: the server-streaming gRPC call that returns an `Iterator<LobDataBlock>`.
> - `ojp-jdbc-driver` — [`Blob`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Blob.java): `getBytes(pos, length)` and `getBinaryStream()` call `readLob`; `setBytes(pos, bytes)` calls `sendBytes`. [`Clob`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Clob.java) mirrors the same pattern for character data.
> - `ojp-jdbc-driver` — [`BinaryStream`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/BinaryStream.java): streams binary content directly via `createLob` without materialising the full byte array.

---

## 20. CallResource Protocol

The `callResource` RPC is a generic mechanism for operations that do not fit a dedicated RPC — primarily `DatabaseMetaData` queries, `ResultSet` cursor/update operations, `Statement` cancellation, savepoint management, and resource lifecycle calls.

### Request

```
CallResourceRequest {
    session:      SessionInfo
    resourceType: ResourceType   // what kind of resource to call
    resourceUUID: string         // the server-side handle for this resource
    target:       TargetCall     // the specific operation to perform
    properties:   PropertyEntry[]
}
```

### TargetCall (supports chaining)

```
TargetCall {
    callType:     CallType        // one of the 47 call type codes
    resourceName: string          // e.g., "Catalog", "TransactionIsolation", "Savepoint"
    params:       ParameterValue[] // input arguments
    nextCall:     TargetCall      // optional chained call (recursive)
}
```

### ResourceType values

| Value | Meaning |
|---|---|
| `RES_RESULT_SET` | An open result set |
| `RES_STATEMENT` | A plain statement |
| `RES_PREPARED_STATEMENT` | A prepared statement |
| `RES_CALLABLE_STATEMENT` | A callable statement |
| `RES_LOB` | A LOB object |
| `RES_CONNECTION` | The connection itself (for metadata, catalog, etc.) |
| `RES_SAVEPOINT` | A savepoint |

### Response

```
CallResourceResponse {
    session:      SessionInfo
    resourceUUID: string         // UUID of a newly created resource, if any
    values:       ParameterValue[]  // return values (may be empty)
}
```

Always update the local `SessionInfo` from `response.session`.

### CallType reference (47 codes)

`CALL_SET`, `CALL_GET`, `CALL_IS`, `CALL_ALL`, `CALL_NULLS`, `CALL_USES`, `CALL_SUPPORTS`, `CALL_STORES`, `CALL_NULL`, `CALL_DOES`, `CALL_DATA`, `CALL_NEXT`, `CALL_CLOSE`, `CALL_WAS`, `CALL_CLEAR`, `CALL_FIND`, `CALL_BEFORE`, `CALL_AFTER`, `CALL_FIRST`, `CALL_LAST`, `CALL_ABSOLUTE`, `CALL_RELATIVE`, `CALL_PREVIOUS`, `CALL_ROW`, `CALL_UPDATE`, `CALL_INSERT`, `CALL_DELETE`, `CALL_REFRESH`, `CALL_CANCEL`, `CALL_MOVE`, `CALL_OWN`, `CALL_OTHERS`, `CALL_UPDATES`, `CALL_DELETES`, `CALL_INSERTS`, `CALL_LOCATORS`, `CALL_AUTO`, `CALL_GENERATED`, `CALL_RELEASE`, `CALL_NATIVE`, `CALL_PREPARE`, `CALL_ROLLBACK`, `CALL_ABORT`, `CALL_EXECUTE`, `CALL_ADD`, `CALL_ENQUOTE`, `CALL_REGISTER`, `CALL_LENGTH`

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient.callResource(CallResourceRequest)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java): the single-node gRPC call.
> - `ojp-jdbc-driver` — [`DatabaseMetaData`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/DatabaseMetaData.java): every `DatabaseMetaData` method (>200 in total) is implemented by calling `callResource` with `RES_CONNECTION` and the appropriate `CallType` (e.g., `CALL_GET` for `getURL()`, `CALL_SUPPORTS` for `supportsXxx()`, `CALL_STORES` for `storesXxx()`). The private helper `newCallBuilder()` creates the skeleton `CallResourceRequest`.
> - `ojp-jdbc-driver` — `Connection.callProxy(callType, resourceName, returnType, params)`: the private convenience wrapper used throughout `Connection` and `DatabaseMetaData` to issue `callResource` calls without building the full request proto by hand.

---

## 21. Error and Exception Mapping

### SQL errors carried in gRPC trailers

When the server encounters a SQL error, it returns `Status.INTERNAL` with a `SqlErrorResponse` message attached to the trailing metadata. Extract it using the proto message key for `SqlErrorResponse`.

```
SqlErrorResponse {
    reason:       string        // human-readable message
    sqlState:     string        // ANSI SQL state code
    vendorCode:   int32         // database-specific error code
    sqlErrorType: SqlErrorType  // SQL_EXCEPTION or SQL_DATA_EXCEPTION
}
```

Map to the host language's exception hierarchy:
- `SQL_EXCEPTION` → standard SQL exception.
- `SQL_DATA_EXCEPTION` → data-specific SQL exception (subtype).

### Error classification matrix

| Condition | gRPC status | Client action |
|---|---|---|
| SQL error (bad query, constraint, etc.) | `INTERNAL` + `SqlErrorResponse` trailer | Throw SQL exception; do not retry; do not mark server unhealthy |
| Pool not found (server restarted) | `NOT_FOUND` | Invalidate connHash cache; reconnect; retry once (§4) |
| Server unreachable | `UNAVAILABLE` | Failover to next server (§8) |
| Request timeout | `DEADLINE_EXCEEDED` | Failover to next server (§8) |
| Client-side cancellation | `CANCELLED` | Do **not** failover; do **not** mark server unhealthy; surface to caller |
| Pool exhausted | `RESOURCE_EXHAUSTED` | Throw pool-exhaustion error; do not retry; do not mark server unhealthy |
| Session invalidated (server failure) | Session-not-found message | Throw session-lost error; do not retry; let caller decide |
| Session stickiness violation (server down) | Local check before RPC | Throw connection error immediately; do not reroute |

> **Note:** Before this classification was established (prior to April 2026) the server incorrectly used `Status.CANCELLED` for SQL errors. The correct status is `Status.INTERNAL` with a `SqlErrorResponse` trailer. Any implementation must use `INTERNAL` for SQL errors and must not treat `CANCELLED` as a server failure.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`GrpcExceptionHandler.handle(StatusRuntimeException)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/GrpcExceptionHandler.java): extracts `SqlErrorResponse` from gRPC trailing metadata on `Status.INTERNAL` and throws the appropriate `SQLException` with SQL state and vendor code.
> - `GrpcExceptionHandler.isPoolNotFoundException(exception)`: returns `true` for `NOT_FOUND`.
> - `GrpcExceptionHandler.isSessionInvalidationError(exception)`: returns `true` for session-invalidation error messages.
> - `GrpcExceptionHandler.isConnectionLevelError(exception)`: returns `true` for `UNAVAILABLE`, `DEADLINE_EXCEEDED`, and connection-related `UNKNOWN` errors.

---

## 22. Configuration System

### Configuration sources (in priority order)

1. **System / environment properties** (highest priority) — e.g., `-Dojp.health.check.interval=10000` or environment variable equivalents.
2. **`ojp.properties` file** — loaded from the classpath or a well-known filesystem path.
3. **Built-in defaults** (lowest priority).

### Property namespacing

Properties can be global or per-datasource. Per-datasource properties are prefixed with the datasource name:

```
# Global
ojp.health.check.interval=5000

# Per-datasource (datasource name: "analytics")
analytics.ojp.health.check.interval=10000
```

### Standard configuration properties

| Property | Default | Meaning |
|---|---|---|
| `ojp.health.check.interval` | `5000` (ms) | Periodic health check interval |
| `ojp.health.check.threshold` | `5000` (ms) | Minimum wait before re-probing an unhealthy server |
| `ojp.health.check.timeout` | `5000` (ms) | Probe call timeout |
| `ojp.redistribution.enabled` | `true` | Enable/disable the health checker and redistribution |
| `ojp.redistribution.idleRebalanceFraction` | `1.0` | Fraction of idle connections to close per rebalance cycle |
| `ojp.redistribution.maxClosePerRecovery` | `100` | Max connections closed per recovery event |
| `ojp.loadaware.selection.enabled` | `true` | Use least-connections; `false` = round-robin |
| `ojp.multinode.retry.attempts` | `3` | Max failover retry attempts |
| `ojp.multinode.retry.delay` | `100` (ms) | Delay between retry attempts |
| `ojp.datasource.name` | `"default"` | Active datasource name (sent to the server) |
| `ojp.grpc.tls.enabled` | `false` | Enable TLS on gRPC channels |
| `ojp.grpc.tls.cert.path` | — | Path to client certificate for mTLS |

### Duration format

Duration values support the following suffixes:
- No suffix — milliseconds (e.g. `5000`)
- `ms` — milliseconds (e.g. `500ms`)
- `s` — seconds (e.g. `10s`)
- `m` — minutes (e.g. `2m`)

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`DatasourcePropertiesLoader`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/DatasourcePropertiesLoader.java): `loadOjpPropertiesForDataSource(datasourceName)` merges file properties, system properties, and environment variables with per-datasource prefix resolution. `loadOjpProperties()` loads the base `ojp.properties` file from the classpath.
> - `ojp-jdbc-driver` — [`HealthCheckConfig`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/HealthCheckConfig.java): the strongly-typed POJO that holds all health-check and redistribution settings, populated by `MultinodeUrlParser` from the loaded `Properties`.
> - `ojp-jdbc-driver` — [`MultinodeUrlParser.readIntProperty(props, key, default)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeUrlParser.java) / `readLongProperty(...)`: reads typed values from the merged `Properties` object.
> - `ojp-grpc-commons` — [`GrpcClientConfig.load()`](../../ojp-grpc-commons/src/main/java/org/openjproxy/config/GrpcClientConfig.java): loads the gRPC-specific settings (max inbound message size, TLS config) from `ojp.properties`.

---

## 23. Query Result Caching

Cache configuration is entirely **client-side to server** — the client reads local cache rules and sends them to the server as `ConnectionDetails.properties` entries during `connect()`. The server applies them transparently; the client does not implement any caching logic itself.

### Properties sent to the server

| Property key | Meaning |
|---|---|
| `ojp.cache.enabled` | `"true"` to enable caching |
| `ojp.cache.queries.<N>.pattern` | Regex pattern matching SQL queries to cache |
| `ojp.cache.queries.<N>.ttl` | TTL in seconds for cached results |
| `ojp.cache.queries.<N>.invalidateOn` | Comma-separated table names that invalidate this rule |
| `ojp.cache.queries.<N>.enabled` | `"true"` / `"false"` to toggle individual rules |

`<N>` is a 1-based integer index. Rules are processed in index order.

### Example configuration

```properties
ojp.cache.enabled=true
ojp.cache.queries.1.pattern=SELECT .* FROM products.*
ojp.cache.queries.1.ttl=600
ojp.cache.queries.1.invalidateOn=products,product_prices
ojp.cache.queries.2.pattern=SELECT .* FROM users.*
ojp.cache.queries.2.ttl=300
ojp.cache.queries.2.invalidateOn=users
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`CacheConfigurationBuilder.addCachePropertiesToMap(propertiesMap, datasourceName)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/CacheConfigurationBuilder.java): reads cache rules from the loaded `Properties` and appends them to the `ConnectionDetails.properties` map that is sent to the server on `connect()`. `parseDurationToSeconds(duration)` handles the same duration format as §22.

---

## 24. Security / Transport

### Plaintext (default)

Create a plaintext gRPC channel targeting `dns:///host:port`. This is suitable for internal networks or local development.

### TLS

When `ojp.grpc.tls.enabled = true`, create a TLS-secured channel:
- Use the platform's default trust store or a custom CA certificate.
- Support mutual TLS (mTLS) when `ojp.grpc.tls.cert.path` is set.
- Certificate paths and key material must be loaded from configurable filesystem paths; do not hard-code them.

### Credential handling

- Passwords must never be logged or included in exception messages.
- Connection keys used for cache lookups (§4) may include the password as a cache key only — they must not be serialised or persisted.

> **Reference implementation:**
> - `ojp-grpc-commons` — [`GrpcChannelFactory.createChannel(host, port)`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/GrpcChannelFactory.java): creates a plaintext `ManagedChannel` with configurable max inbound message size; `createSecureChannel(host, port, size, tlsConfig)` builds the TLS-secured variant; `buildSslContext(tlsConfig)` sets up Netty's `SslContext` from the certificate paths.
> - `ojp-grpc-commons` — [`GrpcClientConfig`](../../ojp-grpc-commons/src/main/java/org/openjproxy/config/GrpcClientConfig.java): loaded by `GrpcClientConfig.load()` from `ojp.properties`; exposes `getTlsConfig()` and `getMaxInboundMessageSize()`.
> - `ojp-grpc-commons` — [`TlsConfig`](../../ojp-grpc-commons/src/main/java/org/openjproxy/config/TlsConfig.java): holds `enabled`, `certPath`, `keyPath`, `caPath`, and `clientAuth` flags.

---

## 25. DataSource / Integration API

### DataSource wrapper

Provide a higher-level `DataSource` (or equivalent) object that:
- Holds connection configuration (URL, user, password, properties).
- Exposes a `getConnection()` method that calls `Driver.connect()` internally.
- Integrates cleanly with the host language's database access conventions (e.g., Python's `DB-API 2.0`, Go's `database/sql`, Node.js connection objects).

### Framework integration (Spring Boot example)

For Java/Spring Boot:
- Provide a `spring-boot-starter-ojp` auto-configuration module.
- Auto-configure an `OjpDataSource` bean when the driver is on the classpath.
- Expose a bridge (`OjpSystemPropertiesBridge`) that copies Spring Boot `application.yml` properties to JVM system properties so the configuration system (§22) can pick them up.
- **Disable** the framework's own built-in connection pool (e.g., HikariCP in Spring Boot) when OJP is in use — double-pooling is the most common misconfiguration and causes incorrect behaviour.

For other languages, document clearly in the library README that the application-side connection pool must be disabled when using OJP.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`OjpDataSource`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/OjpDataSource.java): implements `javax.sql.DataSource`; `getConnection()` / `getConnection(user, password)` delegate to `DriverManager.getConnection(url, info)` which invokes the registered `Driver`.
> - `ojp-jdbc-driver` — [`OjpXADataSource`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/xa/OjpXADataSource.java): implements `javax.sql.XADataSource`; `getXAConnection()` creates an `OjpXAConnection` (and thus an `OjpXAResource`) for JTA integration.
> - `spring-boot-starter-ojp` module: provides the Spring Boot auto-configuration class and the `OjpSystemPropertiesBridge` bean; sets `spring.datasource.type=OjpDataSource` and excludes `DataSourceAutoConfiguration` to prevent double-pooling.

---

## 26. Testing Coverage

A conformant client implementation must ship a test suite that exercises all the aspects above. Tests that require a live OJP server (and optionally a real database) should be **gated behind feature flags** so the suite can run incrementally in CI.

### Test infrastructure requirements

- A running OJP server (see `ojp-server` module and `download-drivers.sh`).
- At minimum, an embedded/in-process database (e.g., H2) for fast baseline tests.
- Optional: containerised databases (PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, DB2, CockroachDB) gated by per-database flags.

### Test categories and required scenarios

#### Basic CRUD
- SELECT, INSERT, UPDATE, DELETE via plain Statement and PreparedStatement.
- Verify affected row counts, returned ResultSet contents.
- Verify empty result sets are handled correctly.

#### Multiple data types
- Round-trip every `ParameterTypeProto` value through INSERT + SELECT.
- Cover: all integer widths, float, double, BigDecimal, string, boolean, byte array, date, time, timestamp (with and without timezone), LocalDate, LocalTime, LocalDateTime, OffsetDateTime, OffsetTime, Instant, URL, UUID, RowId, BLOB, CLOB, array, NULLs for each type.

#### Statement variants
- Plain `Statement`: `executeQuery`, `executeUpdate`, `execute`, `executeBatch`, `getResultSet`, `getUpdateCount`, `getGeneratedKeys`, `cancel`, `close`.
- `PreparedStatement`: all `setXxx` methods, `executeBatch`, multiple executions with the same prepared statement, `getParameterMetaData`.
- `CallableStatement`: IN, OUT, INOUT parameters; `registerOutParameter`; retrieval of OUT values after execution; named parameters where supported.

#### ResultSet navigation
- Forward-only cursors: `next()`, `wasNull()`, `close()`.
- Scrollable cursors: `first()`, `last()`, `beforeFirst()`, `afterLast()`, `absolute(n)`, `relative(n)`, `previous()`.
- Multi-block pagination: queries large enough to exceed one fetch page; verify all rows are retrieved.

#### ResultSet metadata
- `getColumnCount()`, `getColumnName()`, `getColumnType()`, `getColumnTypeName()`, `getPrecision()`, `getScale()`, `isNullable()`, `isAutoIncrement()`.

#### DatabaseMetaData
- `getTables()`, `getColumns()`, `getPrimaryKeys()`, `getIndexInfo()`, `getProcedures()`, `getTypeInfo()`, `supportsXxx()` methods.
- Verify results match the actual database schema.

#### Transactions
- Commit: insert rows in a transaction, commit, verify rows persist.
- Rollback: insert rows in a transaction, rollback, verify rows are absent.
- `autoCommit = false` then `setAutoCommit(true)` — verify implicit commit.
- Transaction isolation level: set, verify via `getTransactionIsolation()`, reset after connection return.

#### Savepoints
- Create a named and an anonymous savepoint.
- Rollback to each; verify partial rollback semantics.
- Release a savepoint.

#### XA transactions
- Full lifecycle: `xaStart`, `xaEnd`, `xaPrepare`, `xaCommit`.
- Rollback path: `xaStart`, `xaEnd`, `xaPrepare`, `xaRollback`.
- One-phase commit (`onePhase=true`).
- `xaRecover`: verify in-doubt XIDs are returned.
- `xaForget`: verify heuristically completed branch is removed.
- Transaction isolation reset after XA session.

#### LOBs
- BLOB: write a small blob (< 1 chunk), a large blob (multiple chunks), read back both; verify byte-for-byte equality.
- CLOB: same as BLOB but with character content.
- Binary stream, ASCII stream, Unicode stream: write via stream API, read back.
- Hydratable LOB: verify that a LOB reference can be passed as a parameter to a second statement.
- NULL LOB: verify that `setBlob(null)` / `setClob(null)` sends a SQL NULL.

#### Session affinity
- Verify that a connection with an open transaction always routes to the same server.
- Verify that a connection holding an open LOB always routes to the same server.
- Verify that when the bound server is down, an appropriate error is raised rather than silent rerouting.

#### Multi-block / large result sets
- Execute a query that returns more rows than one page. Verify all rows arrive and are in the correct order.

#### Multinode load balancing
- With two or more server endpoints, open `N` connections and verify they are distributed across servers (round-robin and least-connections modes separately).

#### Multinode failover
- Kill one server mid-operation; verify the operation is retried on a surviving server (for stateless operations).
- Verify a server is marked unhealthy after failure.
- Verify subsequent connections avoid the unhealthy server.

#### Multinode recovery and redistribution
- Bring a server back; verify it is marked healthy after the health check interval.
- Verify new connections start routing to the recovered server.
- Verify connection redistribution closes a fraction of idle connections on over-loaded servers.

#### XA multinode
- Verify that each XA session binds to exactly one server.
- Verify that failover of an XA session to another server raises an error (not a silent reroute).
- Verify XA redistribution after server recovery.

#### connHash caching / connect-RPC skip
- Open two connections with the same credentials; verify only one `connect()` gRPC call is made.
- Simulate a `NOT_FOUND` response; verify the driver invalidates the cache and re-issues `connect()`.

#### Session stickiness error path
- Establish a session on server A. Mark server A unhealthy. Attempt a SQL operation. Verify an error is raised rather than the request being silently routed to server B.

#### Cluster health propagation
- Fail one server; verify the cluster health string sent in subsequent requests marks it `DOWN`.
- Recover the server; verify the health string marks it `UP`.

#### Concurrency / pool exhaustion
- Send more concurrent requests than the server-side pool size; verify pool-exhaustion errors are surfaced cleanly and do not mark servers unhealthy.

#### Slow query segregation
- Send queries that take longer than the slow-query threshold; verify they use the reserved slow-query slots and do not starve fast queries.

#### Multi-datasource
- Configure two endpoints with different datasource names; verify each endpoint uses its own datasource configuration.

#### Configuration loading
- Verify properties are loaded from `ojp.properties`.
- Verify system properties override file properties.
- Verify per-datasource properties override global properties.

#### Performance / mini stress
- Open and close 100–1000 connections in parallel; verify no connection leaks, no deadlocks, and no degrading error rate.

#### Database-specific test suites

Each database must have a dedicated test class gated by its own flag. The class must cover the full set of above scenarios for that database's specific SQL dialect, type system, and edge cases.

| Database | Feature flag |
|---|---|
| H2 | `enableH2Tests` |
| PostgreSQL | `enablePostgresTests` |
| MySQL | `enableMySQLTests` |
| MariaDB | `enableMariaDBTests` |
| Oracle | `enableOracleTests` |
| SQL Server | `enableSqlServerTests` |
| DB2 | `enableDb2Tests` |
| CockroachDB | `enableCockroachDBTests` |

H2 tests (in-process, no external dependency) must always be runnable in CI without any extra setup and should act as the first gate before any database-specific jobs run.

> **Reference implementation — test classes by area:**
>
> | Test area | Java test class(es) |
> |---|---|
> | Basic CRUD | [`BasicCrudIntegrationTest`](../../ojp-jdbc-driver/src/test/java/openjproxy/jdbc/BasicCrudIntegrationTest.java) |
> | Multiple data types | `H2MultipleTypesIntegrationTest`, `PostgresMultipleTypesIntegrationTest`, `MySQLMultipleTypesIntegrationTest`, `OracleMultipleTypesIntegrationTest`, `SQLServerMultipleTypesIntegrationTest`, `Db2MultipleTypesIntegrationTest`, `CockroachDBMultipleTypesIntegrationTest`, `MariaDBMultipleTypesIntegrationTest` |
> | Statement variants | `H2StatementExtensiveTests`, `H2PreparedStatementExtensiveTests` (and per-DB equivalents) |
> | ResultSet navigation / metadata | `H2ResultSetTest` (and per-DB), `H2ResultSetMetaDataExtensiveTests`, `H2ReadMultipleBlocksOfDataIntegrationTest` |
> | DatabaseMetaData | `H2DatabaseMetaDataExtensiveTests`, `H2ConnectionExtensiveTests` (and per-DB) |
> | Transactions | `H2ConnectionExtensiveTests`, [`TransactionIsolationResetTest`](../../ojp-jdbc-driver/src/test/java/openjproxy/jdbc/TransactionIsolationResetTest.java) |
> | Savepoints | `H2SavepointTests` (and per-DB `*SavepointTests`) |
> | XA transactions | [`PostgresXAIntegrationTest`](../../ojp-jdbc-driver/src/test/java/openjproxy/jdbc/PostgresXAIntegrationTest.java), `MySQLXAIntegrationTest`, `MariaDBXAIntegrationTest`, `OracleXAIntegrationTest`, `SqlServerXAIntegrationTest`, `Db2XAIntegrationTest`, [`XASessionInvalidationTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/XASessionInvalidationTest.java) |
> | LOBs | [`BlobIntegrationTest`](../../ojp-jdbc-driver/src/test/java/openjproxy/jdbc/BlobIntegrationTest.java), [`BinaryStreamIntegrationTest`](../../ojp-jdbc-driver/src/test/java/openjproxy/jdbc/BinaryStreamIntegrationTest.java), [`HydratedLobValidationTest`](../../ojp-jdbc-driver/src/test/java/openjproxy/jdbc/HydratedLobValidationTest.java) (and per-DB `*Blob*` / `*BinaryStream*`) |
> | Session affinity | [`H2SessionAffinityIntegrationTest`](../../ojp-jdbc-driver/src/test/java/openjproxy/jdbc/H2SessionAffinityIntegrationTest.java) (and per-DB `*SessionAffinity*`) |
> | Multi-block result sets | `H2ReadMultipleBlocksOfDataIntegrationTest` (and per-DB) |
> | Multinode load balancing | [`LoadAwareServerSelectionTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/LoadAwareServerSelectionTest.java), [`MultinodeIntegrationTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/MultinodeIntegrationTest.java) |
> | Multinode failover | [`MultinodeFailoverTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/MultinodeFailoverTest.java), [`MultinodeConnectionManagerErrorHandlingTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/MultinodeConnectionManagerErrorHandlingTest.java) |
> | Multinode recovery / redistribution | [`MultinodeRecoveryTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/MultinodeRecoveryTest.java) |
> | XA multinode | [`MultinodeXAIntegrationTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/MultinodeXAIntegrationTest.java) |
> | connHash caching | [`ConnectRpcSkipOptimisationTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/ConnectRpcSkipOptimisationTest.java), [`UnifiedConnectionModeTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/UnifiedConnectionModeTest.java) |
> | Session stickiness error path | [`MultinodeTargetServerBindingTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/MultinodeTargetServerBindingTest.java), `MultinodeStatementServiceTest` |
> | Cluster health propagation | [`MultinodeConnectionManagerClusterHealthTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/MultinodeConnectionManagerClusterHealthTest.java) |
> | Concurrency / pool exhaustion | [`ConcurrencyTimeoutTest`](../../ojp-jdbc-driver/src/test/java/openjproxy/jdbc/ConcurrencyTimeoutTest.java) |
> | Multi-datasource | [`MultiDataSourceIntegrationTest`](../../ojp-jdbc-driver/src/test/java/openjproxy/jdbc/MultiDataSourceIntegrationTest.java), [`MultiDataSourceConfigurationTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/jdbc/MultiDataSourceConfigurationTest.java) |
> | Configuration loading | [`DatasourcePropertiesLoaderSystemPropertyTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/jdbc/DatasourcePropertiesLoaderSystemPropertyTest.java), [`DatasourcePropertiesLoaderEnvironmentTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/jdbc/DatasourcePropertiesLoaderEnvironmentTest.java) |
> | URL parsing | [`MultinodeUrlParserTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/MultinodeUrlParserTest.java), [`UrlParserTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/jdbc/UrlParserTest.java), [`DriverMultinodeUrlTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/jdbc/DriverMultinodeUrlTest.java) |
> | DataSource API | [`OjpDataSourceTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/jdbc/OjpDataSourceTest.java), [`OjpXADataSourceTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/jdbc/xa/OjpXADataSourceTest.java) |
> | Health check config | [`HealthCheckConfigTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/HealthCheckConfigTest.java), [`MultinodeRetryConfigTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/MultinodeRetryConfigTest.java) |
> | Session tracker unit | [`SessionTrackerTest`](../../ojp-jdbc-driver/src/test/java/org/openjproxy/grpc/client/SessionTrackerTest.java) |

---

## Appendix A — Proto file locations

| File | Location |
|---|---|
| Main protocol | `ojp-grpc-commons/src/main/proto/StatementService.proto` |
| Generic value containers | `ojp-grpc-commons/src/main/proto/containers.proto` |
| Echo / heartbeat | `ojp-grpc-commons/src/main/proto/echo.proto` |

## Appendix B — Reference implementation classes

| Aspect | Java class |
|---|---|
| gRPC stubs | `StatementServiceGrpcClient` |
| Multinode routing | `MultinodeStatementService`, `MultinodeConnectionManager` |
| URL parsing | `MultinodeUrlParser`, `UrlParser` |
| Session tracking | `SessionTracker` |
| Health checking | `HealthCheckValidator`, `HealthCheckConfig` |
| Redistribution | `ConnectionRedistributor`, `XAConnectionRedistributor` |
| Error mapping | `GrpcExceptionHandler` |
| Connection lifecycle | `Connection` |
| Statement execution | `Statement`, `PreparedStatement`, `CallableStatement` |
| Result set | `ResultSet`, `RemoteProxyResultSet` |
| LOB handling | `Blob`, `Clob`, `NClob`, `Lob`, `LobServiceImpl` |
| XA | `OjpXAResource`, `OjpXAConnection`, `OjpXADataSource` |
| Driver entry point | `Driver` |
| DataSource wrapper | `OjpDataSource` |
