#!/usr/bin/env python3
"""Script to write the two OJP client spec files."""
import base64
import os

SPEC_DIR = "/home/runner/work/ojp/ojp/documents/multi-language-client-spec"

CLIENT_SPEC = r"""# OJP Multi-Language Client Specification

> **Status:** Draft — April 2026
> **Scope:** This document defines every aspect that a new OJP client library (in any language other than Java) must implement in order to be fully compatible with an OJP server. It is written language-agnostically; where Java-specific concepts appear they are labelled as the reference implementation only.
> **Reference implementation:** `ojp-jdbc-driver` module.
> **Protocol source of truth:** `ojp-grpc-commons/src/main/proto/StatementService.proto` and `echo.proto`.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Core Concepts](#2-core-concepts)
   - [2.1 The Virtual Connection Model](#21-the-virtual-connection-model)
   - [2.2 Session and connHash](#22-session-and-connhash)
   - [2.3 Session Affinity (Stickiness)](#23-session-affinity-stickiness)
   - [2.4 Client vs. Server Responsibilities](#24-client-vs-server-responsibilities)
3. [Architecture and Data Flow](#3-architecture-and-data-flow)
   - [3.1 gRPC Interface and Channel Setup](#31-grpc-interface-and-channel-setup)
   - [3.2 Connection Configuration (ConnectionDetails)](#32-connection-configuration-connectiondetails)
   - [3.3 Client Identity (clientUUID)](#33-client-identity-clientuuid)
   - [3.4 Multinode Load Balancing](#34-multinode-load-balancing)
   - [3.5 Cluster Health Propagation](#35-cluster-health-propagation)
4. [Client Responsibilities](#4-client-responsibilities)
   - [4.1 Connection Establishment and connHash Caching](#41-connection-establishment-and-connhash-caching)
   - [4.2 Session Lifecycle](#42-session-lifecycle)
   - [4.3 Failover](#43-failover)
   - [4.4 Health Checking](#44-health-checking)
   - [4.5 Connection Redistribution on Recovery](#45-connection-redistribution-on-recovery)
5. [Minimal End-to-End Example](#5-minimal-end-to-end-example)
6. [Error Handling](#6-error-handling)
   - [6.1 Error Classification Matrix](#61-error-classification-matrix)
   - [6.2 SQL Errors vs. gRPC Transport Errors](#62-sql-errors-vs-grpc-transport-errors)
7. [Implementation Guidance](#7-implementation-guidance)
   - [7.1 Statement Execution](#71-statement-execution)
   - [7.2 Parameter Type Mapping](#72-parameter-type-mapping)
   - [7.3 Temporal Type Handling](#73-temporal-type-handling)
   - [7.4 Result Set Streaming](#74-result-set-streaming)
   - [7.5 LOB (Large Object) Handling](#75-lob-large-object-handling)
   - [7.6 Transaction Management (non-XA)](#76-transaction-management-non-xa)
   - [7.7 Savepoints](#77-savepoints)
   - [7.8 XA / Distributed Transactions](#78-xa--distributed-transactions)
   - [7.9 callResource Protocol](#79-callresource-protocol)
   - [7.10 Configuration System](#710-configuration-system)
   - [7.11 Query Result Caching](#711-query-result-caching)
   - [7.12 Security / Transport](#712-security--transport)
   - [7.13 DataSource / Integration API](#713-datasource--integration-api)
8. [Testing Coverage](#8-testing-coverage)

---

## 1. Overview

OJP (Open J Proxy) is the world's first open-source JDBC Type 3 driver. It works by placing a gRPC server between applications and their databases. Applications use an OJP client library — and never touch a real database connection directly. The OJP server owns all real connections in a HikariCP pool and services SQL requests on behalf of clients.

This document specifies everything a non-Java OJP client library must implement to be fully protocol-compatible. All communication between client and server uses gRPC over HTTP/2. The proto definitions in `ojp-grpc-commons/src/main/proto/StatementService.proto` and `echo.proto` are the authoritative source for message formats and RPC signatures.

---

## 2. Core Concepts

### 2.1 The Virtual Connection Model

An OJP "connection" is not a real database connection. Real database connections are owned and managed by the OJP server's HikariCP pool. The client holds a `SessionInfo` — a lightweight value object the server uses to route each incoming request to the correct pool.

Opening a connection is inexpensive. For non-XA connections, a cache hit means no RPC is needed at all: the client constructs a `SessionInfo` locally from a cached `connHash` and begins issuing SQL calls immediately. Multiple client connections may share the same server-side pool by sharing the same `connHash`.

Because the server already pools real database connections, the application side must not add another connection pool on top. Double-pooling (e.g., HikariCP on the app side in addition to the server side) causes resource waste and incorrect connection-count accounting.

### 2.2 Session and connHash

`connHash` is a server-computed SHA-256 hash of the tuple `(url, user, password, datasource_name)`. It identifies which pool on the server to route requests to. The client receives `connHash` on the first `connect()` RPC and must cache it for all subsequent connections that share the same credentials.

`sessionUUID` is the server-side handle for a persistent session. It is NOT assigned at connection time. The server assigns it on the first operation that requires a persistent server-side context — for example, `startTransaction()`, LOB creation, or a stored-procedure call. Until a `sessionUUID` exists, each request is effectively stateless: the server picks any available connection from the pool identified by `connHash` and processes the request.

Once a `sessionUUID` is established it is returned in every subsequent response alongside the same `connHash`. The client must replace its local `SessionInfo` with the one returned after every RPC call.

### 2.3 Session Affinity (Stickiness)

Once a `sessionUUID` is set, every subsequent request for that session must go to the same server. The server encodes the target server address in the `targetServer` field of every `SessionInfo` response. The client must record this binding in a `sessionUUID -> host:port` map and enforce it strictly.

Routing a sticky session to a different server is a protocol error — it is not a transparent optimisation. The session state (open transaction, cursor, LOB handle) lives on a specific server and cannot be migrated. If the bound server becomes unreachable, the client must raise an error to the caller rather than silently rerouting.

### 2.4 Client vs. Server Responsibilities

The server owns: real database connections, connection pool management, transaction state, LOB storage, cursor state, and query result caching.

The client owns: `SessionInfo` propagation on every request and response, `connHash` caching and cache-invalidation logic, server endpoint health tracking, load balancing across healthy endpoints, failover on transport errors, cluster health string construction and proactive pushing to healthy servers, and session stickiness enforcement.

---

## 3. Architecture and Data Flow

### 3.1 gRPC Interface and Channel Setup

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

**gRPC channel lifecycle:**

One `ManagedChannel` (or equivalent) per server endpoint. Channels are long-lived and shared across all logical connections to that endpoint. They are created lazily on first connection or eagerly during initialisation when endpoints are known upfront. Use DNS-prefixed targets (`dns:///host:port`) where the gRPC runtime supports it. Blocking stubs are used for synchronous operations; async stubs are required for client-streaming (`createLob`) and server-streaming (`executeQuery`, `readLob`) RPCs. Channel shutdown must be graceful and triggered on client shutdown.

```python
# Create one long-lived channel per OJP server endpoint
channel = grpc.create_channel("localhost:10591", credentials=grpc.local_channel_credentials())
stub    = StatementServiceStub(channel)   # used for all SQL operations
echo    = EchoServiceStub(channel)         # used for health heartbeats

# On process shutdown — drain in-flight calls before closing
channel.shutdown(grace_period_seconds=5)
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`StatementService`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementService.java): the unified interface declaring all RPC methods.
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java): the single-node gRPC implementation; contains the concrete gRPC stub calls.
> - `ojp-jdbc-driver` — [`MultinodeStatementService`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeStatementService.java): the multinode facade that wraps `StatementServiceGrpcClient` per endpoint with routing, failover, and stickiness.
> - `ojp-grpc-commons` — [`GrpcChannelFactory`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/GrpcChannelFactory.java): builds `ManagedChannel` instances with plaintext or TLS.

---

### 3.2 Connection Configuration (ConnectionDetails)

A non-Java OJP client does not use a JDBC URL. Instead, it collects the following configuration items directly from the user or from a configuration file:

| Item | Required | Description |
|---|---|---|
| OJP server endpoints | Yes | One or more `host:port` pairs for the OJP server(s). In multinode mode this is a list. |
| Datasource name | No | A logical name for this datasource, default `"default"`. Used to keep separate connection pools per named datasource on the same server. |
| Database URL | Yes | The connection URL for the **real database** that the OJP server will connect to (e.g., `jdbc:postgresql://db:5432/mydb`). This is sent verbatim to the server. |
| User | Yes | Database username. |
| Password | Yes | Database password. |
| Properties | No | Additional key-value configuration pairs (pool sizing, cache rules, etc. — see §7.10, §7.11). |

Map the collected configuration to the `ConnectionDetails` proto fields as follows:

| Proto field | Type | Value |
|---|---|---|
| `url` | `string` | The **actual database URL** (e.g., `jdbc:postgresql://db:5432/mydb`). The server uses this to create the real database connection pool. |
| `user` | `string` | Database username. |
| `password` | `string` | Database password. |
| `clientUUID` | `string` | Stable process UUID (see §3.3). |
| `properties` | `repeated PropertyEntry` | Configuration key-value pairs; include `ojp.datasource.name = <datasourceName>` when using a named datasource. |
| `serverEndpoints` | `repeated string` | All OJP server addresses as `"host:port"` strings (the full cluster list, not just the chosen endpoint). |
| `clusterHealth` | `string` | Current cluster health string (see §3.5); empty string on the very first connect. |
| `isXA` | `bool` | `true` for XA connections, `false` otherwise. |

The `url` field must be consistent across all client processes that connect to the same logical datasource. The server computes `connHash` as SHA-256(`url + user + password + datasource_name`). If different clients send different `url` strings for the same database, the server creates separate pools.

The client-side cache key for the `connHash` lookup is: `url + "|" + user + "|" + password + "|" + datasource_name`

> **Reference implementation:**
> - `ojp-grpc-commons` — [`ConnectionDetails` proto](../../ojp-grpc-commons/src/main/proto/StatementService.proto): field definitions.
> - `ojp-server` — [`ConnectionHashGenerator.hashConnectionDetails()`](../../ojp-server/src/main/java/org/openjproxy/grpc/server/utils/ConnectionHashGenerator.java): SHA-256 of `url + user + password + datasource_name_from_properties` — the server-side connHash algorithm.
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.computeConnectionKey()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): client-side cache key.
> - `ojp-jdbc-driver` — [`MultinodeUrlParser`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeUrlParser.java): Java reference for how the JDBC URL is parsed to extract server endpoints, datasource names, and the actual DB URL before building `ConnectionDetails` (Java-specific; not needed in non-Java clients).

---

### 3.3 Client Identity (clientUUID)

Generate one random UUID (version 4) when the client library is first loaded or when the process starts. This UUID must remain stable for the entire lifetime of the process. Attach `clientUUID` to every `ConnectionDetails` message sent to the server. The server uses `clientUUID` to group all sessions from the same client process. Do not persist `clientUUID` across process restarts; each new process must generate a fresh UUID.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`ClientUUID`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/ClientUUID.java): `getUUID()` returns the static, process-scoped UUID that is generated once at class-loading time via `UUID.randomUUID()`.

---

### 3.4 Multinode Load Balancing

Two strategies must be supported, selectable via configuration (see §7.10, property `ojp.loadaware.selection.enabled`):

**Least-connections (default, `true`):** Select the healthy server with the lowest number of active sessions. Track session counts in a thread-safe counter per server endpoint. Use round-robin as a tie-breaker when all servers have equal counts.

**Round-robin (`false`):** Cycle through healthy servers in order using an atomic counter modulo the number of healthy servers.

Server selection runs on every new connection attempt (non-XA, first `connect()`) and on every XA `connect()`. Once a session is assigned a server via session stickiness, selection does not run again for that session. Only servers whose `isHealthy() == true` are eligible for selection. If no healthy servers exist, raise a connection error.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.selectHealthyServer()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): the entry point that dispatches to one of the two strategies based on config.
> - `MultinodeConnectionManager.selectByLeastConnections(healthyServers)`: picks the server with the lowest active-session count.
> - `MultinodeConnectionManager.selectByRoundRobin(healthyServers)`: atomically increments `roundRobinCounter` and selects `servers[counter % size]`.
> - `ojp-jdbc-driver` — [`ServerEndpoint`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/ServerEndpoint.java): holds `isHealthy`, `lastFailureTime`, host, and port state for each endpoint.

---

### 3.5 Cluster Health Propagation

The cluster health string format is: `host1:port1(UP);host2:port2(DOWN);host3:port3(UP)`

Each semicolon-separated segment is `host:port(STATUS)` where status is `UP` or `DOWN`.

The client must build the cluster health string from local endpoint state before every `connect()` call. It must also consume the cluster health string returned in `SessionInfo.clusterHealth` on every response, updating local endpoint states: endpoints marked `DOWN` must be treated as unhealthy; endpoints marked `UP` that were previously failed must not be marked healthy immediately — the health-check thread must confirm first.

When the topology changes (a server fails or recovers), the client must proactively push the updated cluster health to all currently healthy servers via two independent triggers — both are necessary:

**Trigger 1 — health-check thread**: When `performHealthCheck()` detects a newly failed or recovered server, it calls `pushClusterHealthToAllHealthyServers()` inline. This covers the case when no SQL traffic is active.

**Trigger 2 — query thread**: When a SQL query thread detects server failure via `handleServerFailure()`, it submits `pushClusterHealthToAllHealthyServers()` to the background scheduler asynchronously. This covers the race where the query thread marks the server unhealthy before the health checker runs.

The push is done by calling `connect()` on each healthy server with a `ConnectionDetails` whose `clusterHealth` field contains the new topology string. The server uses this to resize its pool immediately.

```python
# Build the health string from local endpoint state
def build_cluster_health(endpoints):
    return ";".join(
        f"{ep.host}:{ep.port}({'UP' if ep.is_healthy else 'DOWN'})"
        for ep in endpoints
    )

# Push updated cluster health to all healthy servers via a connect() call.
# The server uses the clusterHealth field to resize its pool immediately.
def push_cluster_health(endpoints, stored_details):
    if not stored_details:
        return   # no connections yet — nothing to push
    health_str = build_cluster_health(endpoints)
    for conn_hash, details in stored_details.items():
        push_req = ConnectionDetails(**details, clusterHealth=health_str)
        for ep in endpoints:
            if ep.is_healthy:
                stubs[ep].connect(push_req)   # no-op for pool creation; resizes pool

# Consume the cluster health returned in every gRPC response
def consume_cluster_health(session_info):
    for segment in session_info.clusterHealth.split(";"):
        host_port, status = segment.rsplit("(", 1)
        status = status.rstrip(")")
        endpoint = find_endpoint(host_port)
        if status == "DOWN" and endpoint.is_healthy:
            handle_server_failure(endpoint)
        elif status == "UP" and not endpoint.is_healthy:
            # do not mark healthy here — let the health-check thread confirm (§4.4)
            pass
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.generateClusterHealth()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): builds the semicolon-delimited health string from `serverEndpoints`.
> - `MultinodeConnectionManager.pushClusterHealthToAllHealthyServers()`: calls `connect()` on every healthy server with the new cluster health embedded in `ConnectionDetails`.
> - `MultinodeConnectionManager.handleServerFailure()` (Trigger 2): submits `pushClusterHealthToAllHealthyServers()` to `healthCheckScheduler` on a genuine healthy->unhealthy transition.
> - `MultinodeConnectionManager.performHealthCheck()` (Trigger 1): calls `pushClusterHealthToAllHealthyServers()` directly after marking a server DOWN or after a recovered server is marked healthy.
> - `MultinodeStatementService.withClusterHealth(sessionInfo)`: attaches the current health string to an outgoing `SessionInfo` before each RPC (reactive secondary path).

---

## 4. Client Responsibilities

### 4.1 Connection Establishment and connHash Caching

#### First connection (cache miss)

1. Build a `ConnectionDetails` message (see §3.2 for field mapping).
2. Call `connect(ConnectionDetails)` on the chosen server. Receive `SessionInfo`.
3. Cache the returned `connHash`, keyed on `url + "|" + user + "|" + password + "|" + datasourceName`. Also store the full `ConnectionDetails` for replay if the server restarts.
4. Return the received `SessionInfo` to the caller.

#### Subsequent connections (cache hit, non-XA only)

When a subsequent connection uses the same credentials:
1. Look up `connHash` from the local cache.
2. Build a `SessionInfo` locally without making any gRPC call:
   ```
   SessionInfo {
     connHash:   <cached value>
     clientUUID: <process UUID>
     isXA:       false
   }
   ```
3. Return this locally-built `SessionInfo`. No `sessionUUID` is set yet; it will be assigned by the server lazily.

XA connections always call the server — caching is disabled for XA because each XA connection must create a dedicated pool entry on a specific server.

#### Cache invalidation (NOT_FOUND recovery)

When any gRPC call returns `Status.NOT_FOUND`, the server has lost its in-memory pool. Recovery procedure:
1. Remove the cached `connHash -> connection-key` entry (but keep the stored `ConnectionDetails`).
2. Re-issue a real `connect()` RPC using the stored `ConnectionDetails`.
3. Cache the new `connHash` returned.
4. Retry the original failed operation once with the new `SessionInfo`.
5. This retry is only safe if the original request had no active `sessionUUID`. If a session was in progress, surface the error to the caller — the transaction state is permanently lost.

```python
# --- First connection (cache miss) ---
req = ConnectionDetails(
    url             = "jdbc:postgresql://db:5432/mydb",  # actual DB URL
    user            = "alice",
    password        = "secret",
    clientUUID      = CLIENT_UUID,                       # stable process UUID (§3.3)
    serverEndpoints = ["host1:10591", "host2:10591"],    # full cluster list
    clusterHealth   = build_cluster_health(endpoints),   # §3.5; "" on very first call
    isXA            = False,
    properties      = [PropertyEntry(key="ojp.datasource.name", string_value="default")]
)

session = stub.connect(req)
# session.connHash   = "abc123..."  — server-computed pool key
# session.clientUUID = CLIENT_UUID

# Cache connHash for subsequent connections
cache_key = f"{req.url}|{req.user}|{req.password}|default"
connhash_cache[cache_key] = session.connHash
stored_details[session.connHash] = req   # kept for NOT_FOUND recovery (see below)

# --- Subsequent connection (cache hit, non-XA) ---
# No RPC call needed — build SessionInfo locally from the cached connHash
session = SessionInfo(
    connHash   = connhash_cache[cache_key],
    clientUUID = CLIENT_UUID,
    isXA       = False
    # sessionUUID is absent; the server assigns it lazily on startTransaction
)

# --- NOT_FOUND recovery ---
# If any RPC returns Status.NOT_FOUND (server restarted, pool lost):
del connhash_cache[cache_key]
session = stub.connect(stored_details[old_conn_hash])  # re-issue real connect()
connhash_cache[cache_key] = session.connHash           # update cache
# then retry the original failed RPC once
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.connect()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): orchestrates first-connect vs. cache-hit logic.
> - `MultinodeConnectionManager.computeConnectionKey()`: builds the `url|user|password|datasourceName` cache key.
> - `MultinodeConnectionManager.invalidateConnHash()`: removes the stale key from `connHashByConnectionKey` on `NOT_FOUND`.
> - `MultinodeConnectionManager.reconnectForConnHash()`: re-issues the real `connect()` RPC using stored `ConnectionDetails` and updates the cache.
> - `MultinodeConnectionManager.buildLocalSessionInfo()`: constructs the in-memory `SessionInfo` for cache-hit connections without an RPC call.

---

### 4.2 Session Lifecycle

**SessionInfo fields:**

| Field | Type | Meaning |
|---|---|---|
| `connHash` | string | Server-side key identifying which connection pool to use |
| `clientUUID` | string | Client process identity (see §3.3) |
| `sessionUUID` | string | Server-side session handle; set once a session is established |
| `transactionInfo` | `TransactionInfo` | Contains `transactionUUID` and `transactionStatus` (`TRX_ACTIVE`, `TRX_COMMITED`, `TRX_ROLLBACK`) |
| `sessionStatus` | `SessionStatus` | `SESSION_ACTIVE` or `SESSION_TERMINATED` |
| `isXA` | bool | Whether this is an XA session |
| `targetServer` | string | `host:port` of the server this session is pinned to (set by the server, used by the client for stickiness) |
| `clusterHealth` | string | Current cluster health snapshot from the server's perspective |

**Lifecycle rules:**

Always propagate the latest `SessionInfo` on every outgoing request. The server updates and returns it in every response; the client must replace its local copy with the one returned. When the response contains a `sessionUUID` that was absent in the request, register it immediately with the session-stickiness layer. On connection close, call `terminateSession(SessionInfo)` — this is mandatory for releasing server-side resources. If `sessionStatus == SESSION_TERMINATED` is received, treat the connection as closed and make no further calls on it.

**Session stickiness enforcement:**

Once a `sessionUUID` is established, every subsequent request for that session must go to the same server. Maintain a thread-safe map of `sessionUUID -> host:port`. On each request, if `sessionUUID` is set, look up the bound server and route the request there exclusively. If the bound server is currently marked unhealthy, raise an error to the caller — do not silently reroute. When a session is closed via `terminateSession`, remove the binding from the map and decrement the session count for that server in the load-balancing tracker.

```python
# Every gRPC call returns an updated SessionInfo — always replace the local copy
resp    = stub.executeUpdate(StatementRequest(session=current_session, sql="..."))
current_session = resp.session   # update after every call

# When a new sessionUUID appears in the response, record the server binding (§2.3)
if resp.session.sessionUUID and resp.session.sessionUUID != current_session.sessionUUID:
    bind_session(resp.session.sessionUUID, resp.session.targetServer)

# Close a connection — release server-side state
stub.terminateSession(current_session)
# After this call, discard current_session and do not make further calls on it
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`Connection`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Connection.java): holds the mutable `session` field (`SessionInfo`); `close()` calls `terminateSession(session)` and nulls the session.
> - `ojp-jdbc-driver` — [`MultinodeStatementService.withClusterHealth()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeStatementService.java): enriches outgoing `SessionInfo` with the current cluster health string before each RPC.
> - `MultinodeStatementService.checkAndBindSession()`: updates the stickiness map whenever the server returns a new or changed `sessionUUID`.
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.terminateSession()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): forwards `terminateSession` to every server that received a `connect()` for this `connHash`.
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.affinityServer(sessionKey)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): returns the bound server for a `sessionUUID`, or selects a new one via load balancing when no binding exists yet.
> - `MultinodeConnectionManager.bindSession(sessionUUID, targetServer)`: records the `sessionUUID -> host:port` mapping in `sessionToServerMap`.
> - `MultinodeConnectionManager.unbindSession(sessionUUID)`: removes the binding on session close.
> - `ojp-jdbc-driver` — [`SessionTracker`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/SessionTracker.java): maintains per-server session counts used by the load-balancer and redistribution logic.

---

### 4.3 Failover

**What triggers failover:**

| Status code | Trigger failover? |
|---|---|
| `UNAVAILABLE` | Yes |
| `DEADLINE_EXCEEDED` | Yes |
| `UNKNOWN` (with "connection" in message) | Yes |
| `INTERNAL` with SQL metadata trailers | **No** — this is a database-level error |
| `INTERNAL` without SQL metadata trailers | Yes — treated as a transport-level failure |
| `NOT_FOUND` | **No** — triggers reconnect (see §4.1), not failover |
| `RESOURCE_EXHAUSTED` (pool exhaustion) | **No** — surface to caller |
| `CANCELLED` | **No** — client-initiated cancellation; must never mark a server unhealthy |
| Any `SQLException` from server | **No** |

**Failover procedure:**

1. Capture whether the server was previously healthy (`wasHealthy`).
2. Mark the server unhealthy (`isHealthy = false`), recording the failure timestamp.
3. Log the failure.
4. If this is a genuine healthy -> unhealthy transition (`wasHealthy == true`), submit `pushClusterHealthToAllHealthyServers()` asynchronously to the background scheduler — do not block the query thread.
5. Shut down the gRPC channel for the failed server gracefully.
6. Select the next healthy server using the configured strategy, excluding the failed server and any already attempted in this retry cycle.
7. Retry the operation on the new server.
8. If all servers have been attempted and all failed, raise a connection error to the caller.

Retry attempts and delay between retries are configurable (see §7.10, properties `ojp.multinode.retry.attempts` and `ojp.multinode.retry.delay`).

**What must NOT trigger failover:** database errors, pool exhaustion, and session-invalidation errors must all be surfaced directly to the caller.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`GrpcExceptionHandler.isConnectionLevelError()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/GrpcExceptionHandler.java): classifies a `StatusRuntimeException` as a connectivity failure vs. a SQL/business error. `CANCELLED` is explicitly **excluded**.
> - `GrpcExceptionHandler.isPoolNotFoundException()`: returns `true` for `NOT_FOUND`, triggering reconnect rather than failover.
> - `GrpcExceptionHandler.isSessionInvalidationError()`: returns `true` when the server indicates the session is gone.
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.handleServerFailure(endpoint, exception)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): marks the server unhealthy, timestamps the failure, shuts down the gRPC channel gracefully, and submits `pushClusterHealthToAllHealthyServers()` on a genuine healthy->unhealthy transition.
> - `MultinodeStatementService.executeOpResultWithSessionStickinessAndBinding()`: the retry loop that catches `StatusRuntimeException`, calls `isConnectionLevelError`, drives the server-selection retry cycle.

---

### 4.4 Health Checking

Run a periodic background task that checks server health. The task must run at a configurable fixed interval (property `ojp.health.check.interval`, default 5 000 ms), not block the main execution thread, and be a daemon task so it does not prevent process shutdown. Start the background scheduler before the first connection is accepted.

**Two-phase check:**

**Phase 1 — probe healthy servers (detect newly failed servers):** Run when there are active XA sessions (`sessionToServerMap` is non-empty) **or** cached non-XA connection details (`connectionDetailsByConnHash` is non-empty). For each currently healthy server that passes the guard, send a probe call. If the call fails, mark the server unhealthy.

**Phase 2 — probe unhealthy servers (detect recovery):** For each currently unhealthy server, check if enough time has passed since the last failure (property `ojp.health.check.threshold`, default 5 000 ms). If so, probe the server. If the probe succeeds, run recovery (see §4.5).

**Health probe modes:**

| Mode | How to probe | When to use |
|---|---|---|
| Heartbeat (lightweight) | Send `connect()` with empty `url`, `user`, `password` — any response means transport is up | Default |
| Full validation | Send `connect()` with real credentials; on success, call `terminateSession` on the returned session | When heartbeat is insufficient |

```python
# Lightweight heartbeat: send empty credentials — any response means transport is up
def heartbeat_probe(stub):
    try:
        stub.connect(ConnectionDetails(url="", user="", password=""))
        return True   # server is reachable
    except grpc.RpcError:
        return False  # mark server unhealthy (§4.3)

# Full validation: connect with real credentials, then immediately terminate
def full_validation_probe(stub, stored_details):
    try:
        session = stub.connect(stored_details)
        stub.terminateSession(session)
        return True
    except grpc.RpcError:
        return False

# Periodic background task
def run_health_check(endpoints, stubs, stored_details):
    for ep in endpoints:
        if ep.is_healthy:
            # Phase 1 — probe healthy server; detect new failures
            if stored_details or xa_sessions:   # guard: skip if no connections yet
                if not heartbeat_probe(stubs[ep]):
                    handle_server_failure(ep)
                    push_cluster_health_async(endpoints, stored_details)
        else:
            # Phase 2 — probe unhealthy server; detect recovery
            if time_since(ep.last_failure) >= HEALTH_CHECK_THRESHOLD:
                if heartbeat_probe(stubs[ep]):
                    reinitialize_pool_on_recovered_server(ep, stored_details)  # §4.5
                    ep.mark_healthy()
                    push_cluster_health_inline(endpoints, stored_details)      # §3.5
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.performHealthCheck()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): the scheduled task body; implements the two-phase check.
> - `ojp-jdbc-driver` — [`HealthCheckValidator.validateServer(endpoint)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/HealthCheckValidator.java): performs a single lightweight probe.
> - `ojp-jdbc-driver` — [`HealthCheckConfig`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/HealthCheckConfig.java): POJO holding `healthCheckIntervalMs`, `healthCheckThresholdMs`, `healthCheckTimeoutMs`, and `redistributionEnabled`.
> - `MultinodeConnectionManager` constructor: schedules `performHealthCheck` on a `ScheduledExecutorService` at the configured interval.

---

### 4.5 Connection Redistribution on Recovery

When a failed server comes back online, rebalance client-side connections so that the recovered server receives its fair share of traffic again.

**Procedure on recovery:**

1. **Reinitialize pools on the recovered server first** (before marking healthy). Check whether any non-XA connections have been cached (`connectionDetailsByConnHash` is non-empty). If so, for every cached `connHash`/`ConnectionDetails` pair, call `connect()` on the recovered server so it creates the HikariCP pool immediately. This eliminates the NOT_FOUND window that would otherwise exist between marking the server healthy and the first SQL call reaching it. Only after all pools are pre-created, proceed to step 2.
2. Mark the server healthy (`endpoint.markHealthy()`).
3. Push the updated cluster health string to all healthy servers (see §3.5) so they can resize their pools.
4. If redistribution is enabled (`ojp.redistribution.enabled = true`), begin rebalancing:
   - Determine the ideal share: `totalConnections / numberOfHealthyServers`.
   - Identify over-loaded servers (connections > ideal share).
   - Close a fraction of idle connections on over-loaded servers so they are returned to the pool, then re-opened.
   - Honour the configurable fraction (`ojp.redistribution.idleRebalanceFraction`, default 1.0) and max-close-per-cycle limit (`ojp.redistribution.maxClosePerRecovery`, default 100).

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`MultinodeConnectionManager.reinitializePoolOnRecoveredServer(recoveredServer)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeConnectionManager.java): always called **before** `endpoint.markHealthy()`.
> - `ojp-jdbc-driver` — [`ConnectionRedistributor.rebalance(recoveredServers, allHealthyServers)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/ConnectionRedistributor.java): closes a fraction of idle connections on over-loaded servers for non-XA mode.
> - `ojp-jdbc-driver` — [`XAConnectionRedistributor.rebalance(recoveredServers, allHealthyServers)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/XAConnectionRedistributor.java): equivalent redistribution for XA connections.
> - `ojp-jdbc-driver` — [`ConnectionTracker`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/ConnectionTracker.java): maintains the per-server `Connection` list consulted by `ConnectionRedistributor`.

---

## 5. Minimal End-to-End Example

The following pseudo-code shows the complete sequence for a basic OJP client session: channel setup, connect, query, transaction with DML, and clean close.

```python
import grpc
import uuid

# -- 1. Channel and stub setup
# One long-lived channel per OJP server endpoint; shared across all connections.
CLIENT_UUID = str(uuid.uuid4())   # stable for this process lifetime
channel     = grpc.insecure_channel("localhost:10591")
stub        = StatementServiceStub(channel)

# In-process state
connhash_cache  = {}   # cache_key -> connHash
stored_details  = {}   # connHash  -> ConnectionDetails (for NOT_FOUND recovery)

# -- 2. connect() — first connection (cache miss)
cache_key = "jdbc:postgresql://db:5432/mydb|alice|secret|default"

if cache_key not in connhash_cache:
    req = ConnectionDetails(
        url             = "jdbc:postgresql://db:5432/mydb",
        user            = "alice",
        password        = "secret",
        clientUUID      = CLIENT_UUID,
        serverEndpoints = ["localhost:10591"],
        clusterHealth   = "",   # empty on very first connect
        isXA            = False,
        properties      = [PropertyEntry(key="ojp.datasource.name",
                                         string_value="default")]
    )
    session = stub.connect(req)
    connhash_cache[cache_key]        = session.connHash
    stored_details[session.connHash] = req
else:
    # Cache hit — build SessionInfo locally; no RPC needed
    session = SessionInfo(connHash=connhash_cache[cache_key],
                          clientUUID=CLIENT_UUID, isXA=False)

# -- 3. executeQuery() — read rows
result_set_uuid = None
rows = []
for op_result in stub.executeQuery(StatementRequest(
        session       = session,
        sql           = "SELECT id, name FROM products WHERE active = ?",
        parameters    = [ParameterProto(index=1, type=PT_BOOLEAN,
                                        values=[ParameterValue(bool_value=True)])],
        statementUUID = str(uuid.uuid4()))):
    qr = op_result.query_result
    if result_set_uuid is None:
        result_set_uuid = qr.resultSetUUID
    rows.extend(qr.rows)
    session = op_result.session   # always update local session

# Close result set when done
stub.callResource(CallResourceRequest(
    session=session, resourceType=RES_RESULT_SET,
    resourceUUID=result_set_uuid,
    target=TargetCall(callType=CALL_CLOSE)))

# -- 4. Transaction — startTransaction, executeUpdate, commitTransaction
session = stub.startTransaction(session)

resp = stub.executeUpdate(StatementRequest(
    session       = session,
    sql           = "INSERT INTO orders(customer, amount) VALUES(?, ?)",
    parameters    = [
        ParameterProto(index=1, type=PT_STRING,
                       values=[ParameterValue(string_value="alice")]),
        ParameterProto(index=2, type=PT_INT,
                       values=[ParameterValue(int_value=99)])
    ],
    statementUUID = str(uuid.uuid4())))
session = resp.session

session = stub.commitTransaction(session)

# -- 5. terminateSession() — release server-side state
stub.terminateSession(session)
# Discard session — do not make further calls on it.
channel.close()
```

---

## 6. Error Handling

### 6.1 Error Classification Matrix

| Condition | gRPC status | Client action |
|---|---|---|
| SQL error (bad query, constraint, etc.) | `INTERNAL` + `SqlErrorResponse` trailer | Throw SQL exception; do not retry; do not mark server unhealthy |
| Pool not found (server restarted) | `NOT_FOUND` | Invalidate connHash cache; reconnect; retry once (§4.1) |
| Server unreachable | `UNAVAILABLE` | Failover to next server (§4.3) |
| Request timeout | `DEADLINE_EXCEEDED` | Failover to next server (§4.3) |
| Client-side cancellation | `CANCELLED` | Do **not** failover; do **not** mark server unhealthy; surface to caller |
| Pool exhausted | `RESOURCE_EXHAUSTED` | Throw pool-exhaustion error; do not retry; do not mark server unhealthy |
| Session invalidated (server failure) | Session-not-found message | Throw session-lost error; do not retry; let caller decide |
| Session stickiness violation (server down) | Local check before RPC | Throw connection error immediately; do not reroute |

### 6.2 SQL Errors vs. gRPC Transport Errors

When the server encounters a SQL error, it returns `Status.INTERNAL` with a `SqlErrorResponse` message attached to the trailing metadata. The client must extract this trailer and use its fields to construct a meaningful error.

```
SqlErrorResponse {
    reason:       string        // human-readable message
    sqlState:     string        // ANSI SQL state code
    vendorCode:   int32         // database-specific error code
    sqlErrorType: SqlErrorType  // SQL_EXCEPTION or SQL_DATA_EXCEPTION
}
```

Map to the host language's exception hierarchy:
- `SQL_EXCEPTION` -> standard SQL exception.
- `SQL_DATA_EXCEPTION` -> data-specific SQL exception (subtype).

A transport error — `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `UNKNOWN` (with "connection" in the message), or `INTERNAL` without a `SqlErrorResponse` trailer — triggers the failover procedure in §4.3.

> **Note:** Prior to April 2026 the server incorrectly used `Status.CANCELLED` for SQL errors. The correct status is `Status.INTERNAL` with a `SqlErrorResponse` trailer. Implementations must use `INTERNAL` for SQL errors and must not treat `CANCELLED` as a server failure.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`GrpcExceptionHandler.handle(StatusRuntimeException)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/GrpcExceptionHandler.java): extracts `SqlErrorResponse` from gRPC trailing metadata on `Status.INTERNAL` and throws the appropriate `SQLException` with SQL state and vendor code.
> - `GrpcExceptionHandler.isPoolNotFoundException(exception)`: returns `true` for `NOT_FOUND`.
> - `GrpcExceptionHandler.isSessionInvalidationError(exception)`: returns `true` for session-invalidation error messages.
> - `GrpcExceptionHandler.isConnectionLevelError(exception)`: returns `true` for `UNAVAILABLE`, `DEADLINE_EXCEEDED`, and connection-related `UNKNOWN` errors.

---

## 7. Implementation Guidance

### 7.1 Statement Execution

All SQL is executed by populating a `StatementRequest` and calling either `executeUpdate` or `executeQuery` on the stub.

**Parameterless SQL:** Set `sql` to the full query string and leave `parameters` empty.

**Parameterized SQL:** Set `sql` with `?` positional placeholders and populate the `parameters` list with one `ParameterProto` per `?`. Parameters are accumulated locally and sent together in a single `StatementRequest`. Assign a `statementUUID` (a random UUID per logical prepared-statement instance) so the server can track resources tied to that statement.

**Stored-procedure calls:** First call `callResource` with `CallType.CALL_PREPARE` to register the procedure on the server and receive a `resourceUUID`. Then call `callResource` with `CallType.CALL_EXECUTE` to run it, passing IN parameters and retrieving OUT/INOUT values from `CallResourceResponse.values`.

**Execution routing:**
- Use `executeUpdate` for INSERT / UPDATE / DELETE / DDL — returns `OpResult` with `type = INTEGER` containing affected row count.
- Use `executeQuery` for SELECT — returns a server-streaming response. Consume the first `OpResult` to get the initial batch; call `fetchNextRows` for subsequent pages (see §7.4).
- After any execution, update the local `SessionInfo` from the `OpResult.session` field.

```python
# DML — INSERT / UPDATE / DELETE (use executeUpdate)
resp = stub.executeUpdate(StatementRequest(
    session       = session,
    sql           = "INSERT INTO orders(customer, amount) VALUES(?, ?)",
    parameters    = [
        ParameterProto(index=1, type=PT_STRING, values=[ParameterValue(string_value="Alice")]),
        ParameterProto(index=2, type=PT_INT,    values=[ParameterValue(int_value=42)])
    ],
    statementUUID = new_uuid()   # random UUID per statement instance
))
session       = resp.session           # always update local session
rows_affected = resp.value.int_value   # e.g., 1

# Query — SELECT (use executeQuery, which is server-streaming)
req = StatementRequest(
    session       = session,
    sql           = "SELECT id, name FROM orders WHERE customer = ?",
    parameters    = [ParameterProto(index=1, type=PT_STRING,
                                    values=[ParameterValue(string_value="Alice")])],
    statementUUID = new_uuid()
)
result_set_uuid = None
for op_result in stub.executeQuery(req):   # iterate the server-streaming response
    qr = op_result.query_result
    if result_set_uuid is None:
        result_set_uuid = qr.resultSetUUID
        labels = qr.labels     # e.g., ["id", "name"]
    for row in qr.rows:
        id_val   = row.values[0].int_value
        name_val = row.values[1].string_value
    session = op_result.session
# Fetch additional pages -> see §7.4

# Stored procedure — CALL_PREPARE then CALL_EXECUTE
prep_resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_CALLABLE_STATEMENT,
    target       = TargetCall(callType=CALL_PREPARE, resourceName="{call my_proc(?,?)}",
                              params=[ParameterValue(int_value=1)])   # IN param
))
proc_uuid = prep_resp.resourceUUID
session   = prep_resp.session

exec_resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_CALLABLE_STATEMENT,
    resourceUUID = proc_uuid,
    target       = TargetCall(callType=CALL_EXECUTE)
))
out_value = exec_resp.values[0]   # first OUT/INOUT parameter value
session   = exec_resp.session
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`Statement`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Statement.java): `executeQuery(sql)` -> `statementService.executeQuery(...)`; `executeUpdate(sql)` -> `statementService.executeUpdate(...)`.
> - `ojp-jdbc-driver` — [`PreparedStatement`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/PreparedStatement.java): accumulates parameters in a `SortedMap<Integer, Parameter>`; all 28 `setXxx(index, value)` methods map to the corresponding `ParameterType` (see §7.2).
> - `ojp-jdbc-driver` — [`CallableStatement`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/CallableStatement.java): issues `callResource(CALL_PREPARE)` on construction; retrieves OUT/INOUT values via `callResource(CALL_EXECUTE)` after execution.

---

### 7.2 Parameter Type Mapping

Each parameter is represented as:
```
ParameterProto {
    index:  int32              // 1-based parameter position
    type:   ParameterTypeProto // one of the 29 type codes
    values: ParameterValue[]   // one value for normal params; multiple for array params
}
```

**ParameterTypeProto values and their ParameterValue encoding:**

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
| `PT_BIG_DECIMAL` | `string_value` | Encode as `"<unscaledInteger> <scale>"` — see §7.2.1 |
| `PT_STRING` | `string_value` | |
| `PT_BYTES` | `bytes_value` | Raw bytes |
| `PT_DATE` | `date_value` | `google.type.Date` (year/month/day, no timezone) |
| `PT_TIME` | `time_value` | `google.type.TimeOfDay` (hours/minutes/seconds/nanos) |
| `PT_TIMESTAMP` | `timestamp_value` | `TimestampWithZone` — see §7.3 |
| `PT_ASCII_STREAM` | `bytes_value` | ASCII bytes |
| `PT_UNICODE_STREAM` | `bytes_value` | Unicode bytes |
| `PT_BINARY_STREAM` | `bytes_value` | Binary bytes |
| `PT_OBJECT` | varies | Best-effort mapping to one of the concrete value types |
| `PT_CHARACTER_READER` | `string_value` | Contents of the character stream |
| `PT_REF` | `string_value` | REF value as string |
| `PT_BLOB` | (LOB reference UUID) | Create LOB first (§7.5); then pass UUID as `string_value` |
| `PT_CLOB` | (LOB reference UUID) | Same as BLOB |
| `PT_ARRAY` | `int_array_value` / `long_array_value` / `string_array_value` | Use the typed array message matching element type |
| `PT_URL` | `url_value` (StringValue) | `URL.toExternalForm()` — presence-aware; unset = null |
| `PT_ROW_ID` | `rowid_value` (StringValue) | Base64-encoded bytes of the RowId — presence-aware |
| `PT_N_STRING` | `string_value` | Same wire format as PT_STRING |
| `PT_N_CHARACTER_STREAM` | `string_value` | Contents of the NCharacter stream |
| `PT_N_CLOB` | (LOB reference UUID) | Same as CLOB |
| `PT_SQL_XML` | `string_value` | XML content as string |

#### 7.2.1 BigDecimal encoding

BigDecimal is serialised as a space-separated string: `"<unscaledInteger> <scale>"`.

- `unscaledInteger`: the decimal string representation of the unscaled value (may be negative).
- `scale`: integer scale (number of decimal places).
- Full value = `unscaledInteger * 10^(-scale)`.

Example: `BigDecimal("123.45")` -> `"12345 2"`.

> **Note:** A separate binary wire format is documented in `documents/protocol/BIGDECIMAL_WIRE_FORMAT.md` for contexts where binary efficiency is needed.

#### 7.2.2 Presence-aware fields

`url_value`, `rowid_value`, `uuid_value`, `biginteger_value`, `rowidlifetime_value` are all `google.protobuf.StringValue` (a wrapper message). An absent (unset) wrapper means SQL NULL. An empty string inside the wrapper is a valid non-null value.

> **Reference implementation:**
> - `ojp-grpc-commons` — [`ProtoConverter.toProto(Parameter)`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/ProtoConverter.java): converts a host-language `Parameter` object to `ParameterProto`; `fromProto(ParameterProto)` is the inverse.
> - `ProtoConverter.toParameterValue(Object value)`: the central dispatcher that routes each Java type to the correct `ParameterValue` oneof field.
> - `ProtoConverter.fromParameterValue(ParameterValue, ParameterType)`: decodes a wire value back to a Java object using both the value and the declared type as hints.
> - `ojp-grpc-commons` — [`ProtoTypeConverters`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/ProtoTypeConverters.java): handles the presence-aware `StringValue` wrappers for UUID, URL, and RowId.
> - `ojp-grpc-commons` — [`BigDecimalWire`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/BigDecimalWire.java): `writeBigDecimal` / `readBigDecimal` — binary wire encoding for BigDecimal.

---

### 7.3 Temporal Type Handling

Timestamps are transmitted as:

```
TimestampWithZone {
    instant:       google.protobuf.Timestamp  // seconds + nanos since Unix epoch (UTC)
    timezone:      string                      // IANA zone ID or UTC offset (e.g., "Europe/Rome", "+02:00")
    original_type: TemporalType               // preserves the caller's original type
}
```

**TemporalType enum:**

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

**Encoding rules:**
1. Convert the host-language datetime value to an absolute UTC instant (seconds + nanoseconds since the Unix epoch).
2. Record the IANA timezone or UTC offset string.
3. Set `original_type` to the closest matching `TemporalType` enum value.

**Decoding rules:** On the receiving side, use `original_type` to reconstruct the correct host-language type.

Date-only values use `google.type.Date` (year, month, day — no timezone). Time-only values use `google.type.TimeOfDay` (hours, minutes, seconds, nanos — no timezone).

The OJP server must always run with `user.timezone=UTC`. Client libraries should normalise to UTC when encoding timestamps, using the `timezone` field to carry the original zone for faithful reconstruction.

> **Reference implementation:**
> - `ojp-grpc-commons` — [`TemporalConverter`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/TemporalConverter.java): the definitive encoding/decoding reference for all temporal types.

---

### 7.4 Result Set Streaming

`executeQuery` is a server-streaming RPC. The response stream contains one or more `OpResult` messages:

1. **First `OpResult`**: always contains the initial data batch in `query_result`:
   - `resultSetUUID` — server-side handle for this result set.
   - `labels` — ordered list of column names.
   - `rows` — first batch of `ResultRow` objects, each containing a `ParameterValue` per column.
   - `flag` — if `"ROW_BY_ROW"`, the server sends one row per stream message.

2. **Subsequent `OpResult` messages** (only in non-row-by-row streaming mode): additional batches until the stream closes.

3. **`fetchNextRows`**: After the initial stream closes, call `fetchNextRows(ResultSetFetchRequest)` with `resultSetUUID` and a page size to fetch additional rows. Repeat until the response contains an empty `rows` list.

Map each `ParameterValue` oneof to the host language's equivalent type following the inverse of the encoding table in §7.2. Pay attention to `is_null = true` for SQL NULL values.

**Cursor navigation:** Scrollable result sets support cursor positioning through `callResource` with `ResourceType.RES_RESULT_SET` and the appropriate `CallType`:

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

```python
# After executeQuery stream closes, fetch additional pages with fetchNextRows
result_set_uuid = ...   # captured from the first op_result (§7.1)
all_rows = []
while True:
    resp = stub.fetchNextRows(ResultSetFetchRequest(
        session       = session,
        resultSetUUID = result_set_uuid,
        size          = 500               # rows per page
    ))
    session = resp.session
    if not resp.query_result.rows:
        break   # no more rows — result set exhausted
    all_rows.extend(resp.query_result.rows)

# Close the result set explicitly when done
stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_RESULT_SET,
    resourceUUID = result_set_uuid,
    target       = TargetCall(callType=CALL_CLOSE)
))

# Cursor navigation — jump to an absolute row (scrollable result sets only)
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_RESULT_SET,
    resourceUUID = result_set_uuid,
    target       = TargetCall(
        callType = CALL_ABSOLUTE,
        params   = [ParameterValue(int_value=10)]   # jump to row 10
    )
))
session      = resp.session
current_row  = resp.values   # column values for row 10
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`ResultSet`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/ResultSet.java): `next()` drives the multi-block iteration; `setNextOpResult()` loads a new batch from the iterator; `nextWithSessionUpdate()` updates the session from each block. All `getXxx(columnIndex)` methods call `ProtoConverter.fromParameterValue()` on the column's `ParameterValue`.
> - `ojp-jdbc-driver` — [`RemoteProxyResultSet`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/RemoteProxyResultSet.java): base class holding `resultSetUUID` and `statementService`; all scrollable-cursor operations issue `callResource(RES_RESULT_SET, CALL_FIRST/LAST/ABSOLUTE/...)`.
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient.fetchNextRows(sessionInfo, resultSetUUID, size)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java): the RPC that fetches the next page.
> - `ojp-grpc-commons` — [`ProtoConverter.fromProto(OpQueryResultProto)`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/ProtoConverter.java): deserialises the initial `OpQueryResult` (labels + rows + resultSetUUID).

---

### 7.5 LOB (Large Object) Handling

**LOB types:**

| LobType enum | Meaning |
|---|---|
| `LT_BLOB` | Binary large object |
| `LT_CLOB` | Character large object |
| `LT_BINARY_STREAM` | Binary stream (column-streaming variant) |
| `LT_ASCII_STREAM` | ASCII character stream |
| `LT_UNICODE_STREAM` | Unicode character stream |
| `LT_CHARACTER_STREAM` | Generic character stream |

**Writing a LOB (createLob):**
1. Open a client-streaming call to `createLob`.
2. Send one or more `LobDataBlock` messages:
   ```
   LobDataBlock {
       session:  SessionInfo
       position: int64   // byte offset of this chunk
       data:     bytes   // chunk content (recommended chunk size: 32-64 KB)
       lobType:  LobType
       metadata: PropertyEntry[]  // used for binary streams to carry prepared statement info
   }
   ```
3. Close the stream. The server responds with a `LobReference`:
   ```
   LobReference {
       session:      SessionInfo
       uuid:         string    // LOB handle
       bytesWritten: int32
       lobType:      LobType
   }
   ```
4. Store the `LobReference.uuid`. This UUID is passed as a parameter value (§7.2) when binding the LOB to a SQL statement.

**Reading a LOB (readLob):** Call `readLob(ReadLobRequest)`:
```
ReadLobRequest {
    lobReference: LobReference  // uuid + session info
    position:     int64          // start byte (1-based for JDBC compatibility)
    length:       int32          // max bytes to return
}
```
Receive a server-streaming response of `LobDataBlock` messages. Concatenate the `data` fields in order to reconstruct the content.

LOB handles are server-side objects. A connection that has an open LOB must remain bound to the same server. Do not reroute such connections during failover; surface the error to the caller.

```python
CHUNK_SIZE = 64 * 1024   # 64 KB recommended chunk size

# --- Write a LOB (createLob is client-streaming) ---
def write_lob(stub, session, data_bytes, lob_type=LT_BLOB):
    def generate_blocks():
        for offset in range(0, len(data_bytes), CHUNK_SIZE):
            yield LobDataBlock(
                session  = session,
                position = offset,
                data     = data_bytes[offset : offset + CHUNK_SIZE],
                lobType  = lob_type
            )
    lob_ref = stub.createLob(generate_blocks())   # client-streaming -> single LobReference
    # lob_ref.uuid         -> the LOB handle; pass as parameter to executeUpdate (see §7.2)
    # lob_ref.bytesWritten -> sanity check
    return lob_ref.uuid

# Bind the LOB UUID when executing a statement
lob_uuid = write_lob(stub, session, my_bytes)
stub.executeUpdate(StatementRequest(
    session    = session,
    sql        = "INSERT INTO docs(content) VALUES(?)",
    parameters = [ParameterProto(index=1, type=PT_BLOB,
                                 values=[ParameterValue(string_value=lob_uuid)])]
))

# --- Read a LOB (readLob is server-streaming) ---
def read_lob(stub, session, lob_uuid, lob_type=LT_BLOB, max_bytes=10_000_000):
    req = ReadLobRequest(
        lobReference = LobReference(uuid=lob_uuid, session=session, lobType=lob_type),
        position     = 1,           # 1-based start position
        length       = max_bytes
    )
    return b"".join(block.data for block in stub.readLob(req))

content = read_lob(stub, session, lob_uuid)
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`LobServiceImpl`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/LobServiceImpl.java): `sendBytes(lobType, pos, inputStream)` opens the client-streaming `createLob` call, chunks the data into `LobDataBlock` messages, and returns the `LobReference`. `parseReceivedBlocks(Iterator<LobDataBlock>)` reassembles chunks from a `readLob` stream into an `InputStream`.
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient.createLob(connection, iterator)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java): the client-streaming gRPC call; uses an async stub and a `CountDownLatch` to bridge the streaming API back to a synchronous return value.
> - `StatementServiceGrpcClient.readLob(lobReference, pos, length)`: the server-streaming gRPC call that returns an `Iterator<LobDataBlock>`.
> - `ojp-jdbc-driver` — [`Blob`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Blob.java): `getBytes(pos, length)` and `getBinaryStream()` call `readLob`; `setBytes(pos, bytes)` calls `sendBytes`. [`Clob`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Clob.java) mirrors the same pattern for character data.
> - `ojp-jdbc-driver` — [`BinaryStream`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/BinaryStream.java): streams binary content directly via `createLob` without materialising the full byte array.

---

### 7.6 Transaction Management (non-XA)

The server tracks open transactions per session. The client controls when transactions begin and end by calling explicit RPCs.

- **Start a transaction**: call `startTransaction(SessionInfo)`. The returned `SessionInfo` contains a `transactionUUID` and `transactionStatus = TRX_ACTIVE`.
- **Commit**: call `commitTransaction(SessionInfo)`. Returns updated `SessionInfo` with `transactionStatus = TRX_COMMITED`.
- **Rollback**: call `rollbackTransaction(SessionInfo)`. Returns updated `SessionInfo` with `transactionStatus = TRX_ROLLBACK`.

Always replace the local `SessionInfo` with the one returned by these calls.

Set or get the isolation level by calling `callResource` with `RES_CONNECTION` and `CallType.CALL_SET` / `CALL_GET` and resource name `"TransactionIsolation"`. The isolation level should be reset to the default after each logical connection is reused.

```python
# Begin an explicit transaction
session = stub.startTransaction(session)
# session.transactionInfo.transactionUUID   = "txn-uuid"
# session.transactionInfo.transactionStatus = TRX_ACTIVE

# Execute SQL within the open transaction
resp    = stub.executeUpdate(StatementRequest(session=session, sql="INSERT INTO orders ..."))
session = resp.session   # always update local session

# Commit
session = stub.commitTransaction(session)
# session.transactionInfo.transactionStatus = TRX_COMMITED

# -- OR -- Rollback
session = stub.rollbackTransaction(session)
# session.transactionInfo.transactionStatus = TRX_ROLLBACK

# Set transaction isolation (READ_COMMITTED = 2)
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_CONNECTION,
    target       = TargetCall(
        callType     = CALL_SET,
        resourceName = "TransactionIsolation",
        params       = [ParameterValue(int_value=2)]
    )
))
session = resp.session

# Get current isolation level
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_CONNECTION,
    target       = TargetCall(callType=CALL_GET, resourceName="TransactionIsolation")
))
isolation_level = resp.values[0].int_value
session         = resp.session
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`Connection.setAutoCommit(boolean)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Connection.java): calls `commitTransaction` when switching on and `startTransaction` when switching off.
> - `Connection.commit()` / `Connection.rollback()`: delegate to `statementService.commitTransaction(session)` / `rollbackTransaction(session)` when `autoCommit == false`.
> - `Connection.close()`: calls `terminateSession(session)` unconditionally.
> - `Connection.setTransactionIsolation(level)` / `getTransactionIsolation()`: forwarded via `callProxy(CallType.CALL_SET/GET, "TransactionIsolation", ...)`.

---

### 7.7 Savepoints

Savepoints are implemented through the `callResource` protocol using `ResourceType.RES_SAVEPOINT`.

**Creating a savepoint:** Call `callResource` with `resourceType = RES_SAVEPOINT`, `target.callType = CALL_SET`, `target.resourceName = "Savepoint"`, and `target.params = [savepointName]` if named; empty for anonymous savepoints. The response contains the savepoint UUID in `CallResourceResponse.resourceUUID`.

**Rolling back to a savepoint:** Call `callResource` with `resourceType = RES_SAVEPOINT`, `resourceUUID = <savepoint UUID from creation>`, `target.callType = CALL_ROLLBACK`.

**Releasing a savepoint:** Call `callResource` with `resourceType = RES_SAVEPOINT`, `resourceUUID = <savepoint UUID>`, `target.callType = CALL_RELEASE`.

```python
# Create a named savepoint
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_SAVEPOINT,
    target       = TargetCall(
        callType     = CALL_SET,
        resourceName = "Savepoint",
        params       = [ParameterValue(string_value="my_savepoint")]  # omit for anonymous
    )
))
savepoint_uuid = resp.resourceUUID   # keep this to roll back or release later
session        = resp.session

# Roll back to the savepoint (partial undo)
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_SAVEPOINT,
    resourceUUID = savepoint_uuid,
    target       = TargetCall(callType=CALL_ROLLBACK, resourceName="Savepoint")
))
session = resp.session

# Release the savepoint (no longer needed)
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_SAVEPOINT,
    resourceUUID = savepoint_uuid,
    target       = TargetCall(callType=CALL_RELEASE, resourceName="Savepoint")
))
session = resp.session
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`Connection.setSavepoint()`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Connection.java) / `setSavepoint(name)`: calls `callProxy` with `CALL_SET`, `"Savepoint"`, and the optional name; wraps the returned resource UUID in a [`Savepoint`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/Savepoint.java) object.
> - `Connection.rollback(Savepoint)`: calls `callProxy` with `CALL_ROLLBACK`, `"Savepoint"`, and the savepoint's resource UUID.
> - `Connection.releaseSavepoint(Savepoint)`: calls `callProxy` with `CALL_RELEASE`.

---

### 7.8 XA / Distributed Transactions

XA support maps the standard XA resource manager protocol to gRPC RPCs. XA connections are always pinned to a single server (see §2.3).

**XA transaction lifecycle:**

```
xaStart(XaStartRequest)       -- Begin branch; safe to retry on connection error
xaEnd(XaEndRequest)           -- End branch; NEVER retry after this point
xaPrepare(XaPrepareRequest)   -- Two-phase prepare; returns XA_OK or XA_RDONLY
xaCommit(XaCommitRequest)     -- Commit (onePhase=true for one-phase optimisation)
xaRollback(XaRollbackRequest) -- Roll back the branch
xaRecover(XaRecoverRequest)   -- List in-doubt XIDs (for recovery after crash)
xaForget(XaForgetRequest)     -- Forget a heuristically completed branch
```

**Xid encoding (XidProto):**

| Field | Type | Meaning |
|---|---|---|
| `formatId` | int32 | Transaction format ID |
| `globalTransactionId` | bytes | Global transaction ID (up to 64 bytes) |
| `branchQualifier` | bytes | Branch qualifier (up to 64 bytes) |

**Retry policy:** `xaStart` only: retry on connection-level errors. All other XA operations: do not retry automatically. Surface failures to the caller's transaction manager.

```python
xid = XidProto(
    formatId            = 1,
    globalTransactionId = b"global-tx-001",
    branchQualifier     = b"branch-1"
)

# 1. Start the XA branch (safe to retry on connection error)
resp    = stub.xaStart(XaStartRequest(session=session, xid=xid, flags=0))
session = resp.session   # bind session.targetServer -> this server for all remaining calls

# 2. Execute SQL within the branch (normal executeUpdate/executeQuery calls)
resp    = stub.executeUpdate(StatementRequest(session=session, sql="UPDATE accounts ..."))
session = resp.session

# 3. End the branch — do NOT retry past this point
resp    = stub.xaEnd(XaEndRequest(session=session, xid=xid, flags=0))
session = resp.session

# 4. Prepare (two-phase commit, phase 1)
prep = stub.xaPrepare(XaPrepareRequest(session=session, xid=xid))
# prep.result = XA_OK (proceed to commit) or XA_RDONLY (read-only; no commit needed)

# 5a. Commit (two-phase)
stub.xaCommit(XaCommitRequest(session=session, xid=xid, onePhase=False))

# 5b. -- OR -- One-phase optimisation (skip xaPrepare)
stub.xaCommit(XaCommitRequest(session=session, xid=xid, onePhase=True))

# 5c. -- OR -- Rollback
stub.xaRollback(XaRollbackRequest(session=session, xid=xid))

# Recovery: list in-doubt XIDs after a crash
resp = stub.xaRecover(XaRecoverRequest(session=session, flag=TMSTARTRSCAN))
for recovered_xid in resp.xids:
    stub.xaCommit(...)   # or xaRollback -- decision belongs to the transaction manager

# Forget a heuristically completed branch
stub.xaForget(XaForgetRequest(session=session, xid=xid))
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`OjpXAResource`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/xa/OjpXAResource.java): implements `XAResource`; all 10 lifecycle methods; contains the `xaStart` retry loop and the `toXidProto` / `fromXidProto` conversion helpers.
> - `ojp-jdbc-driver` — [`OjpXAConnection`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/xa/OjpXAConnection.java): creates the XA-mode `StatementService` connection and vends `OjpXAResource`.
> - `ojp-jdbc-driver` — [`OjpXADataSource`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/xa/OjpXADataSource.java): entry point for XA; calls `MultinodeConnectionManager.connectXA()` to pin the session to a single server.

---

### 7.9 callResource Protocol

The `callResource` RPC is a generic mechanism for operations that do not fit a dedicated RPC — primarily `DatabaseMetaData` queries, `ResultSet` cursor/update operations, `Statement` cancellation, savepoint management, and resource lifecycle calls.

**Request:**

```
CallResourceRequest {
    session:      SessionInfo
    resourceType: ResourceType   // what kind of resource to call
    resourceUUID: string         // the server-side handle for this resource
    target:       TargetCall     // the specific operation to perform
    properties:   PropertyEntry[]
}
```

**TargetCall (supports chaining):**

```
TargetCall {
    callType:     CallType        // one of the 47+ call type codes
    resourceName: string          // e.g., "Catalog", "TransactionIsolation", "Savepoint"
    params:       ParameterValue[] // input arguments
    nextCall:     TargetCall      // optional chained call (recursive)
}
```

**ResourceType values:**

| Value | Meaning |
|---|---|
| `RES_RESULT_SET` | An open result set |
| `RES_STATEMENT` | A plain statement |
| `RES_PREPARED_STATEMENT` | A prepared statement |
| `RES_CALLABLE_STATEMENT` | A callable statement |
| `RES_LOB` | A LOB object |
| `RES_CONNECTION` | The connection itself (for metadata, catalog, etc.) |
| `RES_SAVEPOINT` | A savepoint |

**Response:**

```
CallResourceResponse {
    session:      SessionInfo
    resourceUUID: string         // UUID of a newly created resource, if any
    values:       ParameterValue[]  // return values (may be empty)
}
```

Always update the local `SessionInfo` from `response.session`.

**CallType reference (47 codes):** `CALL_SET`, `CALL_GET`, `CALL_IS`, `CALL_ALL`, `CALL_NULLS`, `CALL_USES`, `CALL_SUPPORTS`, `CALL_STORES`, `CALL_NULL`, `CALL_DOES`, `CALL_DATA`, `CALL_NEXT`, `CALL_CLOSE`, `CALL_WAS`, `CALL_CLEAR`, `CALL_FIND`, `CALL_BEFORE`, `CALL_AFTER`, `CALL_FIRST`, `CALL_LAST`, `CALL_ABSOLUTE`, `CALL_RELATIVE`, `CALL_PREVIOUS`, `CALL_ROW`, `CALL_UPDATE`, `CALL_INSERT`, `CALL_DELETE`, `CALL_REFRESH`, `CALL_CANCEL`, `CALL_MOVE`, `CALL_OWN`, `CALL_OTHERS`, `CALL_UPDATES`, `CALL_DELETES`, `CALL_INSERTS`, `CALL_LOCATORS`, `CALL_AUTO`, `CALL_GENERATED`, `CALL_RELEASE`, `CALL_NATIVE`, `CALL_PREPARE`, `CALL_ROLLBACK`, `CALL_ABORT`, `CALL_EXECUTE`, `CALL_ADD`, `CALL_ENQUOTE`, `CALL_REGISTER`, `CALL_LENGTH`

```python
# --- Get the database catalog name (connection-level metadata) ---
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_CONNECTION,
    resourceUUID = "",        # empty for connection-level calls
    target       = TargetCall(callType=CALL_GET, resourceName="Catalog")
))
catalog_name = resp.values[0].string_value
session      = resp.session   # always update local session

# --- Check a database capability ---
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_CONNECTION,
    target       = TargetCall(callType=CALL_SUPPORTS, resourceName="Transactions")
))
supports_transactions = resp.values[0].bool_value
session               = resp.session

# --- Cancel a running statement ---
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_STATEMENT,
    resourceUUID = statement_uuid,   # UUID of the statement to cancel
    target       = TargetCall(callType=CALL_CANCEL)
))
session = resp.session

# --- Chained call: get Schema and Catalog in one round-trip ---
resp = stub.callResource(CallResourceRequest(
    session      = session,
    resourceType = RES_CONNECTION,
    target       = TargetCall(
        callType     = CALL_GET,
        resourceName = "Schema",
        nextCall     = TargetCall(callType=CALL_GET, resourceName="Catalog")
    )
))
schema_name  = resp.values[0].string_value
catalog_name = resp.values[1].string_value
session      = resp.session
```

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`StatementServiceGrpcClient.callResource(CallResourceRequest)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/StatementServiceGrpcClient.java): the single-node gRPC call.
> - `ojp-jdbc-driver` — [`DatabaseMetaData`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/DatabaseMetaData.java): every `DatabaseMetaData` method (>200 in total) is implemented by calling `callResource` with `RES_CONNECTION` and the appropriate `CallType`.
> - `ojp-jdbc-driver` — `Connection.callProxy(callType, resourceName, returnType, params)`: the private convenience wrapper used throughout `Connection` and `DatabaseMetaData`.

---

### 7.10 Configuration System

**Configuration sources (in priority order):**

1. System / environment properties (highest priority) — e.g., `-Dojp.health.check.interval=10000`.
2. `ojp.properties` file — loaded from the classpath or a well-known filesystem path.
3. Built-in defaults (lowest priority).

**Property namespacing:** Properties can be global or per-datasource. Per-datasource properties are prefixed with the datasource name:

```properties
# Global
ojp.health.check.interval=5000

# Per-datasource (datasource name: "analytics")
analytics.ojp.health.check.interval=10000
```

**Standard configuration properties:**

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

**Duration format:** No suffix = milliseconds; `ms` = milliseconds; `s` = seconds; `m` = minutes.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`DatasourcePropertiesLoader`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/DatasourcePropertiesLoader.java): `loadOjpPropertiesForDataSource(datasourceName)` merges file properties, system properties, and environment variables with per-datasource prefix resolution.
> - `ojp-jdbc-driver` — [`HealthCheckConfig`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/HealthCheckConfig.java): the strongly-typed POJO that holds all health-check and redistribution settings.
> - `ojp-jdbc-driver` — [`MultinodeUrlParser.readIntProperty(props, key, default)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/grpc/client/MultinodeUrlParser.java) / `readLongProperty(...)`: reads typed values from the merged `Properties` object.
> - `ojp-grpc-commons` — [`GrpcClientConfig.load()`](../../ojp-grpc-commons/src/main/java/org/openjproxy/config/GrpcClientConfig.java): loads the gRPC-specific settings (max inbound message size, TLS config) from `ojp.properties`.

---

### 7.11 Query Result Caching

Cache configuration is entirely **client-side to server** — the client reads local cache rules and sends them to the server as `ConnectionDetails.properties` entries during `connect()`. The server applies them transparently; the client does not implement any caching logic itself.

**Properties sent to the server:**

| Property key | Meaning |
|---|---|
| `ojp.cache.enabled` | `"true"` to enable caching |
| `ojp.cache.queries.<N>.pattern` | Regex pattern matching SQL queries to cache |
| `ojp.cache.queries.<N>.ttl` | TTL in seconds for cached results |
| `ojp.cache.queries.<N>.invalidateOn` | Comma-separated table names that invalidate this rule |
| `ojp.cache.queries.<N>.enabled` | `"true"` / `"false"` to toggle individual rules |

`<N>` is a 1-based integer index. Rules are processed in index order.

**Example configuration:**

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
> - `ojp-jdbc-driver` — [`CacheConfigurationBuilder.addCachePropertiesToMap(propertiesMap, datasourceName)`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/CacheConfigurationBuilder.java): reads cache rules from the loaded `Properties` and appends them to the `ConnectionDetails.properties` map that is sent to the server on `connect()`.

---

### 7.12 Security / Transport

**Plaintext (default):** Create a plaintext gRPC channel targeting `dns:///host:port`. Suitable for internal networks or local development.

**TLS:** When `ojp.grpc.tls.enabled = true`, create a TLS-secured channel. Use the platform's default trust store or a custom CA certificate. Support mutual TLS (mTLS) when `ojp.grpc.tls.cert.path` is set. Certificate paths and key material must be loaded from configurable filesystem paths.

**Credential handling:** Passwords must never be logged or included in exception messages. Connection keys used for cache lookups may include the password as a cache key only — they must not be serialised or persisted.

> **Reference implementation:**
> - `ojp-grpc-commons` — [`GrpcChannelFactory.createChannel(host, port)`](../../ojp-grpc-commons/src/main/java/org/openjproxy/grpc/GrpcChannelFactory.java): creates a plaintext `ManagedChannel` with configurable max inbound message size; `createSecureChannel(host, port, size, tlsConfig)` builds the TLS-secured variant.
> - `ojp-grpc-commons` — [`GrpcClientConfig`](../../ojp-grpc-commons/src/main/java/org/openjproxy/config/GrpcClientConfig.java): exposes `getTlsConfig()` and `getMaxInboundMessageSize()`.
> - `ojp-grpc-commons` — [`TlsConfig`](../../ojp-grpc-commons/src/main/java/org/openjproxy/config/TlsConfig.java): holds `enabled`, `certPath`, `keyPath`, `caPath`, and `clientAuth` flags.

---

### 7.13 DataSource / Integration API

Provide a higher-level `DataSource` (or equivalent) object that holds connection configuration (URL, user, password, properties) and exposes a `getConnection()` method that calls `Driver.connect()` internally. Integrate cleanly with the host language's database access conventions.

For Java/Spring Boot, provide a `spring-boot-starter-ojp` auto-configuration module. Auto-configure an `OjpDataSource` bean when the driver is on the classpath. Disable the framework's own built-in connection pool (e.g., HikariCP in Spring Boot) when OJP is in use — double-pooling is the most common misconfiguration and causes incorrect behaviour.

For other languages, document clearly in the library README that the application-side connection pool must be disabled when using OJP.

> **Reference implementation:**
> - `ojp-jdbc-driver` — [`OjpDataSource`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/OjpDataSource.java): implements `javax.sql.DataSource`; `getConnection()` / `getConnection(user, password)` delegate to `DriverManager.getConnection(url, info)`.
> - `ojp-jdbc-driver` — [`OjpXADataSource`](../../ojp-jdbc-driver/src/main/java/org/openjproxy/jdbc/xa/OjpXADataSource.java): implements `javax.sql.XADataSource`; `getXAConnection()` creates an `OjpXAConnection` (and thus an `OjpXAResource`) for JTA integration.
> - `spring-boot-starter-ojp` module: provides the Spring Boot auto-configuration class and the `OjpSystemPropertiesBridge` bean; sets `spring.datasource.type=OjpDataSource` and excludes `DataSourceAutoConfiguration` to prevent double-pooling.

---

## 8. Testing Coverage

A conformant client implementation must ship a test suite that exercises all the aspects above. Tests that require a live OJP server (and optionally a real database) should be **gated behind feature flags** so the suite can run incrementally in CI.

**Test infrastructure requirements:**
- A running OJP server (see `ojp-server` module and `download-drivers.sh`).
- At minimum, an embedded/in-process database (e.g., H2) for fast baseline tests.
- Optional: containerised databases (PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, DB2, CockroachDB) gated by per-database flags.

**Test categories and required scenarios:**

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
- Terminate one server mid-operation; verify the operation is retried on a surviving server (for stateless operations).
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
- Stop one server; verify the cluster health string sent in subsequent requests marks it `DOWN`.
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
- Open and close 100-1000 connections in parallel; verify no connection leaks, no deadlocks, and no degrading error rate.

#### Database-specific test suites

Each database must have a dedicated test class gated by its own flag:

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
"""

with open(os.path.join(SPEC_DIR, "CLIENT_SPEC.md"), "w") as f:
    f.write(CLIENT_SPEC)

print(f"CLIENT_SPEC.md: {len(CLIENT_SPEC)} chars, {len(CLIENT_SPEC.splitlines())} lines")
print("Done writing CLIENT_SPEC.md")
