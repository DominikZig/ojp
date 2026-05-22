
# Open J Proxy

![Release](https://img.shields.io/github/v/release/Open-J-Proxy/ojp?include_prereleases) [![Main CI](https://github.com/Open-J-Proxy/ojp/actions/workflows/main.yml/badge.svg)](https://github.com/Open-J-Proxy/ojp/actions/workflows/main.yml) [![Spring Boot/Micronaut/Quarkus Integration](https://github.com/Open-J-Proxy/ojp-framework-integration/actions/workflows/main.yml/badge.svg)](https://github.com/Open-J-Proxy/ojp-framework-integration/actions/workflows/main.yml) [![License](https://img.shields.io/github/license/Open-J-Proxy/ojp.svg)](https://raw.githubusercontent.com/Open-J-Proxy/ojp/master/LICENSE)

[![security status](https:&#x2F;&#x2F;www.meterian.com/badge/gh/Open-J-Proxy/ojp/security?branch=main)](https:&#x2F;&#x2F;www.meterian.com/report/gh/Open-J-Proxy/ojp) [![stability status](https:&#x2F;&#x2F;www.meterian.com/badge/gh/Open-J-Proxy/ojp/stability?branch=main)](https:&#x2F;&#x2F;www.meterian.com/report/gh/Open-J-Proxy/ojp)

Website 👉 [openjproxy.com](https://openjproxy.com) 

Follow us on LinkedIn 👉 [Open J Proxy](https://www.linkedin.com/company/open-j-proxy) 

[![Discord](https://img.shields.io/discord/1385189361565433927?label=Discord&logo=discord)](https://discord.gg/J5DdHpaUzu)

---

**A smart, open-source database control plane** — delivered as a Type 3 JDBC driver and a Layer 7 proxy server. OJP sits between your applications and your relational databases and provides backpressure, rich observability, client-side reactive throttling, slow-vs-fast query segregation, and load balancing / failover — all behind a standard JDBC API and with a roadmap for non-Java clients.

_"The only open-source JDBC Type 3 driver globally, this project introduces a transparent Quality-of-Service layer that decouples application performance from database bottlenecks. It's a must-try for any team struggling with data access contention, offering easy-to-implement back-pressure and pooling management." (Bruno Bossola - Java Champion and CTO @ Meterian.io)_  

---

## Star History

<a href="https://star-history.com/#Open-J-Proxy/ojp&Timeline">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=Open-J-Proxy/ojp&type=Timeline&theme=dark" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=Open-J-Proxy/ojp&type=Timeline" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=Open-J-Proxy/ojp&type=Timeline" />
  </picture>
</a>


[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://buymeacoffee.com/wqoejbve8z)

---

## Value Proposition

OJP is a **smart database control plane** for relational databases. It is more than a connection-pool proxy: it is a programmable layer that sits between your applications and your databases and gives you operational control over how requests flow into the database — without rewriting application code.

With minimal configuration (drop in the JDBC driver, prefix the connection URL), OJP turns a stock JDBC client into a participant in a managed data-access plane that delivers:

- **Database backpressure & connection-storm protection.** A global, OJP-managed pool fronts the database, so elastic application fleets cannot exhaust connections. Over-limit requests are queued or fast-failed instead of melting the database.
- **Client-side reactive throttling.** When the OJP server detects pressure (e.g. admission queue saturation, `RESOURCE_EXHAUSTED`), it signals clients to throttle themselves. Clients reduce concurrency on the *application side* before the database is impacted, and recover automatically as pressure subsides.
- **Slow vs fast query segregation (SQS).** OJP classifies queries by observed latency and reserves dedicated slots so that long analytical/reporting queries cannot starve short OLTP traffic. Slot borrowing keeps utilization high when one lane is idle. See [Mixed OLTP + OLAP workloads](#mixed-oltp--olap-workloads-enable-slow-query-segregation) below.
- **Rich observability out of the box.** OpenTelemetry traces and Prometheus metrics expose pool usage, admission queues, slow/fast classification, throttling decisions, slow-query slot pressure, and per-database health — so you can *see* what the data tier is doing instead of guessing. See [Telemetry and Observability](documents/telemetry/README.md).
- **Load balancing & failover, built into the driver.** Multinode support is implemented in the OJP JDBC driver itself: `jdbc:ojp[host1:port1,host2:port2]_...` enables load-aware routing, automatic failover, and session stickiness across multiple OJP servers — with no extra infrastructure. See [Multinode Configuration](documents/multinode/README.md).
- **Seamless Java integration.** Standard JDBC 4.2 API, Spring Boot starter, Quarkus and Micronaut guides, drop-in JAR. No application rewrite, no proprietary client API.
- **Path to a universal database control plane.** The gRPC protocol that backs OJP is language-neutral. Non-Java clients (Python, Node, Go, …) can be implemented against the same control plane, giving polyglot stacks a single, consistent place to enforce backpressure, throttling, segregation, and observability across every database they share. See the [multi-language client spec](documents/multi-language-client-spec/) for the work in progress.

OJP is positioned to be one of the very few — and arguably the only open-source — candidates for a truly universal database control plane: one place to apply quality-of-service policy across many databases and many client runtimes.

Tested support for databases: **PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, DB2, and H2**. Also compatible in principle with any database that provides a JDBC driver.

---
## Requirements

- **OJP JDBC Driver**: Java 11 or higher
- **OJP Server**: Java 21 or higher

---
## Quick Start

Get OJP running in under 5 minutes:

### 1. Start OJP Server (Docker)

> **⚠️ Important for v0.4.0-beta and later:** JDBC drivers must be downloaded and mounted. See [Chapter 4: Database Drivers](documents/ebook/part2-chapter4-database-drivers.md) for details.

```bash
# Download drivers first
mkdir -p ojp-libs
cd ojp-server
bash download-drivers.sh ../ojp-libs
cd ..

# Run with drivers mounted
docker run --rm -d \
  --network host \
  -v $(pwd)/ojp-libs:/opt/ojp/ojp-libs \
  rrobetti/ojp:0.4.16-beta
```

**Alternative: Runnable JAR (No Docker)**

```bash
# Download OJP Server JAR from Maven Central
wget https://repo1.maven.org/maven2/org/openjproxy/ojp-server/0.4.16-beta/ojp-server-0.4.16-beta-shaded.jar
chmod +x ojp-server-0.4.16-beta-shaded.jar

# Download open source JDBC drivers
curl -LO https://raw.githubusercontent.com/Open-J-Proxy/ojp/main/ojp-server/download-drivers.sh
bash download-drivers.sh  # Downloads H2, PostgreSQL, MySQL, MariaDB to ojp-libs/
java -Duser.timezone=UTC -jar ojp-server-0.4.16-beta-shaded.jar
```

📖 See [Executable JAR Setup Guide](documents/runnable-jar/README.md) for details.

### 2. Add OJP JDBC Driver to your project
```xml
<dependency>
    <groupId>org.openjproxy</groupId>
    <artifactId>ojp-jdbc-driver</artifactId>
    <version>0.4.16-beta</version>
</dependency>
```

### 3. Update your JDBC URL
Replace your existing connection URL by prefixing with `ojp[host:port]_`:

```java
// Before (PostgreSQL example)
"jdbc:postgresql://user@localhost/mydb"

// After  
"jdbc:ojp[localhost:1059]_postgresql://user@localhost/mydb"

// Oracle example
"jdbc:ojp[localhost:1059]_oracle:thin:@localhost:1521/XEPDB1"

// SQL Server example
"jdbc:ojp[localhost:1059]_sqlserver://localhost:1433;databaseName=mydb"
```
Use the ojp driver: `org.openjproxy.jdbc.Driver`

That's it! Your application now uses intelligent connection pooling through OJP.

**Note**: For detailed driver setup including proprietary databases (Oracle, SQL Server, DB2), see [Chapter 4: Database Drivers](documents/ebook/part2-chapter4-database-drivers.md).

---

## Mixed OLTP + OLAP workloads — Enable Slow Query Segregation

If the same database (and the same OJP server) serves **both** short interactive/OLTP queries **and** long reporting/analytical (OLAP) queries, you should enable **Slow Query Segregation (SQS)**. SQS prevents a few long-running queries from holding all the pool slots and starving fast user-facing traffic.

For pure OLTP-only or pure OLAP-only deployments SQS is usually unnecessary and stays disabled by default.

### How it works (in one paragraph)

OJP observes the average latency of each query shape and classifies it as *fast* or *slow*. The connection pool is split into two logical lanes — by default **80% fast slots / 20% slow slots** — and each query must acquire a slot in its lane before executing. When one lane is idle, the other lane can temporarily borrow its slots, so total throughput is preserved. Full details: [Slow Query Segregation design](documents/designs/SLOW_QUERY_SEGREGATION.md).

### Enable SQS on the OJP server (recommended starting point for mixed workloads)

Pass these JVM properties when starting the server (Docker `-e` or JAR `-D`):

```bash
java -Duser.timezone=UTC \
     -Dojp.server.slowQuerySegregation.enabled=true \
     -Dojp.server.slowQuerySegregation.slowSlotPercentage=20 \
     -jar ojp-server-<version>-shaded.jar
```

Or with Docker:

```bash
docker run --rm -d \
  --network host \
  -v $(pwd)/ojp-libs:/opt/ojp/ojp-libs \
  -e JAVA_OPTS="-Duser.timezone=UTC \
    -Dojp.server.slowQuerySegregation.enabled=true \
    -Dojp.server.slowQuerySegregation.slowSlotPercentage=20" \
  rrobetti/ojp:0.4.16-beta
```

Equivalent environment-variable form:

```bash
OJP_SERVER_SLOWQUERYSEGREGATION_ENABLED=true
OJP_SERVER_SLOWQUERYSEGREGATION_SLOWSLOTPERCENTAGE=20
```

That single flag is all that's needed to turn SQS on. The defaults are tuned for typical mixed workloads: adaptive `RELATIVE_FAST_BASELINE` classification, 20% slow-lane slots, and idle-time slot borrowing.

### Common tuning knobs

| Property | Default | When to change |
|---|---|---|
| `ojp.server.slowQuerySegregation.slowSlotPercentage` | `20` | Increase if you have a *lot* of OLAP traffic (e.g. 30–40); decrease if OLAP is rare. |
| `ojp.server.slowQuerySegregation.classificationMode` | `RELATIVE_FAST_BASELINE` | Switch to `ABSOLUTE_THRESHOLD` if you want a deterministic latency cutoff. |
| `ojp.server.slowQuerySegregation.slowQueryThresholdMs` | `1000` | Used by `ABSOLUTE_THRESHOLD` mode — set to your "this is definitely a slow query" boundary in ms. |
| `ojp.server.slowQuerySegregation.idleTimeout` | `10000` | How long a lane must be idle before its slots can be borrowed by the other lane (ms). |
| `ojp.server.slowQuerySegregation.fastSlotTimeout` / `slowSlotTimeout` | `60000` / `120000` | Max wait time (ms) for a slot in each lane before failing fast. |

See the full reference: [Slow Query Segregation Settings](documents/configuration/ojp-server-configuration.md#slow-query-segregation-settings).

### Pairing SQS with client throttling

For the strongest protection in mixed workloads, leave **client reactive throttling** at its default (`combined`) on the JDBC driver. When SQS is active and the slow lane fills, the server's overload signal causes clients to back off automatically, so application threads don't pile up waiting for slots. See [Client Reactive Throttling](documents/analysis/CLIENT_REACTIVE_THROTTLING_ANALYSIS.md).

---

## Alternative Setup: Executable JAR (No Docker)

If Docker is not available in your environment, you can run OJP Server as a standalone JAR file downloaded directly from Maven Central — no source code or build tools required:

📖 **[Executable JAR Setup Guide](documents/runnable-jar/README.md)** - Complete instructions for downloading from Maven Central and running OJP Server as a standalone executable JAR with all dependencies included.

> **For contributors:** If you need to build the JAR from source, see [Building from Source](documents/runnable-jar/BUILDING_FROM_SOURCE.md).

---

## Documentation
### High Level Solution

<img src="documents/designs/ojp_high_level_design.gif" alt="OJP High Level Design" />

* The OJP JDBC driver is used as a replacement for the native JDBC driver(s) previously used with minimal change, the only change required being prefixing the connection URL with `ojp_`. 
* **Open Source**: OJP is an open-source project that is free to use, modify, and distribute.
* **Smart database control plane**: The OJP server is deployed as an independent service that sits between application(s) and their relational database(s), centrally enforcing connection limits, admission control, throttling and quality-of-service policy.
* **Backpressure & connection-storm protection**: real database connections are allocated only when needed and capped globally, so elastic application fleets cannot overwhelm the database.
* **Client-side reactive throttling**: when the server is under pressure, clients are signaled to throttle themselves and recover automatically — preventing thread pile-ups in the application.
* **Slow vs fast query segregation**: optional lane-based segregation of OLTP and OLAP traffic on the same database — see [Mixed OLTP + OLAP workloads](#mixed-oltp--olap-workloads-enable-slow-query-segregation).
* **Rich observability**: built-in OpenTelemetry tracing and Prometheus metrics expose pool, admission, classification and throttling behaviour. See [Telemetry and Observability](documents/telemetry/README.md).
* **Load balancing & failover in the driver**: the OJP JDBC driver supports multinode URLs (`jdbc:ojp[host1:port1,host2:port2]_...`) with load-aware routing, session stickiness, and automatic failover. See [Multinode Configuration](documents/multinode/README.md).
* **Elastic scalability**: client applications can scale elastically without increasing the pressure on the database.
* **gRPC protocol** between driver and server provides multiplexed, low-latency communication — and is language-neutral, opening the door to non-Java clients (see [multi-language client spec](documents/multi-language-client-spec/)).
* **Multiple relational databases**: in theory any relational database that provides a JDBC driver implementation.
* **Simple setup**: just add the OJP library to the classpath and prefix the connection URL (e.g. `jdbc:ojp[host:port]_h2:~/test`).
* **Drop-In External Libraries**: Add proprietary JDBC drivers (Oracle, SQL Server, DB2) and additional libraries (e.g., Oracle UCP) without recompiling - see [Drop-In Driver Documentation](documents/configuration/DRIVERS_AND_LIBS.md). Simply place JARs in the `ojp-libs` directory.
* **SQL Query Enhancement**: ⚠️ **EXPERIMENTAL (NOT RECOMMENDED)** - Optional SQL enhancer with Apache Calcite for query optimization. **Disabled by default.** Has known limitations with traditional JDBC databases (PostgreSQL, MySQL, Oracle, SQL Server). See [configuration documentation](documents/configuration/ojp-server-configuration.md#sql-enhancer-and-schema-loader-settings) for details.

### Further documents
- [Docker Deployment Guide](documents/configuration/DOCKER_DEPLOYMENT.md) - Comprehensive guide for deploying OJP Server with Docker, including JVM parameter configuration, production examples, and troubleshooting.
- [Drop-In External Libraries Support](documents/configuration/DRIVERS_AND_LIBS.md) - Add proprietary database drivers and libraries (Oracle JDBC, Oracle UCP, SQL Server, DB2) without recompiling.
- [SSL/TLS Certificate Configuration Guide](documents/configuration/ssl-tls-certificate-placeholders.md) - Configure SSL/TLS certificates with server-side property placeholders for PostgreSQL, MySQL, Oracle, SQL Server, and DB2.
- [Architectural decision records (ADRs)](documents/ADRs) - Technical decisions and rationale behind OJP's architecture.
- [Get started: Spring Boot, Quarkus and Micronaut](documents/java-frameworks/README.md) - Framework-specific integration guides and examples.
- [Understanding OJP Service Provider Interfaces (SPIs)](documents/Understanding-OJP-SPIs.md) - Guide for Java developers on implementing custom connection pool providers.
- [Connection Pool Configuration](documents/configuration/ojp-jdbc-configuration.md) - OJP JDBC driver setup, connection pool settings, and environment-specific configuration (ojp-dev.properties, ojp-staging.properties, ojp-prod.properties).
- [OJP Server Configuration](documents/configuration/ojp-server-configuration.md) - Server startup options, runtime configuration, and SQL enhancer with schema loading.
- [Multinode Configuration](documents/multinode/README.md) - High availability and load balancing with multiple OJP servers.
- [Slow query segregation feature](documents/designs/SLOW_QUERY_SEGREGATION.md) - Strongly recommended for mixed fast+slow query workloads; usually not needed for pure OLTP or pure OLAP workloads.
- [Telemetry and Observability](documents/telemetry/README.md) - OpenTelemetry integration and monitoring setup.
- [OJP Components](documents/OJPComponents.md) - Core modules that define OJP’s architecture, including the server, JDBC driver, and shared gRPC contracts.
- [Targeted Problem and Solution](documents/targeted-problem/README.md) - Explanation of the problem OJP solves and how it addresses it.
- [BigDecimal Wire Format](documents/protocol/BIGDECIMAL_WIRE_FORMAT.md) - Protocol specification for language-neutral BigDecimal serialization.

---

## Vision

Provide a free and open-source **universal database control plane** for relational databases — a single, programmable layer where teams can enforce connection limits, backpressure, throttling, slow/fast segregation, and observability across many databases and (in time) many client languages. The project is designed to help microservices, event-driven, and serverless architectures scale elastically without sacrificing database stability, while giving operators a clear view into what the data tier is doing.

---

## Roadmap

See [ROADMAP.md](ROADMAP.md) for planned releases and upcoming features, including the path to 1.0.0 (production ready).

---

## Contributing & Developer Guide

Welcome to OJP! We appreciate your interest in contributing. This guide will help you get started with development.
- [OJP Contributor Recognition Program](documents/contributor-badges/contributor-recognition-program.md) - OJP Contributor Recognition rewards program and badges recognize more than code contributions, check it out!
- [Source code developer setup and local testing](documents/code-contributions/setup_and_testing_ojp_source.md) - Outlines how to get started building OJP source code locally and running tests.

---

## Partners

| Logo                                                                                                                                                                                                                        | Description                                                                                                                                | Website |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|---------|
| <a href="https://www.linkedin.com/in/devsjava/" target="_blank" rel="noopener"><img width="120px" height="120px" src="documents/images/comunidade_brasil_jug.jpeg" alt="Comunidade Brasil JUG" /></a>                       | Brazilian Java User Group connecting developers for knowledge sharing and professional networking.                                         | [linkedin.com/in/devsjava](https://www.linkedin.com/in/devsjava/) |
| <a href="https://github.com/switcherapi" target="_blank" rel="noopener"><img width="180px" src="https://github.com/switcherapi/switcherapi-assets/blob/master/logo/switcherapi_grey.png?raw=true" alt="Switcher API" /></a> | Feature management platform for managing features at scale with performance focus.                                                         | [github.com/switcherapi](https://github.com/switcherapi) |
| <a href="https://www.meterian.io/" target="_blank" rel="noopener"><img width="240px" src="https://www.meterian.io/images/brand/meterian_logo_blue.svg" alt="Meterian"  /></a>                                               | Application security platform that identifies vulnerabilities across open-source dependencies and application code.                        | [meterian.io](https://www.meterian.io/) |
| <a href="https://www.youtube.com/@cbrjar" target="_blank" rel="noopener"><img width="600px" src="/documents/images/cyberjar_logo.png" alt="CyberJAR"  /></a>                                                                | YouTube channel for Java developers covering frameworks, containers, and modern JVM topics.                                                | [youtube.com/@cbrjar](https://www.youtube.com/@cbrjar) |
| <a href="https://javachallengers.com/career-diagnosis" target="_blank" rel="noopener"><img width="150px" src="/documents/images/java_challengers_logo.jpeg" alt="Java Challengers" /></a>                                   | Helps developers go beyond coding, mastering Java fundamentals, building career confidence, and preparing for international opportunities. | [javachallengers.com](https://javachallengers.com/career-diagnosis) |
| <a href="https://omnifish.ee" target="_blank" rel="noopener"><img width="130px" src="/documents/images/omnifish_logo.png" alt="OmniFish" /></a>                                                                             | The team behind Eclipse GlassFish, delivering reliable opensource solutions with enterprise support.                                       | [omnifish.ee](https://omnifish.ee/) |
