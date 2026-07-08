# Protocols & Connections

NodeDB speaks six wire protocols. All SQL-capable protocols share the same query planner and execution engine — the protocol only affects transport and encoding.

## Protocol Overview

| Protocol   | Port         | Default | Query Format                      | Best For                     |
| ---------- | ------------ | ------- | --------------------------------- | ---------------------------- |
| **pgwire** | 6432         | On      | SQL text                          | `psql`, ORMs, BI tools, JDBC |
| **NDB**    | 6433         | On      | SQL (user) / native opcodes (SDK) | `ndb` CLI, Rust SDK, FFI     |
| **HTTP**   | 6480         | On      | SQL via JSON                      | REST clients, browsers       |
| **Sync**   | 9090         | On      | CRDT deltas                       | NodeDB-Lite (mobile, WASM)   |
| **RESP**   | configurable | Off     | Redis commands                    | Cache layer, Redis clients   |
| **ILP**    | configurable | Off     | InfluxDB Line Protocol            | Metrics/telemetry ingest     |

## pgwire (PostgreSQL Protocol)

Standard PostgreSQL wire protocol. Any tool that speaks Postgres works with NodeDB.

```bash
psql -h localhost -p 6432

# Or any Postgres-compatible client
# JDBC, libpq, SQLAlchemy, Prisma, etc.
```

**Supports:** Simple Query, Extended Query (prepared statements), COPY FROM, LISTEN/NOTIFY, SCRAM-SHA-256 auth, TLS.

**SQL coverage:** Everything in the [query language reference](query-language.md).

**Driver compatibility:** NodeDB advertises `server_version` (`NodeDB <version>`) and a PostgreSQL-compatible `server_version_num` in the startup parameter burst, and supports the probes drivers issue on connect: `version()` returns a PostgreSQL-compatible string, `current_setting(name [, missing_ok])` resolves the same settings as `SHOW`, and `::regclass`/`::regtype` casts, `ANY(current_schemas(...))`, and cross-catalog-table JOINs evaluate PostgreSQL-identically. Binary result formats requested at Bind are honored per column; types the encoder cannot yet emit in binary (timestamp, numeric, json/jsonb, arrays) downgrade to text, with the Describe-phase RowDescription kept in sync.

**Introspection:** NodeDB exposes PostgreSQL-compatible `pg_catalog` virtual tables (e.g., `pg_class`, `pg_namespace`, `pg_attribute`, `pg_type`) so that standard Postgres clients, ORMs, and business intelligence tools can introspect the database schema without modification (`psql \d`, driver type caches, ORM bootstraps). Queries against `pg_catalog.*` tables are transparently rewritten to pull from NodeDB's internal catalog.

**Streaming:** Unordered multi-row SELECTs stream lazily to the client — rows are not buffered and merged on the coordinator first. Ordered, aggregate, point-get, and search queries use the materialized path.

## NDB (Native Protocol)

Binary MessagePack protocol used by the `ndb` CLI, Rust SDK, and FFI bindings. It carries two kinds of messages, serving two different audiences:

**SQL (user-facing)** — The primary interface. SQL text is transported as a MessagePack `Sql` message and goes through DataFusion exactly as it does on pgwire:

```sql
-- You type SQL in the ndb TUI; it is sent as a Sql message over the NDB protocol
SELECT * FROM users WHERE age > 30;
```

**Native opcodes (SDK optimization)** — Typed, structured messages used by `nodedb-client` (Rust SDK), `nodedb-lite-ffi` (iOS/Android), and `nodedb-lite-wasm` (WASM) for programmatic access. They skip SQL parsing and serialization overhead, routing directly to `build_plan()`:

```rust
// Rust SDK — dispatches a native VectorSearch opcode; no SQL parsing
client.vector_search("articles", &query_vector, 10, None).await?;

// Equivalent SQL (goes through DataFusion):
// SEARCH articles USING VECTOR(embedding, ARRAY[0.1, ...], 10);
```

Both paths produce the same `PhysicalPlan` and execute on the same Data Plane. SDKs support **both modes** on the same connection:

```rust
// SQL mode — flexible, any query, fast development
let rows = client.sql("SELECT * FROM users WHERE age > 30 ORDER BY name").await?;

// Native mode — typed methods, skip SQL parsing, maximum throughput
let user = client.get("users", "u1").await?;
client.put("users", "u1", &doc).await?;
```

Use SQL for complex queries, ad-hoc exploration, and rapid prototyping. Use native methods for hot-path CRUD, vector search, and high-throughput ingest where parsing overhead matters.

**Transactions:** The native protocol supports full transaction blocks including savepoints (`BEGIN` / `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` / `COMMIT`), sharing the same protocol-neutral session state as pgwire. Native writes issued inside a transaction stage into the same per-transaction overlay, so read-your-own-writes semantics hold across SQL and native opcodes on one connection. HTTP is stateless and does not support transaction blocks.

**Connection:**

```bash
# ndb CLI connects automatically
./target/release/ndb

# Or specify host
./target/release/ndb --host localhost --port 6433
```

## HTTP API

REST API for web clients and services.

```bash
# Execute SQL
curl -X POST http://localhost:6480/v1/query \
  -H "Authorization: Bearer ndb_..." \
  -H "Content-Type: application/json" \
  -H "Accept: application/vnd.nodedb.v1+json" \
  -d '{"sql": "SELECT * FROM users LIMIT 10"}'

# Stream results (NDJSON)
curl -X POST http://localhost:6480/v1/query/stream \
  -d '{"sql": "SELECT * FROM large_table"}'

# k8s readiness probe (503 until startup ready)
curl http://localhost:6480/healthz

# Liveness / Readiness
curl http://localhost:6480/health/live
curl http://localhost:6480/health/ready

# Prometheus metrics
curl http://localhost:6480/metrics
```

All non-probe routes are under `/v1/`. JSON responses carry `Content-Type: application/vnd.nodedb.v1+json; charset=utf-8`. Probes are unversioned and always reachable.

`/v1/query/stream` streams rows lazily as NDJSON — one line per row, produced as shards return batches. A mid-stream error surfaces in-band as a final `{"error": "..."}` line (the HTTP status stays `200` since headers are already sent). HTTP is stateless: transaction blocks (`BEGIN`/`COMMIT`) are not supported.

**Additional endpoints:**

| Endpoint                                 | Method      | Purpose                          |
| ---------------------------------------- | ----------- | -------------------------------- |
| `/v1/query`                              | POST        | Execute SQL, return JSON         |
| `/v1/query/stream`                       | POST        | Stream results as NDJSON         |
| `/v1/status`                             | GET         | Node status                      |
| `/v1/cluster/status`                     | GET         | Cluster status                   |
| `/v1/auth/exchange-key`                  | POST        | API key → session token          |
| `/v1/auth/session`                       | POST/DELETE | Create/delete session            |
| `/v1/collections/{name}/crdt/apply`      | POST        | CRDT delta application           |
| `/v1/cdc/{collection}`                   | GET         | Change Data Capture (SSE stream) |
| `/v1/cdc/{collection}/poll`              | GET         | CDC poll-based                   |
| `/v1/streams/{stream}/events`            | GET         | Named-stream events (SSE)        |
| `/v1/streams/{stream}/poll`              | GET         | Named-stream long-poll           |
| `/v1/cluster/debug/raft/{group_id}`      | GET         | Raft group diagnostics           |
| `/v1/cluster/debug/transport`            | GET         | QUIC transport diagnostics       |
| `/v1/cluster/debug/quarantined-segments` | GET         | Segments in CRC quarantine       |
| `/v1/ws`                                 | GET         | WebSocket upgrade                |
| `/v1/obsv/api/v1/write`                  | POST        | Prometheus remote write          |
| `/v1/obsv/api/v1/query_range`            | POST        | PromQL range queries             |
| `/healthz`                               | GET         | k8s readiness probe              |
| `/health/live`                           | GET         | Liveness                         |
| `/health/ready`                          | GET         | Readiness                        |
| `/health/drain`                          | POST        | Cooperative drain                |
| `/metrics`                               | GET         | Prometheus metrics               |

## RESP (Redis Protocol)

Redis-compatible wire protocol for KV operations. **Disabled by default** — enable by setting a port:

```toml
# nodedb.toml
[server.ports]
resp = 6381
```

Or: `NODEDB_PORT_RESP=6381`

```bash
redis-cli -p 6381

# Switch collection (default: "default")
SELECT sessions

# Standard commands
SET sess_abc '{"user_id":"alice"}' EX 3600
GET sess_abc
DEL sess_abc
EXPIRE sess_abc 7200
TTL sess_abc

# Batch
MSET key1 val1 key2 val2
MGET key1 key2

# Scan
SCAN 0 MATCH sess_* COUNT 100

# Hash fields
HSET sess_abc role admin
HGET sess_abc role

# Pub/Sub
SUBSCRIBE sessions
PUBLISH sessions "user_logged_in"
```

**Supported commands:** `GET`, `SET` (with `EX`/`PX`/`NX`/`XX`), `GETSET`, `DEL`, `EXISTS`, `MGET`, `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `SCAN`, `KEYS`, `HGET`, `HMGET`, `HSET`, `ZADD`, `ZREM`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `FLUSHDB`, `DBSIZE`, `SUBSCRIBE`, `PSUBSCRIBE`, `PUBLISH`, `PING`, `ECHO`, `SELECT`, `INFO`, `QUIT`.

All RESP commands dispatch to the same KV engine as SQL queries. Data written via RESP is queryable via SQL and vice versa.

## ILP (InfluxDB Line Protocol)

High-throughput timeseries ingest. **Disabled by default** — enable by setting a port:

```toml
# nodedb.toml
[server.ports]
ilp = 8086
```

Or: `NODEDB_PORT_ILP=8086`

**Format:**

```
measurement[,tag=val,...] field=value[,field=value,...] [timestamp_ns]
```

**Examples:**

```
cpu,host=server01,region=us-east load=0.65,temp=23.5 1609459200000000000
disk,mount=/home used=1024i
memory free=8192i,cached=4096i
```

**Field types:** Float (`1.0`), Int (`42i`), UInt (`42u`), String (`"hello"`), Bool (`true`/`false`).

Timestamp is optional (server-assigned if omitted). Schema is auto-inferred from the first batch. Data lands in the timeseries engine's columnar memtable with cascading compression.

ILP is write-only. Query ingested data via SQL on any protocol:

```sql
SELECT * FROM cpu WHERE ts > now() - INTERVAL '1 hour';
```

## Sync (WebSocket)

CRDT sync protocol for NodeDB-Lite clients (mobile, WASM, desktop). Bidirectional delta exchange over WebSocket.

**Port:** 9090 (default)

**Flow:**

1. Client connects and sends `Handshake` with JWT + vector clock
2. Peer announces `CollectionSchema` for each synced collection before any shape or delta data — unknown collections are materialized into the local catalog (create-only; an existing collection is never clobbered) and propagate cluster-wide via Raft
3. Server pushes `DeltaPush` messages (CRDT mutations)
4. Client acknowledges with `DeltaAck`
5. Constraint violations rejected with `DeltaReject` + a typed `CompensationHint`

**Message types:** `Handshake`, `HandshakeAck`, `CollectionSchema`, `DeltaPush`, `DeltaAck`, `DeltaReject`, `Throttle`, `PingPong`, `ResyncRequest`, `ShapeSnapshot`, `ShapeSubscribe`, `TimeseriesPush`.

**Durability:** An acknowledged sync write is quorum-durable — the delta is committed through the data group's Raft log before the ack, so it survives leader failover.

**Constraint validation:** UNIQUE, FK, required-field, and CHECK constraints are validated at apply time on Origin. A rejection carries a machine-readable `CompensationHint` (e.g., `UniqueViolation { field, conflicting_value }`, `RetryWithDifferentValue`, `ManualIntervention`) rather than a string, and a rejected delta does not wedge the stream — subsequent deltas continue to apply. Rate-limit rejections are a distinct retryable error, so clients can tell "slow down" apart from a constraint conflict.

**Idempotent producers:** Every per-engine sync message carries `(producer_id, epoch, seq)` provenance; replayed deltas are acknowledged as duplicates instead of double-applied.

This protocol is used by NodeDB-Lite for offline-first sync. See [NodeDB-Lite](lite.md) for details.

## Configuration

All protocols share one bind address. Only the port differs.

```toml
# nodedb.toml
[server]
host = "127.0.0.1"

[server.ports]
pgwire = 6432       # Always on
native = 6433       # Always on
http = 6480         # Always on
resp = 6381         # Set to enable (omit to disable)
ilp = 8086          # Set to enable (omit to disable)

[server.tls]
cert_path = "/etc/nodedb/tls/server.crt"
key_path = "/etc/nodedb/tls/server.key"
pgwire = true       # Per-protocol TLS toggle
native = true
http = true
resp = true
ilp = false         # Example: disable TLS for ILP ingest
```

Environment variables override config: `NODEDB_PORT_PGWIRE`, `NODEDB_PORT_NATIVE`, `NODEDB_PORT_HTTP`, `NODEDB_PORT_RESP`, `NODEDB_PORT_ILP`.

## Native Protocol Opcodes (SDK Reference)

Native opcodes are used internally by the Rust SDK (`nodedb-client`), FFI bindings (`nodedb-lite-ffi`), and WASM bindings (`nodedb-lite-wasm`). Application code does not construct opcodes directly — it calls typed SDK methods that dispatch the appropriate opcode. All opcodes are single-byte identifiers in the MessagePack framing.

The native protocol defines a distinct opcode per operation across all engines (90+ operations total). Common opcodes include:

- `PointGet` (0x10) — Key-value point lookup
- `VectorSearch` (0x13) — Vector similarity search
- `VectorMultiSearch` (0x80) — Batch vector search
- `TimeseriesScan` (0x1A) — Time-range aggregated scan
- `SpatialScan` (0x19) — Spatial range query
- `DocumentGet`, `DocumentUpdate`, `DocumentBulkInsert` — Document operations

The full opcode set is defined by the protocol specification. SDKs expose these via typed methods (`client.get()`, `client.vector_search()`, etc.); direct opcode construction is rarely needed.

## TLS

### Default behaviour

All five listeners (`pgwire`, `native`, `http`, `resp`, `ilp`) default to
**plaintext** when no `[server.tls]` section is present in the configuration file.
This is suitable for local development and for deployments where TLS is
terminated at an external load balancer or sidecar proxy.

**Production deployments MUST configure `[server.tls] cert_path` and
`[server.tls] key_path`** whenever clients connect over any untrusted network.
Without TLS, credentials (including SCRAM-SHA-256 client proofs), query
text, and result data are transmitted in plaintext.

### Enabling TLS

```toml
[server.tls]
cert_path = "/etc/nodedb/tls/server.crt"
key_path  = "/etc/nodedb/tls/server.key"
```

When `[server.tls]` is present, TLS is enabled on all five listeners by default.
The certificate must be PEM-encoded. Generate a self-signed cert for
testing with `openssl req -x509 -newkey rsa:4096 -nodes ...` or a
production certificate from any ACME-compatible CA.

### Per-protocol overrides

Individual listeners can opt out of TLS using the per-protocol flags:

```toml
[server.tls]
cert_path = "/etc/nodedb/tls/server.crt"
key_path  = "/etc/nodedb/tls/server.key"
pgwire    = true   # default when [tls] is set
native    = true
http      = true
resp      = true
ilp       = false  # example: trusted loopback ingest
```

Setting any flag to `false` on an internet-facing deployment is **not
recommended** even for write-only ingest protocols.

### Certificate hot-reload

NodeDB watches cert/key files for modification-time changes and atomically
swaps the TLS configuration without restarting listeners:

```toml
[server.tls]
cert_path                 = "/etc/nodedb/tls/server.crt"
key_path                  = "/etc/nodedb/tls/server.key"
cert_reload_interval_secs = 3600   # default: 1 hour; 0 to disable
```

This supports automated rotation (certbot, cert-manager) without downtime.

### Cipher suites and TLS version

NodeDB delegates cipher suite selection to
[rustls](https://github.com/rustls/rustls) defaults, which enable
TLS 1.2 and TLS 1.3. TLS 1.3 is preferred by clients that support it.
TLS 1.0 and 1.1 are not offered. Production environments that require
TLS 1.3 exclusively should enforce the version constraint at the load
balancer or TLS terminator.

### pgwire TLS negotiation

NodeDB follows the PostgreSQL wire protocol SSLRequest flow:

1. Client sends an 8-byte `SSLRequest` packet.
2. Server replies with single byte `S` (TLS available) or `N` (plaintext only).
3. When the server replies `S`, the client initiates a TLS handshake on the
   same connection.

Standard PostgreSQL client libraries (libpq, JDBC, tokio-postgres, etc.)
handle this automatically. No application-level changes are required.

## Which Protocol Should I Use?

| Use case                                  | Protocol                                      |
| ----------------------------------------- | --------------------------------------------- |
| Standard SQL tooling (psql, ORMs, BI)     | pgwire                                        |
| NodeDB CLI (`ndb`)                        | NDB — SQL mode (automatic)                    |
| Rust application (programmatic)           | NDB — via `nodedb-client` (native opcodes)    |
| iOS / Android (FFI)                       | NDB — via `nodedb-lite-ffi` (native opcodes)  |
| WASM / browser                            | NDB — via `nodedb-lite-wasm` (native opcodes) |
| Web app / REST API                        | HTTP                                          |
| Existing Redis client / cache replacement | RESP                                          |
| High-throughput metrics ingest            | ILP                                           |
| Mobile/WASM offline-first sync            | Sync (WebSocket)                              |
| Prometheus scraping                       | HTTP (`/metrics`)                             |

## Related

- [Query Language](query-language.md) — Full SQL reference (works on all SQL-capable protocols)
- [Getting Started](getting-started.md) — Build, connect, first queries
- [CLI](cli.md) — `ndb` terminal client usage
- [KV](kv.md) — Redis-compatible access details
- [Timeseries](timeseries.md) — ILP ingest details
- [NodeDB-Lite](lite.md) — Sync protocol details

[Back to docs](README.md)
