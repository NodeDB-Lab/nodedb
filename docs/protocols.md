# Protocols & Connections

NodeDB speaks six wire protocols. All SQL-capable protocols share the same query planner and execution engine — the protocol only affects transport and encoding.

## Protocol Overview

| Protocol   | Port         | Default | Query Format           | Best For                     |
| ---------- | ------------ | ------- | ---------------------- | ---------------------------- |
| **pgwire** | 6432         | On      | SQL text               | `psql`, ORMs, BI tools, JDBC |
| **NDB**    | 6433         | On      | SQL + binary opcodes   | `ndb` CLI, Rust SDK          |
| **HTTP**   | 6480         | On      | SQL via JSON           | REST clients, browsers       |
| **Sync**   | 9090         | On      | CRDT deltas            | NodeDB-Lite (mobile, WASM)   |
| **RESP**   | configurable | Off     | Redis commands         | Cache layer, Redis clients   |
| **ILP**    | configurable | Off     | InfluxDB Line Protocol | Metrics/telemetry ingest     |

## pgwire (PostgreSQL Protocol)

Standard PostgreSQL wire protocol. Any tool that speaks Postgres works with NodeDB.

```bash
psql -h localhost -p 6432

# Or any Postgres-compatible client
# JDBC, libpq, SQLAlchemy, Prisma, etc.
```

**Supports:** Simple Query, Extended Query (prepared statements), COPY FROM, LISTEN/NOTIFY, SCRAM-SHA-256 auth, TLS.

**SQL coverage:** Everything in the [query language reference](query-language.md).

## NDB (Native Protocol)

Binary MessagePack protocol used by the `ndb` CLI and Rust SDK. Supports two modes:

**1. SQL mode** — Same SQL as pgwire, transported as MessagePack:

```sql
-- You type SQL in the ndb TUI, it gets sent as OpCode::Sql
SELECT * FROM users WHERE age > 30;
```

**2. Direct opcode mode** — Structured operations that bypass SQL parsing. Used by the SDK for performance-critical paths:

```rust
// Rust SDK — sends OpCode::VectorSearch directly
client.vector_search("articles", &query_vector, 10, None).await?;

// Equivalent SQL (goes through DataFusion):
// SELECT * FROM articles WHERE embedding <-> [0.1, ...] LIMIT 10;
```

Both modes produce the same `PhysicalPlan` and execute on the same Data Plane. The opcode path is faster for simple operations (zero parsing overhead) but limited to operations that have defined opcodes.

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
curl -X POST http://localhost:6480/query \
  -H "Authorization: Bearer ndb_..." \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users LIMIT 10"}'

# Stream results (NDJSON)
curl -X POST http://localhost:6480/query/stream \
  -d '{"sql": "SELECT * FROM large_table"}'

# Health check
curl http://localhost:6480/health

# Readiness (WAL recovered)
curl http://localhost:6480/health/ready

# Prometheus metrics
curl http://localhost:6480/metrics
```

**Additional endpoints:**

| Endpoint                         | Method | Purpose                                |
| -------------------------------- | ------ | -------------------------------------- |
| `/query`                         | POST   | Execute SQL, return JSON               |
| `/query/stream`                  | POST   | Stream results as NDJSON               |
| `/health`                        | GET    | Health check                           |
| `/health/ready`                  | GET    | Readiness probe                        |
| `/metrics`                       | GET    | Prometheus metrics                     |
| `/ws`                            | GET    | WebSocket upgrade (live subscriptions) |
| `/cdc/{collection}`              | GET    | Change Data Capture (SSE stream)       |
| `/cdc/{collection}/poll`         | GET    | CDC poll-based                         |
| `/api/auth/exchange-key`         | POST   | API key authentication                 |
| `/collections/{name}/crdt/apply` | POST   | CRDT delta application                 |
| `/obsv/api/v1/write`             | POST   | Prometheus remote write                |
| `/obsv/api/v1/query_range`       | POST   | PromQL range queries                   |

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

**Supported commands:** `GET`, `SET` (with `EX`/`PX`/`NX`/`XX`), `DEL`, `EXISTS`, `MGET`, `MSET`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `SCAN`, `KEYS`, `HGET`, `HMGET`, `HSET`, `FLUSHDB`, `DBSIZE`, `SUBSCRIBE`, `PSUBSCRIBE`, `PUBLISH`, `PING`, `ECHO`, `SELECT`, `INFO`, `QUIT`.

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
2. Server responds with `HandshakeAck`
3. Server pushes `DeltaPush` messages (CRDT mutations)
4. Client acknowledges with `DeltaAck`
5. Conflicts rejected with `DeltaReject` + `CompensationHint`

**Message types:** `Handshake`, `HandshakeAck`, `DeltaPush`, `DeltaAck`, `DeltaReject`, `Throttle`, `PingPong`, `ResyncRequest`, `ShapeSnapshot`, `ShapeSubscribe`, `TimeseriesPush`.

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

## Native Protocol Opcodes

The NDB protocol's direct opcode mode covers 18 engine-specific operations added across the engines. All opcodes are single-byte identifiers in the MessagePack framing.

| Opcode               | Hex    | Operation                                        |
| -------------------- | ------ | ------------------------------------------------ |
| `TimeseriesScan`     | `0x1A` | Time-range scan with optional bucket aggregation |
| `TimeseriesIngest`   | `0x1B` | Batch ingest into a timeseries collection        |
| `SpatialScan`        | `0x19` | R\*-tree lookup with OGC predicate               |
| `KvScan`             | `0x72` | Full scan over a KV collection                   |
| `KvGet`              | `0x73` | Point lookup by key                              |
| `KvSet`              | `0x74` | Set a key-value pair with optional TTL           |
| `KvDelete`           | `0x75` | Delete by key                                    |
| `KvExpire`           | `0x76` | Set TTL on an existing key                       |
| `KvMultiGet`         | `0x77` | Batch point lookups                              |
| `KvMultiSet`         | `0x78` | Batch set                                        |
| `KvFieldSet`         | `0x79` | Set individual fields on a KV value              |
| `DocumentUpdate`     | `0x7A` | Update fields on a document by ID                |
| `DocumentPatch`      | `0x7B` | JSON-patch a document by ID                      |
| `DocumentGet`        | `0x7C` | Fetch a document by ID                           |
| `DocumentBulkInsert` | `0x7D` | Batch insert documents                           |
| `DocumentBulkDelete` | `0x7E` | Batch delete documents by predicate              |
| `VectorInsert`       | `0x7F` | Insert a vector with metadata                    |
| `VectorSearch`       | `0x80` | ANN search (HNSW) with optional pre-filter       |
| `VectorDelete`       | `0x81` | Delete a vector by ID                            |

## Which Protocol Should I Use?

| Use case                                  | Protocol          |
| ----------------------------------------- | ----------------- |
| Standard SQL tooling (psql, ORMs, BI)     | pgwire            |
| NodeDB CLI                                | NDB (automatic)   |
| Rust/Go/Python application                | NDB SDK or pgwire |
| Web app / REST API                        | HTTP              |
| Existing Redis client / cache replacement | RESP              |
| High-throughput metrics ingest            | ILP               |
| Mobile/WASM offline-first sync            | Sync (WebSocket)  |
| Prometheus scraping                       | HTTP (`/metrics`) |

## Related

- [Query Language](query-language.md) — Full SQL reference (works on all SQL-capable protocols)
- [Getting Started](getting-started.md) — Build, connect, first queries
- [CLI](cli.md) — `ndb` terminal client usage
- [KV](kv.md) — Redis-compatible access details
- [Timeseries](timeseries.md) — ILP ingest details
- [NodeDB-Lite](lite.md) — Sync protocol details

[Back to docs](README.md)
