# Getting Started

This guide walks you through building NodeDB, starting a server, and running your first queries.

## Prerequisites

- **Rust 1.89+** (edition 2024)
- **Linux** recommended — the data plane uses io_uring for storage I/O
- macOS and Windows work for development (without io_uring acceleration)

## Build from Source

```bash
git clone https://github.com/NodeDB-Lab/nodedb.git
cd nodedb

# Release build (all crates)
cargo build --release

# Run tests
cargo test --all-features
```

The build produces two binaries:

- `target/release/nodedb` — the database server
- `target/release/ndb` — the terminal client

## Start the Server

```bash
# Single-node, default ports
./target/release/nodedb
```

By default, NodeDB listens on:

- **6432** — PostgreSQL wire protocol (pgwire)
- **6433** — Native MessagePack protocol
- **6480** — HTTP API (REST, SSE, WebSocket)
- **9090** — WebSocket sync (NodeDB-Lite clients)

Two additional protocols are available but **disabled by default**:

- **RESP** (Redis-compatible KV protocol) — `GET`/`SET`/`DEL`/`EXPIRE`/`SCAN`/`SUBSCRIBE`
- **ILP** (InfluxDB Line Protocol) — high-throughput timeseries ingest

Enable them by setting a listen address in config or via env var (see below).

### Configuration

All protocols share one bind address (`host`). Only the port differs per protocol. Env vars take precedence over the TOML file.

```toml
# nodedb.toml
[server]
host = "127.0.0.1"               # Shared bind address (use 0.0.0.0 for all interfaces)
data_dir = "/var/lib/nodedb"
memory_limit = "4GiB"
data_plane_cores = 4
max_connections = 1024
log_format = "text"               # "text" or "json"

[server.ports]
native = 6433                     # Always-on protocols have defaults
pgwire = 6432
http = 6480
resp = 6381                       # Optional: set to enable, omit to disable
ilp = 8086                        # Optional: set to enable, omit to disable

[server.tls]
cert_path = "/etc/nodedb/tls/server.crt"
key_path = "/etc/nodedb/tls/server.key"
native = true                     # Per-protocol TLS toggle (all default true)
pgwire = true
http = true
resp = true
ilp = false                       # Example: disable TLS for ILP ingest
```

**Server settings:**

| Config field       | Environment variable      | Default          |
| ------------------ | ------------------------- | ---------------- |
| `host`             | `NODEDB_HOST`             | `127.0.0.1`      |
| `ports.native`     | `NODEDB_PORT_NATIVE`      | `6433`           |
| `ports.pgwire`     | `NODEDB_PORT_PGWIRE`      | `6432`           |
| `ports.http`       | `NODEDB_PORT_HTTP`        | `6480`           |
| `ports.resp`       | `NODEDB_PORT_RESP`        | disabled         |
| `ports.ilp`        | `NODEDB_PORT_ILP`         | disabled         |
| `data_dir`         | `NODEDB_DATA_DIR`         | `~/.nodedb/data` |
| `memory_limit`     | `NODEDB_MEMORY_LIMIT`     | `1GiB`           |
| `data_plane_cores` | `NODEDB_DATA_PLANE_CORES` | CPUs - 1         |
| `max_connections`  | `NODEDB_MAX_CONNECTIONS`  | `1024`           |
| `log_format`       | `NODEDB_LOG_FORMAT`       | `text`           |

**Per-protocol TLS** (only applies when `[server.tls]` is configured):

| Config field | Environment variable | Default |
| ------------ | -------------------- | ------- |
| `tls.native` | `NODEDB_TLS_NATIVE`  | `true`  |
| `tls.pgwire` | `NODEDB_TLS_PGWIRE`  | `true`  |
| `tls.http`   | `NODEDB_TLS_HTTP`    | `true`  |
| `tls.resp`   | `NODEDB_TLS_RESP`    | `true`  |
| `tls.ilp`    | `NODEDB_TLS_ILP`     | `true`  |

## Connect

### With the `ndb` CLI

```bash
./target/release/ndb
```

You get a full TUI with syntax highlighting, tab completion, and history search. See the [CLI guide](cli.md) for details.

### With psql

```bash
psql -h localhost -p 6432
```

NodeDB speaks PostgreSQL's wire protocol, so standard tools like `psql`, ORMs, and BI tools work out of the box.

### With the Rust SDK or FFI

The `nodedb-client` crate connects over the NDB protocol (port 6433) and supports both SQL and native modes on the same connection:

```rust
// SQL — same as psql/HTTP, full query support
let rows = client.sql("SELECT * FROM users WHERE age > 30").await?;

// Native — typed methods, skip SQL parsing for hot paths
let user = client.get("users", "u1").await?;
client.put("users", "u1", &doc).await?;
```

Use SQL for complex queries and rapid prototyping. Use native methods for high-throughput CRUD and ingest where parsing overhead matters. The same dual-mode access is available via `nodedb-lite-ffi` (iOS/Android) and `nodedb-lite-wasm` (WASM/browser).

## First Queries

### Documents (schemaless)

```sql
-- Create a schemaless collection
CREATE COLLECTION users TYPE document;

-- Insert some data
INSERT INTO users { name: 'Alice', email: 'alice@example.com', age: 30 };
INSERT INTO users { name: 'Bob', email: 'bob@example.com', role: 'admin' };

-- Query
SELECT * FROM users WHERE name = 'Alice';
SELECT name, email FROM users WHERE age > 25;
```

### Strict Documents (schema-enforced)

```sql
-- Create a strict collection with a defined schema
CREATE COLLECTION orders TYPE strict (
    id UUID DEFAULT gen_uuid_v7(),
    customer_id UUID,
    total DECIMAL,
    status STRING,
    created_at DATETIME DEFAULT now()
);

INSERT INTO orders (customer_id, total, status)
VALUES ('550e8400-e29b-41d4-a716-446655440000', 99.99, 'pending');

SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC;
```

### Vector Search

```sql
-- Create a collection with a vector index
CREATE COLLECTION articles TYPE document;
CREATE VECTOR INDEX ON articles FIELDS embedding DIMENSION 384 METRIC cosine;

-- Insert documents with embeddings
INSERT INTO articles { title: 'Intro to AI', embedding: [0.1, 0.2, ...] };

-- Search by similarity
SELECT title, vector_distance() AS score
FROM articles
WHERE embedding <-> [0.1, 0.3, ...]
LIMIT 10;
```

### Graph

```sql
-- Create a graph collection
CREATE COLLECTION knows TYPE graph;

-- Add edges
INSERT INTO knows { from: 'users:alice', to: 'users:bob', since: 2020 };
INSERT INTO knows { from: 'users:bob', to: 'users:carol', since: 2021 };

-- Traverse
SELECT * FROM knows WHERE from = 'users:alice' DEPTH 1..3;

-- Run an algorithm
SELECT * FROM graph::pagerank('knows', { iterations: 20 });
```

### Key-Value

```sql
-- Create a KV collection
CREATE COLLECTION sessions TYPE kv;

-- Set with TTL
INSERT INTO sessions { key: 'sess_abc', user_id: 'alice', ttl: 3600 };

-- Get by key
SELECT * FROM sessions WHERE key = 'sess_abc';
```

### Columnar (Analytics)

Columnar collections store data in compressed columns with block-level skip. Three variants are available via column modifiers and the `WITH` clause:

```sql
-- Plain columnar: general analytics
CREATE COLLECTION web_events (
    ts TIMESTAMP,
    user_id UUID,
    page VARCHAR,
    duration_ms INT
) WITH (storage = 'columnar');

SELECT page, AVG(duration_ms), COUNT(*)
FROM web_events
WHERE ts > now() - INTERVAL '7 days'
GROUP BY page
ORDER BY COUNT(*) DESC;

-- Timeseries profile: TIME_KEY column drives partition-by-time and block skip
CREATE COLLECTION cpu_metrics (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    cpu FLOAT
) WITH (storage = 'columnar', profile = 'timeseries', partition_by = '1h');

-- CREATE TIMESERIES is a convenience alias for the timeseries profile
-- CREATE TIMESERIES cpu_metrics;

SELECT time_bucket('5 minutes', ts) AS bucket, host, AVG(cpu)
FROM cpu_metrics
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY bucket, host;

-- Spatial profile: SPATIAL_INDEX column gets an automatic R*-tree
CREATE COLLECTION locations (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
) WITH (storage = 'columnar');

SELECT name, ST_Distance(geom, ST_Point(-73.98, 40.75)) AS dist
FROM locations
WHERE ST_DWithin(geom, ST_Point(-73.98, 40.75), 1000)
ORDER BY dist;
```

### Triggers

```sql
-- Fire asynchronously after each insert (default, zero write-latency impact)
CREATE TRIGGER notify_on_order AFTER INSERT ON orders FOR EACH ROW
BEGIN
    INSERT INTO notifications { user_id: NEW.customer_id, message: 'Order received' };
END;

-- Fire synchronously in the same transaction (ACID, adds trigger latency to writes)
CREATE TRIGGER enforce_balance AFTER UPDATE ON accounts FOR EACH ROW
WITH (EXECUTION = SYNC)
BEGIN
    IF NEW.balance < 0 THEN
        RAISE EXCEPTION 'Balance cannot go negative';
    END IF;
END;
```

### User-Defined Functions

```sql
-- SQL expression function (inlined into query plans by the optimizer)
CREATE FUNCTION full_name(first VARCHAR, last VARCHAR) RETURNS VARCHAR
LANGUAGE SQL IMMUTABLE AS $$ first || ' ' || last $$;

-- Use in queries
SELECT full_name(first_name, last_name) AS name FROM users;

-- Procedural function with BEGIN...END body
CREATE FUNCTION tier_label(amount DECIMAL) RETURNS VARCHAR
LANGUAGE SQL STABLE
BEGIN
    IF amount > 1000 THEN
        RETURN 'premium';
    ELSIF amount > 100 THEN
        RETURN 'standard';
    ELSE
        RETURN 'basic';
    END IF;
END;
```

### Change Streams

```sql
-- Create a change stream with webhook delivery
CREATE CHANGE STREAM order_changes
ON orders
WITH (
    WEBHOOK_URL = 'https://hooks.example.com/orders',
    WEBHOOK_SECRET = 'whsec_abc123'
);

-- Create a consumer group to track read offsets
CREATE CONSUMER GROUP processors ON order_changes;

-- Commit offsets after processing (batch: all partitions at latest)
COMMIT OFFSETS ON order_changes CONSUMER GROUP processors;

-- Show all streams
SHOW CHANGE STREAMS;
```

### Backup, Restore, and Purge

```sql
-- Backup all tenant data across all engines (encrypted with AES-256-GCM)
BACKUP TENANT acme TO '/backups/acme-2026-03-31.bak';

-- Validate a backup without restoring
RESTORE TENANT acme FROM '/backups/acme-2026-03-31.bak' DRY RUN;

-- Restore
RESTORE TENANT acme FROM '/backups/acme-2026-03-31.bak';

-- Permanently delete all tenant data (GDPR erasure) — requires CONFIRM
PURGE TENANT acme CONFIRM;

-- Inspect resource usage and limits
SHOW TENANT USAGE FOR acme;
SHOW TENANT QUOTA FOR acme;
```

## What's Next

- [Architecture](architecture.md) — understand how the three-plane execution model works
- Engine deep dives: [Vectors](vectors.md) | [Graph](graph.md) | [Documents](documents.md) | [Columnar](columnar.md) | [Timeseries](timeseries.md) | [Spatial](spatial.md) | [KV](kv.md) | [Full-Text](full-text-search.md)
- [NodeDB-Lite](lite.md) — embed NodeDB in your app (mobile, WASM, desktop)
- [Security](security.md) — set up authentication and access control

[Back to docs](README.md)
