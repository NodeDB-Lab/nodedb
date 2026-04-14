# Getting Started

This guide walks you through starting a NodeDB server and running your first queries. NodeDB requires Linux kernel ≥ 5.1 (for io_uring) regardless of how you run it.

There are three ways to install NodeDB:

1. [Prebuilt binary](#run-a-prebuilt-binary-linux) — **recommended on Linux.** Direct kernel access to io_uring, no virtualization overhead, best raw performance.
2. [Docker](#run-with-docker) — **recommended on macOS / Windows / WSL2**, or when you want a one-command setup with zero host configuration.
3. [Build from source](#build-from-source) — for development or custom features.

All three share the same [configuration](#configuration), [connection](#connect), and [query](#first-queries) sections below.

## Run with Docker

The easiest way to get started, and the right choice on macOS, Windows, or any host where you don't want to manage a binary directly. On native Linux, the [prebuilt binary](#run-a-prebuilt-binary-linux) gives you better performance.

### Docker Compose

```bash
docker compose up -d
```

That's it. NodeDB starts on the default ports with data persisted to a named volume.

To stop:

```bash
docker compose down
```

To stop and wipe all data:

```bash
docker compose down -v
```

### Plain `docker run`

If you'd rather not use Compose:

```bash
docker run -d --name nodedb \
  -p 6432:6432 \
  -p 6433:6433 \
  -p 6480:6480 \
  -p 9090:9090 \
  -v nodedb-data:/var/lib/nodedb \
  farhansyah/nodedb
```

The container entrypoint runs as root just long enough to fix ownership on the data volume, then drops privileges to the `nodedb` user (uid 10001). To skip the root step, pass `--user 10001:10001` and pre-create the volume with matching ownership.

### Default ports

- **6432** — PostgreSQL wire protocol (pgwire)
- **6433** — Native MessagePack protocol
- **6480** — HTTP API (REST, SSE, WebSocket)
- **9090** — WebSocket sync (NodeDB-Lite clients)

### Verify it's running

```bash
curl http://localhost:6480/health
# {"status":"ok", ...}
```

### Custom port mapping

Edit `docker-compose.yml` to remap any port. The container always listens internally on the same ports — only the host-side mapping changes:

```yaml
ports:
  - "5432:6432" # expose pgwire on host port 5432 instead
  - "6433:6433"
  - "6480:6480"
```

### Common env vars

| Variable                  | Default    | Description                  |
| ------------------------- | ---------- | ---------------------------- |
| `NODEDB_MEMORY_LIMIT`     | 75% of RAM | e.g. `4GiB`                  |
| `NODEDB_DATA_PLANE_CORES` | CPUs - 1   | number of Data Plane threads |
| `NODEDB_LOG_FORMAT`       | `text`     | `text` or `json`             |

Set them under `environment:` in `docker-compose.yml` or pass with `-e` to `docker run`.

---

## Run a prebuilt binary (Linux)

Each tagged release ships a static `nodedb` tarball on GitHub for `linux-x64` and `linux-arm64`. macOS and Windows users should use Docker until those targets ship.

```bash
# Resolve the latest tag and your architecture
TAG=$(curl -fsSL https://api.github.com/repos/NodeDB-Lab/nodedb/releases/latest \
        | grep '"tag_name"' | cut -d'"' -f4)
ARCH=$(uname -m | sed 's/aarch64/arm64/; s/x86_64/x64/')

# Download and extract
curl -L -o nodedb.tar.gz \
  "https://github.com/NodeDB-Lab/nodedb/releases/download/${TAG}/nodedb-${TAG#v}-linux-${ARCH}.tar.gz"
tar -xzf nodedb.tar.gz

# Optional: install system-wide
sudo mv nodedb /usr/local/bin/

# Run with all defaults (data goes to ~/.nodedb/data)
nodedb
```

If you have the [GitHub CLI](https://cli.github.com/) installed, this is one command:

```bash
gh release download --repo NodeDB-Lab/nodedb --pattern 'nodedb-*-linux-x64.tar.gz' \
  && tar -xzf nodedb-*-linux-x64.tar.gz
```

To run with a config file or a custom data directory:

```bash
# Point at an explicit data dir
NODEDB_DATA_DIR=/var/lib/nodedb nodedb

# Or load a config file (env vars still override TOML keys)
nodedb --config /etc/nodedb/nodedb.toml
```

For a long-running server, drop a unit file at `/etc/systemd/system/nodedb.service`:

```ini
[Unit]
Description=NodeDB
After=network.target

[Service]
Type=simple
User=nodedb
Group=nodedb
ExecStart=/usr/local/bin/nodedb --config /etc/nodedb/nodedb.toml
Restart=on-failure
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

Then `sudo systemctl enable --now nodedb`. The user/group must be able to read the config file and write `data_dir`.

> For a specific version or to browse changelogs, see the release page: <https://github.com/NodeDB-Lab/nodedb/releases>. The SQL surface is still pre-1.0 and changes between tags, so pin a version in production.

---

## Build from Source

```bash
git clone https://github.com/NodeDB-Lab/nodedb.git
cd nodedb

# Release build (all crates)
cargo build --release

# Run tests (use nextest — see .config/nextest.toml)
cargo install cargo-nextest --locked  # one-time
cargo nextest run --all-features
```

Requires Rust 1.94+ and Linux (the Data Plane uses io_uring). The build produces two binaries:

- `target/release/nodedb` — the database server
- `target/release/ndb` — the terminal client

## Start the Server (from source)

```bash
# Single-node, default ports
./target/release/nodedb

# Or with a config file
./target/release/nodedb --config nodedb.toml
```

---

## Configuration

This section applies to **every** install method — Docker, prebuilt binary, and source builds all read the same TOML schema and respond to the same environment variables. Pick whichever is convenient:

- **TOML file** — pass `--config /path/to/nodedb.toml` on the command line. Best for production / systemd / pre-baked images.
- **Environment variables** — prefix `NODEDB_*`. Best for Docker (`-e`), Compose (`environment:`), and Kubernetes. Env vars **override** values from the TOML file when both are set.

### Default ports

By default, NodeDB listens on:

- **6432** — PostgreSQL wire protocol (pgwire)
- **6433** — Native MessagePack protocol
- **6480** — HTTP API (REST, SSE, WebSocket)
- **9090** — WebSocket sync (NodeDB-Lite clients)

Two additional protocols are available but **disabled by default**:

- **RESP** (Redis-compatible KV protocol) — `GET`/`SET`/`DEL`/`EXPIRE`/`SCAN`/`SUBSCRIBE`
- **ILP** (InfluxDB Line Protocol) — high-throughput timeseries ingest

Enable them by setting a listen address in the config or via env var (see below).

### Example config file

All protocols share one bind address (`host`). Only the port differs per protocol.

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

| Config field       | Environment variable      | Default                                               |
| ------------------ | ------------------------- | ----------------------------------------------------- |
| `host`             | `NODEDB_HOST`             | `127.0.0.1`                                           |
| `ports.native`     | `NODEDB_PORT_NATIVE`      | `6433`                                                |
| `ports.pgwire`     | `NODEDB_PORT_PGWIRE`      | `6432`                                                |
| `ports.http`       | `NODEDB_PORT_HTTP`        | `6480`                                                |
| `ports.resp`       | `NODEDB_PORT_RESP`        | disabled                                              |
| `ports.ilp`        | `NODEDB_PORT_ILP`         | disabled                                              |
| `data_dir`         | `NODEDB_DATA_DIR`         | `~/.nodedb/data` (binary), `/var/lib/nodedb` (Docker) |
| `memory_limit`     | `NODEDB_MEMORY_LIMIT`     | `1GiB`                                                |
| `data_plane_cores` | `NODEDB_DATA_PLANE_CORES` | CPUs - 1                                              |
| `max_connections`  | `NODEDB_MAX_CONNECTIONS`  | `1024`                                                |
| `log_format`       | `NODEDB_LOG_FORMAT`       | `text`                                                |

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
CREATE COLLECTION users;

-- Insert some data (standard SQL)
INSERT INTO users (id, name, email, age) VALUES ('u1', 'Alice', 'alice@example.com', 30);
INSERT INTO users (id, name, email, role) VALUES ('u2', 'Bob', 'bob@example.com', 'admin');

-- Object literal syntax also works
INSERT INTO users { name: 'Alice', email: 'alice@example.com', age: 30 };
INSERT INTO users { name: 'Bob', email: 'bob@example.com', role: 'admin' };

-- Query
SELECT * FROM users WHERE name = 'Alice';
SELECT name, email FROM users WHERE age > 25;
```

### Strict Documents (schema-enforced)

```sql
-- Create a strict collection with a defined schema
CREATE COLLECTION orders TYPE DOCUMENT STRICT (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    total FLOAT,
    status TEXT,
    created_at TIMESTAMP
);

INSERT INTO orders (id, customer_id, total, status)
VALUES ('o1', 'u1', 99.99, 'pending');

SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC;
```

### Vector Search

```sql
-- Create a collection with a vector index
CREATE COLLECTION articles;
CREATE VECTOR INDEX idx_articles_embedding ON articles METRIC cosine DIM 384;

-- Insert documents with embeddings (standard SQL)
INSERT INTO articles (id, title, embedding) VALUES ('a1', 'Intro to AI', ARRAY[0.1, 0.2, 0.3]);
-- Or:
INSERT INTO articles { id: 'a1', title: 'Intro to AI', embedding: [0.1, 0.2, 0.3] };

-- Search by similarity
SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, ...], 10);
```

### Graph

```sql
-- Graph is an overlay on document collections, not a separate collection type
CREATE COLLECTION social;

-- Insert nodes
INSERT INTO social (id, name) VALUES ('alice', 'Alice');
INSERT INTO social (id, name) VALUES ('bob', 'Bob');

-- Add edges
-- JSON string form:
GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'knows' PROPERTIES '{"since": 2020}';
-- Object literal form (equivalent):
GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'knows' PROPERTIES { since: 2020 };

-- Traverse
GRAPH TRAVERSE FROM 'alice' DEPTH 2;

-- Run an algorithm
GRAPH ALGO PAGERANK ON social DAMPING 0.85 ITERATIONS 20 TOLERANCE 1e-7;
```

### Key-Value

```sql
-- Create a KV collection
CREATE COLLECTION sessions TYPE KEY_VALUE (key TEXT PRIMARY KEY);

-- Set a key-value pair (standard SQL)
INSERT INTO sessions (key, value) VALUES ('sess_abc', 'token-abc');
-- Or:
INSERT INTO sessions { key: 'sess_abc', value: 'token-abc' };

-- Get by key
SELECT * FROM sessions WHERE key = 'sess_abc';
```

### Columnar (Analytics)

Columnar collections store data in compressed columns with block-level skip. Three variants are available via column modifiers and the `WITH` clause:

```sql
-- Plain columnar: general analytics
CREATE COLLECTION web_events TYPE COLUMNAR (
    ts TIMESTAMP,
    user_id UUID,
    page VARCHAR,
    duration_ms INT
);

SELECT page, AVG(duration_ms), COUNT(*)
FROM web_events
WHERE ts > now() - INTERVAL '7 days'
GROUP BY page
ORDER BY COUNT(*) DESC;

-- Timeseries profile: TIME_KEY column drives partition-by-time and block skip
CREATE COLLECTION cpu_metrics TYPE COLUMNAR (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    cpu FLOAT
) WITH profile = 'timeseries', partition_by = '1h';

-- CREATE TIMESERIES is a convenience alias for the timeseries profile
-- CREATE TIMESERIES cpu_metrics;

SELECT time_bucket('5 minutes', ts) AS bucket, host, AVG(cpu)
FROM cpu_metrics
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY bucket, host;

-- Spatial profile: SPATIAL_INDEX column gets an automatic R*-tree
CREATE COLLECTION locations TYPE COLUMNAR (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
);

SELECT name, ST_Distance(geom, ST_Point(-73.98, 40.75)) AS dist
FROM locations
WHERE ST_DWithin(geom, ST_Point(-73.98, 40.75), 1000)
ORDER BY dist;
```

### Triggers

```sql
-- Fire asynchronously after each insert (default, zero write-latency impact)
CREATE TRIGGER notify_on_order AFTER INSERT ON orders FOR EACH ROW $$
BEGIN
    INSERT INTO notifications (id, user_id, message)
    VALUES (NEW.id || '_notif', NEW.customer_id, 'Order received');
END;
$$;

-- Fire synchronously in the same transaction (ACID, adds trigger latency to writes)
CREATE TRIGGER enforce_balance AFTER UPDATE ON accounts FOR EACH ROW
WITH (EXECUTION = SYNC) $$
BEGIN
    IF NEW.balance < 0 THEN
        RAISE EXCEPTION 'Balance cannot go negative';
    END IF;
END;
$$;
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
- [Security](security/README.md) — set up authentication and access control

[Back to docs](README.md)
