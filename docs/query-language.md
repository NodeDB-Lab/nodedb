# Query Language

NodeDB uses SQL as its primary query language across all protocols. Whether you connect via `ndb`, `psql`, or HTTP — the same SQL works everywhere. NodeDB extends standard SQL with engine-specific syntax for vectors, graphs, spatial, CRDT, and timeseries operations.

## How Queries Execute

SQL is the primary interface. All user-visible entry points feed the same pipeline:

```
ndb CLI (NDB protocol)   ──┐
psql    (pgwire)         ──┼──► DataFusion Planner ──► PhysicalPlan ──► Data Plane
HTTP    (REST/JSON)      ──┘
```

Three doors, one room. Same parser, same optimizer, same execution engine.

The Rust SDK (`nodedb-client`), FFI bindings (`nodedb-lite-ffi`), and WASM bindings (`nodedb-lite-wasm`) take a parallel path for programmatic access: they dispatch native opcodes over the NDB protocol, which the Control Plane converts directly to a `PhysicalPlan` via `build_plan()` — skipping SQL parsing. The resulting plan is identical in structure and executes on the same Data Plane. This is an SDK-internal optimization; application code always interacts through SQL or typed SDK methods, never raw opcodes.

## SELECT

```sql
SELECT [DISTINCT] <columns>
FROM <collection>
[WHERE <predicate>]
[GROUP BY <fields>] [HAVING <predicate>]
[ORDER BY <field> [ASC|DESC], ...]
[LIMIT <n>] [OFFSET <m>]
```

### Filtering

```sql
-- Equality, comparison
SELECT * FROM users WHERE age > 30 AND status = 'active';

-- Pattern matching
SELECT * FROM users WHERE name LIKE 'Ali%';
SELECT * FROM users WHERE email ILIKE '%@EXAMPLE.COM';

-- Range
SELECT * FROM orders WHERE total BETWEEN 10 AND 100;

-- Set membership
SELECT * FROM users WHERE role IN ('admin', 'editor');

-- Null checks
SELECT * FROM users WHERE deleted_at IS NULL;
```

### Aggregates

```sql
SELECT status, COUNT(*), AVG(age), MIN(salary), MAX(salary)
FROM employees
WHERE department = 'sales'
GROUP BY status
HAVING COUNT(*) > 5;

SELECT COUNT(DISTINCT user_id) FROM orders;
```

### Joins

```sql
-- Inner join
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id;

-- Left join
SELECT u.name, o.total
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;

-- Cross join
SELECT * FROM sizes CROSS JOIN colors;

-- Semi join (rows from left that have a match on the right)
SELECT * FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

-- Anti join (rows from left with no match on the right)
SELECT * FROM users u
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);
```

### Window Functions

```sql
SELECT id,
    ROW_NUMBER() OVER (ORDER BY created_at) AS rn,
    RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rank,
    LAG(salary, 1) OVER (ORDER BY created_at) AS prev_salary,
    SUM(amount) OVER (ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM employees;
```

### Subqueries & CTEs

```sql
-- Common Table Expression
WITH active AS (
    SELECT id FROM users WHERE status = 'active'
)
SELECT * FROM orders WHERE user_id IN (SELECT id FROM active);

-- Derived table
SELECT * FROM (SELECT id, name FROM users WHERE age > 30) AS mature_users;

-- EXISTS
SELECT * FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

-- Recursive CTE (iterative fixed-point execution)
WITH RECURSIVE subordinates AS (
    SELECT id, name, manager_id FROM employees WHERE id = 'emp_root'
    UNION ALL
    SELECT e.id, e.name, e.manager_id
    FROM employees e
    JOIN subordinates s ON e.manager_id = s.id
)
SELECT * FROM subordinates;
```

### Set Operations

```sql
SELECT id FROM collection_a
UNION ALL
SELECT id FROM collection_b;

SELECT id FROM collection_a
INTERSECT
SELECT id FROM collection_b;

SELECT id FROM collection_a
EXCEPT
SELECT id FROM collection_b;
```

### Computed Columns & Expressions

```sql
SELECT
    price * qty AS total,
    UPPER(name) AS name_upper,
    CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END AS tier,
    CAST(created_at AS TEXT) AS created_str
FROM orders;
```

## DML

### INSERT

```sql
-- Single row
INSERT INTO users (id, name, email) VALUES ('u1', 'Alice', 'alice@example.com');

-- Multiple rows
INSERT INTO users (id, name) VALUES ('u1', 'Alice'), ('u2', 'Bob');

-- Schemaless (JSON-like syntax)
INSERT INTO users { name: 'Alice', email: 'alice@example.com', age: 30 };

-- INSERT ... SELECT
INSERT INTO archive SELECT * FROM orders WHERE created_at < '2025-01-01';
```

### UPSERT

```sql
-- Insert or merge if document ID exists
UPSERT INTO users { id: 'u1', name: 'Alice', role: 'admin' };
```

### UPDATE

```sql
-- Point update (O(1) by ID)
UPDATE users SET role = 'admin' WHERE id = 'u1';

-- Bulk update
UPDATE users SET status = 'inactive' WHERE last_login < '2025-01-01';
```

### DELETE

```sql
-- Point delete
DELETE FROM users WHERE id = 'u1';

-- Bulk delete
DELETE FROM orders WHERE status = 'cancelled';

-- Delete all
TRUNCATE users;
```

## DDL

### Collections

```sql
-- Schemaless document collection (default)
CREATE COLLECTION users TYPE document;

-- Strict schema (Binary Tuple encoding, O(1) field extraction)
CREATE COLLECTION orders TYPE strict (
    id UUID DEFAULT gen_uuid_v7(),
    customer_id UUID,
    total DECIMAL,
    status STRING,
    created_at DATETIME DEFAULT now()
);

-- Key-Value collection
CREATE COLLECTION sessions TYPE kv;

-- Graph collection
CREATE COLLECTION knows TYPE graph;

-- Timeseries collection (convenience alias)
CREATE TIMESERIES metrics;

-- Drop
DROP COLLECTION users;

-- Show all
SHOW COLLECTIONS;

-- Describe schema
DESCRIBE users;
```

#### Unified Columnar DDL

All columnar variants (plain, timeseries, spatial) use `CREATE COLLECTION ... WITH (storage = 'columnar', ...)`. Column modifiers designate special columns:

| Modifier        | Column type              | Effect                                                                                                                          |
| --------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `TIME_KEY`      | `TIMESTAMP` / `DATETIME` | Primary time column. Required for timeseries profile. Enables partition-by-time, block-level time skip, and retention policies. |
| `SPATIAL_INDEX` | `GEOMETRY`               | Automatically builds and maintains an R\*-tree index on this column. Required for spatial profile.                              |

```sql
-- Plain columnar
CREATE COLLECTION logs (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    level VARCHAR,
    message VARCHAR
) WITH (storage = 'columnar');

-- Timeseries profile (TIME_KEY required)
CREATE COLLECTION metrics (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    cpu FLOAT
) WITH (storage = 'columnar', profile = 'timeseries', partition_by = '1h', retention = '90d');

-- CREATE TIMESERIES is a convenience alias equivalent to profile = 'timeseries'
CREATE TIMESERIES metrics;

-- Spatial profile (SPATIAL_INDEX required)
CREATE COLLECTION locations (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
) WITH (storage = 'columnar');
```

### Schema Evolution

```sql
ALTER TABLE orders ADD COLUMN priority INT;
```

### Storage Conversion

```sql
-- Convert to kv, document, or strict
CONVERT COLLECTION cache TO STORAGE='kv';
CONVERT COLLECTION events TO STORAGE='document';
CONVERT COLLECTION users TO STORAGE='strict' WITH SCHEMA { ... };

-- Convert to columnar (plain or with profile)
CONVERT COLLECTION logs TO STORAGE='columnar';
CONVERT COLLECTION metrics TO STORAGE='columnar' WITH (profile = 'timeseries');
```

`CONVERT COLLECTION` works for document, strict, and kv targets. Columnar conversions re-encode existing data into compressed segments.

### Triggers

```sql
-- ASYNC (default): fires after commit via the Event Plane, zero write-latency impact
CREATE TRIGGER notify_shipped AFTER INSERT ON orders FOR EACH ROW
BEGIN
    INSERT INTO notifications { user_id: NEW.customer_id, message: 'Order placed' };
END;

-- SYNC: fires in the same transaction on the Data Plane (ACID, adds trigger latency)
CREATE TRIGGER enforce_balance AFTER UPDATE ON accounts FOR EACH ROW
WITH (EXECUTION = SYNC)
BEGIN
    IF NEW.balance < 0 THEN
        RAISE EXCEPTION 'Balance cannot go negative';
    END IF;
END;

-- DEFERRED: fires at COMMIT time, batched (ACID)
CREATE TRIGGER validate_totals AFTER INSERT ON line_items FOR EACH ROW
WITH (EXECUTION = DEFERRED)
BEGIN
    -- validation logic
END;

DROP TRIGGER notify_shipped ON orders;
SHOW TRIGGERS;
```

| Execution mode | Where | Atomicity | Write latency impact | Rollback on failure |
| -------------- | ----- | --------- | -------------------- | ------------------- |
| `ASYNC` (default) | Event Plane | Eventually consistent | None | No — original write committed |
| `SYNC` | Data Plane | Same transaction (ACID) | Trigger time added | Yes |
| `DEFERRED` | Data Plane at COMMIT | Same transaction, batched | At COMMIT time | Yes |

### Stored Procedures

```sql
-- Create or replace
CREATE OR REPLACE PROCEDURE transfer_funds(from_id UUID, to_id UUID, amount DECIMAL)
WITH (TIMEOUT = '5s', MAX_ITERATIONS = 1000)
SECURITY DEFINER
BEGIN
    DECLARE balance DECIMAL;
    SELECT balance INTO balance FROM accounts WHERE id = from_id;
    IF balance < amount THEN
        RAISE EXCEPTION 'Insufficient funds';
    END IF;
    UPDATE accounts SET balance = balance - amount WHERE id = from_id;
    UPDATE accounts SET balance = balance + amount WHERE id = to_id;
END;

-- Call
CALL transfer_funds('acc_a', 'acc_b', 50.00);

-- Drop
DROP PROCEDURE transfer_funds;

-- List
SHOW PROCEDURES;
```

### User-Defined Functions

```sql
-- SQL expression body (inlined into query plans by the optimizer — zero overhead)
CREATE OR REPLACE FUNCTION full_name(first VARCHAR, last VARCHAR)
RETURNS VARCHAR LANGUAGE SQL IMMUTABLE
AS $$ first || ' ' || last $$;

-- Procedural body
CREATE OR REPLACE FUNCTION tier_label(amount DECIMAL)
RETURNS VARCHAR LANGUAGE SQL STABLE
BEGIN
    IF amount > 1000 THEN
        RETURN 'premium';
    ELSIF amount > 100 THEN
        RETURN 'standard';
    ELSE
        RETURN 'basic';
    END IF;
END;

-- Volatility levels
-- IMMUTABLE: same inputs always produce same output (safe to fold at plan time)
-- STABLE: consistent within a single query (safe to push down)
-- VOLATILE: may change on each call (default)

-- Grant execute permission
GRANT EXECUTE ON FUNCTION full_name TO analyst;

-- Drop
DROP FUNCTION full_name;

-- List
SHOW FUNCTIONS;
```

### Change Streams

```sql
-- Basic change stream
CREATE CHANGE STREAM order_changes ON orders;

-- With webhook delivery
CREATE CHANGE STREAM order_events ON orders
WITH (
    WEBHOOK_URL = 'https://hooks.example.com/orders',
    WEBHOOK_SECRET = 'whsec_abc123'
);

-- With log compaction (keeps only the latest value per key field)
CREATE CHANGE STREAM user_state ON users
WITH (COMPACTION = 'key', KEY = 'id');

DROP CHANGE STREAM order_changes;
SHOW CHANGE STREAMS;
```

### Consumer Groups

```sql
-- Create a consumer group to track read position in a change stream
CREATE CONSUMER GROUP processors ON order_changes;

-- Commit offset for a specific partition
COMMIT OFFSET PARTITION 0 AT 42 ON order_changes CONSUMER GROUP processors;

-- Batch commit all partitions at their latest consumed position
COMMIT OFFSETS ON order_changes CONSUMER GROUP processors;

DROP CONSUMER GROUP processors ON order_changes;
```

### Durable Topics

```sql
-- Create a durable topic with retention
CREATE TOPIC order_events WITH (RETENTION = '1 hour');

-- Publish a message
PUBLISH TO order_events 'order 123 shipped';

-- Consume with a consumer group
CREATE CONSUMER GROUP processors ON order_events;
SELECT * FROM TOPIC order_events CONSUMER GROUP processors LIMIT 100;
COMMIT OFFSETS ON order_events CONSUMER GROUP processors;

DROP TOPIC order_events;
SHOW TOPICS;
```

### Cron Scheduler

```sql
-- Run a SQL block on a cron schedule
CREATE SCHEDULE nightly_cleanup
CRON '0 2 * * *'
AS BEGIN
    DELETE FROM sessions WHERE expires_at < now();
    INSERT INTO maintenance_log { task: 'nightly_cleanup', ran_at: now() };
END;

DROP SCHEDULE nightly_cleanup;
SHOW SCHEDULES;
```

### Backup, Restore, and Purge

```sql
-- Backup a tenant across all 7 engines (encrypted AES-256-GCM, MessagePack)
BACKUP TENANT acme TO '/backups/acme-2026-03-31.bak';

-- Validate without restoring
RESTORE TENANT acme FROM '/backups/acme-2026-03-31.bak' DRY RUN;

-- Restore
RESTORE TENANT acme FROM '/backups/acme-2026-03-31.bak';

-- Remove ALL tenant data across all engines and caches (requires CONFIRM)
PURGE TENANT acme CONFIRM;
```

### Indexes

```sql
-- Secondary index
CREATE INDEX idx_email ON users(email);
CREATE UNIQUE INDEX idx_username ON users(username);
DROP INDEX idx_email;
SHOW INDEXES;

-- Vector index
CREATE VECTOR INDEX ON articles FIELDS embedding DIMENSION 384 METRIC cosine;
CREATE VECTOR INDEX ON articles FIELDS embedding DIMENSION 384 METRIC cosine M 32 EF_CONSTRUCTION 400;
DROP VECTOR INDEX idx_name;

-- Full-text index
CREATE FULLTEXT INDEX ON articles(body);
DROP FULLTEXT INDEX idx_name;

-- Spatial index
CREATE SPATIAL INDEX ON locations(geom) USING RTREE;
DROP SPATIAL INDEX idx_name;
```

### Materialized Views (HTAP Bridge)

```sql
-- CDC from strict to columnar (auto-refreshed on writes)
CREATE MATERIALIZED VIEW order_stats AS
    SELECT status, COUNT(*), SUM(total) FROM orders GROUP BY status;

-- Trigger a full refresh (re-executes the defining query against current data)
REFRESH MATERIALIZED VIEW order_stats;

DROP MATERIALIZED VIEW order_stats;

-- List all materialized views
SHOW MATERIALIZED VIEWS;
```

### Continuous Aggregates

Continuous aggregates are incrementally maintained views over timeseries collections. Unlike `REFRESH MATERIALIZED VIEW` (full re-scan), continuous aggregates update only the new watermark window.

```sql
CREATE CONTINUOUS AGGREGATE cpu_hourly
ON cpu_metrics
AS
    SELECT time_bucket('1 hour', ts) AS hour,
           host,
           AVG(cpu) AS avg_cpu
    FROM cpu_metrics
    GROUP BY hour, host
WITH (refresh_interval = '1m');

-- Manually trigger a refresh
REFRESH CONTINUOUS AGGREGATE cpu_hourly;

SHOW CONTINUOUS AGGREGATES;

DROP CONTINUOUS AGGREGATE cpu_hourly;
```

## Engine-Specific SQL

### Vector Search

```sql
-- Nearest neighbor search
SELECT title, vector_distance() AS score
FROM articles
WHERE embedding <-> [0.1, 0.3, -0.2, ...]
LIMIT 10;

-- Filtered vector search (adaptive pre-filtering)
SELECT title, vector_distance() AS score
FROM articles
WHERE category = 'machine-learning'
  AND embedding <-> [0.1, 0.3, -0.2, ...]
LIMIT 10;
```

### Full-Text Search

```sql
-- BM25 search
SELECT title, bm25_score(body, 'distributed database') AS score
FROM articles
ORDER BY score DESC
LIMIT 10;

-- Full-text match in WHERE
SELECT * FROM articles WHERE text_match(body, 'distributed database');

-- Hybrid vector + text (RRF fusion)
SELECT title, rrf_score(
    vector_distance(embedding, [0.1, 0.3, ...]),
    bm25_score(body, 'distributed database'),
    60, 60
) AS score
FROM articles
ORDER BY score DESC
LIMIT 10;
```

### Graph

```sql
-- Add edges
INSERT INTO knows { from: 'users:alice', to: 'users:bob', since: 2020 };

-- Traversal
GRAPH TRAVERSE FROM 'users:alice' DEPTH 3 LABEL 'follows' DIRECTION out;

-- Neighbors
GRAPH NEIGHBORS OF 'users:alice' LABEL 'follows' DIRECTION out;

-- Shortest path
GRAPH PATH FROM 'users:alice' TO 'users:carol' MAX_DEPTH 5;

-- Pattern matching (Cypher subset)
MATCH (u:User)-[follows]->(other:User)
WHERE u.id = 'alice'
RETURN other.id, other.name;

-- Run algorithms
GRAPH ALGO pagerank ON knows OPTIONS (iterations: 20);
GRAPH ALGO wcc ON knows;
```

Available algorithms: `pagerank`, `wcc`, `label_propagation`, `lcc`, `sssp`, `betweenness`, `closeness`, `harmonic`, `degree`, `louvain`, `triangles`, `diameter`, `kcore`.

### Key-Value

```sql
-- Create with TTL
INSERT INTO sessions { key: 'sess_abc', user_id: 'alice', ttl: 3600 };

-- Point lookup (O(1) hash)
SELECT * FROM sessions WHERE key = 'sess_abc';

-- Analytical queries on KV data
SELECT role, COUNT(*) FROM sessions GROUP BY role;

-- Joins with other collections
SELECT u.name, s.role FROM users u JOIN sessions s ON u.id = s.user_id;
```

KV collections also support the [Redis wire protocol](kv.md#redis-compatible-access-resp) for `GET`/`SET`/`DEL`/`EXPIRE`/`SCAN` access.

### Timeseries

```sql
-- Create (convenience alias; equivalent to WITH (storage = 'columnar', profile = 'timeseries'))
CREATE TIMESERIES metrics;

-- Full form with TIME_KEY modifier
CREATE COLLECTION metrics (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    cpu_load FLOAT
) WITH (storage = 'columnar', profile = 'timeseries', partition_by = '1h');

-- Ingest (also via ILP protocol on port 8086)
INSERT INTO metrics (ts, host, cpu_load) VALUES (now(), 'server01', 0.65);

-- Time-range query
SELECT * FROM metrics
WHERE ts >= 1704067200000 AND ts <= 1704153600000;

-- Time-bucketed aggregation using time_bucket() UDF
SELECT time_bucket('1h', ts) AS hour, AVG(cpu_load)
FROM metrics
GROUP BY hour;
```

### Spatial

```sql
-- Spatial predicates
SELECT * FROM locations WHERE ST_DWithin(geom, ST_Point(-73.98, 40.75), 500);
SELECT * FROM locations WHERE ST_Contains(region, geom);
SELECT * FROM locations WHERE ST_Intersects(geom, boundary);
SELECT * FROM locations WHERE ST_Within(geom, area);
SELECT ST_Distance(a.geom, b.geom) FROM locations a, locations b;
```

### Document Navigation

```sql
-- Extract from schemaless documents
SELECT doc_get(payload, '$.user.name') AS user_name FROM events;
SELECT * FROM events WHERE doc_exists(payload, '$.user.email');
SELECT * FROM events WHERE doc_array_contains(payload, '$.tags', 'important');
```

### CRDT

```sql
-- Read CRDT state
SELECT crdt_state('collab_docs', 'doc123');

-- Apply delta
SELECT crdt_apply('collab_docs', 'doc123', '<delta_bytes>');
```

## Transactions

```sql
BEGIN;
INSERT INTO orders (id, total) VALUES ('o1', 99.99);
UPDATE inventory SET stock = stock - 1 WHERE id = 'item1';
COMMIT;

-- Rollback on error
BEGIN;
DELETE FROM users WHERE id = 'u1';
ROLLBACK;

-- Savepoints
BEGIN;
SAVEPOINT sp1;
INSERT INTO users (id, name) VALUES ('u1', 'Alice');
ROLLBACK TO sp1;
COMMIT;
```

Isolation level: **Snapshot Isolation (SI)**. Reads see a consistent snapshot from `BEGIN` time. Write conflicts detected at `COMMIT`.

## Bulk Import

```sql
-- Import from file (NDJSON, JSON array, or CSV — auto-detected)
COPY users FROM '/path/to/users.ndjson';
COPY users FROM '/path/to/users.csv' WITH (FORMAT csv);
```

## Introspection

```sql
-- Query plan
EXPLAIN SELECT * FROM users WHERE age > 30;

-- Session variables
SET nodedb.consistency = 'eventual';
SHOW nodedb.consistency;
SHOW ALL;
RESET nodedb.consistency;
```

## Change Tracking

```sql
-- Query change stream
SHOW CHANGES FOR users SINCE '2025-01-01' LIMIT 100;

-- Live subscription (changes delivered via async notifications)
LIVE SELECT * FROM users WHERE role = 'admin';
```

`LIVE SELECT` works over all SQL-capable protocols including pgwire. See [Real-Time Features](real-time.md#live-select) for delivery details.

## Admin & Security

```sql
-- Users and roles
CREATE USER alice PASSWORD 'secret';
CREATE ROLE analyst;
GRANT READ ON analytics TO analyst;
GRANT ROLE analyst TO alice;
REVOKE READ ON analytics FROM analyst;

-- Row-Level Security
CREATE RLS POLICY tenant_isolation ON orders AS (tenant_id = current_tenant());
SHOW RLS POLICIES;

-- API keys
CREATE API KEY my_key SCOPE analytics_read;
REVOKE API KEY my_key;

-- Multi-tenancy
CREATE TENANT acme;
SHOW TENANTS;
ALTER TENANT acme SET QUOTA max_qps = 5000;
SHOW TENANT USAGE FOR acme;
SHOW TENANT QUOTA FOR acme;
EXPORT USAGE FOR TENANT acme PERIOD '2026-03' FORMAT 'json';
PURGE TENANT acme CONFIRM;

-- Cluster
SHOW CLUSTER;
SHOW NODES;
SHOW RAFT GROUPS;

-- Audit
SHOW AUDIT LOG LIMIT 100;

-- Sessions
SHOW CONNECTIONS;
SHOW USERS;
```

## Built-in Functions

### String

`LENGTH`, `SUBSTR`, `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM`, `CONCAT` (or `||`), `REPLACE`, `SPLIT`

### Numeric

`ABS`, `CEIL`, `FLOOR`, `ROUND`, `SQRT`, `POWER`, `MOD` (or `%`), `GREATEST`, `LEAST`

### Date/Time

`NOW()`, `CURRENT_TIMESTAMP()`, `EXTRACT(field FROM ts)`, `DATE_TRUNC(unit, ts)`, `DATE_FORMAT(ts, fmt)`, `time_bucket(interval, ts)`

`time_bucket(interval, ts)` — truncates `ts` to the nearest `interval` boundary. Registered as a DataFusion ScalarUDF. Accepts interval literals (`'5m'`, `'1h'`, `'1d'`) and ISO 8601 durations (`'PT5M'`, `'PT1H'`). Used in timeseries aggregations and continuous aggregate definitions.

### Type

`CAST(expr AS type)`, `TRY_CAST(expr AS type)`, `expr::type`

## Limitations

| Feature                       | Status        | Reason                                                                                                                                                                                                                                                                                                                              |
| ----------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WITH RECURSIVE`              | Supported     | Iterative fixed-point execution. For graph traversal, the native `GRAPH TRAVERSE`, `GRAPH PATH`, and algorithm functions remain more efficient.                                                                                                                                                                                      |
| `UPDATE/DELETE ... JOIN`      | Not supported | The Data Plane executes mutations as single-collection atomic operations through the SPSC bridge. Multi-collection mutations would require cross-engine coordination that breaks the isolation model. Rewrite as a subquery: `DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE ...)`.                                |
| `FOREIGN KEY`                 | Not enforced  | In a distributed system with CRDT sync and eventual consistency at the edge, enforcing FK constraints across collections would require cross-shard coordination on every write — killing write throughput. CRDT constraint validation (UNIQUE, FK) is enforced at Raft commit time for synced collections, but not for general SQL. |
| `COPY TO` (export)            | Not supported | The Data Plane is write-optimized with io_uring for ingest, but export requires serialization across all shards and cores. Use the HTTP API (`/query/stream`) for NDJSON export or query into Parquet via L2 cold storage.                                                                                                          |
| `UPDATE/DELETE` on timeseries | Not supported | Timeseries collections use append-only columnar memtables with cascading compression (ALP + FastLanes + FSST + Gorilla + LZ4). In-place mutation would break compression chains and invalidate block statistics. Use retention policies to age out old data.                                                                        |
| `EXPLAIN ANALYZE`             | Not yet       | Requires instrumentation across the SPSC bridge to collect per-core execution stats from the Data Plane and merge them on the Control Plane. The bridge currently returns results but not timing metadata. Planned.                                                                                                                 |

## Related

- [Getting Started](getting-started.md) — First queries walkthrough
- [Architecture](architecture.md) — How the three-plane execution model works
- Engine guides: [Vectors](vectors.md) | [Graph](graph.md) | [Documents](documents.md) | [KV](kv.md) | [Timeseries](timeseries.md) | [Spatial](spatial.md) | [Full-Text](full-text-search.md)

[Back to docs](README.md)
