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
[GROUP BY <exprs>] [HAVING <predicate>]
[ORDER BY <field> [ASC|DESC], ...]
[LIMIT <n>] [OFFSET <m>]
```

An unbounded `SELECT` (no `LIMIT`) is capped by a per-core memory budget (`[tuning.query] max_scan_result_bytes`, default 512 MiB) rather than a silent row cap. Exceeding the budget returns a typed `ResourcesExhausted` error suggesting a `LIMIT` — results are never silently truncated. Unordered multi-row SELECTs stream lazily to the client on pgwire, native, and HTTP.

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

Timestamp-valued strings compare by instant, not bytes: a stored ISO-8601 value (`'2026-07-02T13:00:00.000000Z'`) matches a SQL-style literal (`'2026-07-02 13:00:00'`) in `=` and range predicates when both sides parse as datetimes. Non-datetime strings keep lexicographic comparison.

### Aggregates

```sql
SELECT status, COUNT(*), AVG(age), MIN(salary), MAX(salary)
FROM employees
WHERE department = 'sales'
GROUP BY status
HAVING COUNT(*) > 5;

SELECT COUNT(DISTINCT user_id) FROM orders;

-- Computed expressions as GROUP BY keys
SELECT UPPER(label) AS u, SUM(score) FROM events GROUP BY UPPER(label);

-- GROUP BY a SELECT alias or ordinal also works
SELECT UPPER(label) AS u, SUM(score) FROM events GROUP BY u;
SELECT UPPER(label), SUM(score) FROM events GROUP BY 1;
```

GROUP BY keys follow PostgreSQL semantics: output columns appear in SELECT-list order, and a key projected as `SELECT k AS label` is emitted under its alias.

A scalar aggregate (no GROUP BY) over zero input rows returns one identity row: `COUNT(*)` → `0`; `SUM`/`AVG`/`MIN`/`MAX` → `NULL`. A `GROUP BY` aggregate over zero rows returns zero rows.

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

-- Schemaless document insert (standard form)
INSERT INTO users (id, name, email, age) VALUES ('u1', 'Alice', 'alice@example.com', 30);
-- Or: JSON-like syntax also accepted
INSERT INTO users { name: 'Alice', email: 'alice@example.com', age: 30 };

-- INSERT ... SELECT
INSERT INTO archive SELECT * FROM orders WHERE created_at < '2025-01-01';
```

`INSERT` is strict: a duplicate primary key raises `unique_violation` (SQLSTATE `23505`). To write "insert if absent" or "insert or overwrite" semantics, use `ON CONFLICT` or `UPSERT`.

### INSERT ... ON CONFLICT

```sql
-- Skip rows that would collide with an existing PK (no error)
INSERT INTO users (id, name) VALUES ('u1', 'Alice')
ON CONFLICT DO NOTHING;

-- Overwrite selected fields on conflict; EXCLUDED refers to the incoming row,
-- bare column names refer to the existing row.
INSERT INTO users (id, name, login_count) VALUES ('u1', 'Alice', 1)
ON CONFLICT (id) DO UPDATE SET
  name        = EXCLUDED.name,
  login_count = users.login_count + EXCLUDED.login_count;
```

`ON CONFLICT DO UPDATE` reroutes to the upsert path internally and fires `AFTER UPDATE` triggers (not `AFTER INSERT`) when a row is overwritten.

### UPSERT

```sql
-- Insert or overwrite if the primary key exists (standard form)
UPSERT INTO users (id, name, role) VALUES ('u1', 'Alice', 'admin');
-- Or: JSON-like syntax also accepted
UPSERT INTO users { id: 'u1', name: 'Alice', role: 'admin' };
```

`UPSERT` is equivalent to `INSERT ... ON CONFLICT (<pk>) DO UPDATE SET <all-columns> = EXCLUDED.<col>` and fires `AFTER UPDATE` on overwrite, `AFTER INSERT` on first write.

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

### RETURNING

Both `UPDATE` and `DELETE` support a `RETURNING` clause to read back affected rows in the same statement:

```sql
-- UPDATE RETURNING: returns the post-update image
UPDATE users SET role = 'admin' WHERE id = 'u1' RETURNING id, role;
UPDATE orders SET status = 'shipped' WHERE id = 'o1' RETURNING *;

-- DELETE RETURNING: returns the pre-delete image
DELETE FROM users WHERE id = 'u1' RETURNING id, name;
DELETE FROM orders WHERE status = 'cancelled' RETURNING *;
```

`RETURNING *` expands to all columns. Named columns are returned as bare values — arithmetic expressions in `RETURNING` are not supported. Works in both simple-query and extended-query (prepared statement) protocols.

## DDL

### Database

Databases are top-level containers that own collection namespaces, quota budgets, and serve as the atomic unit for clone, mirror, and backup operations. See [Databases](databases.md) for full reference.

```sql
-- Create a database with optional quotas and settings
CREATE DATABASE prod_us;
CREATE DATABASE staging WITH (
    priority_class='standard',
    max_memory_bytes=5368709120,
    max_qps=5000,
    max_connections=500
);

-- Rename a database (numeric identity unchanged)
ALTER DATABASE staging RENAME TO staging_old;

-- Update quotas
ALTER DATABASE prod_us SET QUOTA (max_qps=10000, max_connections=1000);

-- Materialize a clone (force copy-on-write rows to target storage, blocks)
ALTER DATABASE my_clone MATERIALIZE;

-- Promote a mirror to writable (one-way, irreversible)
ALTER DATABASE replica PROMOTE;

-- Switch session to a different database (aborts transaction, invalidates prepared statements)
USE DATABASE prod_us;
-- Or in psql: \c prod_us

-- Clone a database at a point in time
CLONE DATABASE staging FROM prod_us AS OF SYSTEM TIME 1730000000000;
CLONE DATABASE latest FROM prod_us;   -- uses prod_us's current LSN

-- Mirror a database (read-only replica, optionally cross-cluster)
MIRROR DATABASE prod_eu FROM prod_us MODE = async;
MIRROR DATABASE prod_eu FROM prod_us MODE = sync;

-- Move a tenant to a different database
MOVE TENANT acme FROM prod_us TO prod_eu;

-- View databases
SHOW DATABASES;

-- View clone lineage, mirror status, quotas, or usage
SHOW DATABASE LINEAGE FOR my_clone;
SHOW DATABASE MIRROR STATUS FOR prod_eu;
SHOW DATABASE QUOTA FOR prod_us;
SHOW DATABASE USAGE FOR prod_us;

-- Drop a database
DROP DATABASE staging;              -- fails if non-empty without CASCADE
DROP DATABASE staging CASCADE;      -- drops all collections
DROP DATABASE staging FORCE;        -- force-materializes clones before dropping

-- Audit and tenant quotas
ALTER DATABASE prod_us SET AUDIT_DML = 'all';       -- all DML + SELECT
ALTER DATABASE prod_us SET AUDIT_DML = 'writes';    -- INSERT/UPDATE/DELETE only
ALTER DATABASE prod_us SET AUDIT_DML = 'none';      -- disabled (default)
ALTER DATABASE prod_us SET IDLE_TIMEOUT 1800;       -- idle session timeout in seconds

ALTER TENANT acme IN DATABASE prod_us SET QUOTA (max_memory_bytes=1073741824);

-- View sessions and kill idle ones
SHOW SESSIONS [IN DATABASE prod_us];
KILL SESSION 'sid_abc123';

-- Audit events tagged by database
SHOW AUDIT IN DATABASE prod_us [WHERE event_type = 'database_quota_changed'];
```

All database DDL is replicated via the metadata Raft group and is consistent across the cluster. For full examples and context, see [Databases](databases.md).

### Collections

```sql
-- Schemaless document collection (default)
CREATE COLLECTION users;

-- Strict schema (Binary Tuple encoding, O(1) field extraction)
CREATE COLLECTION orders (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    total FLOAT,
    status TEXT,
    created_at TIMESTAMP
) WITH (engine='document_strict');

-- Key-Value collection
CREATE COLLECTION sessions (key TEXT PRIMARY KEY) WITH (engine='kv');
-- extra columns are optional typed value fields

-- Trailing ENGINE suffix (MySQL-style) is equivalent to WITH (engine='...')
CREATE COLLECTION metrics (ts TIMESTAMP TIME_KEY, cpu FLOAT) ENGINE = timeseries;

-- Graph edges are overlays on document collections, not a separate collection type.
-- Use GRAPH INSERT EDGE to add edges between documents in any collection.

-- Timeseries collection (convenience alias)
CREATE TIMESERIES metrics;

-- Drop (two-phase: soft-delete, reversible within retention window)
DROP COLLECTION users;

-- Restore a soft-deleted collection (within retention window)
UNDROP COLLECTION users;

-- Hard-delete immediately (admin-only, skips retention, no UNDROP)
DROP COLLECTION users PURGE;

-- Show all
SHOW COLLECTIONS;

-- Describe schema
DESCRIBE users;

-- Inspect soft-deleted collections and the L2 delete backlog
SELECT * FROM _system.dropped_collections;
SELECT * FROM _system.l2_cleanup_queue;

-- Change retention (superuser; live — sweeper picks up next tick)
ALTER SYSTEM SET deactivated_collection_retention_days = 14;
-- Per-tenant override:
ALTER TENANT 42 SET QUOTA deactivated_collection_retention_days = 30;
```

`DROP COLLECTION` is a soft-delete by default — the catalog row and
all on-disk bytes are preserved for a retention window (default: 7
days) during which `UNDROP COLLECTION` restores the collection with
zero data loss. After the window the Event-Plane GC sweeper proposes
`PurgeCollection` and reclaims storage on every node. `DROP ... PURGE`
skips the window and is admin-only. If any triggers, RLS policies,
materialized views, change streams, schedules, or implicit sequences
reference the collection, the handler rejects with SQLSTATE `2BP01`
listing every dependent.

#### Columnar Family DDL

`columnar`, `timeseries`, and `spatial` are peer engines sharing the same compressed-column storage core. Pick one per collection via `WITH (engine='<name>')` or the trailing `ENGINE = <name>` suffix (the two forms are equivalent; specifying both with different values is rejected). Valid engine names: `document_schemaless`, `document_strict`, `kv`, `columnar`, `timeseries`, `spatial`, `vector`. Column modifiers designate special columns:

| Modifier        | Column type              | Effect                                                                                                              |
| --------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| `TIME_KEY`      | `TIMESTAMP` / `DATETIME` | Primary time column. Required for `engine='timeseries'`. Drives partition-by-time, block-level skip, and retention. |
| `SPATIAL_INDEX` | `GEOMETRY`               | Automatically builds and maintains an R\*-tree index on this column. Required for `engine='spatial'`.               |

```sql
-- Plain columnar
CREATE COLLECTION logs (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    level VARCHAR,
    message VARCHAR
) WITH (engine='columnar');

-- Timeseries (TIME_KEY required)
CREATE COLLECTION metrics (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    cpu FLOAT
) WITH (engine='timeseries', partition_by='1h', retention='90d');

-- CREATE TIMESERIES is a convenience alias equivalent to engine='timeseries'
CREATE TIMESERIES metrics;

-- Spatial (SPATIAL_INDEX required)
CREATE COLLECTION locations (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
) WITH (engine='spatial');
```

### Schema Evolution

```sql
ALTER TABLE orders ADD COLUMN priority INT;
```

### Storage Conversion

```sql
CONVERT COLLECTION cache  TO kv;
CONVERT COLLECTION events TO document_schemaless;
CONVERT COLLECTION users  TO document_strict;
-- Optional column definitions when converting into document_strict / kv:
CONVERT COLLECTION users  TO document_strict (id TEXT PRIMARY KEY, email TEXT);
```

`CONVERT COLLECTION` accepts `document_schemaless`, `document_strict`, or `kv` as the target type. The columnar / timeseries / spatial engines are picked at collection-creation time, not via CONVERT.

### Triggers

```sql
-- ASYNC (default): fires after commit via the Event Plane, zero write-latency impact
CREATE TRIGGER notify_shipped AFTER INSERT ON orders FOR EACH ROW
$$ BEGIN
    INSERT INTO notifications (user_id, message) VALUES (NEW.customer_id, 'Order placed');
END; $$;

-- SYNC: fires in the same transaction on the Data Plane (ACID, adds trigger latency)
CREATE TRIGGER enforce_balance AFTER UPDATE ON accounts FOR EACH ROW
WITH (EXECUTION = SYNC)
$$ BEGIN
    IF NEW.balance < 0 THEN
        RAISE EXCEPTION 'Balance cannot go negative';
    END IF;
END; $$;

-- DEFERRED: fires at COMMIT time, batched (ACID)
CREATE TRIGGER validate_totals AFTER INSERT ON line_items FOR EACH ROW
WITH (EXECUTION = DEFERRED)
$$ BEGIN
    -- validation logic
END; $$;

DROP TRIGGER notify_shipped ON orders;
SHOW TRIGGERS;
```

| Execution mode    | Where                | Atomicity                 | Write latency impact | Rollback on failure           |
| ----------------- | -------------------- | ------------------------- | -------------------- | ----------------------------- |
| `ASYNC` (default) | Event Plane          | Eventually consistent     | None                 | No — original write committed |
| `SYNC`            | Data Plane           | Same transaction (ACID)   | Trigger time added   | Yes                           |
| `DEFERRED`        | Data Plane at COMMIT | Same transaction, batched | At COMMIT time       | Yes                           |

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

Backup bytes stream over the pgwire COPY framing; the client redirects
output to (or reads input from) a file under the operator's UID.

```sql
-- Backup a tenant across all 7 engines (encrypted AES-256-GCM, MessagePack)
COPY (BACKUP TENANT acme) TO STDOUT;

-- Validate without restoring
COPY tenant_restore(acme) FROM STDIN DRY RUN;

-- Restore
COPY tenant_restore(acme) FROM STDIN;

-- SQL form (server-side path)
RESTORE TENANT acme FROM '/backups/acme.ndb';
RESTORE TENANT acme FROM '/backups/acme.ndb' DRY RUN;

-- Bypass the staleness guard
RESTORE TENANT acme FROM '/backups/acme.ndb' FORCE;
-- COPY form accepts the same modifiers:
COPY tenant_restore(acme) FROM STDIN FORCE;

-- Remove ALL tenant data across all engines and caches (requires CONFIRM)
PURGE TENANT acme CONFIRM;
```

**Staleness guard.** A restore is refused when the backup's watermark is older than the cluster's last observed write for that tenant — newer writes would be silently overwritten. `FORCE` overrides the guard (and logs that newer writes will be overwritten). `FORCE` and `DRY RUN` are independent modifiers and may appear in either order.

### Indexes

```sql
-- Secondary index
CREATE INDEX idx_email ON users(email);
CREATE UNIQUE INDEX idx_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_email ON users(email);
DROP INDEX idx_email;
DROP INDEX IF EXISTS idx_email;
SHOW INDEXES;

-- Vector index
CREATE VECTOR INDEX idx_articles_embedding ON articles METRIC cosine DIM 384;
CREATE VECTOR INDEX idx_articles_embedding ON articles METRIC cosine DIM 384 M 32 EF_CONSTRUCTION 400;
CREATE VECTOR INDEX IF NOT EXISTS idx_articles_embedding ON articles METRIC cosine DIM 384;
DROP VECTOR INDEX idx_name;
DROP VECTOR INDEX IF EXISTS idx_name;

-- Full-text index
CREATE FULLTEXT INDEX idx_body ON articles(body);
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
SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, -0.2, ...], 10);

-- Filtered vector search (adaptive pre-filtering)
SELECT title, vector_distance(embedding, ARRAY[0.1, 0.3, -0.2]) AS score
FROM articles
WHERE category = 'machine-learning'
  AND id IN (
    SEARCH articles USING VECTOR(embedding, ARRAY[0.1, 0.3, -0.2, ...], 10)
  );
```

### Sparse Vector Search

```sql
-- Dimensionless sparse-vector column; values are '{dimension: weight}' literals
CREATE TABLE sparse_docs (id TEXT PRIMARY KEY, terms SPARSEVECTOR);
INSERT INTO sparse_docs (id, terms) VALUES ('a', '{3: 1.0, 7: 1.0}');

-- Dot-product top-k ranking; LIMIT sets k
SELECT id FROM sparse_docs
ORDER BY sparse_score(terms, '{3: 1.0, 7: 0.5}') DESC
LIMIT 3;
```

The inverted index is maintained automatically on document writes (insert, update, delete). Documents sharing no dimension with the query are never scanned. `sparse_score` is a standalone surface — it is not fused into `rrf_score` hybrid ranking.

### Full-Text Search

```sql
-- BM25 search
SELECT title, bm25_score(body, 'distributed database') AS score
FROM articles
ORDER BY score DESC
LIMIT 10;

-- Full-text match in WHERE
SELECT * FROM articles WHERE text_match(body, 'distributed database');

-- SEARCH() as an alias for WHERE-clause full-text matching
SELECT * FROM articles WHERE SEARCH(body, 'distributed database');

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
-- JSON string form:
GRAPH INSERT EDGE IN 'edges' FROM 'alice' TO 'bob' TYPE 'knows' PROPERTIES '{"since": 2020}';
-- Object literal form (equivalent):
GRAPH INSERT EDGE IN 'edges' FROM 'alice' TO 'bob' TYPE 'knows' PROPERTIES { since: 2020 };

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
GRAPH ALGO PAGERANK ON 'knows' DAMPING 0.85 ITERATIONS 20 TOLERANCE 1e-7;
GRAPH ALGO PAGERANK ON 'knows' DAMPING 0.85 PERSONALIZATION {"alice": 1.0, "bob": 0.5};
GRAPH ALGO wcc ON 'knows';
```

Available algorithms: `pagerank`, `wcc`, `label_propagation`, `lcc`, `sssp`, `betweenness`, `closeness`, `harmonic`, `degree`, `louvain`, `triangles`, `diameter`, `kcore`.

**Personalized PageRank** — The `PERSONALIZATION` parameter takes a JSON object mapping node identifiers to seed weights. This biases the random-walk teleport set toward the named seed nodes, enabling importance scoring relative to a reference set (e.g., recommendations or "related to these nodes").

### Key-Value

```sql
-- Create with TTL (standard form)
INSERT INTO sessions (key, user_id) VALUES ('sess_abc', 'alice');
-- Or: JSON-like syntax also accepted
INSERT INTO sessions { key: 'sess_abc', user_id: 'alice' };

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
-- Create (convenience alias; equivalent to WITH (engine='timeseries'))
CREATE TIMESERIES metrics;

-- Full form with TIME_KEY modifier
CREATE COLLECTION metrics (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    cpu_load FLOAT
) WITH (engine='timeseries', partition_by='1h');

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

-- Geometry constructors in INSERT VALUES
INSERT INTO locations (id, geom) VALUES ('p1', ST_MakePoint(-122.4, 37.8));
INSERT INTO locations (id, geom) VALUES ('p2', ST_GeomFromText('POINT(-122.4 37.8)'));
INSERT INTO locations (id, geom) VALUES ('l1', ST_GeomFromText('LINESTRING(-122.4 37.8, -121.0 36.5)'));
INSERT INTO locations (id, geom) VALUES ('w1', ST_GeomFromWKB(X'0101000000...'));
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
-- Declare a document collection CRDT-backed: plain DML converges via LWW
CREATE COLLECTION notes (id TEXT PRIMARY KEY, title TEXT, body TEXT) WITH (crdt=true);
UPDATE notes SET title = 't2' WHERE id = 'a';   -- per-field LWW merge
DELETE FROM notes WHERE id = 'a' RETURNING id;  -- tombstone

-- Low-level delta path
SELECT crdt_state('collab_docs', 'doc123');
SELECT crdt_apply('collab_docs', 'doc123', '<delta_bytes>');
```

`WITH (crdt=true)` is document-collection-only. Non-PK-targeted UPDATE/DELETE, non-literal SET values, and `ON CONFLICT DO UPDATE` are rejected on CRDT collections (no representable CRDT operation). See [Documents — CRDT Document Collections](documents.md#crdt-document-collections).

### Array (NDArray)

```sql
-- Create array with dimensions and attributes
CREATE ARRAY spatial_grid
  DIMS (
    x INT64 DOMAIN [0, 1000),
    y INT64 DOMAIN [0, 1000),
    z INT64 DOMAIN [0, 1000)
  )
  ATTRS (
    temperature FLOAT32,
    pressure FLOAT32
  )
  TILE_EXTENTS (64, 64, 64);

-- Insert cells
INSERT INTO spatial_grid (x, y, z, temperature, pressure) VALUES
  (10, 20, 5, 23.5, 1013.2),
  (10, 20, 10, 22.1, 1013.1);

-- Slice: range query over dimensions
SELECT x, y, z, temperature FROM ARRAY_SLICE(
  'spatial_grid',
  {x: [0, 100), y: [0, 100), z: [0, 50)},
  ['temperature']
);

-- Project: select specific attributes
SELECT * FROM ARRAY_PROJECT('spatial_grid', ['temperature', 'pressure']);

-- Aggregate: reduce dimensionality via aggregation
SELECT * FROM ARRAY_AGG('spatial_grid', 'temperature', 'AVG', 'x');

-- Element-wise operations
SELECT * FROM ARRAY_ELEMENTWISE(
  'current',
  'baseline',
  'SUBTRACT',
  'temperature'
);

-- Flush and compact
SELECT ARRAY_FLUSH('spatial_grid');
SELECT ARRAY_COMPACT('spatial_grid');
```

## Temporal Queries (Bitemporal)

Temporal queries use `AS OF` clauses to query data at a point in time. Both system time (when data entered) and valid time (when it represents) are supported across Document, Columnar, Timeseries, Spatial, Graph, and Array engines.

```sql
-- Read as of system time (historical database state)
SELECT * FROM orders
AS OF SYSTEM TIME 1700000000000;

-- Read rows valid at a specific time (temporal semantics)
SELECT * FROM metrics
AS OF VALID TIME 1700000000000;

-- Both dimensions: what did we know at a past moment about a past date?
SELECT * FROM orders
AS OF SYSTEM TIME 1700000000000
AS OF VALID TIME 1700000001000;

-- With predicates
SELECT customer_id, total FROM orders
WHERE status = 'shipped'
AS OF SYSTEM TIME (extract(epoch from now()) * 1000 - 86400000);

-- Column-level temporal
SELECT id, balance FROM accounts
WHERE balance > 0
AS OF VALID TIME 1700000000000;
```

Time values are milliseconds since Unix epoch. For current time, use `extract(epoch from now()) * 1000` or `(SELECT extract(epoch from now() at time zone 'utc') * 1000)`.

See [Bitemporal Queries](../bitemporal.md) for detailed use cases.

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
ROLLBACK TO SAVEPOINT sp1;   -- the SAVEPOINT keyword is optional
RELEASE SAVEPOINT sp1;       -- also: RELEASE sp1
COMMIT;
```

Isolation level: **Snapshot Isolation (SI)**. Reads see a consistent snapshot from `BEGIN` time. Write conflicts detected at `COMMIT` and surfaced as SQLSTATE `40001` (`could not serialize access due to concurrent update`).

### Cross-Shard Transactions

An interactive `BEGIN ... COMMIT` block whose statements span multiple vShards or nodes commits **atomically** by default — the whole block flushes through the Calvin sequencer's durable vote/verdict barrier at COMMIT. Reads taken during the transaction (point reads, predicate scans, index probes, both sides of distributed JOINs) are OCC-validated at COMMIT; a stale read aborts with `40001` (retry the transaction). `SET cross_shard_txn = 'best_effort_non_atomic'` opts bulk loads out of cross-shard atomicity. Single-node deployments run the same path by default (`single_node_calvin = true`), so transactions spanning cores commit atomically too. See [Architecture — Cross-Shard Transactions](architecture.md#cross-shard-transactions).

### Read-Your-Own-Writes

Statements inside `BEGIN ... COMMIT` are staged in a per-transaction overlay on the Data Plane. Reads within the same transaction observe staged writes **across every engine**: KV, document (schemaless and strict), columnar, timeseries, graph (single-hop, multi-hop, shortest path), vector search, spatial predicates, full-text search, and hybrid search fusion. Constraint violations (e.g., primary-key uniqueness) surface immediately at statement time, not at COMMIT.

### Savepoints

Savepoints work on pgwire and the native protocol (HTTP is stateless — no transaction blocks). `ROLLBACK TO SAVEPOINT` rewinds the staging overlay itself via an undo journal — staged inserts disappear from reads, updates restore the prior staged value, deletes make rows reappear, and graph-edge staging reverts. Rollback undo covers index side-effects too: secondary indexes, FTS postings, HNSW vectors, R-tree entries, and column statistics.

| Error                                        | SQLSTATE |
| -------------------------------------------- | -------- |
| `ROLLBACK TO` / `RELEASE` unknown savepoint | `3B001`  |
| Savepoint outside a transaction              | `25P01`  |

### Atomic Transfers

Higher-level transaction primitives for moving currency or items between entities. Built on `TransactionBatch` with automatic validation and deterministic lock ordering.

```sql
-- Fungible transfer (currency, resources)
-- Atomically: source.gold -= 500, dest.gold += 500
-- Fails if source.gold < 500 (INSUFFICIENT_BALANCE)
SELECT TRANSFER('player_wallets', 'player-A', 'player-B', 'gold', 500);

-- Non-fungible transfer (unique items)
-- Atomically: delete from source owner, add to dest owner
-- Fails if source doesn't own the item (NOT_FOUND)
SELECT TRANSFER_ITEM('inventory', 'inventory', 'sword-of-doom', 'player-A', 'player-B');
```

### Weighted Random Selection

Server-side weighted random sampling with optional deterministic seeds and audit trail.

```sql
-- Pick 1 item weighted by drop_rate column
SELECT * FROM WEIGHTED_PICK('loot_table', weight => 'drop_rate', count => 1);

-- Pick 10 items with deterministic seed (provably fair — same seed = same result)
SELECT * FROM WEIGHTED_PICK('gacha_pool', weight => 'probability', count => 10,
    SEED => 'player-123:pull-456');

-- With audit trail (logged to _system_random_audit)
SELECT * FROM WEIGHTED_PICK('gacha_pool', weight => 'probability', count => 1,
    SEED => 'player-123:pull-789', AUDIT => TRUE);

-- Allow duplicates
SELECT * FROM WEIGHTED_PICK('reward_pool', weight => 'chance', count => 5,
    WITH REPLACEMENT);
```

Returns rows with columns: `pick_index`, `key`, `weight`. Uses Vose's alias method (O(N) setup, O(1) per pick). Default RNG is ChaCha-based CSPRNG; `SEED` enables deterministic reproducibility.

## Bulk Import

Bytes stream from the client over pgwire COPY; the database never opens
a file by a caller-named path.

```sql
-- Import from a client-side file (NDJSON, JSON array, or CSV)
COPY users FROM STDIN;
COPY users FROM STDIN WITH (FORMAT csv);
```

## Introspection

```sql
-- Query plan
EXPLAIN SELECT * FROM users WHERE age > 30;

-- Collect per-column statistics for the cost-based planner
-- (row_count, distinct_count, avg_value_len; used to pick distributed
-- join/aggregate strategies). Auto-ANALYZE re-runs after ~10% of rows change.
ANALYZE orders;
ANALYZE;             -- all collections

-- Session variables
SET nodedb.consistency = 'eventual';
SHOW nodedb.consistency;
SHOW ALL;
RESET nodedb.consistency;

-- Server version (PostgreSQL-compatible)
SELECT version();                              -- 'PostgreSQL 15 ... NodeDB ...'
SHOW server_version_num;
SELECT current_setting('server_version_num');
SELECT current_setting('nodedb.foo', true);    -- missing_ok: NULL if unset

-- Roles and permissions
SHOW ROLES;

-- Server statistics
SHOW STATS;           -- alias SHOW SERVER STATS
SHOW METRICS;
SHOW MEMORY;

-- Tenants
SHOW TENANTS;
SHOW TENANTS WITH NAME 'acme';

-- Switch session tenant (superuser only)
SET TENANT = 'acme';
SET TENANT = DEFAULT;
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
CREATE ROLE IF NOT EXISTS analyst;
CREATE USER IF NOT EXISTS alice PASSWORD 'secret';
DROP ROLE analyst;
DROP ROLE IF EXISTS analyst;
DROP USER alice;                 -- owned objects reassigned to {tenant}_admin, grants revoked
DROP USER IF EXISTS alice;
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

### System

`version()`, `current_setting(name [, missing_ok])`

### KV Atomic

`KV_INCR(collection, key, delta [, TTL => secs])`, `KV_DECR(collection, key, delta)`, `KV_INCR_FLOAT(collection, key, delta)`, `KV_CAS(collection, key, expected, new_value)`, `KV_GETSET(collection, key, new_value)`

### Leaderboard

`RANK(index_name, key)`, `TOPK(index_name, k)` (TVF), `RANGE(index_name, min, max)` (TVF), `SORTED_COUNT(index_name)`

### Rate Limiting

`RATE_CHECK(gate, key, max_count, window_secs)`, `RATE_REMAINING(gate, key, max_count, window_secs)`, `RATE_RESET(gate, key)`

### Transfer

`TRANSFER(collection, source_key, dest_key, field, amount)`, `TRANSFER_ITEM(source_coll, dest_coll, item_id, source_owner, dest_owner)`

### Random Selection

`WEIGHTED_PICK(collection, weight => 'col', count => N [, SEED => 'str'] [, AUDIT => TRUE] [, WITH REPLACEMENT])` (TVF)

## Limitations

| Feature                       | Status        | Reason                                                                                                                                                                                                                                                                                                                              |
| ----------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WITH RECURSIVE`              | Supported     | Iterative fixed-point execution. For graph traversal, the native `GRAPH TRAVERSE`, `GRAPH PATH`, and algorithm functions remain more efficient.                                                                                                                                                                                     |
| `UPDATE/DELETE ... JOIN`      | Not supported | The Data Plane executes mutations as single-collection atomic operations through the SPSC bridge. Multi-collection mutations would require cross-engine coordination that breaks the isolation model. Rewrite as a subquery: `DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE ...)`.                                |
| `FOREIGN KEY`                 | Not enforced  | In a distributed system with CRDT sync and eventual consistency at the edge, enforcing FK constraints across collections would require cross-shard coordination on every write — killing write throughput. CRDT constraint validation (UNIQUE, FK, CHECK) is enforced at delta-apply time for synced collections, but not for general SQL. |
| `COPY TO` (export)            | Not supported | The Data Plane is write-optimized with io_uring for ingest, but export requires serialization across all shards and cores. Use the HTTP API (`/v1/query/stream`) for NDJSON export or query into Parquet via L2 cold storage.                                                                                                       |
| `UPDATE/DELETE` on timeseries | Not supported | Timeseries collections use append-only columnar memtables with cascading compression (ALP + FastLanes + FSST + Gorilla + LZ4). In-place mutation would break compression chains and invalidate block statistics. Use retention policies to age out old data.                                                                        |
| `EXPLAIN ANALYZE`             | Not yet       | Requires instrumentation across the SPSC bridge to collect per-core execution stats from the Data Plane and merge them on the Control Plane. The bridge currently returns results but not timing metadata. Planned.                                                                                                                 |

## Related

- [Getting Started](getting-started.md) — First queries walkthrough
- [Architecture](architecture.md) — How the three-plane execution model works
- Engine guides: [Vectors](vectors.md) | [Graph](graph.md) | [Documents](documents.md) | [KV](kv.md) | [Timeseries](timeseries.md) | [Spatial](spatial.md) | [Full-Text](full-text-search.md) | [Array](array.md)
- [Bitemporal Queries](../bitemporal.md) — System time and valid time semantics
- [WASM](../wasm.md) — Browser and Node.js deployment

[Back to docs](README.md)
