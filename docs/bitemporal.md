# Bitemporal Queries

Bitemporal databases track data along two time dimensions: **system time** (when data entered the system) and **valid time** (when the data represents). NodeDB supports bitemporal queries across multiple engines, enabling audit trails, temporal corrections, and compliance-grade history.

## Concepts

**System Time** — The timestamp assigned by the database when a row is inserted, updated, or deleted. Used for audit trails and understanding what the database thought was true at a point in the past.

**Valid Time** — The timestamp that the row represents. Used for capturing temporal semantics — e.g., a price change that becomes effective tomorrow, or a correction to historical data.

## Supported Engines

| Engine                                 | System Time | Valid Time | Example Use Case                  |
| -------------------------------------- | ----------- | ---------- | --------------------------------- |
| [Graph](graph.md)                      | Yes         | Yes        | Entity relationship timelines     |
| [Document (strict)](documents.md)      | Yes         | Yes        | Versioned user profiles, ledgers  |
| [Document (schemaless)](documents.md)  | Yes         | Yes        | Event logs with backdated entries |
| [Columnar (Plain)](columnar.md)        | Yes         | Yes        | Audit tables, corrected metrics   |
| [Columnar (Timeseries)](timeseries.md) | Yes         | Yes        | Forecast corrections, data repair |
| [Array](array.md)                      | Yes         | Yes        | Historical spatial snapshots      |

**Index engines — temporal by composition, not natively:** Vector, Full-Text Search, Key-Value, Spatial. These engines are indexes over records; the records carry time, not the indexes. See [Index engines and temporal composition](#index-engines-and-temporal-composition).

## SQL Syntax

All temporal queries use `AS OF` clauses in the `FROM` clause:

```sql
-- Read as of a system time (historical database state)
SELECT * FROM collection
AS OF SYSTEM TIME 1700000000000;

-- Read rows valid at a specific time
SELECT * FROM collection
AS OF VALID TIME 1700000000000;

-- Read both dimensions: rows that were valid AND in the system at a point
SELECT * FROM collection
AS OF SYSTEM TIME 1700000000000
AS OF VALID TIME 1700000001000;
```

Times can be specified as:
- Integer milliseconds since Unix epoch (e.g., `1700000000000`)
- `NOW()` for current time
- ISO-8601 string literal (e.g., `'2024-01-15T00:00:00Z'`)

## Examples

### Audit Trail (System Time)

Retrieve the state of a collection at a past moment:

```sql
-- Document collection with system time tracking
CREATE COLLECTION user_accounts (
    id UUID DEFAULT gen_uuid_v7(),
    email VARCHAR,
    balance DECIMAL,
    created_at TIMESTAMP DEFAULT now()
) WITH (engine='document_strict', bitemporal=true);

INSERT INTO user_accounts (email, balance) VALUES
    ('alice@example.com', 100.00);

-- Some time later, update the balance
UPDATE user_accounts SET balance = 150.00 WHERE email = 'alice@example.com';

-- Query the database as it existed 10 minutes ago
SELECT email, balance FROM user_accounts
AS OF SYSTEM TIME 1700000000000;
-- Returns: alice@example.com, 100.00 (the original balance)

-- Query current state
SELECT email, balance FROM user_accounts;
-- Returns: alice@example.com, 150.00
```

### Backdated Corrections (Valid Time)

Insert a correction that becomes valid in the past:

```sql
-- Columnar timeseries with corrected measurements
CREATE COLLECTION sensor_readings (
    ts TIMESTAMP TIME_KEY,
    location VARCHAR,
    temperature FLOAT
) WITH (engine='timeseries');

-- Original reading was wrong; re-insert with correct value at original timestamp
INSERT INTO sensor_readings (ts, location, temperature, valid_time)
VALUES ('2026-04-01T10:00:00Z', 'warehouse-a', 21.5, '2026-04-01T10:00:00Z');

-- Later, discover the reading was incorrect and insert a correction
INSERT INTO sensor_readings (ts, location, temperature, valid_time)
VALUES ('2026-04-01T10:00:00Z', 'warehouse-a', 22.3, '2026-04-02T15:30:00Z');

-- Query what we knew at April 1st (before correction)
SELECT location, temperature FROM sensor_readings
WHERE ts BETWEEN '2026-04-01' AND '2026-04-02'
AS OF VALID TIME '2026-04-01T00:00:00Z';

-- Query what we know now (after correction)
SELECT location, temperature FROM sensor_readings
WHERE ts BETWEEN '2026-04-01' AND '2026-04-02'
AS OF VALID TIME 1712040000000;  -- April 2nd (after correction was entered)
```

### Bitemporal Array Snapshot

Retrieve array cells as they existed at a point in time:

```sql
-- Multidimensional scientific data with temporal audit trail
CREATE ARRAY climate_grid
  DIMS (
    lon INT32 DOMAIN [-180, 180),
    lat INT32 DOMAIN [-90, 90)
  )
  ATTRS (
    temp_c FLOAT32
  )
  TILE_EXTENTS (64, 64)
  WITH (audit_retain_ms = 7776000000);  -- 90 days

-- Query cells as they were committed yesterday
SELECT lon, lat, temp_c FROM ARRAY_SLICE(
    'climate_grid',
    {lon: [-10, 10), lat: [0, 20)},
    ['temp_c']
)
AS OF SYSTEM TIME '2026-06-06T00:00:00Z';
```

### Lineage and Compliance

Track row lineage across corrections:

```sql
-- Strict document with full temporal history
CREATE COLLECTION transactions STRICT (
    id UUID DEFAULT gen_uuid_v7(),
    account_id UUID,
    amount DECIMAL,
    status VARCHAR,
    created_at TIMESTAMP DEFAULT now()
);

-- Insert original transaction
INSERT INTO transactions (account_id, amount, status)
VALUES ('acc-123', 500.00, 'PENDING');

-- Status updates over time
UPDATE transactions SET status = 'CONFIRMED' WHERE id = 'txn-1';
UPDATE transactions SET status = 'SETTLED' WHERE id = 'txn-1';

-- Audit: show full timeline of this transaction
SELECT status, _ts_system FROM transactions
WHERE id = 'txn-1'
AS OF SYSTEM TIME NULL  -- returns every system-time version, ascending
ORDER BY _ts_system ASC;
```

`AS OF SYSTEM TIME NULL` returns every system-time version of each matching row, ordered ascending. Each version row carries the user columns plus three synthetic temporal columns: `_ts_system`, `_ts_valid_from`, and `_ts_valid_until`. Unbounded valid-time ends surface as the raw `i64::MIN` / `i64::MAX` sentinels (matching the Columnar and Timeseries engines). It is supported on the Document (strict and schemaless), Columnar, and Timeseries engines. It is not supported on the Graph or Array engines, nor when reading through a database clone — those return a typed error rather than collapsing to a single version.

`AS OF` reads run at parity with non-temporal reads: `WHERE` predicates (including on strict Binary-Tuple collections), `ORDER BY`, computed columns, and window functions all apply to temporal queries.

**Reserved columns never leak.** Bitemporal collections store their temporal envelope in reserved columns (`__system_from_ms`, `__valid_from_ms`, `__valid_until_ms`). These are hidden from user projections: `SELECT *` on a bitemporal collection returns exactly the declared user columns. Temporal data is only exposed through the synthetic `_ts_*` columns of audit queries.

## Index Engines and Temporal Composition

Vector, FTS, KV, and Spatial do not carry temporal columns. This is not an oversight — it is the correct architectural decomposition. These four are **index engines**: they point at records that live in data-bearing engines. Indexes don't have time; the records they point to do.

To query any of these engines at a point in time, attach the index to a data-bearing collection with `bitemporal=true`. The collection holds the payload and temporal columns. The index returns candidate IDs. `AS OF` filtering happens at the collection layer.

### Bitemporal Vector Search

```sql
-- 1. Create a Document collection with temporal tracking enabled
CREATE COLLECTION product_embeddings STRICT (
    id UUID DEFAULT gen_uuid_v7(),
    product_id UUID,
    description TEXT,
    embedding FLOAT[384],
    updated_at TIMESTAMP DEFAULT now()
) WITH (bitemporal = true);

-- 2. Attach a Vector index — the index points at rows in the collection
CREATE VECTOR INDEX idx_product_vec ON product_embeddings METRIC cosine DIM 384;

-- 3. Bitemporal vector search: find nearest neighbors as of 30 days ago
--    The Vector index returns candidate IDs; the Document layer filters by time
SELECT p.product_id, p.description,
       vector_distance(p.embedding, $query_vec) AS score
FROM product_embeddings p
AS OF SYSTEM TIME '2026-05-08T00:00:00Z'
WHERE p.id IN (
    SEARCH product_embeddings USING VECTOR(embedding, $query_vec, 20)
)
ORDER BY score
LIMIT 10;
```

The `AS OF SYSTEM TIME` clause is evaluated against `product_embeddings`. The Vector index is used to narrow the candidate set; temporal filtering removes candidates that did not exist at the target system time.

### Bitemporal Full-Text Search

```sql
-- 1. Create a Document collection with bitemporal tracking
CREATE COLLECTION articles STRICT (
    id UUID DEFAULT gen_uuid_v7(),
    title VARCHAR,
    body TEXT,
    published_at TIMESTAMP
) WITH (bitemporal = true);

-- 2. Attach an FTS index
CREATE SEARCH INDEX ON articles FIELDS title, body ANALYZER 'english';

-- 3. Bitemporal FTS: search the index state as it existed yesterday
SELECT id, title
FROM articles
AS OF SYSTEM TIME '2026-06-06T00:00:00Z'
WHERE text_match(body, 'distributed consensus raft')
ORDER BY bm25_score(body, 'distributed consensus raft') DESC
LIMIT 20;
```

To query what was valid at a past business date (rather than what was in the database):

```sql
SELECT id, title
FROM articles
AS OF VALID TIME 1711953600000
WHERE text_match(body, 'distributed consensus raft')
LIMIT 20;
```

### Bitemporal Spatial Queries

```sql
-- 1. Create a Document collection with bitemporal tracking and a geometry column
CREATE COLLECTION store_locations STRICT (
    id UUID DEFAULT gen_uuid_v7(),
    name VARCHAR,
    location GEOMETRY,
    opened_at TIMESTAMP,
    closed_at TIMESTAMP
) WITH (bitemporal = true);

-- 2. Attach a Spatial index
CREATE SPATIAL INDEX ON store_locations FIELDS location;

-- 3. Bitemporal spatial query: which stores existed within 5km at a past date?
SELECT id, name, ST_Distance(location, ST_Point(-73.990, 40.750)) AS dist_m
FROM store_locations
AS OF SYSTEM TIME 1704067200000
WHERE ST_DWithin(location, ST_Point(-73.990, 40.750), 5000)
ORDER BY dist_m;
```

The Spatial index narrows by geometry. `AS OF SYSTEM TIME` filters rows to those present in the database at that timestamp.

### Temporal Key-Value: Use Document Strict

The KV engine is designed for O(1) point lookups. It does not carry temporal columns. If you need temporal semantics on key-value-shaped data — versioned configuration, auditable feature flags, time-stamped session state — use a Document strict collection with a unique-key index instead:

```sql
-- Temporal key-value: Document strict with a unique-key constraint
CREATE COLLECTION config_entries STRICT (
    id UUID DEFAULT gen_uuid_v7(),
    key VARCHAR UNIQUE,
    value TEXT,
    updated_at TIMESTAMP DEFAULT now()
) WITH (bitemporal = true);

-- Set a value
INSERT INTO config_entries (key, value) VALUES ('feature_x_enabled', 'true')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Point-lookup (fast — unique index on key)
SELECT value FROM config_entries WHERE key = 'feature_x_enabled';

-- Audit: what was the value 7 days ago?
SELECT value FROM config_entries
AS OF SYSTEM TIME '2026-05-31T00:00:00Z'
WHERE key = 'feature_x_enabled';
```

O(1) lookup is preserved by the unique index. Temporal history comes from the Document layer.

## GDPR and Data Minimization

Use `audit_retain_ms` to enforce automatic purge of old versions:

```sql
-- Columnar table with 30-day retention (GDPR compliance)
CREATE COLLECTION user_activity (
    user_id UUID,
    action VARCHAR,
    ts TIMESTAMP TIME_KEY
) WITH (engine='columnar', audit_retain_ms=2592000000);  -- 30 days

-- Tiles/versions older than 30 days are purged during compaction
-- Historical queries beyond retention window will raise an error or return no rows
```

Once purged, historical queries beyond the retention window cannot access that data.

## Performance Considerations

- **System time queries** — Read from historical snapshots; performance depends on snapshot availability
- **Valid time queries** — Scan all versions and filter by valid time; slower than single-version reads
- **Both dimensions** — Intersection query; slower still, but enables precise audit trails

For large collections with many corrections, consider:

1. Archiving old versions to L2 (S3) cold storage periodically
2. Reducing `audit_retain_ms` once compliance window expires
3. Using columnar compression to minimize storage overhead

## Related

- [Array](array.md) — Bitemporal array engine with tile-level purge
- [Documents (strict)](documents.md) — Row-level system time tracking
- [Columnar](columnar.md) — Bitemporal profiles
- [Timeseries](timeseries.md) — Continuous data with valid-time semantics
- [Vector Search](vectors.md) — Temporal vector search via composition
- [Full-Text Search](full-text-search.md) — Temporal FTS via composition
- [Spatial](spatial.md) — Temporal spatial queries via composition
- [Key-Value](kv.md) — For temporal KV patterns, use Document strict

[Back to docs](README.md)
