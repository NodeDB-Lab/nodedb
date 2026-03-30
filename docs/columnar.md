# Columnar Engine

NodeDB's columnar engine stores data in typed columns with per-column compression — the same approach as ClickHouse and DuckDB, but living alongside your OLTP data in the same database.

## When to Use

- Analytical queries (GROUP BY, aggregations, window functions)
- Reporting dashboards
- Data science workloads
- Any scan-heavy workload where you read a few columns from many rows
- HTAP: pair with strict documents for combined OLTP + OLAP

## Key Features

- **Per-column compression** — Each column gets a codec chain tuned for its data type:
  - **ALP** — Lossless floating-point to integer conversion
  - **FastLanes** — SIMD bit-packing for integers
  - **FSST** — Substring dictionary compression for strings
  - **Gorilla** — XOR-based compression for metrics
  - **Pcodec** — Complex numeric compression
  - **rANS** — Entropy coding for cold-tier data
  - **LZ4** — Terminal stage compression
- **Block statistics** — 1024-row blocks with min/max/null-count stats. The query engine skips entire blocks that can't match the predicate — no decompression needed.
- **Predicate pushdown** — Filters push down to the block level, reading only what's needed.
- **Delete bitmaps** — Roaring Bitmaps track deleted rows. Three-phase crash-safe compaction reclaims space.
- **20-40x compression** — Multi-stage pipeline achieves significantly better ratios than single-codec approaches on typical workloads.

## Profiles

Columnar collections can specialize via profiles:

| Profile                         | Optimized for        | Extra features                                                      |
| ------------------------------- | -------------------- | ------------------------------------------------------------------- |
| **Plain**                       | General analytics    | Full UPDATE/DELETE support                                          |
| **[Timeseries](timeseries.md)** | Time-ordered metrics | Append-only, retention policies, continuous aggregation, ILP ingest |
| **[Spatial](spatial.md)**       | Geospatial data      | Auto R\*-tree indexing, geohash proximity queries                   |

## DDL Syntax

The unified `CREATE COLLECTION` syntax for columnar collections uses `WITH (storage = 'columnar')`. Column modifiers designate special columns:

| Modifier | Applies to | Effect |
| --- | --- | --- |
| `TIME_KEY` | `TIMESTAMP` / `DATETIME` columns | Designates the primary time column. Enables partition-by-time, block-level skip, and retention policies. Required for the timeseries profile. |
| `SPATIAL_INDEX` | `GEOMETRY` columns | Automatically builds and maintains an R*-tree index on this column. Required for the spatial profile. |

```sql
-- Plain columnar collection
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
) WITH (storage = 'columnar', profile = 'timeseries', partition_by = '1h');

-- Spatial profile (SPATIAL_INDEX required)
CREATE COLLECTION locations (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
) WITH (storage = 'columnar');
```

`CREATE TIMESERIES <name>` remains as a convenience alias for the timeseries profile — it is equivalent to the `WITH (storage = 'columnar', profile = 'timeseries')` form.

## Physical Plan Types

The query planner emits different physical plan nodes for columnar queries:

- **`ColumnarOp`** — Base plan for plain columnar collections. All profiles extend this.
- **`TimeseriesOp`** — Extends `ColumnarOp` with `time_range` bounds and time bucketing. Used when a `TIME_KEY` column and time predicate are present.
- **`SpatialOp`** — Extends `ColumnarOp` with R*-tree lookup. Used when a `SPATIAL_INDEX` column is referenced in a spatial predicate.

All three share the same `columnar_memtables` (the in-memory L0 structure). Profile-specific logic is layered on top.

## Examples

```sql
-- Plain columnar collection with a time column
CREATE COLLECTION logs (
    ts TIMESTAMP TIME_KEY,
    host VARCHAR,
    level VARCHAR,
    message VARCHAR
) WITH (storage = 'columnar');

-- Ingest
INSERT INTO logs (ts, host, level, message)
VALUES (now(), 'web-01', 'error', 'connection refused');

-- Analytical query — only reads the columns it needs
SELECT level, COUNT(*) AS count
FROM logs
WHERE ts > now() - INTERVAL '1 hour'
GROUP BY level
ORDER BY count DESC;

-- Window functions
SELECT host, message,
       ROW_NUMBER() OVER (PARTITION BY host ORDER BY ts DESC) AS recency_rank
FROM logs;
```

## HTAP Bridge

The real power of columnar is combining it with strict documents for hybrid transactional/analytical processing (HTAP).

```sql
-- OLTP: strict collection for real-time writes
CREATE COLLECTION orders (
    id UUID DEFAULT gen_uuid_v7(),
    customer_id UUID,
    total DECIMAL,
    status STRING,
    created_at DATETIME DEFAULT now()
) WITH (storage = 'strict');

-- OLAP: materialized view auto-replicates to columnar via CDC
CREATE MATERIALIZED VIEW order_analytics AS
SELECT status, DATE_TRUNC('day', created_at) AS day, COUNT(*), SUM(total)
FROM orders
GROUP BY status, day;

-- Point lookups hit the strict engine (fast)
SELECT * FROM orders WHERE id = '...';

-- Analytical scans hit the columnar engine (fast)
SELECT * FROM order_analytics WHERE day > '2024-06-01';
```

The query planner routes automatically — no application logic needed. Changes to the strict collection propagate to the materialized view via CDC with staleness tracking.

## Converting to Columnar

```sql
-- Convert an existing collection to plain columnar
CONVERT COLLECTION logs TO STORAGE='columnar';

-- Convert to columnar with timeseries profile
CONVERT COLLECTION metrics TO STORAGE='columnar' WITH (profile = 'timeseries');

-- Convert to document or kv
CONVERT COLLECTION events TO STORAGE='document';
CONVERT COLLECTION sessions TO STORAGE='kv';
```

## Related

- [Timeseries](timeseries.md) — Columnar profile for time-ordered data
- [Spatial](spatial.md) — Columnar profile for geospatial data
- [Documents (strict)](documents.md) — HTAP bridge source for OLTP + OLAP

[Back to docs](README.md)
