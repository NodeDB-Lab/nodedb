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

**Not supported:** Vector, Full-Text Search, Key-Value, Spatial. These engines are optimized for current state, not historical queries.

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

Times are milliseconds since Unix epoch. Use `extract(epoch from now()) * 1000` for current time.

## Examples

### Audit Trail (System Time)

Retrieve the state of a collection at a past moment:

```sql
-- Document collection with system time tracking
CREATE COLLECTION user_accounts STRICT (
    id UUID DEFAULT gen_uuid_v7(),
    email VARCHAR,
    balance DECIMAL,
    created_at TIMESTAMP DEFAULT now()
);

INSERT INTO user_accounts (email, balance) VALUES
    ('alice@example.com', 100.00);

-- Some time later, update the balance
UPDATE user_accounts SET balance = 150.00 WHERE email = 'alice@example.com';

-- Query the database as it existed 10 minutes ago
SELECT email, balance FROM user_accounts
AS OF SYSTEM TIME (extract(epoch from now()) * 1000 - 600000);
-- Returns: alice@example.com, 100.00 (the original balance)

-- Query current state
SELECT email, balance FROM user_accounts;
-- Returns: alice@example.com, 150.00
```

### Backdated Corrections (Valid Time)

Insert a correction that becomes valid in the past:

```sql
-- Columnar timeseries with corrected measurements
CREATE TIMESERIES sensor_readings TYPE COLUMNAR (
    ts TIMESTAMP TIME_KEY,
    location VARCHAR,
    temperature FLOAT
);

-- Original reading was wrong; re-insert with correct value at original timestamp
INSERT INTO sensor_readings (ts, location, temperature, valid_time)
VALUES ('2026-04-01T10:00:00Z', 'warehouse-a', 21.5, '2026-04-01T10:00:00Z');

-- Later, discover the reading was incorrect and insert a correction
INSERT INTO sensor_readings (ts, location, temperature, valid_time)
VALUES ('2026-04-01T10:00:00Z', 'warehouse-a', 22.3, '2026-04-02T15:30:00Z');

-- Query what we knew at April 1st (before correction)
SELECT location, temperature FROM sensor_readings
WHERE ts BETWEEN '2026-04-01' AND '2026-04-02'
AS OF VALID TIME 1711953600000;  -- April 1st

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
SELECT lon, lat, temp_c FROM NDARRAY_SLICE(
    'climate_grid',
    {lon: [-10, 10), lat: [0, 20)},
    ['temp_c']
)
AS OF SYSTEM TIME (extract(epoch from now()) * 1000 - 86400000);
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
SELECT status, system_time FROM transactions
WHERE id = 'txn-1'
AS OF SYSTEM TIME NULL  -- special: returns all versions in system time order
ORDER BY system_time ASC;
```

## GDPR and Data Minimization

Use `audit_retain_ms` to enforce automatic purge of old versions:

```sql
-- Columnar table with 30-day retention (GDPR compliance)
CREATE COLLECTION user_activity TYPE COLUMNAR (
    user_id UUID,
    action VARCHAR,
    ts TIMESTAMP TIME_KEY
)
WITH (profile = 'plain', audit_retain_ms = 2592000000);  -- 30 days

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

[Back to docs](README.md)
