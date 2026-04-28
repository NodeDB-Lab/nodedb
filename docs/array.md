# Array Engine

NodeDB's array engine stores multi-dimensional data with bitemporal support — system time (when data was entered) and valid time (what the data represents). Use it for scientific computing, geospatial grids, medical imaging, and time-evolving spatial data. Cells are compressed, indexed by Z-order curves, and queryable via SQL table-valued functions.

## When to Use

- Scientific simulations and climate models
- Medical imaging and volumetric analysis
- GIS raster data (elevation maps, satellite imagery)
- Time-evolving geospatial grids
- Sparse multi-dimensional datasets
- Hypervolume analysis with bitemporal audit trails

## Key Features

- **Multi-dimensional storage** — Define arrays with arbitrary dimensions (e.g., 3D space + time)
- **Tile-based compression** — Cells grouped into tiles, each tile independently compressed (ALP, FastLanes, Gorilla, LZ4)
- **Z-order indexing** — Hilbert/Z-order curve linearization for spatial locality and fast range queries
- **Bitemporal support** — Both system time (audit trail) and valid time (temporal semantics) tracked per tile
- **Row-major or column-major layout** — Choose `CELL_ORDER` to match your access patterns
- **Cross-engine identity** — Cells linked via surrogate bitmaps to vector, graph, document, and columnar queries
- **Distributed execution** — Sharded by tile, queries scatter-gather across cores/nodes
- **Tile-level retention** — Purge old versions by system time for compliance (GDPR, data minimization)

## DDL Syntax

Array schemas are defined by dimensions (axes), attributes (stored values), and tile extents (chunk size):

```sql
CREATE ARRAY spatial_grid
  DIMS (
    x INT64 DOMAIN [0, 1000),
    y INT64 DOMAIN [0, 1000),
    z INT64 DOMAIN [0, 1000)
  )
  ATTRS (
    temperature FLOAT32,
    pressure FLOAT32,
    humidity FLOAT32
  )
  TILE_EXTENTS (64, 64, 64)
  WITH (
    cell_order = 'Z-ORDER',
    audit_retain_ms = 86400000
  );
```

### Parameters

| Parameter         | Required | Default     | Description                                                                                                        |
| ----------------- | -------- | ----------- | ------------------------------------------------------------------------------------------------------------------ |
| `DIMS`            | Yes      | —           | List of dimensions. Each has a name, type (`INT64`, `INT32`, `FLOAT64`), and domain bounds `[lo, hi)`.             |
| `ATTRS`           | Yes      | —           | List of attributes (cell values). Each has a name and type (`FLOAT32`, FLOAT64`, `INT32`, `INT64`, `STRING`).      |
| `TILE_EXTENTS`    | Yes      | —           | Tuple of tile extent per dimension. All > 0. Determines cell locality and compression block granularity.           |
| `cell_order`      | No       | `'Z-ORDER'` | `'Z-ORDER'` (Hilbert curve) or `'ROW-MAJOR'`. Affects spatial cache locality.                                      |
| `audit_retain_ms` | No       | `NULL`      | Milliseconds. Tiles older than now - `audit_retain_ms` (by system time) are eligible for purge. `NULL` = keep all. |

## Examples

### Basic Creation and Insertion

```sql
-- Create a 2D elevation map with tiles of 256x256 cells
CREATE ARRAY elevation_map
  DIMS (
    lon FLOAT64 DOMAIN [-180, 180),
    lat FLOAT64 DOMAIN [-90, 90)
  )
  ATTRS (
    height FLOAT32
  )
  TILE_EXTENTS (256, 256);

-- Insert cells (or rows of cells)
-- Cells are written as objects with dim names as keys
INSERT INTO elevation_map (lon, lat, height) VALUES
  (-73.5, 40.7, 10.5),
  (-73.6, 40.8, 12.3),
  (-73.7, 40.6, 8.9);

-- Flush memtable to persistent storage
SELECT NDARRAY_FLUSH('elevation_map');
```

### Temporal Array (System Time + Valid Time)

```sql
-- Create a 3D climate model with temporal tracking
CREATE ARRAY climate_forecast
  DIMS (
    lon INT32 DOMAIN [-180, 180),
    lat INT32 DOMAIN [-90, 90),
    altitude_m INT32 DOMAIN [0, 50000)
  )
  ATTRS (
    temp_c FLOAT32,
    humidity FLOAT32
  )
  TILE_EXTENTS (32, 32, 20)
  WITH (audit_retain_ms = 7776000000);  -- 90 days

-- Insert forecast data at a specific valid time
INSERT INTO climate_forecast (lon, lat, altitude_m, temp_c, humidity) VALUES
  (10, 20, 5000, -10.5, 65.0),
  (10, 20, 10000, -20.3, 40.0);

-- Query at a specific moment in time (system time)
SELECT lon, lat, altitude_m, temp_c
FROM NDARRAY_SLICE('climate_forecast', {lon: [10, 15), lat: [20, 25), altitude_m: [5000, 15000)}, ['temp_c'])
AS OF SYSTEM TIME 1700000000000;
```

## Query Functions

All array queries use table-valued functions in the `FROM` clause. System time and valid time can be specified via `AS OF` clauses.

### NDARRAY_SLICE — Range Query

Returns cells within a multi-dimensional range:

```sql
SELECT * FROM NDARRAY_SLICE(
  'elevation_map',
  {lon: [-74.0, -73.0), lat: [40.0, 41.0)},
  ['height'],  -- projecting only 'height' (optional)
  1000         -- limit to 1000 cells (optional)
);
```

| Parameter | Required | Type          | Description                                                     |
| --------- | -------- | ------------- | --------------------------------------------------------------- |
| `array`   | Yes      | STRING        | Array name                                                      |
| `bounds`  | Yes      | OBJECT        | Dict of dim name → `[lo, hi)` bounds. Omitted dims = full range |
| `attrs`   | No       | ARRAY[STRING] | Attributes to project. `NULL` = all attributes                  |
| `limit`   | No       | INT64         | Max cells returned. `NULL` = no limit                           |

### NDARRAY_PROJECT — Select Attributes

Returns all cells, optionally filtered to specific attributes:

```sql
SELECT * FROM NDARRAY_PROJECT(
  'spatial_grid',
  ['temperature', 'pressure']  -- only these attributes
);
```

### NDARRAY_AGG — Aggregate Over Dimensions

Aggregates an attribute over a dimension, reducing dimensionality:

```sql
-- Sum temperature over the x dimension, keeping y and z
SELECT * FROM NDARRAY_AGG(
  'spatial_grid',
  'temperature',
  'SUM',
  'x'  -- aggregate over x; result has one less dimension
);
```

Supported reducers: `'SUM'`, `'AVG'`, `'MIN'`, `'MAX'`, `'COUNT'`.

### NDARRAY_ELEMENTWISE — Element-wise Operations

Applies an operation between two arrays (or array and scalar) with the same shape:

```sql
-- Subtract a baseline from all cells
SELECT * FROM NDARRAY_ELEMENTWISE(
  'current_grid',
  'baseline_grid',
  'SUBTRACT',
  'temperature'  -- the attribute to operate on
);
```

## Maintenance Functions

### NDARRAY_FLUSH

Forces in-memory cells to durable storage:

```sql
SELECT NDARRAY_FLUSH('spatial_grid') AS result;
-- Returns: {result: true}
```

Returns a single row `{result: BOOL}`. Always returns `true`; failure is fatal and raises an error.

### NDARRAY_COMPACT

Merges tile versions and reclaims space:

```sql
SELECT NDARRAY_COMPACT('spatial_grid') AS result;
-- Returns: {result: true}
```

Compaction is background-automatic, but can be triggered manually.

## Bitemporal Support

Arrays support dual timestamping:

- **System Time** — When the cell value was written (audit trail, compliance)
- **Valid Time** — When the cell represents (temporal semantics, forecasts, corrections)

Query as of either or both:

```sql
-- Read cells as they existed at a point in the past
SELECT * FROM NDARRAY_SLICE(
  'data',
  {x: [0, 100), y: [0, 100)},
  ['value']
)
AS OF SYSTEM TIME 1700000000000;

-- Read cells that were valid at a specific time
SELECT * FROM NDARRAY_SLICE(
  'forecast',
  {x: [0, 100), y: [0, 100)},
  ['temp']
)
AS OF VALID TIME 1700000000000;

-- Read cells that were valid AND existed at a specific time
SELECT * FROM NDARRAY_SLICE(
  'forecast',
  {x: [0, 100), y: [0, 100)},
  ['temp']
)
AS OF SYSTEM TIME 1700000000000 AS OF VALID TIME 1700000001000;
```

## Cross-Engine Integration

Array cells are addressable via surrogate identity alongside other engines. Combine array queries with vector search, graph traversal, and full-text search:

```sql
-- Find cells near a vector embedding, return array slice
SELECT *
FROM NDARRAY_SLICE('spatial_data', slice_bounds, ['attr1', 'attr2'])
WHERE id IN (
  SEARCH vectors USING VECTOR(embedding, query_vec, 100)
);
```

See [Architecture — Cross-engine identity](architecture.md#cross-engine-identity) for details.

## Performance

- **Tile-level parallelism** — Each tile is read/processed in parallel on separate cores
- **Compression** — Typical 5-20x compression depending on data homogeneity
- **Range queries** — Z-order indexing provides cache-friendly access; skip irrelevant tiles via block statistics
- **Sparse data** — Only materialized cells stored; implicit zeros and empty regions not persisted

## Temporal Purge and Compliance

System time–based retention enables GDPR and data minimization compliance:

```sql
ALTER NDARRAY spatial_grid SET (audit_retain_ms = 86400000);  -- keep 1 day

-- Tiles older than now - 1 day are candidates for purge
-- Purge is automatic during compaction
```

Purged tiles are irreversibly removed; historical queries beyond the retention window will see gaps.

## Related

- [Bitemporal](bitemporal.md) — Cross-engine bitemporal architecture
- [Architecture — Cross-engine identity](architecture.md#cross-engine-identity) — Surrogate bitmap linking
- [Columnar](columnar.md) — Related structured analytics engine

[Back to docs](README.md)
