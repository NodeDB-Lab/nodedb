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
    x INT64 [0..1000],
    y INT64 [0..1000],
    z INT64 [0..1000]
  )
  ATTRS (
    temperature FLOAT64,
    pressure FLOAT64,
    humidity FLOAT64
  )
  TILE_EXTENTS (64, 64, 64)
  WITH (
    prefix_bits = 8,
    audit_retain_ms = 86400000
  );
```

### Parameters

| Parameter         | Required | Default     | Description                                                                                                        |
| ----------------- | -------- | ----------- | ------------------------------------------------------------------------------------------------------------------ |
| `DIMS`            | Yes      | —           | List of dimensions. Each has a name, type (`INT64`, `FLOAT64`, `TIMESTAMP_MS`, `STRING`), and optional domain bounds `[lo..hi]`. |
| `ATTRS`           | Yes      | —           | List of attributes (cell values). Each has a name, a type (see [Attribute Types](#attribute-types)), and an optional `NOT NULL`. |
| `TILE_EXTENTS`    | Yes      | —           | Tuple of tile extent per dimension. All > 0. Determines cell locality and compression block granularity.           |
| `CELL_ORDER`      | No       | `HILBERT`   | Unquoted: `ROW_MAJOR`, `COL_MAJOR`, `HILBERT`, or `ZORDER` (`Z_ORDER` is accepted). Affects spatial cache locality. |
| `TILE_ORDER`      | No       | `HILBERT`   | Unquoted: `ROW_MAJOR`, `COL_MAJOR`, `HILBERT`, or `ZORDER` (`Z_ORDER` is accepted). Affects tile layout.           |
| `prefix_bits`     | No       | `8`         | Bits for key prefix compression (1–16). Lower = better compression, higher = faster prefix filtering.              |
| `audit_retain_ms` | No       | `NULL`      | Milliseconds. Tiles older than now - `audit_retain_ms` (by system time) are eligible for purge. `NULL` = keep all. |
| `minimum_audit_retain_ms` | No | —        | Minimum retention window enforced for compliance.                                                                  |

### Attribute Types

An attribute stores each cell at its declared width. A value outside the width is refused with SQLSTATE `22003` on `INSERT`, `COPY`, and the native bulk load. It is never wrapped or clamped.

| Type      | Aliases                    | Range                              | Column type (OID)         |
| --------- | -------------------------- | ---------------------------------- | ------------------------- |
| `INT8`    | `TINYINT`                  | -128 to 127                        | `smallint` (21)           |
| `UINT8`   |                            | 0 to 255                           | `smallint` (21)           |
| `INT16`   | `SMALLINT`, `INT2`         | -32768 to 32767                    | `smallint` (21)           |
| `UINT16`  |                            | 0 to 65535                         | `integer` (23)            |
| `INT32`   | `INTEGER`, `INT`, `INT4`   | -2147483648 to 2147483647          | `integer` (23)            |
| `UINT32`  |                            | 0 to 4294967295                    | `bigint` (20)             |
| `INT64`   | `BIGINT`                   | 64-bit signed                      | `bigint` (20)             |
| `FLOAT32` | `REAL`, `FLOAT4`           | 32-bit IEEE float                  | `real` (700)              |
| `FLOAT64` | `DOUBLE`, `FLOAT`          | 64-bit IEEE float                  | `double precision` (701)  |
| `STRING`  | `TEXT`, `VARCHAR`          | UTF-8 text                         | `text` (25)               |
| `BYTES`   | `BLOB`, `BYTEA`            | raw bytes                          | `bytea` (17)              |

- An unsigned type reads as the next wider signed column type, so every stored value fits.
- The canonical names give the width in bits, so `INT8` is the 1-byte signed type, like `UINT8`. This differs from PostgreSQL, where `int8` is an 8-byte integer. For an 8-byte attribute, write `INT64` or `BIGINT`. Collections keep the PostgreSQL meaning of `INT8`.
- The PostgreSQL aliases give the width in bytes: `INT2` is `INT16` and `INT4` is `INT32`.
- `INT` is the 4-byte `INT32`, as in PostgreSQL. A bare `FLOAT` is the 8-byte `FLOAT64`.
- A GeoTIFF band of signed 8-bit samples loads into `INT8` or any wider signed or float type. A band of signed 32-bit samples loads into `INT32`, `INT64`, or `FLOAT64`.
- A float written to a `FLOAT32` attribute rounds to the nearest `f32`. Only a finite value past the `f32` range is refused. `0.1` stored as `FLOAT32` reads back as `0.1`.
- An integer literal is accepted by every integer and float type. A float literal is accepted by `FLOAT32` and `FLOAT64` only.
- An integer column compresses by its value range, so a `UINT16` column takes the same bytes on disk as an `INT64` column of the same values. A `FLOAT32` column stores at most 4 bytes per value, half the raw width of `FLOAT64`.
- A `NULL` stays `NULL` through a flush and a compaction, in every type.

### Attribute Metadata

An attribute can carry reading metadata after its type: `NODATA`, `SCALE`, `OFFSET`, and `UNITS`, in any order and mixed with `NOT NULL`.

```sql
CREATE ARRAY scene
  DIMS (row INT64 [0..10979], col INT64 [0..10979])
  ATTRS (
    red UINT16 NODATA 0 SCALE 0.0000275 OFFSET -0.2 UNITS 'reflectance',
    qa  UINT8  NOT NULL
  )
  TILE_EXTENTS (512, 512);
```

| Clause   | Applies to        | Meaning |
| -------- | ----------------- | ------- |
| `NODATA` | numeric attributes | The raw value that marks a cell as holding no data. A number, `NAN`, `INF`, or `'-inf'` (quoted). |
| `SCALE`  | numeric attributes | The factor a physical read multiplies the raw value by. Finite and non-zero. |
| `OFFSET` | numeric attributes | The term a physical read adds after the scale. Finite. |
| `UNITS`  | every attribute   | The unit of the physical value, as a quoted string. Not empty. |

- A cell stores the raw value. Metadata never changes a stored cell.
- `NODATA` must be a value of the attribute type: a whole number in range for an integer type, and a value inside the `f32` range (or `NAN`, `INF`, `'-inf'`) for `FLOAT32`. A value the type cannot hold is refused with SQLSTATE `22000`.
- A `FLOAT32` cell matches the nearest `f32` of `NODATA`, as GDAL compares. `NAN` matches `NAN`.
- A physical read gives `raw * scale + offset` as `FLOAT64`. A missing `SCALE` reads as 1 and a missing `OFFSET` as 0. A nodata cell reads as `NULL`. See `physical => true` on [`ARRAY_SLICE`](#array_slice--range-query) and [`ARRAY_AGG`](#array_agg--aggregate-an-attribute).
- Nodata is not data: `ARRAY_AGG` skips a nodata cell for every reducer, `COUNT` included, with or without `physical`.
- `DESCRIBE` shows the metadata after the type, in the DDL form that recreates it: `UINT16 NODATA 0.0 SCALE 2.75e-5 OFFSET -0.2 UNITS 'reflectance'`.

Change the metadata with `ALTER ARRAY`:

```sql
-- Set or replace fields. A field the clause omits keeps its value.
ALTER ARRAY scene ALTER ATTR red SET (NODATA 0, SCALE 0.0000275);

-- Remove fields. Each named field must be set (SQLSTATE 55000 otherwise).
ALTER ARRAY scene ALTER ATTR red DROP OFFSET, UNITS;
```

Metadata is not part of the schema hash. Setting or dropping it keeps every stored segment readable and replicates through the array catalog. An unknown attribute is refused with SQLSTATE `42703`.

`COPY <array> FROM '<file>.tif' WITH (FORMAT geotiff)` picks a pixel's nodata in this order: the COPY `NODATA` option, then the file's `GDAL_NODATA`, then each attribute's `NODATA`. The first two apply to every band. Attribute metadata gives each band the nodata of the attribute it loads into. A pixel is skipped when every loaded band holds its own nodata. A band that holds nodata in a kept pixel writes `NULL`.

## Examples

### Basic Creation and Insertion

```sql
-- Create a 2D elevation map with tiles of 256x256 cells
CREATE ARRAY elevation_map
  DIMS (
    lon FLOAT64 [-180..180],
    lat FLOAT64 [-90..90]
  )
  ATTRS (
    height FLOAT64
  )
  TILE_EXTENTS (256, 256);

-- Insert cells: COORDS lists the dims in schema order, VALUES lists the attrs in schema order.
-- Values are literals: numbers, strings, or NULL for an attribute.
INSERT INTO ARRAY elevation_map
  COORDS (-73.5, 40.7) VALUES (10.5),
  COORDS (-73.6, 40.8) VALUES (12.3),
  COORDS (-73.7, 40.6) VALUES (8.9);

-- Remove cells by coordinate
DELETE FROM ARRAY elevation_map WHERE COORDS IN ((-73.7, 40.6));

-- Writing a coordinate again overwrites its cell.
-- DROP ARRAY [IF EXISTS] <name> removes the whole array.

-- Flush memtable to persistent storage
SELECT ARRAY_FLUSH('elevation_map');
```

### Temporal Array (System Time + Valid Time)

```sql
-- Create a 3D climate model with temporal tracking
CREATE ARRAY climate_forecast
  DIMS (
    lon INT64 [-180..180],
    lat INT64 [-90..90],
    altitude_m INT64 [0..50000]
  )
  ATTRS (
    temp_c FLOAT64,
    humidity FLOAT64
  )
  TILE_EXTENTS (32, 32, 20)
  WITH (audit_retain_ms = 7776000000);  -- 90 days

-- Insert forecast data at a specific valid time
INSERT INTO ARRAY climate_forecast
  COORDS (10, 20, 5000) VALUES (-10.5, 65.0),
  COORDS (10, 20, 10000) VALUES (-20.3, 40.0);

-- Query at a specific moment in time (system time)
SELECT lon, lat, altitude_m, temp_c
FROM ARRAY_SLICE('climate_forecast', '{lon: [10, 14], lat: [20, 24], altitude_m: [5000, 14999]}', ['temp_c'])
AS OF SYSTEM TIME 1700000000000;
```

## Query Functions

All array queries use table-valued functions in the `FROM` clause. System time and valid time can be specified via `AS OF` clauses.

### ARRAY_SLICE — Range Query

Returns cells within a multi-dimensional range:

```sql
SELECT * FROM ARRAY_SLICE(
  'elevation_map',
  '{lon: [-74.0, -73.0], lat: [40.0, 41.0]}',
  ['height'],  -- projecting only 'height' (optional)
  1000         -- limit to 1000 cells (optional)
);
```

| Parameter | Required | Type          | Description                                                     |
| --------- | -------- | ------------- | --------------------------------------------------------------- |
| `array`   | Yes      | STRING        | Array name, as a string literal                                 |
| `bounds`  | Yes      | STRING        | Quoted object literal `'{ dim: [lo, hi] }'`. Each range is closed: both ends are included. Omitted dims = full range |
| `attrs`   | No       | ARRAY[STRING] | Attributes to project, such as `['height']`. `'*'` = all attributes. Omit it to read all attributes |
| `limit`   | No       | INT         | Max cells returned, as a non-negative integer literal. Omit it for no limit |
| `physical => true` | No | BOOL      | Named argument. Read each attribute physically: `raw * scale + offset` as `double precision` for an attribute with a `SCALE` or `OFFSET`, and `NULL` for a nodata cell. Default `false` |

`ARRAY_SLICE` takes one named argument, `physical`. `ARRAY_PROJECT` and `ARRAY_ELEMENTWISE` take none. An unknown named argument, or `=` in place of `=>`, is refused:

```sql
SELECT x, y, red
FROM ARRAY_SLICE('scene', '{row: [0, 511], col: [0, 511]}', ['red'], physical => true);
```

#### Result Relation

`ARRAY_SLICE` and `ARRAY_PROJECT` return one row per cell. The row has one column per dimension, then one column per requested attribute, or every attribute when the list is empty. Each column carries its declared type:

| Declared type | Column type        |
| ------------- | ------------------ |
| `INT8`, `UINT8`, `INT16` (attribute only) | `smallint` |
| `UINT16`, `INT32` (attribute only) | `integer` |
| `UINT32` (attribute only) | `bigint` |
| `INT64`       | `bigint`           |
| `FLOAT32` (attribute only) | `real` |
| `FLOAT64`     | `double precision` |
| `TIMESTAMP_MS` (dimension) | `timestamp` |
| `STRING`      | `text`             |
| `BYTES`       | `bytea` (attribute only) |

`SELECT *` lists the dimensions first, then the attributes, in schema order. For a 2-D image with `nir` and `red` bands:

```sql
SELECT * FROM ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}');
--  x | y | nir  | red
-- ---+---+------+------
--  0 | 0 | 0.80 | 0.20
--  0 | 1 | 0.60 | 0.30
```

Every SELECT clause applies to the rows: `WHERE`, computed columns, `GROUP BY`, aggregates, `HAVING`, `ORDER BY`, `DISTINCT`, `LIMIT` / `OFFSET`, and window functions. Qualify a column with the call's alias, or with the array name when the call has no alias.

```sql
-- NDVI per cell, vegetated cells only, greenest first
SELECT x, y, (nir - red) / (nir + red) AS ndvi
FROM ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}') AS t
WHERE t.nir > t.red
ORDER BY ndvi DESC
LIMIT 10;

-- Mean NIR per column of pixels
SELECT x, AVG(nir) AS mean_nir
FROM ARRAY_SLICE('img', '{x: [0, 3], y: [0, 3]}')
GROUP BY x;
```

The 4th-argument `limit` caps the cells the slice reads before any clause applies. An outer `LIMIT` applies to the query's result, and a smaller one also lowers the read cap when no other clause reads past the cut. A `WHERE` bound on a dimension narrows the read range only when the slice has no cap, so a capped slice keeps the cells its cap selects.

`AS OF SYSTEM TIME NULL` adds a `_ts_system` column: the version's system time in milliseconds. `SELECT *` omits it. Name it to read it.

`ARRAY_AGG` and `ARRAY_ELEMENTWISE` return rows of their own shape, not dimensions and attributes. A query over them takes `*` or their own output columns, and `LIMIT`. Every other clause is refused with an error that names the clause. Use `ARRAY_SLICE` to filter, group, or order cells.

### ARRAY_PROJECT — Select Attributes

Returns all cells with only the listed attributes. The attribute list is required and must not be empty. It reads the whole domain, as an `ARRAY_SLICE` with no bounds does, and honors `AS OF` the same way:

```sql
SELECT * FROM ARRAY_PROJECT(
  'spatial_grid',
  ['temperature', 'pressure']  -- only these attributes
);
```

### ARRAY_AGG — Aggregate an Attribute

Reduces one attribute with a reducer. The optional 4th argument names a dimension to group by:

```sql
-- One result for the whole array
SELECT * FROM ARRAY_AGG('spatial_grid', 'temperature', 'SUM');

-- One result per value of x
SELECT * FROM ARRAY_AGG('spatial_grid', 'temperature', 'SUM', 'x');
```

The result has a `result` column. A grouped call also has a `group` column that holds the dimension value. The `group` column takes the group dimension's declared type: `TIMESTAMP_MS` gives `timestamp`, `FLOAT64` gives `float8`, `INT64` gives `int8`, and `STRING` gives `text`. A temporal call (`AS OF`) appends one summary row with the `bool` column `truncated_before_horizon`. Reducer names are case-insensitive.

| Reducer | Parameters | Integer attribute | Float attribute | Definition |
| ------- | ---------- | ----------------- | --------------- | ---------- |
| `'count'` | — | `bigint` | `bigint` | The non-null, non-nodata cells. |
| `'sum'` | — | `bigint`, exact | `double precision` | |
| `'min'`, `'max'` | — | the attribute's own column type, exact | the attribute's own column type | `NaN` sorts above every other float. |
| `'mean'` (`'avg'`) | — | `double precision` | `double precision` | Sum over count, merged exactly. |
| `'median'` | — | `double precision` | `double precision` | `'percentile'` with `p => 0.5`. |
| `'percentile'` | `p => 0..1` | `double precision` | `double precision` | Exact. With `N` values sorted ascending and numbered from 0, the value at rank `h = p * (N - 1)`: `v[floor(h)] + (h - floor(h)) * (v[ceil(h)] - v[floor(h)])`. This is PostgreSQL's `percentile_cont`. |
| `'mode'` | — | the attribute's own column type | the attribute's own column type | The value the most cells hold. Among ties, the smallest value. |
| `'var_pop'`, `'var_samp'` (`'variance'`) | — | `double precision` | `double precision` | Population variance `m2 / N`, sample variance `m2 / (N - 1)`. `'variance'` is the sample form, as in PostgreSQL. |
| `'stddev_pop'`, `'stddev_samp'` (`'stddev'`) | — | `double precision` | `double precision` | Square roots of the variances. `'stddev'` is the sample form. |
| `'histogram'` | `bins => 1..65536`, `lo => n`, `hi => n` | see below | see below | Fixed-width bin counts over `[lo, hi]`. |

`ARRAY_AGG` takes these named arguments after its positional ones: `p`, `bins`, `lo`, `hi`, and `physical`. A reducer refuses a parameter it does not take, and refuses to run without one it needs. An unknown named argument, or `=` in place of `=>`, is refused.

```sql
-- 90th percentile of reflectance per scene date, physical values
SELECT * FROM ARRAY_AGG('scene', 'red', 'percentile', 'date', p => 0.9, physical => true);

-- Population standard deviation of the whole array
SELECT * FROM ARRAY_AGG('scene', 'red', 'stddev_pop');
```

A histogram splits `[lo, hi]` into `bins` bins of width `(hi - lo) / bins`. Bin `i` holds the values in `[lo + i*w, lo + (i+1)*w)`, and the last bin also holds `hi`. A value below `lo`, above `hi`, or `NaN` falls in no bin and is not counted. A histogram returns one row per bin, zero-count bins included, with `bin_lo` and `bin_hi` (`double precision`) and `count` (`bigint`) in place of `result`. A grouped histogram returns one row per bin per group, after `group`:

```sql
SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'histogram', bins => 4, lo => -1, hi => 1);
--  bin_lo | bin_hi | count
-- --------+--------+-------
--    -1   |  -0.5  |   12
--   -0.5  |   0    |   40
--    0    |   0.5  |  311
--    0.5  |   1    |  145
```

Every reducer accepts integer and float attributes. `COUNT` also accepts `STRING` and `BYTES`. Another reducer over a `STRING` or `BYTES` attribute is refused at plan time with a type mismatch error. Every reducer groups by a dimension.

Every reducer is exact across tiles, cores, and nodes: each core and shard sends a partial that merges without loss. An integer `SUM` adds exactly, so `2^53 + 1` stays `9007199254740993`. A `SUM` past the `bigint` range is refused with SQLSTATE `22003`. `'median'`, `'percentile'`, and `'mode'` carry every distinct value with its count. They hold at most 4,194,304 distinct values per group, about 64 MiB. Past that budget the query is refused with SQLSTATE `54000`, never approximated. An integer attribute of 16 bits or less never reaches the budget. To stay under it on a wide attribute, narrow the box or zone, or group by a dimension. The variances use Welford's method, merged by Chan's formula.

With `physical => true`, every reducer reads `raw * scale + offset` for an attribute with a `SCALE` or `OFFSET`, so the result type is that of a `FLOAT64` attribute. A nodata cell never contributes, with or without `physical`. An attribute whose every cell in the region is nodata returns `NULL` for every scalar reducer, `COUNT` included.

Three more optional arguments bound the cells the reducer reads:

- **5th, box**: a slice predicate in the `ARRAY_SLICE` form, such as `'{lon: [101, 103], lat: [2, 4]}'`. Bounds are inclusive.
- **6th and 7th, zone**: a WKT `POLYGON` or `MULTIPOLYGON`, then the two dimensions it is drawn over, x first, such as `ARRAY['lon', 'lat']`. Both dimensions must be `INT64` or `FLOAT64`. Pass both arguments or neither.

Pass `NULL` for an argument to skip it:

```sql
-- Mean NDVI inside a field boundary
SELECT * FROM ARRAY_AGG(
  'scene', 'ndvi', 'MEAN', NULL, NULL,
  'POLYGON((101 2, 103 2, 103 4, 101 4, 101 2))', ARRAY['lon', 'lat']
);

-- Sum per lon value inside a box
SELECT * FROM ARRAY_AGG('scene', 'ndvi', 'SUM', 'lon', '{lon: [101, 103]}');
```

A cell is in a zone when it lies inside one member polygon and outside that polygon's holes. Edges are half-open: on a rectangle, a cell on the lower x or y edge is inside and a cell on the upper edge is outside. A region that holds no cell returns `NULL` for every reducer, `COUNT` included. Tiles whose bounding boxes miss the region are skipped before decode.

### ARRAY_ELEMENTWISE — Element-wise Operations

Applies an operation between two arrays of the same shape. Both arrays must have the same dimensions, attributes, tile extents, cell order, and `prefix_bits`. Arrays that differ are refused at planning:

```sql
-- Subtract a baseline from all cells
SELECT * FROM ARRAY_ELEMENTWISE(
  'current_grid',
  'baseline_grid',
  'sub',          -- 'add', 'sub', 'mul', or 'div' (case-insensitive)
  'temperature'   -- the attribute to operate on
);
```

The result has `coords` and `attrs` columns, both JSON. `attrs` holds one value: the operation applied to the named attribute. A coordinate present in one array only takes that array's value.

On integer attributes, `add`, `sub`, and `mul` are exact 64-bit integers, and a result past the `bigint` range is refused with SQLSTATE `22003`. `div`, and every operation on a float attribute, yields a 64-bit float. Division by zero yields `NULL`.

`AS OF SYSTEM TIME` and `AS OF VALID TIME` apply to both arrays. A temporal query appends one `{"truncated_before_horizon": bool}` row, as `ARRAY_AGG` does. `AS OF SYSTEM TIME NULL` is refused.

## Maintenance Functions

### ARRAY_FLUSH

Forces in-memory cells to durable storage:

```sql
SELECT ARRAY_FLUSH('spatial_grid');
-- Returns: {"flushed": 1}
```

Returns one object `{"flushed": 1}` on success. A failure raises an error.

### ARRAY_COMPACT

Merges tile versions and reclaims space:

```sql
SELECT ARRAY_COMPACT('spatial_grid');
-- Returns: {"compacted": 1}
```

Returns one object `{"compacted": n}`. `n` counts the cores that merged segments. It is `0` when the compaction picker found nothing to merge. A failure raises an error. Compaction is background-automatic, but can be triggered manually.

## Bulk Load: COPY

`COPY <array> FROM STDIN` and `COPY <array> FROM '<file>.csv'` load CSV rows: the dims in schema order, then the attributes. `COPY <array> FROM ... WITH (FORMAT geotiff)` loads a raster. Every `COPY ... FROM` needs a superuser.

### Raster Sources

```sql
-- One file: band i loads into attribute i.
COPY scene FROM '/data/LC09_SR.tif' WITH (FORMAT geotiff, TIME '2024-03-15T03:12:00Z');

-- One file per band group, read in lockstep: each cell gets every attribute.
COPY scene FROM (
  's3://usgs-landsat/collection02/level-2/.../LC09_..._SR_B4.TIF',
  's3://usgs-landsat/collection02/level-2/.../LC09_..._SR_B5.TIF',
  's3://usgs-landsat/collection02/level-2/.../LC09_..._QA_PIXEL.TIF'
) WITH (FORMAT geotiff, BANDS (red, nir, qa), CREDENTIAL landsat, REQUESTER_PAYS true,
        TIME '2024-03-15T03:12:00Z');

-- An HTTPS source behind Earthdata Login.
COPY scene FROM 'https://data.lpdaac.earthdatacloud.nasa.gov/.../HLS.S30....B04.tif'
  WITH (FORMAT geotiff, CREDENTIAL earthdata);
```

A source is one of:

| Source                 | Read with |
| ---------------------- | --------- |
| `'/abs/path.tif'`      | Positional reads of a server file. The path must be absolute and hold no `..`. |
| `'https://host/a.tif'` | HTTP range requests through the egress policy. |
| `'s3://bucket/key'`    | Ranged S3 `GET`s through the egress policy. |

- A list `('<a>', '<b>', ...)` loads one raster per band group. The bands of the load are every source's bands in source order. `BANDS` maps them in that order.
- Every source of one COPY shares one image size, geotransform, CRS and block layout. A mismatch is refused with SQLSTATE `22023` naming the source and what differs.
- The load reads the same block index from every source, decodes them, and writes one cell per pixel with every attribute. A cell write carries the full attribute vector, so loading band files one COPY at a time overwrites earlier attributes. Use one COPY with a source list instead.
- A remote answer must be `206 Partial Content` with a `Content-Range` that names the same total length every time. The first strong `ETag` is pinned and sent as `If-Match` on every later request. A source that changes mid-load fails with `55000`. A server that ignores `Range` (`200`) is refused with `0A000`.
- An S3 object pins its size and ETag the same way. Without a credential the bucket is read unsigned, in the region AWS reports for it.
- Text formats read one server file. An HTTP or S3 source and a source list need `FORMAT geotiff` or a `.tif` / `.tiff` path.

### Raster Options

| Option                       | Meaning |
| ---------------------------- | ------- |
| `BANDS (a, SKIP, b)`         | Where each band of the load goes. Without it, band `i` loads into attribute `i`. |
| `NODATA <number>`            | The nodata of every band, in place of the file's. |
| `TIME '<iso>'`               | The instant the one `TIMESTAMP` dim takes for every cell. |
| `CREDENTIAL <name>`          | The credential remote sources authenticate with. |
| `REQUESTER_PAYS true`        | An S3 read bills the credential's account. |
| `OVERVIEW <n>`               | Read image `n`: 0 is full resolution, 1 the first overview. The geotransform scales with the overview, so the array's GEOREF must be on the overview grid. |
| `WINDOW (x0, y0, x1, y1)`    | Load the pixel columns `x0..x1` and rows `y0..y1` only (half-open). The load fetches only the blocks the window touches. The window's footprint alone must fit the dim domains. |
| `ADOPT_METADATA true`        | An attribute with no metadata takes the band's nodata, scale, offset and units from the file. Default `false`: a load never changes the schema unasked. |

- A band loads only into an attribute that holds every value of its sample type exactly (`42804` otherwise).
- A pixel the file masks out (an internal mask, or the band's own LERC mask) holds no data in that band. A pixel where no loaded band holds data is skipped. A band with no data in a kept pixel writes `NULL`, or is refused with `23502` when the attribute is `NOT NULL`.
- `ADOPT_METADATA` changes the schema, so it runs outside a transaction block only (`25001`). An attribute with any metadata keeps all of it.

### Credentials

```sql
CREATE CREDENTIAL landsat (TYPE 's3', ACCESS_KEY_ID 'AKIA...', SECRET_ACCESS_KEY '...',
                           REGION 'us-west-2', SESSION_TOKEN '...');
CREATE CREDENTIAL minio   (TYPE 's3', ACCESS_KEY_ID '...', SECRET_ACCESS_KEY '...',
                           REGION 'us-east-1', ENDPOINT 'https://minio.internal:9000');
CREATE CREDENTIAL earthdata (TYPE 'bearer', TOKEN 'eyJ...');
CREATE CREDENTIAL blob      (TYPE 'sas', TOKEN 'sv=2022-11-02&sig=...');
SHOW CREDENTIALS;
DROP CREDENTIAL [IF EXISTS] earthdata;
```

- Every credential statement needs a superuser (`42501`). A credential belongs to the tenant that created it.
- `TYPE 'bearer'` sends `Authorization: Bearer <token>`. `TYPE 'sas'` appends the token to the URL query. `TYPE 's3'` signs S3 requests.
- The secret is sealed with AES-256-GCM before the statement proposes it, so the metadata log, the catalog and every snapshot hold ciphertext only. The key is the `[credential_encryption]` key file. Every node of a cluster needs the same file.
- `SHOW CREDENTIALS` lists the name, type, owner and creation time. No statement, error message or audit record shows a secret.
- Without `[credential_encryption]`, `CREATE CREDENTIAL` and `COPY ... CREDENTIAL` are refused with `55000`.

### Egress Policy

One `[egress]` policy governs every outbound request: remote sources, JWKS fetches, change-stream and alert webhooks, SIEM export, OTLP trace and metrics export, and the Vault key provider. A request reaches only what the policy allows:

- `https://` only. `http://` needs the host on the allow-list.
- No IP-literal host, at any hop.
- Every address a host resolves to must be global unicast. An IPv6 address that embeds an IPv4 address (IPv4-mapped, IPv4-compatible, NAT64 `64:ff9b::/96`, 6to4 `2002::/16`) passes only when that IPv4 address does. Teredo, site-local, discard-only and NAT64 local-use addresses never pass. A host on the allow-list may also resolve into `allow_cidrs`.
- Every redirect hop is checked, and every resolved address is checked at connect time, so a rebinding DNS answer never reaches a private address.
- The `ENDPOINT` of an S3 credential passes the policy at `CREATE CREDENTIAL` and again at every `COPY`.

```toml
[egress]
allow_http  = false
allow_hosts = ["mirror.internal"]
allow_cidrs = ["10.42.0.0/16"]

[credential_encryption]
# 32 bytes, mode 0600, owned by the server's user:
#   head -c 32 /dev/urandom > /etc/nodedb/keys/credential.key
key_path = "/etc/nodedb/keys/credential.key"
```

A refused URL or address is `42501`.

### Memory and Planes

At most 8 range fetches run at once, and close ranges merge into one request. Decoded block sets queue two deep between the reader and the load. Network reads run on the Control Plane, decoding on its blocking pool. The Data Plane receives only the array writes.

### Raster Load Errors

| SQLSTATE | Cause |
| -------- | ----- |
| `22023`  | A CRS, grid, block layout or window mismatch, or a nodata value the sample type cannot hold. |
| `22003`  | A footprint or `TIME` outside a dim domain. |
| `22P04`  | A malformed or corrupt file, or a malformed HTTP answer. |
| `0A000`  | A file feature the reader lacks, a missing overview, or a server that ignores `Range`. |
| `23502`  | No data for a `NOT NULL` attribute. |
| `28000`  | The remote source refused the credentials (HTTP 401 or 403). |
| `42501`  | The egress policy refused a URL or address. |
| `42704`  | An unknown credential. |
| `42804`  | A band type the attribute cannot hold exactly. |
| `54000`  | A block or header over the decode cap. |
| `55000`  | No GEOREF, no credential key, or a source that changed mid-load. |
| `58030`  | A source that cannot be read or does not exist. |
| `08006`  | A connection to a remote source failed. |

## Bitemporal Support

Arrays support dual timestamping:

- **System Time** — When the cell value was written (audit trail, compliance)
- **Valid Time** — When the cell represents (temporal semantics, forecasts, corrections)

Query as of either or both:

```sql
-- Read cells as they existed at a point in the past
SELECT * FROM ARRAY_SLICE(
  'data',
  '{x: [0, 99], y: [0, 99]}',
  ['value']
)
AS OF SYSTEM TIME 1700000000000;

-- Read cells that were valid at a specific time
SELECT * FROM ARRAY_SLICE(
  'forecast',
  '{x: [0, 99], y: [0, 99]}',
  ['temp']
)
AS OF VALID TIME 1700000000000;

-- Read cells that were valid AND existed at a specific time
SELECT * FROM ARRAY_SLICE(
  'forecast',
  '{x: [0, 99], y: [0, 99]}',
  ['temp']
)
AS OF SYSTEM TIME 1700000000000 AS OF VALID TIME 1700000001000;
```

A time is an integer in milliseconds since the Unix epoch, `NOW()`, or an ISO-8601 string such as `'2024-01-15T00:00:00Z'`.

## Cross-Engine Integration

Array cells are addressable via surrogate identity alongside other engines. An `ARRAY_SLICE` or `ARRAY_PROJECT` call can be a `JOIN` source. A slice row holds one field per dimension and attribute, keyed by name, so a join key reads them directly. A slice row has no `id` column.

```sql
-- Join slice cells to a collection on a dimension value
SELECT c.x, c.y, c.attr1, s.name
FROM ARRAY_SLICE('spatial_data', '{x: [0, 999], y: [0, 999]}', ['attr1']) AS c
JOIN sites s ON c.x = s.grid_x;
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
ALTER ARRAY spatial_grid SET (audit_retain_ms = 86400000);  -- keep 1 day

-- Tiles older than now - 1 day are candidates for purge
-- Purge is automatic during compaction
```

Purged tiles are irreversibly removed; historical queries beyond the retention window will see gaps.

## Related

- [Bitemporal](bitemporal.md) — Cross-engine bitemporal architecture
- [Architecture — Cross-engine identity](architecture.md#cross-engine-identity) — Surrogate bitmap linking
- [Columnar](columnar.md) — Related structured analytics engine

[Back to docs](README.md)
