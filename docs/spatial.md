# Spatial Engine

NodeDB's spatial engine provides native geospatial indexing and querying — R\*-tree indexes, OGC predicates, geohash and H3 hexagonal indexing, and hybrid spatial-vector search. It runs identically across Origin, Lite, and WASM.

Spatial is a **columnar profile**. Collections with a `SPATIAL_INDEX` column modifier store data in the same `columnar_memtables` as plain columnar collections. The R\*-tree is maintained as a secondary index over the geometry column; full scans (no spatial predicate) read directly from the columnar memtable.

## When to Use

- Fleet tracking and logistics (GPS + timeseries)
- Location-based search and recommendations
- Geofencing and proximity alerts
- Map-based applications
- Any workload combining location with other data types

## Key Features

- **R\*-tree index** — Bulk loading, nearest neighbor, range queries
- **Geohash** — Encode/decode, neighbor cells, area covering
- **H3 hexagonal index** — Uber's H3 via h3o for uniform-area spatial binning
- **OGC predicates** — `ST_Contains`, `ST_Intersects`, `ST_Within`, `ST_DWithin`, `ST_Distance`, `ST_Intersection`, `ST_Buffer`, `ST_Envelope`, `ST_Union`
- **Format support** — WKB, WKT, GeoJSON interchange. GeoParquet v1.1.0 + GeoArrow metadata.
- **Hybrid spatial-vector** — Spatial R\*-tree narrows candidates by location, then HNSW ranks by semantic similarity in one query
- **Spatial join** — R\*-tree probe join between two collections
- **Distributed** — Scatter-gather across shards, shard routing by geometry, geofencing

## Examples

```sql
-- Create a columnar collection with an automatic R*-tree on the geometry column
CREATE COLLECTION restaurants (
    location GEOMETRY SPATIAL_INDEX,
    name VARCHAR,
    cuisine VARCHAR,
    rating FLOAT
) WITH (storage = 'columnar');

-- Alternatively, add a spatial index to a document collection
CREATE COLLECTION restaurants TYPE document;
CREATE SPATIAL INDEX ON restaurants FIELDS location;

-- Insert with GeoJSON geometry
INSERT INTO restaurants {
    name: 'Sushi Place',
    location: { type: 'Point', coordinates: [-73.985, 40.748] },
    cuisine: 'japanese',
    rating: 4.5
};

-- Find restaurants within 1km
SELECT name, rating, ST_Distance(location, ST_Point(-73.990, 40.750)) AS dist_m
FROM restaurants
WHERE ST_DWithin(location, ST_Point(-73.990, 40.750), 1000)
ORDER BY dist_m;

-- Geofencing: find all points within a polygon
SELECT name FROM restaurants
WHERE ST_Within(location, ST_GeomFromGeoJSON('{
    "type": "Polygon",
    "coordinates": [[[-74.0, 40.7], [-73.9, 40.7], [-73.9, 40.8], [-74.0, 40.8], [-74.0, 40.7]]]
}'));

-- Hybrid spatial-vector: nearby AND semantically similar
SELECT name, vector_distance() AS similarity
FROM restaurants
WHERE ST_DWithin(location, ST_Point(-73.990, 40.750), 2000)
  AND embedding <-> $query_vec
LIMIT 10;

-- H3 hexagonal binning
SELECT h3_to_string(h3_lat_lng_to_cell(40.748, -73.985, 9)) AS hex;

-- Spatial join
SELECT r.name, z.zone_name
FROM restaurants r, delivery_zones z
WHERE ST_Contains(z.boundary, r.location);
```

## Spatial as a Columnar Profile

The `SPATIAL_INDEX` column modifier designates a geometry column for automatic R\*-tree indexing. Use the unified `CREATE COLLECTION` DDL:

```sql
-- Spatial columnar collection
CREATE COLLECTION locations (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
) WITH (storage = 'columnar');

-- Combine with TIME_KEY for fleet tracking or IoT
CREATE COLLECTION fleet_positions (
    ts TIMESTAMP TIME_KEY,
    vehicle_id VARCHAR,
    position GEOMETRY SPATIAL_INDEX,
    speed FLOAT
) WITH (storage = 'columnar', profile = 'timeseries', partition_by = '1d');
```

**Query execution model:**

- `SELECT * FROM locations` — full scan reads from the columnar memtable directly (no R\*-tree involved)
- `SELECT * FROM locations WHERE ST_DWithin(geom, ...)` — R\*-tree narrows the candidate set, then the columnar sparse index and predicate pushdown do the final refinement

```sql
-- Bare scan: columnar memtable read, all columns
SELECT name FROM locations WHERE name LIKE 'Park%';

-- Spatial predicate: R*-tree lookup -> sparse refinement -> result
SELECT name, ST_Distance(geom, ST_Point(-73.98, 40.75)) AS dist
FROM locations
WHERE ST_DWithin(geom, ST_Point(-73.98, 40.75), 500)
ORDER BY dist;
```

## Related

- [Vector Search](vectors.md) — Hybrid spatial-vector queries
- [Timeseries](timeseries.md) — Combine location with time (fleet tracking)
- [Columnar](columnar.md) — Spatial is a columnar profile

[Back to docs](README.md)
