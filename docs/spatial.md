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
- **OGC predicates** — `ST_Contains`, `ST_Intersects`, `ST_Within`, `ST_Disjoint`, `ST_DWithin`, `ST_Touches`, `ST_Crosses`, `ST_Overlaps`, `ST_Equals`, `ST_Covers`, `ST_CoveredBy`, `ST_Relate`, `ST_3DDWithin`, `ST_3DIntersects`
- **Z and M ordinates** — every geometry is XY, XYZ, XYM or XYZM. Every storage form and encoding keeps Z and M.
- **Geodesic measures** — distance, length, perimeter and area on the CRS's own ellipsoid (Karney's algorithms)
- **Overlay and shape operations** — `ST_Buffer`, `ST_Union`, `ST_Intersection`, `ST_Difference`, `ST_SymDifference`, `ST_ConvexHull`, `ST_ConcaveHull`, `ST_Simplify`, `ST_MakeValid`, `ST_Split`, `ST_Subdivide`
- **Linear referencing** — `ST_LineInterpolatePoint`, `ST_LocateAlong`, `ST_LocateBetween`, `ST_InterpolatePoint`
- **Format support** — WKT, EWKT, ISO WKB, EWKB and GeoJSON interchange. GeoParquet v1.1.0 and GeoArrow metadata builders in the `nodedb-spatial` crate.
- **Hybrid spatial-vector** — Spatial R\*-tree narrows candidates by location, then HNSW ranks by semantic similarity in one query
- **Spatial join** — R\*-tree probe join between two collections
- **Distributed** — Scatter-gather across shards, shard routing by geometry, geofencing

## Examples

```sql
-- Spatial peer engine with automatic R*-tree on the geometry column
CREATE COLLECTION restaurants (
    location GEOMETRY SPATIAL_INDEX,
    name VARCHAR,
    cuisine VARCHAR,
    rating FLOAT
) WITH (engine='spatial');

-- Alternatively, add a spatial index to a document collection
CREATE COLLECTION restaurants;
CREATE SPATIAL INDEX ON restaurants FIELDS location;

-- Insert with GeoJSON geometry
INSERT INTO restaurants {
    name: 'Sushi Place',
    location: { type: 'Point', coordinates: [-73.985, 40.748] },
    cuisine: 'japanese',
    rating: 4.5
};

-- Insert with geometry constructors (WKT, WKB, or coordinates)
INSERT INTO restaurants (name, location) VALUES
    ('Taco Stand', ST_MakePoint(-73.982, 40.742)),
    ('Pho House',  ST_GeomFromText('POINT(-73.978 40.751)')),
    ('Deli',       ST_GeomFromWKB(X'01010000000000000000000040000000000000F03F'));

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
SELECT name, vector_distance(embedding, $query_vec) AS similarity
FROM restaurants
WHERE ST_DWithin(location, ST_Point(-73.990, 40.750), 2000)
  AND embedding <-> $query_vec
LIMIT 10;

-- H3 hexagonal binning
SELECT h3_latlngtocell(40.748, -73.985, 9) AS hex;

-- Spatial join
SELECT r.name, z.zone_name
FROM restaurants r, delivery_zones z
WHERE ST_Contains(z.boundary, r.location);
```

## Geometry Functions

Every geometry function works in every position a geometry expression may
appear: an `INSERT` value, a `SELECT` projection, and a spatial predicate's
query-geometry argument. Constructors nest, so `ST_Buffer(ST_Point(...), 500)`
is a valid search area.

A string literal in geometry position is read as either WKT or GeoJSON, so
`ST_DWithin(loc, 'POINT(1 2)', 500)` and
`ST_DWithin(loc, '{"type":"Point","coordinates":[1,2]}', 500)` are equivalent.

### Constructors

| Function | Result |
| --- | --- |
| `ST_Point(lng, lat)` | Point |
| `ST_MakePoint(x, y [, z [, m]])` | Point: XY, XYZ or XYZM |
| `ST_MakePointM(x, y, m)` | XYM Point |
| `ST_GeomFromText(text [, srid])` / `ST_GeomFromEWKT` | Geometry parsed from WKT or EWKT |
| `ST_GeomFromGeoJSON(json)` | Geometry parsed from GeoJSON |
| `ST_GeomFromWKB(bytes [, srid])` / `ST_GeomFromEWKB` | Geometry parsed from ISO WKB or EWKB; accepts an `X'...'` literal or its hex string |
| `ST_MakeLine(point, point, ...)` | LineString in the points' SRID and dimension |
| `ST_MakePolygon(ring, ...)` | Polygon, each ring an array of `[lng, lat]` pairs |
| `ST_MakeEnvelope(min_lng, min_lat, max_lng, max_lat)` | Rectangular Polygon |
| `ST_TileEnvelope(z, x, y)` | Web Mercator (`3857`) Polygon of one XYZ map tile |

A parse error fails the statement with SQLSTATE `22000`. The error names the
function and the byte offset of the error. A malformed geometry is never read
as NULL.

### Accessors

| Function | Result |
| --- | --- |
| `ST_AsText(geom)` | WKT rendering, Z and M included |
| `ST_AsEWKT(geom)` | `SRID=<code>;` then WKT |
| `ST_AsGeoJSON(geom)` | GeoJSON rendering, Z included, M dropped |
| `ST_AsBinary(geom [, 'NDR'\|'XDR'])` | ISO WKB bytes, little-endian unless `'XDR'` |
| `ST_AsEWKB(geom [, 'NDR'\|'XDR'])` | EWKB bytes with the SRID |
| `ST_X(geom)` / `ST_Y(geom)` / `ST_Z(geom)` / `ST_M(geom)` | Ordinate of a Point; NULL for any other geometry, an EMPTY point, or a missing Z or M |
| `ST_ZMin(geom)` / `ST_ZMax(geom)` | Least / greatest Z; `0` for a geometry without Z |
| `ST_CoordDim(geom)` / `ST_NDims(geom)` | Ordinates per position: 2, 3 or 4 |
| `ST_ZMFlag(geom)` | `0` XY, `1` XYM, `2` XYZ, `3` XYZM |
| `ST_HasZ(geom)` / `ST_HasM(geom)` | Whether the geometry carries Z / M |
| `ST_GeometryType(geom)` | Type name, e.g. `Point` |
| `ST_NPoints(geom)` | Total vertex count; `0` for EMPTY |
| `ST_IsEmpty(geom)` | Whether the geometry is EMPTY |
| `ST_SRID(geom)` | SRID of the geometry; `4326` unless the geometry carries another SRID. See [Spatial reference systems](#spatial-reference-systems) |
| `ST_IsValid(geom)` | Whether the geometry is valid. See [Validity](#validity) |
| `ST_IsValidReason(geom)` | `Valid Geometry`, or every reason the geometry is invalid |

### Z and M ordinates

A geometry is XY, XYZ, XYM or XYZM. Every position in one geometry has the
same dimension. Z is a height and M is a measure. Two-dimensional predicates
and measures read X and Y only.

| Input | Z and M |
| --- | --- |
| WKT | `POINT Z (1 2 3)`, `POINT M (1 2 4)`, `POINT ZM (1 2 3 4)`. Untagged text with three numbers is XYZ, four is XYZM. |
| WKB / EWKB | ISO type codes 1000 (Z), 2000 (M), 3000 (ZM), and the EWKB Z, M and SRID flags |
| GeoJSON | A third position member is Z. GeoJSON has no M. |

| Function | Result |
| --- | --- |
| `ST_Force2D(geom)` | Geometry without Z and M |
| `ST_Force3D(geom [, z])` / `ST_Force3DZ` | XYZ geometry; a missing Z becomes `z`, default `0`; M is dropped |
| `ST_Force3DM(geom [, m])` | XYM geometry; a missing M becomes `m`, default `0`; Z is dropped |
| `ST_Force4D(geom [, z [, m]])` | XYZM geometry; missing ordinates default to `0` |

### EMPTY geometries

Every geometry kind has an EMPTY form: `POINT EMPTY`, `LINESTRING EMPTY`,
`POLYGON EMPTY`, the three multi kinds, and `GEOMETRYCOLLECTION EMPTY`. WKB
writes an EMPTY point with NaN ordinates, as PostGIS does. GeoJSON writes an
empty `coordinates` array.

- An EMPTY geometry is stored, and never enters the R\*-tree.
- An EMPTY operand matches no predicate.
- `ST_Distance` with an EMPTY operand is infinite.
- `ST_Area`, `ST_Length` and `ST_Perimeter` of EMPTY are `0`.

### Storage

| Engine | Stored form |
| --- | --- |
| Document (schemaless) | Native geometry value: SRID, shape, Z and M |
| Document (strict), Columnar, Timeseries, Spatial | EWKB |

A geometry read back renders the same WKT it was written with.

### Spatial reference systems

Every geometry carries an SRID. The default is `4326` (WGS 84, longitude and latitude in degrees). GeoJSON reads and writes a `crs` member for any SRID other than `4326`.

NodeDB defines every geographic 2D and projected CRS of the EPSG dataset v12.029: 611 geographic and 5,305 projected CRSs. The SRID is the EPSG code. NodeDB adds two CRSs of its own:

| SRID | System | Unit |
| --- | --- | --- |
| `990001` | MODIS sinusoidal (`SR-ORG:6842`) on a sphere of radius 6,371,007.181 m | metres |
| `990002` | Himawari geostationary view, sub-satellite longitude 140.7°E, on WGS 84 | metres |

The EPSG CRSs of these methods are not defined. Their SRIDs are refused:

| Method | CRSs | Example SRIDs |
| --- | --- | --- |
| Colombia Urban | 32 | `6244`, `6245` |
| Bonne (South Orientated) | 2 | `2963`, `5017` |
| Transverse Mercator Zoned Grid System | 2 | `32600`, `32700` |
| Lambert Azimuthal Equal Area (Spherical) | 1 | `9311` |
| Transverse Mercator 3D | 1 | `9895` |
| Local Orthographic | 1 | `10622` |
| Tunisia Mining Grid | 1 | `22300` |
| Lambert Conic Near-Conformal | 1 | `22700` |

NodeDB also defines EPSG CRSs published after v12.029:

| SRID | System | Source |
| --- | --- | --- |
| `12287` | GDM2020, Geocentric Datum of Malaysia 2020 (EPSG datum `1471`), GRS 1980, aligned with ITRF2014 at epoch 2020.0 | EPSG change 2026.081 |
| `25314` to `25322` | GDM2020 state Cassini-Soldner grids: Johor, Sembilan and Melaka, Pahang, Selangor, Terengganu, Pinang, Kedah and Perlis, Perak, Kelantan | EPSG change 2026.081, JUPEM circular KPUP 3/2021 Table 7 |

GDM2020 relates to WGS 84 through EPSG transformation `12291`: a null shift with an accuracy of 1 m. These GDM2020 definitions are not available:

- GDM2000 to GDM2020. JUPEM defines it through velocity and post-seismic deformation grids that it does not publish openly.
- GDM2020 Peninsular RSO and East Malaysia BRSO. EPSG has no codes for them, and JUPEM does not publish their parameters openly.

A transform between two datums runs through WGS 84 with the datum shift EPSG records for each CRS. Two CRSs on one datum skip the shift. A GeoJSON `crs` name takes the forms `EPSG:n`, `urn:ogc:def:crs:EPSG::n`, `http://www.opengis.net/def/crs/EPSG/0/n`, the OGC CRS84 URN or URL, and `SR-ORG:6842`.

| Function | Result |
| --- | --- |
| `ST_SRID(geom)` | SRID of the geometry |
| `ST_SetSRID(geom, srid)` | Geometry relabeled with `srid`. Coordinates stay as they are. |
| `ST_Transform(geom, srid [, epoch])` | Geometry with every coordinate reprojected into `srid`. `epoch` is the coordinate epoch as a decimal year. |

`ST_GeomFromText` and `ST_GeomFromWKB` take an optional `srid` argument. `ST_SetSRID` and `ST_Transform` return NULL when `srid` is NULL. `ST_Transform` returns NULL when `epoch` is NULL.

```sql
SELECT ST_Transform(ST_Point(101.7117, 3.1579), 32647);
-- 910001: a user-defined CRS with a time-dependent shift to WGS 84.
SELECT ST_Transform(ST_SetSRID(ST_Point(128.0, -16.0), 910001), 4326, 2013.9);
```

#### Time-dependent datum shifts

A datum shift is a 3-, 7- or 15-parameter Helmert transformation. The 15-parameter form adds a yearly rate for each of the seven parameters and a parameter reference epoch t0 (IOGP Guidance Note 7-2 §4.2.5, EPSG methods 1053 to 1058). At coordinate epoch t each parameter p becomes p + rate × (t − t0).

- `epoch` must lie in 1900..=2200. Any other value raises a data exception with SQLSTATE `22000`.
- Without `epoch`, each shift runs at its own reference epoch, where only the seven static parameters act.
- A static shift gives the same result at every epoch.

A user-defined CRS (`CREATE SPATIAL_REF_SYS`) carries a time-dependent shift in WKT2 or PROJJSON: a `BOUNDCRS` / `BoundCRS` to WGS 84 whose transformation uses a time-dependent Helmert method with the rate parameters (EPSG 1040 to 1046) and the parameter reference epoch (EPSG 1047). A PROJ string cannot carry rates, so `nodedb.spatial_ref_sys` shows `proj4text` as NULL for such a CRS, as PostGIS does for a CRS PROJ strings cannot express. Its `srtext` holds the full WKT2.

Geometry functions read the SRID of each operand. They never transform implicitly. These cases raise a data exception with SQLSTATE `22000`:

- Two operands in different SRIDs. The error reads `operands have different SRIDs <left> and <right>; use ST_Transform`.
- An SRID that names no defined CRS. The error reads `SRID <code> is not defined`, or `SRID <code> is outside 1..=998999` for a code out of range.
- A coordinate outside the region the target projection is defined on.
- An `ST_Transform` epoch outside 1900..=2200, or not a finite number.

Call `ST_Transform` on one operand to bring both into the same SRID.

A spatial index fixes its SRID at its first entry. It refuses geometries in any other SRID.

### Measures

Every measure is in meters, and square meters for area. A geometry in a geographic CRS, such as `4326`, is measured on geodesics of that CRS's ellipsoid, with Karney's algorithms (GeographicLib). A geometry in a projected SRID is measured in planar grid meters, the CRS unit converted to meters. Two-geometry measures need both operands in the same SRID.

| Function | Result |
| --- | --- |
| `ST_Distance(a, b)` | Minimum distance between two geometries |
| `ST_Length(geom)` | Total length of linear components; `0` for points and polygons |
| `ST_Perimeter(geom)` | Boundary length of areal components, holes included |
| `ST_Area(geom)` | Area of areal components, less holes; `0` for points and lines |
| `ST_3DDistance(a, b)` | Minimum 3D distance |
| `ST_3DLength(geom)` | Total 3D length of linear components |

3D measures read Z as meters, and a missing Z as `0`. A projected CRS
measures Euclidean distance in grid meters. A geographic CRS maps every
position to Earth-centred (ECEF) coordinates on its ellipsoid, with Z as
ellipsoidal height, and measures the straight chord.

### Predicates and operations

| Function | Result |
| --- | --- |
| `ST_Contains(a, b)` | Whether `a` contains `b` |
| `ST_Within(a, b)` | Whether `a` lies within `b` — the converse of `ST_Contains` |
| `ST_Intersects(a, b)` / `ST_Disjoint(a, b)` | Whether the two geometries share any point |
| `ST_Touches(a, b)` | Whether the boundaries meet and the interiors do not |
| `ST_Crosses(a, b)` | Whether the interiors meet in a lower dimension |
| `ST_Overlaps(a, b)` | Whether two geometries of one dimension share part of their interiors |
| `ST_Equals(a, b)` | Whether the two geometries cover the same points |
| `ST_Covers(a, b)` / `ST_CoveredBy(a, b)` | Whether no point of `b` lies outside `a` / the converse |
| `ST_Relate(a, b)` | DE-9IM intersection matrix, e.g. `212101212` |
| `ST_Relate(a, b, pattern)` | Whether the matrix matches a DE-9IM pattern of `T`, `F`, `*`, `0`, `1`, `2` |
| `ST_DWithin(a, b, meters)` | Whether the two geometries lie within a distance: geodesic in a geographic CRS, planar in a projected one |
| `ST_3DDWithin(a, b, meters)` | Whether the two geometries lie within a 3D distance |
| `ST_3DIntersects(a, b)` | Whether the two geometries meet in 3D |
| `ST_Buffer(geom, meters [, segments])` | Geometry grown by a distance. See [Buffer](#buffer) |
| `ST_Envelope(geom)` | Bounding rectangle; a point or a line for a degenerate box |
| `ST_Centroid(geom)` | Centroid, weighted by the highest dimension present: geodesic area or length in a geographic CRS |
| `ST_Union(a, b)` | Every point of either geometry, overlaps dissolved |
| `ST_Intersection(a, b)` | Points shared by both geometries |
| `ST_Difference(a, b)` | Points of `a` outside `b` |
| `ST_SymDifference(a, b)` | Points in exactly one geometry |
| `ST_ConvexHull(geom)` | Smallest convex polygon holding every vertex |
| `ST_ConcaveHull(geom, ratio)` | Concave polygon holding every vertex; `ratio` 1 gives the convex hull |
| `ST_Simplify(geom, tolerance)` | Ramer–Douglas–Peucker simplification |
| `ST_SimplifyPreserveTopology(geom, tolerance)` | Visvalingam–Whyatt simplification that creates no self-intersection |
| `ST_MakeValid(geom)` | Valid geometry covering the same area |
| `ST_Subdivide(geom [, max_vertices])` | GEOMETRYCOLLECTION of pieces with at most `max_vertices` vertices each, default 256 |
| `ST_Segmentize(geom, max_length)` | Geometry with extra vertices so no edge exceeds `max_length` meters |
| `ST_ClosestPoint(a, b)` | Point of `a` closest to `b` |
| `ST_Split(input, blade)` | GEOMETRYCOLLECTION of the pieces `blade` cuts `input` into |
| `ST_ShiftLongitude(geom)` | Longitudes moved between the -180..180 and 0..360 ranges |

The overlay operations (`ST_Union`, `ST_Intersection`, `ST_Difference`,
`ST_SymDifference`) accept every geometry kind. A mixed result is a
GEOMETRYCOLLECTION. The overlay, simplification, hulls, `ST_Split` and
`ST_Subdivide` work in the coordinates of the geometry's CRS. `tolerance`
is in CRS units.

`ST_Contains`, `ST_Within`, `ST_Intersects`, `ST_DWithin` and `ST_3DDWithin`
are answered from the R\*-tree when the first argument is an indexed geometry
column; the rest evaluate per row. The distance predicates widen the search
box to cover every point within the distance, on the smallest radius of
curvature of the ellipsoid.

### Buffer

`ST_Buffer` takes a distance in meters in every CRS. A negative distance
shrinks polygons and erases points and lines. Curves have `segments` sides
per full circle, default 32.

- A projected geometry buffers in its grid.
- A geographic geometry within 12° of longitude and away from the poles buffers in a Transverse Mercator projection centred on it, on its own ellipsoid. The scale error stays under 0.6 % at its edge.
- A geographic geometry past 80° latitude buffers in a polar Lambert azimuthal equal-area projection.
- Any other geographic geometry buffers each component in its own local projection, and unions the pieces.

### Validity

`ST_IsValid` follows the OGC simple-features rules:

- Every coordinate is finite.
- A LineString has at least 2 points.
- A ring has at least 4 points and is closed.
- A ring does not cross itself.
- A hole lies inside its shell, and rings do not overlap or share a line.
- The polygons of a MultiPolygon do not overlap or share a line.

Ring winding order is not a validity rule. `ST_IsValidReason` names the
component and ring of each error, e.g. `polygon 1 shell: ring crosses
itself`.

### Linear referencing

| Function | Result |
| --- | --- |
| `ST_LineInterpolatePoint(line, fraction)` | Point at `fraction` (0 to 1) of the line's length |
| `ST_LocateAlong(geom, m [, offset])` | MultiPoint of every position with measure `m`, moved `offset` CRS units to the left |
| `ST_LocateBetween(geom, from_m, to_m [, offset])` | Parts of the geometry whose measure lies between `from_m` and `to_m`, moved `offset` CRS units to the left |
| `ST_InterpolatePoint(line, point)` | M of the line at the position closest to `point` |

`ST_LocateAlong`, `ST_LocateBetween` and `ST_InterpolatePoint` need a
geometry with M.

```sql
-- Read stored geometry back in either interchange format
SELECT name, ST_AsText(location), ST_X(location), ST_Y(location)
FROM restaurants;

-- Measure and describe
SELECT name, ST_GeometryType(boundary), ST_Area(boundary), ST_Perimeter(boundary)
FROM delivery_zones;

-- A buffered point is a valid query geometry
SELECT name FROM restaurants
WHERE ST_Within(location, ST_Buffer(ST_Point(-73.990, 40.750), 1000));
```

## Spatial as a Peer Engine

The `SPATIAL_INDEX` column modifier designates a geometry column for automatic R\*-tree indexing. Spatial is a peer engine alongside columnar, timeseries, and the rest:

```sql
-- Spatial peer engine
CREATE COLLECTION locations (
    geom GEOMETRY SPATIAL_INDEX,
    name VARCHAR
) WITH (engine='spatial');

-- Combine with TIME_KEY for fleet tracking or IoT (timeseries engine + spatial column)
CREATE COLLECTION fleet_positions (
    ts TIMESTAMP TIME_KEY,
    vehicle_id VARCHAR,
    position GEOMETRY SPATIAL_INDEX,
    speed FLOAT
) WITH (engine='timeseries', partition_by='1d');
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

## Temporal Spatial Queries

The Spatial engine is an index. It points at records; it does not carry temporal columns itself. To query geometry at a point in time, attach a Spatial index to a Document collection that has `bitemporal = true`. The Document collection holds the geometry column and temporal columns; the R\*-tree narrows candidates by location; `AS OF` filtering happens at the Document layer.

See [Bitemporal Queries — Bitemporal Spatial Queries](bitemporal.md#bitemporal-spatial-queries) for a worked example.

## Related

- [Vector Search](vectors.md) — Hybrid spatial-vector queries
- [Timeseries](timeseries.md) — Combine location with time (fleet tracking)
- [Columnar](columnar.md) — Spatial is a columnar profile
- [Bitemporal Queries](bitemporal.md) — Temporal composition pattern

[Back to docs](README.md)
