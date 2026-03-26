//! Well-Known Text (WKT) serialization and parsing for Geometry types.
//!
//! Standard interchange format used by PostGIS, ArcadeDB (Spatial4J),
//! and most GIS tools. Format examples:
//! - `POINT(lng lat)`
//! - `LINESTRING(lng1 lat1, lng2 lat2)`
//! - `POLYGON((lng1 lat1, lng2 lat2, ...), (hole1...))`
//! - `MULTIPOINT((lng1 lat1), (lng2 lat2))`
//! - `GEOMETRYCOLLECTION(POINT(...), LINESTRING(...))`

use nodedb_types::geometry::Geometry;

/// Serialize a Geometry to WKT string.
pub fn geometry_to_wkt(geom: &Geometry) -> String {
    match geom {
        Geometry::Point { coordinates } => {
            format!("POINT({} {})", coordinates[0], coordinates[1])
        }
        Geometry::LineString { coordinates } => {
            format!("LINESTRING({})", coords_to_wkt(coordinates))
        }
        Geometry::Polygon { coordinates } => {
            let rings: Vec<String> = coordinates
                .iter()
                .map(|ring| format!("({})", coords_to_wkt(ring)))
                .collect();
            format!("POLYGON({})", rings.join(", "))
        }
        Geometry::MultiPoint { coordinates } => {
            let pts: Vec<String> = coordinates
                .iter()
                .map(|c| format!("({} {})", c[0], c[1]))
                .collect();
            format!("MULTIPOINT({})", pts.join(", "))
        }
        Geometry::MultiLineString { coordinates } => {
            let lines: Vec<String> = coordinates
                .iter()
                .map(|ls| format!("({})", coords_to_wkt(ls)))
                .collect();
            format!("MULTILINESTRING({})", lines.join(", "))
        }
        Geometry::MultiPolygon { coordinates } => {
            let polys: Vec<String> = coordinates
                .iter()
                .map(|poly| {
                    let rings: Vec<String> = poly
                        .iter()
                        .map(|ring| format!("({})", coords_to_wkt(ring)))
                        .collect();
                    format!("({})", rings.join(", "))
                })
                .collect();
            format!("MULTIPOLYGON({})", polys.join(", "))
        }
        Geometry::GeometryCollection { geometries } => {
            let geoms: Vec<String> = geometries.iter().map(geometry_to_wkt).collect();
            format!("GEOMETRYCOLLECTION({})", geoms.join(", "))
        }
    }
}

/// Parse a WKT string into a Geometry.
///
/// Returns `None` if the input is malformed.
pub fn geometry_from_wkt(input: &str) -> Option<Geometry> {
    let s = input.trim();
    if let Some(rest) = strip_prefix_ci(s, "GEOMETRYCOLLECTION") {
        parse_geometry_collection(rest.trim())
    } else if let Some(rest) = strip_prefix_ci(s, "MULTIPOLYGON") {
        parse_multipolygon(rest.trim())
    } else if let Some(rest) = strip_prefix_ci(s, "MULTILINESTRING") {
        parse_multilinestring(rest.trim())
    } else if let Some(rest) = strip_prefix_ci(s, "MULTIPOINT") {
        parse_multipoint(rest.trim())
    } else if let Some(rest) = strip_prefix_ci(s, "POLYGON") {
        parse_polygon(rest.trim())
    } else if let Some(rest) = strip_prefix_ci(s, "LINESTRING") {
        parse_linestring(rest.trim())
    } else if let Some(rest) = strip_prefix_ci(s, "POINT") {
        parse_point(rest.trim())
    } else {
        None
    }
}

// ── Serialization helpers ──

fn coords_to_wkt(coords: &[[f64; 2]]) -> String {
    coords
        .iter()
        .map(|c| format!("{} {}", c[0], c[1]))
        .collect::<Vec<_>>()
        .join(", ")
}

// ── Parsing helpers ──

/// Case-insensitive prefix strip.
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

/// Strip outer parentheses: "(content)" → "content"
fn strip_parens(s: &str) -> Option<&str> {
    let s = s.trim();
    if s.starts_with('(') && s.ends_with(')') {
        Some(&s[1..s.len() - 1])
    } else {
        None
    }
}

/// Parse "lng lat" pair.
fn parse_coord(s: &str) -> Option<[f64; 2]> {
    let s = s.trim();
    let mut parts = s.split_whitespace();
    let lng: f64 = parts.next()?.parse().ok()?;
    let lat: f64 = parts.next()?.parse().ok()?;
    Some([lng, lat])
}

/// Parse "lng1 lat1, lng2 lat2, ..." into coordinate vec.
fn parse_coord_list(s: &str) -> Option<Vec<[f64; 2]>> {
    s.split(',').map(parse_coord).collect()
}

fn parse_point(s: &str) -> Option<Geometry> {
    let inner = strip_parens(s)?;
    let coord = parse_coord(inner)?;
    Some(Geometry::Point { coordinates: coord })
}

fn parse_linestring(s: &str) -> Option<Geometry> {
    let inner = strip_parens(s)?;
    let coords = parse_coord_list(inner)?;
    Some(Geometry::LineString {
        coordinates: coords,
    })
}

fn parse_polygon(s: &str) -> Option<Geometry> {
    let inner = strip_parens(s)?;
    let rings = split_top_level_parens(inner)?;
    let ring_coords: Option<Vec<Vec<[f64; 2]>>> = rings
        .iter()
        .map(|r| parse_coord_list(strip_parens(r.trim())?))
        .collect();
    Some(Geometry::Polygon {
        coordinates: ring_coords?,
    })
}

fn parse_multipoint(s: &str) -> Option<Geometry> {
    let inner = strip_parens(s)?;
    // MULTIPOINT((x y), (x y)) or MULTIPOINT(x y, x y)
    let coords = if inner.contains('(') {
        let parts = split_top_level_parens(inner)?;
        parts
            .iter()
            .map(|p| parse_coord(strip_parens(p.trim())?))
            .collect::<Option<Vec<_>>>()?
    } else {
        parse_coord_list(inner)?
    };
    Some(Geometry::MultiPoint {
        coordinates: coords,
    })
}

fn parse_multilinestring(s: &str) -> Option<Geometry> {
    let inner = strip_parens(s)?;
    let parts = split_top_level_parens(inner)?;
    let lines: Option<Vec<Vec<[f64; 2]>>> = parts
        .iter()
        .map(|p| parse_coord_list(strip_parens(p.trim())?))
        .collect();
    Some(Geometry::MultiLineString {
        coordinates: lines?,
    })
}

fn parse_multipolygon(s: &str) -> Option<Geometry> {
    let inner = strip_parens(s)?;
    let poly_parts = split_top_level_parens(inner)?;
    let polys: Option<Vec<Vec<Vec<[f64; 2]>>>> = poly_parts
        .iter()
        .map(|p| {
            let rings_str = strip_parens(p.trim())?;
            let ring_parts = split_top_level_parens(rings_str)?;
            ring_parts
                .iter()
                .map(|r| parse_coord_list(strip_parens(r.trim())?))
                .collect::<Option<Vec<_>>>()
        })
        .collect();
    Some(Geometry::MultiPolygon {
        coordinates: polys?,
    })
}

fn parse_geometry_collection(s: &str) -> Option<Geometry> {
    let inner = strip_parens(s)?;
    // Split by top-level commas (not inside parentheses).
    let parts = split_top_level_items(inner);
    let geoms: Option<Vec<Geometry>> = parts.iter().map(|p| geometry_from_wkt(p.trim())).collect();
    Some(Geometry::GeometryCollection { geometries: geoms? })
}

/// Split by commas at the top level of parentheses nesting.
/// "(...), (...)" → ["(...)", "(...)"]
fn split_top_level_parens(s: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(s[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        parts.push(s[start..].to_string());
    }
    if parts.is_empty() { None } else { Some(parts) }
}

/// Split geometry collection items by top-level commas, handling nested parens.
fn split_top_level_items(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(s[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        parts.push(s[start..].to_string());
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_roundtrip() {
        let geom = Geometry::point(-73.9857, 40.7484);
        let wkt = geometry_to_wkt(&geom);
        assert!(wkt.starts_with("POINT("));
        let parsed = geometry_from_wkt(&wkt).unwrap();
        assert_eq!(geom, parsed);
    }

    #[test]
    fn linestring_roundtrip() {
        let geom = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]);
        let wkt = geometry_to_wkt(&geom);
        let parsed = geometry_from_wkt(&wkt).unwrap();
        assert_eq!(geom, parsed);
    }

    #[test]
    fn polygon_roundtrip() {
        let geom = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]]);
        let wkt = geometry_to_wkt(&geom);
        assert!(wkt.starts_with("POLYGON("));
        let parsed = geometry_from_wkt(&wkt).unwrap();
        assert_eq!(geom, parsed);
    }

    #[test]
    fn polygon_with_hole_roundtrip() {
        let geom = Geometry::polygon(vec![
            vec![
                [0.0, 0.0],
                [10.0, 0.0],
                [10.0, 10.0],
                [0.0, 10.0],
                [0.0, 0.0],
            ],
            vec![[2.0, 2.0], [8.0, 2.0], [8.0, 8.0], [2.0, 8.0], [2.0, 2.0]],
        ]);
        let wkt = geometry_to_wkt(&geom);
        let parsed = geometry_from_wkt(&wkt).unwrap();
        assert_eq!(geom, parsed);
    }

    #[test]
    fn multipoint_roundtrip() {
        let geom = Geometry::MultiPoint {
            coordinates: vec![[1.0, 2.0], [3.0, 4.0]],
        };
        let wkt = geometry_to_wkt(&geom);
        let parsed = geometry_from_wkt(&wkt).unwrap();
        assert_eq!(geom, parsed);
    }

    #[test]
    fn multilinestring_roundtrip() {
        let geom = Geometry::MultiLineString {
            coordinates: vec![vec![[0.0, 0.0], [1.0, 1.0]], vec![[2.0, 2.0], [3.0, 3.0]]],
        };
        let wkt = geometry_to_wkt(&geom);
        let parsed = geometry_from_wkt(&wkt).unwrap();
        assert_eq!(geom, parsed);
    }

    #[test]
    fn multipolygon_roundtrip() {
        let geom = Geometry::MultiPolygon {
            coordinates: vec![
                vec![vec![[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]],
                vec![vec![[5.0, 5.0], [6.0, 5.0], [6.0, 6.0], [5.0, 5.0]]],
            ],
        };
        let wkt = geometry_to_wkt(&geom);
        let parsed = geometry_from_wkt(&wkt).unwrap();
        assert_eq!(geom, parsed);
    }

    #[test]
    fn geometry_collection_roundtrip() {
        let geom = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::point(1.0, 2.0),
                Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]),
            ],
        };
        let wkt = geometry_to_wkt(&geom);
        assert!(wkt.starts_with("GEOMETRYCOLLECTION("));
        let parsed = geometry_from_wkt(&wkt).unwrap();
        assert_eq!(geom, parsed);
    }

    #[test]
    fn case_insensitive_parse() {
        let parsed = geometry_from_wkt("point(5 10)").unwrap();
        assert_eq!(parsed, Geometry::point(5.0, 10.0));
    }

    #[test]
    fn invalid_wkt_returns_none() {
        assert!(geometry_from_wkt("").is_none());
        assert!(geometry_from_wkt("GARBAGE(1 2)").is_none());
        assert!(geometry_from_wkt("POINT(abc def)").is_none());
    }
}
