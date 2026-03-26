//! Spatial predicate extraction from DataFusion WHERE expressions.
//!
//! Detects ST_DWithin, ST_Contains, ST_Intersects, ST_Within function calls
//! in WHERE clauses and converts them to SpatialScan physical plans.
//!
//! Pattern: `WHERE ST_DWithin(geometry_column, geo_point(-73.98, 40.75), 500)`
//! becomes a SpatialScan with predicate=DWithin, query_geometry=Point, distance=500.

use crate::bridge::physical_plan::SpatialPredicate;

/// A recognized spatial predicate extracted from a WHERE clause.
#[derive(Debug, Clone)]
pub struct ExtractedSpatialFilter {
    /// The geometry field being tested (column reference).
    pub field: String,
    /// Which spatial predicate.
    pub predicate: SpatialPredicate,
    /// Query geometry as GeoJSON bytes.
    pub query_geometry: Vec<u8>,
    /// Distance threshold in meters (for ST_DWithin, 0.0 for others).
    pub distance_meters: f64,
}

/// Try to extract a spatial predicate from a function name and arguments.
///
/// Returns `Some(ExtractedSpatialFilter)` if the function is a recognized
/// spatial predicate with valid arguments, `None` otherwise.
///
/// Expected argument patterns:
/// - `st_dwithin(field, geometry, distance)` → DWithin
/// - `st_contains(geometry, field)` → Contains (query contains doc)
/// - `st_intersects(field, geometry)` → Intersects
/// - `st_within(field, geometry)` → Within (doc within query)
pub fn try_extract_spatial_predicate(
    func_name: &str,
    args: &[serde_json::Value],
) -> Option<ExtractedSpatialFilter> {
    let lower = func_name.to_lowercase();

    match lower.as_str() {
        "st_dwithin" => {
            // st_dwithin(field_or_geom, geom_or_field, distance)
            if args.len() < 3 {
                return None;
            }
            let (field, geom) = extract_field_and_geometry(&args[0], &args[1])?;
            let distance = args[2].as_f64().unwrap_or(0.0);
            let query_geometry = serde_json::to_vec(&geom).ok()?;
            Some(ExtractedSpatialFilter {
                field,
                predicate: SpatialPredicate::DWithin,
                query_geometry,
                distance_meters: distance,
            })
        }
        "st_contains" => {
            // st_contains(container, contained) — container is the query geometry.
            if args.len() < 2 {
                return None;
            }
            // The geometry arg is the container (first arg for contains).
            let (field, geom) = extract_field_and_geometry_contains(&args[0], &args[1])?;
            let query_geometry = serde_json::to_vec(&geom).ok()?;
            Some(ExtractedSpatialFilter {
                field,
                predicate: SpatialPredicate::Contains,
                query_geometry,
                distance_meters: 0.0,
            })
        }
        "st_intersects" => {
            if args.len() < 2 {
                return None;
            }
            let (field, geom) = extract_field_and_geometry(&args[0], &args[1])?;
            let query_geometry = serde_json::to_vec(&geom).ok()?;
            Some(ExtractedSpatialFilter {
                field,
                predicate: SpatialPredicate::Intersects,
                query_geometry,
                distance_meters: 0.0,
            })
        }
        "st_within" => {
            // st_within(inner, outer) — inner is doc geometry, outer is query.
            if args.len() < 2 {
                return None;
            }
            let (field, geom) = extract_field_and_geometry(&args[0], &args[1])?;
            let query_geometry = serde_json::to_vec(&geom).ok()?;
            Some(ExtractedSpatialFilter {
                field,
                predicate: SpatialPredicate::Within,
                query_geometry,
                distance_meters: 0.0,
            })
        }
        _ => None,
    }
}

/// Determine which arg is the field reference and which is the geometry literal.
///
/// In SQL, one argument is a column reference (string name) and the other is
/// a geometry literal (GeoJSON object). Returns (field_name, geometry_json).
fn extract_field_and_geometry(
    a: &serde_json::Value,
    b: &serde_json::Value,
) -> Option<(String, serde_json::Value)> {
    // If a is a string (column name) and b is an object (geometry), use that order.
    if a.is_string() && b.is_object() && b.get("type").is_some() {
        return Some((a.as_str()?.to_string(), b.clone()));
    }
    // Reverse: b is column, a is geometry.
    if b.is_string() && a.is_object() && a.get("type").is_some() {
        return Some((b.as_str()?.to_string(), a.clone()));
    }
    None
}

/// For ST_Contains, the first arg is the container geometry (query),
/// the second is the field being tested. This is the reverse of the
/// standard extract_field_and_geometry.
fn extract_field_and_geometry_contains(
    container: &serde_json::Value,
    contained: &serde_json::Value,
) -> Option<(String, serde_json::Value)> {
    // container is geometry literal, contained is field name.
    if container.is_object() && container.get("type").is_some() && contained.is_string() {
        return Some((contained.as_str()?.to_string(), container.clone()));
    }
    // Reverse: container is field, contained is geometry — not standard for contains
    // but handle gracefully.
    if contained.is_object() && contained.get("type").is_some() && container.is_string() {
        return Some((container.as_str()?.to_string(), contained.clone()));
    }
    None
}

/// Check if a function name is a spatial predicate (for filter classification).
pub fn is_spatial_predicate(func_name: &str) -> bool {
    matches!(
        func_name.to_lowercase().as_str(),
        "st_dwithin" | "st_contains" | "st_intersects" | "st_within"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_dwithin() {
        let args = vec![
            json!("location"),
            json!({"type": "Point", "coordinates": [-73.98, 40.75]}),
            json!(500.0),
        ];
        let filter = try_extract_spatial_predicate("st_dwithin", &args).unwrap();
        assert_eq!(filter.field, "location");
        assert_eq!(filter.predicate, SpatialPredicate::DWithin);
        assert!((filter.distance_meters - 500.0).abs() < 0.01);
    }

    #[test]
    fn extract_contains() {
        let args = vec![
            json!({"type": "Polygon", "coordinates": [[[0.0,0.0],[10.0,0.0],[10.0,10.0],[0.0,10.0],[0.0,0.0]]]}),
            json!("geom"),
        ];
        let filter = try_extract_spatial_predicate("st_contains", &args).unwrap();
        assert_eq!(filter.field, "geom");
        assert_eq!(filter.predicate, SpatialPredicate::Contains);
    }

    #[test]
    fn extract_intersects() {
        let args = vec![
            json!("boundary"),
            json!({"type": "Point", "coordinates": [5.0, 5.0]}),
        ];
        let filter = try_extract_spatial_predicate("st_intersects", &args).unwrap();
        assert_eq!(filter.field, "boundary");
        assert_eq!(filter.predicate, SpatialPredicate::Intersects);
    }

    #[test]
    fn non_spatial_returns_none() {
        assert!(try_extract_spatial_predicate("upper", &[json!("hello")]).is_none());
    }

    #[test]
    fn is_spatial_predicate_check() {
        assert!(is_spatial_predicate("st_dwithin"));
        assert!(is_spatial_predicate("ST_CONTAINS"));
        assert!(!is_spatial_predicate("geo_distance"));
    }

    #[test]
    fn reversed_args_still_work() {
        // Geometry first, field second.
        let args = vec![
            json!({"type": "Point", "coordinates": [5.0, 5.0]}),
            json!("location"),
        ];
        let filter = try_extract_spatial_predicate("st_intersects", &args).unwrap();
        assert_eq!(filter.field, "location");
    }
}
