//! Geospatial SQL function evaluation.
//!
//! All ST_* predicates, geometry operations, and geo_* utility functions.
//! Called from `functions::eval_function` for any geo-prefixed or ST_-prefixed name.

use crate::json_ops::{json_to_f64, to_json_number};

/// Try to evaluate a geo/spatial function. Returns `Some(result)` if the
/// function name matched, `None` if unrecognized (caller falls through).
pub fn eval_geo_function(name: &str, args: &[serde_json::Value]) -> Option<serde_json::Value> {
    let result = match name {
        "geo_distance" | "haversine_distance" => {
            let lng1 = num_arg(args, 0).unwrap_or(0.0);
            let lat1 = num_arg(args, 1).unwrap_or(0.0);
            let lng2 = num_arg(args, 2).unwrap_or(0.0);
            let lat2 = num_arg(args, 3).unwrap_or(0.0);
            to_json_number(nodedb_types::geometry::haversine_distance(
                lng1, lat1, lng2, lat2,
            ))
        }
        "geo_bearing" | "haversine_bearing" => {
            let lng1 = num_arg(args, 0).unwrap_or(0.0);
            let lat1 = num_arg(args, 1).unwrap_or(0.0);
            let lng2 = num_arg(args, 2).unwrap_or(0.0);
            let lat2 = num_arg(args, 3).unwrap_or(0.0);
            to_json_number(nodedb_types::geometry::haversine_bearing(
                lng1, lat1, lng2, lat2,
            ))
        }
        "geo_point" => {
            let lng = num_arg(args, 0).unwrap_or(0.0);
            let lat = num_arg(args, 1).unwrap_or(0.0);
            let point = nodedb_types::geometry::Geometry::point(lng, lat);
            serde_json::to_value(&point).unwrap_or(serde_json::Value::Null)
        }
        "geo_geohash" => {
            let lng = num_arg(args, 0).unwrap_or(0.0);
            let lat = num_arg(args, 1).unwrap_or(0.0);
            let precision = num_arg(args, 2).unwrap_or(6.0) as u8;
            serde_json::Value::String(nodedb_spatial::geohash_encode(lng, lat, precision))
        }
        "geo_geohash_decode" => {
            let hash = str_arg(args, 0).unwrap_or_default();
            match nodedb_spatial::geohash_decode(&hash) {
                Some(bb) => serde_json::json!({
                    "min_lng": bb.min_lng, "min_lat": bb.min_lat,
                    "max_lng": bb.max_lng, "max_lat": bb.max_lat,
                }),
                None => serde_json::Value::Null,
            }
        }
        "geo_geohash_neighbors" => {
            let hash = str_arg(args, 0).unwrap_or_default();
            let neighbors = nodedb_spatial::geohash_neighbors(&hash);
            let arr: Vec<serde_json::Value> = neighbors
                .into_iter()
                .map(|(dir, h)| serde_json::json!({"direction": format!("{dir:?}"), "hash": h}))
                .collect();
            serde_json::Value::Array(arr)
        }

        // ── Spatial predicates (ST_*) ──
        "st_contains" => geo_predicate_2(args, nodedb_spatial::st_contains),
        "st_intersects" => geo_predicate_2(args, nodedb_spatial::st_intersects),
        "st_within" => geo_predicate_2(args, nodedb_spatial::st_within),
        "st_disjoint" => geo_predicate_2(args, nodedb_spatial::st_disjoint),
        "st_dwithin" => {
            let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
                return Some(serde_json::Value::Null);
            };
            let dist = num_arg(args, 2).unwrap_or(0.0);
            serde_json::Value::Bool(nodedb_spatial::st_dwithin(&a, &b, dist))
        }
        "st_distance" => {
            let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
                return Some(serde_json::Value::Null);
            };
            to_json_number(nodedb_spatial::st_distance(&a, &b))
        }
        "st_buffer" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let dist = num_arg(args, 1).unwrap_or(0.0);
            let segs = num_arg(args, 2).unwrap_or(32.0) as usize;
            let result = nodedb_spatial::st_buffer(&geom, dist, segs);
            serde_json::to_value(&result).unwrap_or(serde_json::Value::Null)
        }
        "st_envelope" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            let result = nodedb_spatial::st_envelope(&geom);
            serde_json::to_value(&result).unwrap_or(serde_json::Value::Null)
        }
        "st_union" => {
            let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
                return Some(serde_json::Value::Null);
            };
            let result = nodedb_spatial::st_union(&a, &b);
            serde_json::to_value(&result).unwrap_or(serde_json::Value::Null)
        }

        // ── Extended geo functions ──
        "geo_length" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            to_json_number(geo_linestring_length(&geom))
        }
        "geo_perimeter" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            to_json_number(geo_polygon_perimeter(&geom))
        }
        "geo_line" => {
            let coords: Vec<[f64; 2]> = args
                .iter()
                .filter_map(|v| {
                    let g: nodedb_types::geometry::Geometry =
                        serde_json::from_value(v.clone()).ok()?;
                    if let nodedb_types::geometry::Geometry::Point { coordinates } = g {
                        Some(coordinates)
                    } else {
                        None
                    }
                })
                .collect();
            if coords.len() < 2 {
                serde_json::Value::Null
            } else {
                let ls = nodedb_types::geometry::Geometry::line_string(coords);
                serde_json::to_value(&ls).unwrap_or(serde_json::Value::Null)
            }
        }
        "geo_polygon" => {
            let rings: Vec<Vec<[f64; 2]>> = args
                .iter()
                .filter_map(|v| serde_json::from_value::<Vec<[f64; 2]>>(v.clone()).ok())
                .collect();
            if rings.is_empty() {
                serde_json::Value::Null
            } else {
                let poly = nodedb_types::geometry::Geometry::polygon(rings);
                serde_json::to_value(&poly).unwrap_or(serde_json::Value::Null)
            }
        }
        "geo_circle" => {
            let lng = num_arg(args, 0).unwrap_or(0.0);
            let lat = num_arg(args, 1).unwrap_or(0.0);
            let radius = num_arg(args, 2).unwrap_or(0.0);
            let segs = num_arg(args, 3).unwrap_or(32.0) as usize;
            let circle = nodedb_spatial::st_buffer(
                &nodedb_types::geometry::Geometry::point(lng, lat),
                radius,
                segs,
            );
            serde_json::to_value(&circle).unwrap_or(serde_json::Value::Null)
        }
        "geo_bbox" => {
            let min_lng = num_arg(args, 0).unwrap_or(0.0);
            let min_lat = num_arg(args, 1).unwrap_or(0.0);
            let max_lng = num_arg(args, 2).unwrap_or(0.0);
            let max_lat = num_arg(args, 3).unwrap_or(0.0);
            let poly = nodedb_types::geometry::Geometry::polygon(vec![vec![
                [min_lng, min_lat],
                [max_lng, min_lat],
                [max_lng, max_lat],
                [min_lng, max_lat],
                [min_lng, min_lat],
            ]]);
            serde_json::to_value(&poly).unwrap_or(serde_json::Value::Null)
        }
        "geo_as_geojson" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            match serde_json::to_string(&geom) {
                Ok(s) => serde_json::Value::String(s),
                Err(_) => serde_json::Value::Null,
            }
        }
        "geo_from_geojson" => {
            let s = str_arg(args, 0).unwrap_or_default();
            match serde_json::from_str::<nodedb_types::geometry::Geometry>(&s) {
                Ok(g) => serde_json::to_value(&g).unwrap_or(serde_json::Value::Null),
                Err(_) => serde_json::Value::Null,
            }
        }
        "geo_as_wkt" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            serde_json::Value::String(nodedb_spatial::geometry_to_wkt(&geom))
        }
        "geo_from_wkt" => {
            let s = str_arg(args, 0).unwrap_or_default();
            match nodedb_spatial::geometry_from_wkt(&s) {
                Some(g) => serde_json::to_value(&g).unwrap_or(serde_json::Value::Null),
                None => serde_json::Value::Null,
            }
        }
        "geo_x" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            if let nodedb_types::geometry::Geometry::Point { coordinates } = geom {
                to_json_number(coordinates[0])
            } else {
                serde_json::Value::Null
            }
        }
        "geo_y" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            if let nodedb_types::geometry::Geometry::Point { coordinates } = geom {
                to_json_number(coordinates[1])
            } else {
                serde_json::Value::Null
            }
        }
        "geo_num_points" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            serde_json::Value::Number(serde_json::Number::from(count_points(&geom) as i64))
        }
        "geo_type" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            serde_json::Value::String(geom.geometry_type().to_string())
        }
        "geo_is_valid" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(serde_json::Value::Null);
            };
            serde_json::Value::Bool(nodedb_spatial::is_valid(&geom))
        }

        _ => return None,
    };
    Some(result)
}

// ── Helpers ──

fn str_arg(args: &[serde_json::Value], idx: usize) -> Option<String> {
    args.get(idx)?.as_str().map(|s| s.to_string())
}

fn num_arg(args: &[serde_json::Value], idx: usize) -> Option<f64> {
    args.get(idx).and_then(|v| json_to_f64(v, true))
}

fn geom_arg(args: &[serde_json::Value], idx: usize) -> Option<nodedb_types::geometry::Geometry> {
    serde_json::from_value::<nodedb_types::geometry::Geometry>(args.get(idx)?.clone()).ok()
}

fn geo_predicate_2(
    args: &[serde_json::Value],
    f: fn(&nodedb_types::geometry::Geometry, &nodedb_types::geometry::Geometry) -> bool,
) -> serde_json::Value {
    let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
        return serde_json::Value::Null;
    };
    serde_json::Value::Bool(f(&a, &b))
}

fn geo_linestring_length(geom: &nodedb_types::geometry::Geometry) -> f64 {
    let coords = match geom {
        nodedb_types::geometry::Geometry::LineString { coordinates } => coordinates,
        _ => return 0.0,
    };
    let mut total = 0.0;
    for i in 0..coords.len().saturating_sub(1) {
        total += nodedb_types::geometry::haversine_distance(
            coords[i][0],
            coords[i][1],
            coords[i + 1][0],
            coords[i + 1][1],
        );
    }
    total
}

fn geo_polygon_perimeter(geom: &nodedb_types::geometry::Geometry) -> f64 {
    let rings = match geom {
        nodedb_types::geometry::Geometry::Polygon { coordinates } => coordinates,
        _ => return 0.0,
    };
    let Some(exterior) = rings.first() else {
        return 0.0;
    };
    let mut total = 0.0;
    for i in 0..exterior.len().saturating_sub(1) {
        total += nodedb_types::geometry::haversine_distance(
            exterior[i][0],
            exterior[i][1],
            exterior[i + 1][0],
            exterior[i + 1][1],
        );
    }
    total
}

fn count_points(geom: &nodedb_types::geometry::Geometry) -> usize {
    use nodedb_types::geometry::Geometry;
    match geom {
        Geometry::Point { .. } => 1,
        Geometry::LineString { coordinates } => coordinates.len(),
        Geometry::Polygon { coordinates } => coordinates.iter().map(|r| r.len()).sum(),
        Geometry::MultiPoint { coordinates } => coordinates.len(),
        Geometry::MultiLineString { coordinates } => coordinates.iter().map(|ls| ls.len()).sum(),
        Geometry::MultiPolygon { coordinates } => coordinates
            .iter()
            .flat_map(|poly| poly.iter())
            .map(|ring| ring.len())
            .sum(),
        Geometry::GeometryCollection { geometries } => geometries.iter().map(count_points).sum(),
    }
}
