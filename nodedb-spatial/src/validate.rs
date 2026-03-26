//! Geometry validation (geo_is_valid).
//!
//! Checks structural validity per OGC Simple Features:
//! - Polygon rings must be closed (first == last coordinate)
//! - Polygon rings must have at least 4 points (triangle + close)
//! - Polygon exterior ring should be counter-clockwise (right-hand rule)
//! - Polygon holes should be clockwise
//! - No self-intersection of ring edges
//! - LineStrings must have at least 2 points

use nodedb_types::geometry::Geometry;

use crate::predicates::edge::segments_intersect;

/// Validate a geometry. Returns a list of issues (empty = valid).
pub fn validate_geometry(geom: &Geometry) -> Vec<String> {
    let mut issues = Vec::new();
    validate_recursive(geom, &mut issues);
    issues
}

/// Whether a geometry is valid (no issues).
pub fn is_valid(geom: &Geometry) -> bool {
    validate_geometry(geom).is_empty()
}

fn validate_recursive(geom: &Geometry, issues: &mut Vec<String>) {
    match geom {
        Geometry::Point { coordinates } => {
            if coordinates[0].is_nan()
                || coordinates[1].is_nan()
                || coordinates[0].is_infinite()
                || coordinates[1].is_infinite()
            {
                issues.push("Point has NaN or Infinite coordinate".to_string());
            }
        }

        Geometry::LineString { coordinates } => {
            if coordinates.len() < 2 {
                issues.push(format!(
                    "LineString has {} points, minimum is 2",
                    coordinates.len()
                ));
            }
        }

        Geometry::Polygon { coordinates } => {
            if coordinates.is_empty() {
                issues.push("Polygon has no rings".to_string());
                return;
            }

            for (ring_idx, ring) in coordinates.iter().enumerate() {
                let label = if ring_idx == 0 {
                    "Exterior ring".to_string()
                } else {
                    format!("Hole ring {ring_idx}")
                };

                if ring.len() < 4 {
                    issues.push(format!(
                        "{label} has {} points, minimum is 4 (triangle + close)",
                        ring.len()
                    ));
                    continue;
                }

                // Check closed.
                if let (Some(first), Some(last)) = (ring.first(), ring.last())
                    && ((first[0] - last[0]).abs() > 1e-10 || (first[1] - last[1]).abs() > 1e-10)
                {
                    issues.push(format!("{label} is not closed (first != last)"));
                }

                // Check winding order.
                let area = signed_area(ring);
                if ring_idx == 0 && area < 0.0 {
                    issues.push(
                        "Exterior ring has clockwise winding (should be counter-clockwise)"
                            .to_string(),
                    );
                } else if ring_idx > 0 && area > 0.0 {
                    issues.push(format!(
                        "{label} has counter-clockwise winding (holes should be clockwise)"
                    ));
                }

                // Check self-intersection.
                check_ring_self_intersection(ring, &label, issues);
            }
        }

        Geometry::MultiPoint { coordinates } => {
            for (i, c) in coordinates.iter().enumerate() {
                if c[0].is_nan() || c[1].is_nan() || c[0].is_infinite() || c[1].is_infinite() {
                    issues.push(format!("MultiPoint[{i}] has NaN or Infinite coordinate"));
                }
            }
        }

        Geometry::MultiLineString { coordinates } => {
            for (i, ls) in coordinates.iter().enumerate() {
                if ls.len() < 2 {
                    issues.push(format!(
                        "MultiLineString[{i}] has {} points, minimum is 2",
                        ls.len()
                    ));
                }
            }
        }

        Geometry::MultiPolygon { coordinates } => {
            for (i, poly) in coordinates.iter().enumerate() {
                validate_recursive(
                    &Geometry::Polygon {
                        coordinates: poly.clone(),
                    },
                    issues,
                );
                // Prefix existing issues from this sub-polygon.
                // (Already added by recursive call.)
                let _ = i; // used for context only
            }
        }

        Geometry::GeometryCollection { geometries } => {
            for geom in geometries {
                validate_recursive(geom, issues);
            }
        }
    }
}

/// Signed area of a ring (Shoelace formula). Positive = CCW, Negative = CW.
fn signed_area(ring: &[[f64; 2]]) -> f64 {
    let n = ring.len();
    if n < 3 {
        return 0.0;
    }
    let mut sum = 0.0;
    for i in 0..n {
        let j = (i + 1) % n;
        sum += ring[i][0] * ring[j][1];
        sum -= ring[j][0] * ring[i][1];
    }
    sum / 2.0
}

/// Check if any non-adjacent edges of a ring intersect.
fn check_ring_self_intersection(ring: &[[f64; 2]], label: &str, issues: &mut Vec<String>) {
    let n = if ring.first() == ring.last() && ring.len() > 1 {
        ring.len() - 1
    } else {
        ring.len()
    };

    if n < 4 {
        return; // Triangle can't self-intersect.
    }

    for i in 0..n {
        let i_next = (i + 1) % n;
        // Only check non-adjacent edges (skip i-1, i, i+1).
        for j in (i + 2)..n {
            let j_next = (j + 1) % n;
            // Skip if edges share a vertex.
            if j_next == i {
                continue;
            }
            if segments_intersect(ring[i], ring[i_next], ring[j], ring[j_next]) {
                // Check if it's just a shared endpoint (adjacent-ish for closed rings).
                let shared_endpoint = ring[i] == ring[j]
                    || ring[i] == ring[j_next]
                    || ring[i_next] == ring[j]
                    || ring[i_next] == ring[j_next];
                if !shared_endpoint {
                    issues.push(format!(
                        "{label} has self-intersection at edges {i}-{i_next} and {j}-{j_next}"
                    ));
                    return; // One self-intersection is enough to report.
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_polygon() {
        let geom = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]]);
        assert!(is_valid(&geom));
    }

    #[test]
    fn unclosed_ring() {
        let geom = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
        ]]);
        let issues = validate_geometry(&geom);
        assert!(issues.iter().any(|i| i.contains("not closed")));
    }

    #[test]
    fn too_few_points() {
        let geom = Geometry::polygon(vec![vec![[0.0, 0.0], [10.0, 0.0], [10.0, 10.0]]]);
        let issues = validate_geometry(&geom);
        assert!(issues.iter().any(|i| i.contains("minimum is 4")));
    }

    #[test]
    fn self_intersecting_ring() {
        // Bowtie: edges cross.
        let geom = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [10.0, 10.0],
            [10.0, 0.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]]);
        let issues = validate_geometry(&geom);
        assert!(issues.iter().any(|i| i.contains("self-intersection")));
    }

    #[test]
    fn valid_linestring() {
        let geom = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]);
        assert!(is_valid(&geom));
    }

    #[test]
    fn linestring_too_short() {
        let geom = Geometry::LineString {
            coordinates: vec![[0.0, 0.0]],
        };
        assert!(!is_valid(&geom));
    }

    #[test]
    fn valid_point() {
        assert!(is_valid(&Geometry::point(0.0, 0.0)));
    }

    #[test]
    fn nan_point_invalid() {
        let geom = Geometry::Point {
            coordinates: [f64::NAN, 0.0],
        };
        assert!(!is_valid(&geom));
    }

    #[test]
    fn clockwise_exterior_warned() {
        // Clockwise exterior ring.
        let geom = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [0.0, 10.0],
            [10.0, 10.0],
            [10.0, 0.0],
            [0.0, 0.0],
        ]]);
        let issues = validate_geometry(&geom);
        assert!(issues.iter().any(|i| i.contains("clockwise")));
    }

    #[test]
    fn geometry_collection_validates_all() {
        let gc = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::point(0.0, 0.0),
                Geometry::LineString {
                    coordinates: vec![[0.0, 0.0]],
                }, // invalid
            ],
        };
        let issues = validate_geometry(&gc);
        assert!(!issues.is_empty());
    }
}
