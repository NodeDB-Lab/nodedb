// SPDX-License-Identifier: Apache-2.0

//! `ST_MakeValid`.
//!
//! Polygons repair through `geo::MakeValid` (constrained Delaunay
//! triangulation, odd-even fill, Ledoux et al. 2014): a self-intersecting
//! ring splits into its valid pieces, a dangling spike drops, an unclosed
//! ring closes. A line with fewer than two distinct points becomes a point.
//! Repeated consecutive vertices drop.

use geo::MakeValid;
use nodedb_types::geometry::Geometry;

use super::convert::{multipolygon, polygon, simplest};
use crate::error::SpatialError;

/// A valid geometry with the same point set where one exists.
pub fn make_valid(geom: &Geometry) -> Result<Geometry, SpatialError> {
    Ok(match geom {
        g if g.is_empty() => g.clone(),
        Geometry::Point { .. } | Geometry::MultiPoint { .. } => geom.clone(),
        Geometry::LineString { coordinates } => line(coordinates),
        Geometry::MultiLineString { coordinates } => {
            let parts: Vec<Geometry> = coordinates.iter().map(|l| line(l)).collect();
            if parts
                .iter()
                .all(|p| matches!(p, Geometry::LineString { .. }))
            {
                Geometry::MultiLineString {
                    coordinates: parts
                        .into_iter()
                        .filter_map(|p| match p {
                            Geometry::LineString { coordinates } => Some(coordinates),
                            _ => None,
                        })
                        .collect(),
                }
            } else {
                Geometry::GeometryCollection { geometries: parts }
            }
        }
        Geometry::Polygon { coordinates } => repaired(polygon(coordinates).make_valid())?,
        Geometry::MultiPolygon { coordinates } => repaired(
            geo::MultiPolygon::new(coordinates.iter().map(|p| polygon(p)).collect()).make_valid(),
        )?,
        Geometry::GeometryCollection { geometries } => Geometry::GeometryCollection {
            geometries: geometries
                .iter()
                .map(make_valid)
                .collect::<Result<_, _>>()?,
        },
    })
}

fn repaired<E: std::fmt::Display>(
    result: Result<geo::MultiPolygon<f64>, E>,
) -> Result<Geometry, SpatialError> {
    let mp = result.map_err(|e| SpatialError::Repair {
        reason: e.to_string().into(),
    })?;
    let g = multipolygon(&mp);
    Ok(if g.is_empty() {
        Geometry::empty(nodedb_types::geometry::GeometryKind::Polygon)
    } else {
        simplest(g)
    })
}

/// A line without repeated consecutive vertices, or a point when one
/// distinct vertex remains.
fn line(coords: &[[f64; 2]]) -> Geometry {
    let mut out: Vec<[f64; 2]> = Vec::with_capacity(coords.len());
    for c in coords {
        if out.last() != Some(c) {
            out.push(*c);
        }
    }
    match out.as_slice() {
        [only] => Geometry::point(only[0], only[1]),
        _ => Geometry::line_string(out),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planar::planar_area;
    use crate::validate::is_valid;

    /// shapely 2.2.0: make_valid(bowtie) is a MultiPolygon of area 50.
    #[test]
    fn bowtie_splits_into_two_triangles() {
        let bowtie = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [0.0, 10.0],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 0.0],
        ]]);
        assert!(!is_valid(&bowtie));
        let fixed = make_valid(&bowtie).unwrap();
        assert!(is_valid(&fixed), "{fixed:?}");
        assert!((planar_area(&fixed) - 50.0).abs() < 1e-9);
        assert!(
            matches!(fixed, Geometry::MultiPolygon { ref coordinates } if coordinates.len() == 2)
        );
    }

    #[test]
    fn collapsed_line_becomes_a_point() {
        let line = Geometry::line_string(vec![[1.0, 1.0], [1.0, 1.0]]);
        assert_eq!(make_valid(&line).unwrap(), Geometry::point(1.0, 1.0));
    }
}
