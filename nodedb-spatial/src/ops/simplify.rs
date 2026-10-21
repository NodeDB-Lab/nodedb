// SPDX-License-Identifier: Apache-2.0

//! `ST_Simplify` and `ST_SimplifyPreserveTopology`, planar in the
//! coordinates.
//!
//! `ST_Simplify` is Ramer–Douglas–Peucker: a vertex goes when it lies
//! within `tolerance` of the simplified line. `ST_SimplifyPreserveTopology`
//! is Visvalingam–Whyatt with topology preservation: a vertex goes when its
//! triangle with its neighbours has an area under `tolerance² / 2` and
//! removing it creates no self-intersection. Points pass through.

use geo::{Simplify, SimplifyVwPreserve};
use nodedb_types::geometry::Geometry;

use super::convert::{from_geo, to_geo};

/// The simplification algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Simplifier {
    DouglasPeucker,
    PreserveTopology,
}

/// Simplify every linear and areal component of `geom`.
pub fn simplify(geom: &Geometry, tolerance: f64, simplifier: Simplifier) -> Geometry {
    match geom {
        Geometry::GeometryCollection { geometries } => Geometry::GeometryCollection {
            geometries: geometries
                .iter()
                .map(|g| simplify(g, tolerance, simplifier))
                .collect(),
        },
        g if g.is_empty() => g.clone(),
        g => {
            let area = tolerance * tolerance / 2.0;
            let out = match (to_geo(g), simplifier) {
                (geo::Geometry::LineString(l), Simplifier::DouglasPeucker) => {
                    geo::Geometry::LineString(l.simplify(tolerance))
                }
                (geo::Geometry::LineString(l), Simplifier::PreserveTopology) => {
                    geo::Geometry::LineString(l.simplify_vw_preserve(area))
                }
                (geo::Geometry::MultiLineString(l), Simplifier::DouglasPeucker) => {
                    geo::Geometry::MultiLineString(l.simplify(tolerance))
                }
                (geo::Geometry::MultiLineString(l), Simplifier::PreserveTopology) => {
                    geo::Geometry::MultiLineString(l.simplify_vw_preserve(area))
                }
                (geo::Geometry::Polygon(p), Simplifier::DouglasPeucker) => {
                    geo::Geometry::Polygon(p.simplify(tolerance))
                }
                (geo::Geometry::Polygon(p), Simplifier::PreserveTopology) => {
                    geo::Geometry::Polygon(p.simplify_vw_preserve(area))
                }
                (geo::Geometry::MultiPolygon(p), Simplifier::DouglasPeucker) => {
                    geo::Geometry::MultiPolygon(p.simplify(tolerance))
                }
                (geo::Geometry::MultiPolygon(p), Simplifier::PreserveTopology) => {
                    geo::Geometry::MultiPolygon(p.simplify_vw_preserve(area))
                }
                (other, _) => other,
            };
            from_geo(&out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn douglas_peucker_drops_near_collinear_vertices() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [5.0, 0.1], [10.0, 0.0]]);
        let out = simplify(&line, 0.5, Simplifier::DouglasPeucker);
        assert_eq!(out, Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0]]));
        let kept = simplify(&line, 0.01, Simplifier::DouglasPeucker);
        assert_eq!(kept, line);
    }

    #[test]
    fn preserve_topology_keeps_a_valid_polygon() {
        let poly = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [5.0, 0.01],
            [10.0, 0.0],
            [10.0, 10.0],
            [0.0, 10.0],
            [0.0, 0.0],
        ]]);
        let out = simplify(&poly, 1.0, Simplifier::PreserveTopology);
        let Geometry::Polygon { coordinates } = &out else {
            panic!("{out:?}");
        };
        assert_eq!(coordinates[0].len(), 5);
    }
}
