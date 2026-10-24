// SPDX-License-Identifier: Apache-2.0

//! `ST_ConvexHull` and `ST_ConcaveHull` over every vertex of a geometry.
//!
//! One distinct vertex gives a point, collinear vertices give a line
//! between the extremes, as PostGIS returns them.

use geo::concave_hull::ConcaveHullOptions;
use geo::{ConcaveHull, ConvexHull, Coord, MultiPoint, Point};
use nodedb_types::geometry::{Geometry, GeometryKind};

use super::convert::rings;
use crate::predicates::edge::{Orientation, orientation};

/// The hull algorithm and its parameter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Hull {
    Convex,
    /// `pct_convex` in [0, 1]: 1 is the convex hull, smaller values drill
    /// the hull inward. Maps to concaveman concavity `2·p / (1 − p)`.
    Concave {
        pct_convex: f64,
    },
}

/// The hull of every vertex of `geom`. EMPTY input gives
/// `GEOMETRYCOLLECTION EMPTY`.
pub fn hull(geom: &Geometry, kind: Hull) -> Geometry {
    let mut coords: Vec<[f64; 2]> = geom.coords().collect();
    coords.sort_by(|a, b| a[0].total_cmp(&b[0]).then(a[1].total_cmp(&b[1])));
    coords.dedup();
    match coords.as_slice() {
        [] => return Geometry::empty(GeometryKind::GeometryCollection),
        [only] => return Geometry::point(only[0], only[1]),
        [first, .., last] if all_collinear(&coords) => {
            return Geometry::line_string(vec![*first, *last]);
        }
        _ => {}
    }
    let points = MultiPoint::new(
        coords
            .iter()
            .map(|c| Point(Coord { x: c[0], y: c[1] }))
            .collect(),
    );
    let polygon = match kind {
        Hull::Convex => points.convex_hull(),
        Hull::Concave { pct_convex } if pct_convex >= 1.0 => points.convex_hull(),
        Hull::Concave { pct_convex } => {
            let p = pct_convex.max(0.0);
            points.concave_hull_with_options(
                ConcaveHullOptions::default().concavity(2.0 * p / (1.0 - p)),
            )
        }
    };
    Geometry::polygon(rings(&polygon))
}

/// Whether every point lies on the line through the first and last of a
/// sorted, de-duplicated list.
fn all_collinear(sorted: &[[f64; 2]]) -> bool {
    let (Some(first), Some(last)) = (sorted.first(), sorted.last()) else {
        return true;
    };
    sorted
        .iter()
        .all(|p| orientation(*first, *last, *p) == Orientation::Collinear)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planar::planar_area;

    fn u_points() -> Geometry {
        Geometry::MultiPoint {
            coordinates: vec![
                [0.0, 0.0],
                [3.0, 0.0],
                [3.0, 3.0],
                [2.0, 3.0],
                [2.0, 1.0],
                [1.0, 1.0],
                [1.0, 3.0],
                [0.0, 3.0],
            ],
        }
    }

    #[test]
    fn convex_hull_area() {
        assert!((planar_area(&hull(&u_points(), Hull::Convex)) - 9.0).abs() < 1e-9);
    }

    #[test]
    fn concave_hull_is_inside_the_convex_hull() {
        let concave = planar_area(&hull(&u_points(), Hull::Concave { pct_convex: 0.0 }));
        assert!(concave <= 9.0 + 1e-9 && concave > 0.0, "{concave}");
        let convex = planar_area(&hull(&u_points(), Hull::Concave { pct_convex: 1.0 }));
        assert!((convex - 9.0).abs() < 1e-9);
    }

    #[test]
    fn degenerate_inputs() {
        assert_eq!(
            hull(&Geometry::point(1.0, 2.0), Hull::Convex),
            Geometry::point(1.0, 2.0)
        );
        let line = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]]);
        assert_eq!(
            hull(&line, Hull::Convex),
            Geometry::line_string(vec![[0.0, 0.0], [2.0, 2.0]])
        );
        assert!(hull(&Geometry::empty_point(), Hull::Convex).is_empty());
    }
}
