// SPDX-License-Identifier: Apache-2.0

//! DE-9IM: `ST_Relate` and the named predicates it defines (`ST_Touches`,
//! `ST_Crosses`, `ST_Overlaps`, `ST_Equals`, `ST_Covers`, `ST_CoveredBy`),
//! through `geo::Relate`. Planar in the coordinates.

use geo::Relate;
use geo::coordinate_position::CoordPos;
use geo::dimensions::Dimensions;
use geo::relate::IntersectionMatrix;
use nodedb_types::geometry::Geometry;

use super::convert::to_geo;
use crate::error::SpatialError;

fn matrix(a: &Geometry, b: &Geometry) -> IntersectionMatrix {
    to_geo(a).relate(&to_geo(b))
}

/// The DE-9IM matrix of `a` and `b`, as the 9-character string PostGIS
/// returns: rows interior, boundary, exterior of `a`, columns the same of
/// `b`.
pub fn relate(a: &Geometry, b: &Geometry) -> String {
    let m = matrix(a, b);
    let order = [CoordPos::Inside, CoordPos::OnBoundary, CoordPos::Outside];
    let mut out = String::with_capacity(9);
    for lhs in order {
        for rhs in order {
            out.push(match m.get(lhs, rhs) {
                Dimensions::Empty => 'F',
                Dimensions::ZeroDimensional => '0',
                Dimensions::OneDimensional => '1',
                Dimensions::TwoDimensional => '2',
            });
        }
    }
    out
}

/// Whether the DE-9IM matrix of `a` and `b` matches `pattern`
/// (`ST_Relate(a, b, pattern)`). Characters: `0 1 2 T F *`.
pub fn relate_pattern(a: &Geometry, b: &Geometry, pattern: &str) -> Result<bool, SpatialError> {
    matrix(a, b)
        .matches(pattern)
        .map_err(|e| SpatialError::Pattern {
            pattern: pattern.into(),
            reason: e.to_string().into(),
        })
}

/// The named DE-9IM predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Relation {
    Touches,
    Crosses,
    Overlaps,
    Equals,
    Covers,
    CoveredBy,
}

/// Whether `a` and `b` stand in `relation`.
pub fn relates(a: &Geometry, b: &Geometry, relation: Relation) -> bool {
    let m = matrix(a, b);
    match relation {
        Relation::Touches => m.is_touches(),
        Relation::Crosses => m.is_crosses(),
        Relation::Overlaps => m.is_overlaps(),
        Relation::Equals => m.is_equal_topo(),
        Relation::Covers => m.is_covers(),
        Relation::CoveredBy => m.is_coveredby(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square(x0: f64, y0: f64, side: f64) -> Geometry {
        Geometry::polygon(vec![vec![
            [x0, y0],
            [x0 + side, y0],
            [x0 + side, y0 + side],
            [x0, y0 + side],
            [x0, y0],
        ]])
    }

    /// shapely 2.2.0: box(0,0,2,2).relate(box(1,1,3,3)) == '212101212'.
    #[test]
    fn overlapping_squares_matrix() {
        assert_eq!(
            relate(&square(0.0, 0.0, 2.0), &square(1.0, 1.0, 2.0)),
            "212101212"
        );
        assert!(relates(
            &square(0.0, 0.0, 2.0),
            &square(1.0, 1.0, 2.0),
            Relation::Overlaps
        ));
    }

    #[test]
    fn named_predicates() {
        let (a, b) = (square(0.0, 0.0, 1.0), square(1.0, 0.0, 1.0));
        assert!(relates(&a, &b, Relation::Touches));
        let line = Geometry::line_string(vec![[-1.0, 0.5], [0.5, 0.5]]);
        assert!(relates(&line, &a, Relation::Crosses));
        assert!(relates(&a, &a.clone(), Relation::Equals));
        assert!(relates(&square(0.0, 0.0, 4.0), &a, Relation::Covers));
        assert!(relates(&a, &square(0.0, 0.0, 4.0), Relation::CoveredBy));
        assert!(relate_pattern(&a, &b, "FF*F0****").is_ok());
        assert!(relate_pattern(&a, &b, "bad").is_err());
    }
}
