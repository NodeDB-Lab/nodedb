// SPDX-License-Identifier: Apache-2.0

//! Ring orientation by the right-hand rule: exterior rings counter-clockwise,
//! holes clockwise.
//!
//! A caller whose use of a polygon does not depend on ring direction (an
//! even-odd point-in-polygon test) orients the rings first, so a polygon
//! drawn clockwise, as shapefiles store it, is accepted.

use nodedb_types::geometry::Geometry;

use crate::planar::area::signed_area;

/// Reverse every ring of `geom` that breaks the right-hand rule. Polygons
/// nested in a `MultiPolygon` or `GeometryCollection` are oriented too.
/// Other geometry types are unchanged.
pub fn orient_rings(geom: &mut Geometry) {
    match geom {
        Geometry::Polygon { coordinates } => orient_polygon(coordinates),
        Geometry::MultiPolygon { coordinates } => {
            coordinates.iter_mut().for_each(|p| orient_polygon(p));
        }
        Geometry::GeometryCollection { geometries } => {
            geometries.iter_mut().for_each(orient_rings);
        }
        _ => {}
    }
}

fn orient_polygon(rings: &mut [Vec<[f64; 2]>]) {
    for (index, ring) in rings.iter_mut().enumerate() {
        let area = signed_area(ring);
        let wrong = if index == 0 { area < 0.0 } else { area > 0.0 };
        if wrong {
            ring.reverse();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whether every shell runs counter-clockwise and every hole clockwise.
    fn right_handed(geom: &Geometry) -> bool {
        let polygon_ok = |rings: &Vec<Vec<[f64; 2]>>| {
            rings.iter().enumerate().all(|(i, r)| {
                let a = signed_area(r);
                if i == 0 { a > 0.0 } else { a < 0.0 }
            })
        };
        match geom {
            Geometry::Polygon { coordinates } => polygon_ok(coordinates),
            Geometry::MultiPolygon { coordinates } => coordinates.iter().all(polygon_ok),
            _ => true,
        }
    }

    fn square(clockwise: bool, lo: f64, hi: f64) -> Vec<[f64; 2]> {
        let mut ring = vec![[lo, lo], [hi, lo], [hi, hi], [lo, hi], [lo, lo]];
        if clockwise {
            ring.reverse();
        }
        ring
    }

    #[test]
    fn clockwise_exterior_and_ccw_hole_are_reversed() {
        let mut geom = Geometry::polygon(vec![square(true, 0.0, 10.0), square(false, 2.0, 4.0)]);
        assert!(!right_handed(&geom));
        orient_rings(&mut geom);
        assert!(right_handed(&geom), "{geom:?}");
    }

    #[test]
    fn oriented_polygon_is_unchanged() {
        let original = Geometry::polygon(vec![square(false, 0.0, 10.0), square(true, 2.0, 4.0)]);
        let mut geom = original.clone();
        orient_rings(&mut geom);
        assert_eq!(geom, original);
    }

    #[test]
    fn multipolygon_members_are_oriented() {
        let mut geom = Geometry::MultiPolygon {
            coordinates: vec![vec![square(true, 0.0, 1.0)], vec![square(false, 5.0, 6.0)]],
        };
        orient_rings(&mut geom);
        assert!(right_handed(&geom), "{geom:?}");
    }
}
