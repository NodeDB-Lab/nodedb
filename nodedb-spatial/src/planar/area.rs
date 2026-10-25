// SPDX-License-Identifier: Apache-2.0

//! Planar area of projected geometries, in square metres.

use nodedb_types::geometry::Geometry;

/// Signed area of one ring by the shoelace formula, positive
/// counter-clockwise. A ring with fewer than three vertices encloses
/// nothing. An unclosed ring is closed implicitly.
///
/// Coordinates are taken relative to the first vertex. Projected eastings and
/// northings run to hundreds of kilometres, and summing their raw cross
/// products cancels away the digits a small ring's area lives in.
pub fn signed_area(ring: &[[f64; 2]]) -> f64 {
    if ring.len() < 3 {
        return 0.0;
    }
    let origin = ring[0];
    let rel = |p: [f64; 2]| [p[0] - origin[0], p[1] - origin[1]];
    let mut twice = 0.0;
    for i in 0..ring.len() {
        let a = rel(ring[i]);
        let b = rel(ring[(i + 1) % ring.len()]);
        twice += a[0] * b[1] - b[0] * a[1];
    }
    twice / 2.0
}

/// Unsigned area of one ring by the shoelace formula.
pub fn planar_ring_area(ring: &[[f64; 2]]) -> f64 {
    signed_area(ring).abs()
}

/// Area of one polygon: exterior ring less every hole, never negative.
fn polygon_area(rings: &[Vec<[f64; 2]>]) -> f64 {
    let mut iter = rings.iter();
    let Some(exterior) = iter.next() else {
        return 0.0;
    };
    let holes: f64 = iter.map(|ring| planar_ring_area(ring)).sum();
    (planar_ring_area(exterior) - holes).max(0.0)
}

/// Planar area of a geometry in square coordinate units (square metres for a
/// projected SRID).
///
/// Points and lines have zero area, matching PostGIS. A GeometryCollection
/// sums its members.
pub fn planar_area(geom: &Geometry) -> f64 {
    match geom {
        Geometry::Polygon { coordinates } => polygon_area(coordinates),
        Geometry::MultiPolygon { coordinates } => {
            coordinates.iter().map(|rings| polygon_area(rings)).sum()
        }
        Geometry::GeometryCollection { geometries } => geometries.iter().map(planar_area).sum(),
        Geometry::Point { .. }
        | Geometry::LineString { .. }
        | Geometry::MultiPoint { .. }
        | Geometry::MultiLineString { .. } => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hundred_metre_square_is_ten_thousand_square_metres() {
        let square = Geometry::polygon(vec![vec![
            [500_000.0, 100_000.0],
            [500_100.0, 100_000.0],
            [500_100.0, 100_100.0],
            [500_000.0, 100_100.0],
            [500_000.0, 100_000.0],
        ]]);
        assert!((planar_area(&square) - 10_000.0).abs() < 1e-6);
    }

    #[test]
    fn hole_is_subtracted_and_orientation_ignored() {
        let cw = vec![
            [0.0, 0.0],
            [0.0, 10.0],
            [10.0, 10.0],
            [10.0, 0.0],
            [0.0, 0.0],
        ];
        let hole = vec![[2.0, 2.0], [4.0, 2.0], [4.0, 4.0], [2.0, 4.0], [2.0, 2.0]];
        let poly = Geometry::polygon(vec![cw, hole]);
        assert!((planar_area(&poly) - 96.0).abs() < 1e-9);
    }

    #[test]
    fn lines_and_points_have_no_area() {
        assert_eq!(planar_area(&Geometry::point(1.0, 2.0)), 0.0);
        let line = Geometry::line_string(vec![[0.0, 0.0], [5.0, 5.0]]);
        assert_eq!(planar_area(&line), 0.0);
    }
}
