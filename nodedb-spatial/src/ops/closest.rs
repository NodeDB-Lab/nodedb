// SPDX-License-Identifier: Apache-2.0

//! `ST_ClosestPoint(a, b)`: the point of `a` nearest to `b`, planar in the
//! coordinates. Where the two meet, a shared point.

use nodedb_types::geometry::Geometry;

use super::noding::{Meet, fraction_along, meet};
use crate::parts::{Parts, polygon_covers};
use crate::predicates::edge::point_to_segment_dist_sq;

/// The point of `a` closest to `b`. `None` when either is EMPTY.
pub fn closest_point(a: &Geometry, b: &Geometry) -> Option<[f64; 2]> {
    let pa = Parts::of(a);
    let pb = Parts::of(b);
    if pa.segments.is_empty() || pb.segments.is_empty() {
        return None;
    }
    if let Some(v) = pb
        .vertices
        .iter()
        .find(|v| pa.polygons.iter().any(|rings| polygon_covers(rings, **v)))
    {
        return Some(*v);
    }
    if let Some(v) = pa
        .vertices
        .iter()
        .find(|v| pb.polygons.iter().any(|rings| polygon_covers(rings, **v)))
    {
        return Some(*v);
    }
    let mut best: Option<(f64, [f64; 2])> = None;
    let mut consider = |d: f64, p: [f64; 2]| {
        if best.is_none_or(|(bd, _)| d < bd) {
            best = Some((d, p));
        }
    };
    for &(a1, a2) in &pa.segments {
        for &(b1, b2) in &pb.segments {
            match meet(a1, a2, b1, b2) {
                Meet::Point(p) | Meet::Overlap(p, _) => return Some(p),
                Meet::Disjoint => {}
            }
            for v in [b1, b2] {
                let t = fraction_along(a1, a2, v);
                let foot = [a1[0] + (a2[0] - a1[0]) * t, a1[1] + (a2[1] - a1[1]) * t];
                consider(point_to_segment_dist_sq(v, a1, a2), foot);
            }
            for u in [a1, a2] {
                consider(point_to_segment_dist_sq(u, b1, b2), u);
            }
        }
    }
    best.map(|(_, p)| p)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foot_of_the_perpendicular_on_a_line() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0]]);
        assert_eq!(
            closest_point(&line, &Geometry::point(3.0, 5.0)),
            Some([3.0, 0.0])
        );
        assert_eq!(
            closest_point(&Geometry::point(3.0, 5.0), &line),
            Some([3.0, 5.0])
        );
    }

    #[test]
    fn point_inside_a_polygon_is_its_own_closest() {
        let square = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [4.0, 0.0],
            [4.0, 4.0],
            [0.0, 4.0],
            [0.0, 0.0],
        ]]);
        assert_eq!(
            closest_point(&square, &Geometry::point(1.0, 1.0)),
            Some([1.0, 1.0])
        );
        assert_eq!(
            closest_point(&square, &Geometry::point(6.0, 2.0)),
            Some([4.0, 2.0])
        );
    }

    #[test]
    fn empty_has_no_closest_point() {
        assert_eq!(
            closest_point(&Geometry::empty_point(), &Geometry::point(0.0, 0.0)),
            None
        );
    }
}
