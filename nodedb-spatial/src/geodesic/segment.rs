// SPDX-License-Identifier: Apache-2.0

//! Distance from a point to a geodesic segment on an ellipsoid.

use nodedb_crs::method::geodesic::{Geodesic, GeodesicLine};

/// Golden-section iterations. Each shrinks the bracket by 0.618, so 80
/// reach below 1e-16 of the segment length.
const GOLDEN_ITERATIONS: usize = 80;

/// Bracket width, metres, below which the search stops.
const GOLDEN_TOLERANCE_M: f64 = 1e-6;

/// 1 / golden ratio.
const INV_PHI: f64 = 0.618_033_988_749_894_9;

/// Geodesic distance between two (x = longitude, y = latitude) points,
/// in the units of the ellipsoid's semi-major axis.
pub(crate) fn point_distance(g: &Geodesic, a: [f64; 2], b: [f64; 2]) -> f64 {
    g.inverse(a[1], a[0], b[1], b[0]).0
}

/// Shortest geodesic distance from `p` to the geodesic segment `a`–`b`.
///
/// The distance from a point to the positions along a geodesic is
/// unimodal for segments shorter than half a meridian, so a golden-section
/// search over the arc length finds the foot of the perpendicular.
pub(crate) fn point_segment_distance(g: &Geodesic, p: [f64; 2], a: [f64; 2], b: [f64; 2]) -> f64 {
    let (s12, azi1, _) = g.inverse(a[1], a[0], b[1], b[0]);
    let to_a = point_distance(g, p, a);
    if s12 <= GOLDEN_TOLERANCE_M {
        return to_a;
    }
    let to_b = point_distance(g, p, b);
    let line = GeodesicLine::new(g, a[1], a[0], azi1);
    let at = |s: f64| {
        let pos = line.position(s);
        g.inverse(p[1], p[0], pos.lat2, pos.lon2).0
    };
    let (mut lo, mut hi) = (0.0, s12);
    let mut x1 = hi - INV_PHI * (hi - lo);
    let mut x2 = lo + INV_PHI * (hi - lo);
    let mut f1 = at(x1);
    let mut f2 = at(x2);
    for _ in 0..GOLDEN_ITERATIONS {
        if hi - lo <= GOLDEN_TOLERANCE_M {
            break;
        }
        if f1 <= f2 {
            hi = x2;
            x2 = x1;
            f2 = f1;
            x1 = hi - INV_PHI * (hi - lo);
            f1 = at(x1);
        } else {
            lo = x1;
            x1 = x2;
            f1 = f2;
            x2 = lo + INV_PHI * (hi - lo);
            f2 = at(x2);
        }
    }
    to_a.min(to_b).min(f1).min(f2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wgs84() -> Geodesic {
        let e = nodedb_crs::Ellipsoid::WGS84;
        Geodesic::new(e.a, e.f())
    }

    /// A point north of the middle of an equatorial segment is one
    /// meridian arc away: 0.1° of latitude at the equator is 11057.43 m.
    #[test]
    fn perpendicular_foot_on_the_equator() {
        let g = wgs84();
        let d = point_segment_distance(&g, [0.5, 0.1], [0.0, 0.0], [1.0, 0.0]);
        assert!((d - 11_057.43).abs() < 0.1, "{d}");
    }

    #[test]
    fn beyond_an_end_measures_to_the_end() {
        let g = wgs84();
        let d = point_segment_distance(&g, [2.0, 0.0], [0.0, 0.0], [1.0, 0.0]);
        let e = point_distance(&g, [2.0, 0.0], [1.0, 0.0]);
        assert!((d - e).abs() < 1e-6, "{d} {e}");
    }
}
