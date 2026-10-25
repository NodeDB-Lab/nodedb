// SPDX-License-Identifier: Apache-2.0

//! Area and perimeter of a geodesic polygon, ported from GeographicLib's C
//! library (PROJ 9.8.1 `src/geodesic.c`, `geod_polygon_*`, `transit`,
//! `areareduceA`). Each edge is a geodesic. The area is exact to the
//! precision of the inverse solution for polygons of any size.

use super::math::{ang_diff, ang_normalize, remainder, sum_x};
use super::model::Geodesic;

/// A sum carried with its rounding error (GeographicLib `accadd`).
#[derive(Debug, Clone, Copy, Default)]
struct Accumulator {
    s: f64,
    t: f64,
}

impl Accumulator {
    fn add(&mut self, y: f64) {
        let (z, u) = sum_x(y, self.t);
        let (s, t) = sum_x(z, self.s);
        self.s = s;
        self.t = t;
        if self.s == 0.0 {
            self.s = u;
        } else {
            self.t += u;
        }
    }

    fn remainder(&mut self, y: f64) {
        self.s = remainder(self.s, y);
        self.add(0.0);
    }

    fn negate(&mut self) {
        self.s = -self.s;
        self.t = -self.t;
    }

    fn value(&self) -> f64 {
        self.s + self.t
    }
}

/// 1 or −1 when the edge from `lon1` to `lon2` crosses the prime meridian
/// eastward or westward, else 0.
fn transit(lon1: f64, lon2: f64) -> i32 {
    let (lon12, _) = ang_diff(lon1, lon2);
    let lon1 = ang_normalize(lon1);
    let lon2 = ang_normalize(lon2);
    if lon12 > 0.0 && ((lon1 < 0.0 && lon2 >= 0.0) || (lon1 > 0.0 && lon2 == 0.0)) {
        1
    } else if lon12 < 0.0 && lon1 >= 0.0 && lon2 < 0.0 {
        -1
    } else {
        0
    }
}

/// The area and perimeter of a closed geodesic polygon.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PolygonMeasure {
    /// Signed area in the square of the semi-major axis units, positive
    /// for counter-clockwise traversal, in (−area0/2, area0/2].
    pub area: f64,
    /// Perimeter in the units of the semi-major axis.
    pub perimeter: f64,
}

impl Geodesic {
    /// Total area of the ellipsoid, 4π c².
    pub fn ellipsoid_area(&self) -> f64 {
        4.0 * core::f64::consts::PI * self.c2
    }

    /// Area and perimeter of the polygon through `vertices` (latitude,
    /// longitude degrees), closed back to the first vertex. A repeated
    /// closing vertex adds a zero-length edge and changes nothing. Fewer
    /// than three distinct vertices give zero area.
    pub fn polygon_measure(&self, vertices: &[(f64, f64)]) -> PolygonMeasure {
        if vertices.len() < 2 {
            return PolygonMeasure {
                area: 0.0,
                perimeter: 0.0,
            };
        }
        let mut perimeter = Accumulator::default();
        let mut area = Accumulator::default();
        let mut crossings = 0i32;
        let n = vertices.len();
        for i in 0..n {
            let (lat1, lon1) = vertices[i];
            let (lat2, lon2) = vertices[(i + 1) % n];
            let sol = self.inverse_full(lat1, lon1, lat2, lon2);
            perimeter.add(sol.s12);
            area.add(sol.area_s12);
            crossings += transit(lon1, lon2);
        }
        let area0 = self.ellipsoid_area();
        area.remainder(area0);
        if crossings & 1 != 0 {
            area.add(if area.s < 0.0 { 1.0 } else { -1.0 } * area0 / 2.0);
        }
        // The sum runs clockwise-positive. Report counter-clockwise
        // positive, then centre the value on zero.
        area.negate();
        if area.s > area0 / 2.0 {
            area.add(-area0);
        } else if area.s <= -area0 / 2.0 {
            area.add(area0);
        }
        PolygonMeasure {
            area: area.value(),
            perimeter: perimeter.value(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WGS84_A: f64 = 6_378_137.0;
    const WGS84_F: f64 = 1.0 / 298.257_223_563;

    fn wgs84() -> Geodesic {
        Geodesic::new(WGS84_A, WGS84_F)
    }

    /// GeographicLib Planimeter reference: the polygon (0,0) (0,1) (1,1)
    /// (1,0) degrees on WGS 84 has area 12308778361.469 m² and perimeter
    /// 443770.917248 m.
    #[test]
    fn one_degree_square_matches_planimeter() {
        let g = wgs84();
        let m = g.polygon_measure(&[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]);
        assert!(
            (m.area.abs() - 12_308_778_361.469).abs() < 1.0,
            "{}",
            m.area
        );
        assert!(
            (m.perimeter - 443_770.917_248).abs() < 1e-3,
            "{}",
            m.perimeter
        );
    }

    /// Reversing the traversal flips the sign only.
    #[test]
    fn orientation_sets_the_sign() {
        let g = wgs84();
        let ccw = [(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)];
        let mut cw = ccw;
        cw.reverse();
        let a = g.polygon_measure(&ccw).area;
        let b = g.polygon_measure(&cw).area;
        assert!((a + b).abs() < 1e-3, "{a} {b}");
    }

    /// A polygon across the antimeridian measures its true extent.
    #[test]
    fn antimeridian_polygon() {
        let g = wgs84();
        let across = g.polygon_measure(&[(0.0, 179.5), (0.0, -179.5), (1.0, -179.5), (1.0, 179.5)]);
        let plain = g.polygon_measure(&[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)]);
        assert!((across.area.abs() - plain.area.abs()).abs() < 1.0);
    }

    #[test]
    fn transit_detects_prime_meridian_crossings() {
        assert_eq!(transit(-1.0, 1.0), 1);
        assert_eq!(transit(1.0, -1.0), -1);
        assert_eq!(transit(10.0, 20.0), 0);
    }
}
