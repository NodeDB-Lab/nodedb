// SPDX-License-Identifier: Apache-2.0

//! The measure basis of a CRS: geodesic on its ellipsoid, or planar in its
//! linear unit. Every result is metres or square metres.

use nodedb_crs::datum::geodetic_to_ecef;
use nodedb_crs::{CrsContext, CrsDef, CrsKind, Ellipsoid, UnknownSrid};
use nodedb_types::geometry::{Geometry, Srid, SridGeometry};
use nodedb_types::{BboxSpace, BoundingBox, geometry_bbox_in};

use crate::distance3d::{distance_3d, length_3d};
use crate::geodesic::GeodesicMeasure;
use crate::planar;

/// Smallest radius of curvature of any Earth ellipsoid in use, metres: the
/// WGS 84 meridional radius at the equator, a(1 − e²), rounded down. A box
/// widened with it covers every point within the distance.
const MIN_RADIUS_OF_CURVATURE_M: f64 = 6_335_000.0;

/// Smallest distance from the Earth's centre to its surface, metres: the
/// WGS 84 polar radius 6356752 m, less 11 km for the deepest trench.
const MIN_SURFACE_RADIUS_M: f64 = 6_345_000.0;

/// How a CRS measures.
#[derive(Debug, Clone, PartialEq)]
pub enum Metric {
    /// A geographic CRS: geodesics on its ellipsoid.
    Geodesic(Box<GeodesicMeasure>),
    /// A projected CRS: Euclidean in the grid, scaled to metres.
    Planar { metres_per_unit: f64 },
}

impl Metric {
    /// The metric of `def`.
    pub fn of(def: &CrsDef) -> Self {
        match def.kind {
            CrsKind::Geographic => {
                Metric::Geodesic(Box::new(GeodesicMeasure::new(def.datum.ellipsoid)))
            }
            CrsKind::Projected => Metric::Planar {
                metres_per_unit: def.unit.metres_per_unit,
            },
        }
    }

    /// The metric of `srid` in `ctx`.
    pub fn resolve(ctx: &CrsContext<'_>, srid: Srid) -> Result<Self, UnknownSrid> {
        ctx.resolve(srid).map(|def| Metric::of(&def))
    }

    /// Geodesics on WGS 84.
    pub fn wgs84() -> Self {
        Metric::Geodesic(Box::new(GeodesicMeasure::new(Ellipsoid::WGS84)))
    }

    pub fn space(&self) -> BboxSpace {
        match self {
            Metric::Geodesic(_) => BboxSpace::Geographic,
            Metric::Planar { .. } => BboxSpace::Planar,
        }
    }

    /// Metres in one coordinate unit of a projected CRS. `None` for a
    /// geographic CRS, whose degrees have no fixed length.
    pub fn metres_per_unit(&self) -> Option<f64> {
        match self {
            Metric::Geodesic(_) => None,
            Metric::Planar { metres_per_unit } => Some(*metres_per_unit),
        }
    }

    /// Minimum distance in metres. Infinite when either is EMPTY.
    pub fn distance(&self, a: &Geometry, b: &Geometry) -> f64 {
        if a.is_empty() || b.is_empty() {
            return f64::INFINITY;
        }
        match self {
            Metric::Geodesic(m) => m.distance(a, b),
            Metric::Planar { metres_per_unit } => planar::planar_distance(a, b) * metres_per_unit,
        }
    }

    /// Whether `a` and `b` lie within `metres`. Never true for an EMPTY
    /// operand. A widened bbox rejects distant pairs before the exact
    /// distance runs.
    pub fn dwithin(&self, a: &Geometry, b: &Geometry, metres: f64) -> bool {
        if a.is_empty() || b.is_empty() {
            return false;
        }
        let a_box = self.widen(&geometry_bbox_in(a, self.space()), metres);
        if !a_box.intersects(&geometry_bbox_in(b, self.space())) {
            return false;
        }
        self.distance(a, b) <= metres
    }

    /// Widen `bbox` so it covers every point within `metres` of it.
    ///
    /// A geographic box grows by the spherical cap on the smallest Earth
    /// radius of curvature, measured at the box's poleward edge. A box
    /// that reaches a pole spans every longitude.
    pub fn widen(&self, bbox: &BoundingBox, metres: f64) -> BoundingBox {
        match self {
            Metric::Planar { metres_per_unit } => {
                let units = metres / metres_per_unit;
                BoundingBox::new(
                    bbox.min_lng - units,
                    bbox.min_lat - units,
                    bbox.max_lng + units,
                    bbox.max_lat + units,
                )
            }
            Metric::Geodesic(_) => widen_geographic(bbox, metres),
        }
    }

    /// Total length of every linear component, metres.
    pub fn length(&self, geom: &Geometry) -> f64 {
        match self {
            Metric::Geodesic(m) => m.length(geom),
            Metric::Planar { metres_per_unit } => planar::planar_length(geom) * metres_per_unit,
        }
    }

    /// Total boundary length of every areal component, metres.
    pub fn perimeter(&self, geom: &Geometry) -> f64 {
        match self {
            Metric::Geodesic(m) => m.perimeter(geom),
            Metric::Planar { metres_per_unit } => planar::planar_perimeter(geom) * metres_per_unit,
        }
    }

    /// Area, square metres.
    pub fn area(&self, geom: &Geometry) -> f64 {
        match self {
            Metric::Geodesic(m) => m.area(geom),
            Metric::Planar { metres_per_unit } => {
                planar::planar_area(geom) * metres_per_unit * metres_per_unit
            }
        }
    }

    /// Unsigned area enclosed by one ring, square metres.
    pub fn ring_area(&self, ring: &[[f64; 2]]) -> f64 {
        match self {
            Metric::Geodesic(m) => m.ring_area(ring),
            Metric::Planar { metres_per_unit } => {
                planar::planar_ring_area(ring) * metres_per_unit * metres_per_unit
            }
        }
    }

    /// Length of one edge, metres.
    pub fn segment_length(&self, a: [f64; 2], b: [f64; 2]) -> f64 {
        match self {
            Metric::Geodesic(m) => m.point_distance(a, b),
            Metric::Planar { metres_per_unit } => {
                (b[0] - a[0]).hypot(b[1] - a[1]) * metres_per_unit
            }
        }
    }

    /// One position in Cartesian metres: grid x, y scaled to metres and Z
    /// for a projected CRS, ECEF on the ellipsoid for a geographic one.
    pub fn cartesian(&self, c: [f64; 2], z: f64) -> [f64; 3] {
        match self {
            Metric::Geodesic(m) => {
                geodetic_to_ecef(c[0].to_radians(), c[1].to_radians(), z, m.ellipsoid())
            }
            Metric::Planar { metres_per_unit } => {
                [c[0] * metres_per_unit, c[1] * metres_per_unit, z]
            }
        }
    }

    /// Minimum 3D distance, metres: Euclidean in grid metres for a
    /// projected CRS, the ECEF chord for a geographic one. Infinite when
    /// either is EMPTY.
    pub fn distance_3d(&self, a: &SridGeometry, b: &SridGeometry) -> f64 {
        distance_3d(a, b, &|c, z| self.cartesian(c, z))
    }

    /// Whether `a` and `b` lie within `metres` in 3D. Never true for an
    /// EMPTY operand. A widened bbox rejects distant pairs first: by
    /// `metres` in a projected CRS, by the longest surface arc a chord of
    /// `metres` spans in a geographic one.
    pub fn dwithin_3d(&self, a: &SridGeometry, b: &SridGeometry, metres: f64) -> bool {
        if a.is_empty() || b.is_empty() {
            return false;
        }
        let reach = self.reach_3d(metres);
        let a_box = self.widen(&geometry_bbox_in(a.geometry(), self.space()), reach);
        if !a_box.intersects(&geometry_bbox_in(b.geometry(), self.space())) {
            return false;
        }
        self.distance_3d(a, b) <= metres
    }

    /// The horizontal distance a 3D distance of `metres` can span: itself
    /// in a projected CRS, the arc of a chord of `metres` on the smallest
    /// sphere a surface point lies on in a geographic one.
    pub fn reach_3d(&self, metres: f64) -> f64 {
        match self {
            Metric::Planar { .. } => metres,
            Metric::Geodesic(_) => {
                let half = metres / (2.0 * MIN_SURFACE_RADIUS_M);
                if half >= 1.0 {
                    std::f64::consts::PI * MIN_SURFACE_RADIUS_M
                } else {
                    2.0 * MIN_SURFACE_RADIUS_M * half.asin()
                }
            }
        }
    }

    /// Total 3D length of every linear component, metres. A geographic
    /// edge is the chord between its ECEF endpoints.
    pub fn length_3d(&self, g: &SridGeometry) -> f64 {
        length_3d(g, &|c, z| self.cartesian(c, z))
    }
}

fn widen_geographic(bbox: &BoundingBox, metres: f64) -> BoundingBox {
    let angle = metres / MIN_RADIUS_OF_CURVATURE_M;
    let lat_delta = angle.to_degrees();
    let min_lat = (bbox.min_lat - lat_delta).max(-90.0);
    let max_lat = (bbox.max_lat + lat_delta).min(90.0);
    let poleward = min_lat.abs().max(max_lat.abs());
    let cos_lat = poleward.to_radians().cos();
    let sin_angle = angle.min(std::f64::consts::FRAC_PI_2).sin();
    if poleward >= 90.0 || sin_angle >= cos_lat {
        return BoundingBox::new(-180.0, min_lat, 180.0, max_lat);
    }
    let lng_delta = (sin_angle / cos_lat).asin().to_degrees();
    BoundingBox::new(
        bbox.min_lng - lng_delta,
        min_lat,
        bbox.max_lng + lng_delta,
        max_lat,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widen_covers_the_true_distance() {
        let m = Metric::wgs84();
        let point = BoundingBox::new(10.0, 0.0, 10.0, 0.0);
        let wide = m.widen(&point, 111_000.0);
        // One degree of latitude at the equator is 110 574 m.
        assert!(wide.max_lat >= 1.0, "{wide:?}");
        let north = BoundingBox::new(10.0, 70.0, 10.0, 70.0);
        let wide = m.widen(&north, 100_000.0);
        let east = m.segment_length([10.0, 70.0], [wide.max_lng, 70.0]);
        assert!(east >= 100_000.0, "{east}");
    }

    #[test]
    fn widen_reaching_a_pole_spans_every_longitude() {
        let m = Metric::wgs84();
        let wide = m.widen(&BoundingBox::new(0.0, 89.5, 0.0, 89.5), 100_000.0);
        assert_eq!((wide.min_lng, wide.max_lng), (-180.0, 180.0));
    }

    #[test]
    fn planar_scales_by_the_unit() {
        let feet = Metric::Planar {
            metres_per_unit: 0.3048,
        };
        let d = feet.distance(&Geometry::point(0.0, 0.0), &Geometry::point(1000.0, 0.0));
        assert!((d - 304.8).abs() < 1e-9);
        assert!(feet.dwithin(
            &Geometry::point(0.0, 0.0),
            &Geometry::point(1000.0, 0.0),
            305.0
        ));
    }

    #[test]
    fn empty_operands_are_never_within() {
        let m = Metric::wgs84();
        assert!(!m.dwithin(&Geometry::empty_point(), &Geometry::point(0.0, 0.0), 1e9));
        assert!(
            m.distance(&Geometry::empty_point(), &Geometry::point(0.0, 0.0))
                .is_infinite()
        );
    }

    /// A chord shorter than the arc it spans still passes the prefilter.
    #[test]
    fn dwithin_3d_reaches_past_the_chord() {
        let m = Metric::wgs84();
        let a = SridGeometry::wgs84(Geometry::point(0.0, 0.0));
        let b = SridGeometry::wgs84(Geometry::point(30.0, 0.0));
        let chord = m.distance_3d(&a, &b);
        let arc = m.distance(a.geometry(), b.geometry());
        assert!(chord < arc);
        assert!(m.dwithin_3d(&a, &b, chord + 1.0));
        assert!(!m.dwithin_3d(&a, &b, chord - 1.0));
    }

    /// ECEF chord between two points 1 km apart in height on one spot.
    #[test]
    fn geographic_3d_distance_is_the_ecef_chord() {
        use nodedb_types::geometry::Ordinates;
        let m = Metric::wgs84();
        let at = |h: f64| {
            SridGeometry::with_ordinates(
                Srid::WGS84,
                Geometry::point(101.7, 3.1),
                Ordinates::new(Some(vec![h]), None),
            )
            .unwrap()
        };
        let d = m.distance_3d(&at(0.0), &at(1000.0));
        assert!((d - 1000.0).abs() < 1e-6, "{d}");
    }
}
