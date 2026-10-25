// SPDX-License-Identifier: Apache-2.0

//! Axis-aligned bounding box for 2D geometries.
//!
//! In [`BboxSpace::Geographic`] space the box handles the anti-meridian
//! (±180° date line): when a geometry crosses the date line,
//! `min_lng > max_lng`. All intersection and containment logic accounts for
//! this split case. In [`BboxSpace::Planar`] space x is a projected easting in
//! metres, nothing wraps, and `min_lng <= max_lng` always holds for a
//! non-empty box.

use crate::geometry::{Geometry, Srid};
use nodedb_srid::{CrsKind, SridResolver, UnknownSrid};
use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

/// The coordinate space a bounding box lives in.
///
/// Derived from the kind of the SRID of the geometries it bounds
/// ([`BboxSpace::of`]). Union and enlargement depend on it: only
/// geographic longitude wraps at ±180°.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum BboxSpace {
    /// Longitude / latitude degrees. Longitude wraps at the anti-meridian.
    #[default]
    Geographic,
    /// Projected metres. No axis wraps.
    Planar,
}

impl From<CrsKind> for BboxSpace {
    fn from(kind: CrsKind) -> Self {
        match kind {
            CrsKind::Geographic => BboxSpace::Geographic,
            CrsKind::Projected => BboxSpace::Planar,
        }
    }
}

impl BboxSpace {
    /// The space of `srid`, from its kind in `ctx`.
    pub fn of(ctx: &dyn SridResolver, srid: Srid) -> Result<BboxSpace, UnknownSrid> {
        ctx.kind(srid).map(BboxSpace::from)
    }
}

/// An axis-aligned bounding box. The `lng` fields hold x and the `lat` fields
/// hold y: degrees for a geographic SRID, metres for a projected one.
///
/// **Anti-meridian convention (geographic only):** When `min_lng > max_lng`, the bbox wraps
/// across the ±180° date line. For example, a geometry spanning 170°E to
/// 170°W has `min_lng = 170.0, max_lng = -170.0`.
///
/// `#[non_exhaustive]` — elevation (Z) bounds or a CRS tag may be added
/// when 3D geometries are supported.
#[non_exhaustive]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Serialize,
    Deserialize,
    ToMessagePack,
    FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct BoundingBox {
    pub min_lng: f64,
    pub min_lat: f64,
    pub max_lng: f64,
    pub max_lat: f64,
}

impl BoundingBox {
    pub fn new(min_lng: f64, min_lat: f64, max_lng: f64, max_lat: f64) -> Self {
        Self {
            min_lng,
            min_lat,
            max_lng,
            max_lat,
        }
    }

    pub fn from_point(lng: f64, lat: f64) -> Self {
        Self {
            min_lng: lng,
            min_lat: lat,
            max_lng: lng,
            max_lat: lat,
        }
    }

    pub fn crosses_antimeridian(&self) -> bool {
        self.min_lng > self.max_lng
    }

    /// Area in approximate square degrees (for R-tree heuristics).
    pub fn area(&self) -> f64 {
        let lat_span = self.max_lat - self.min_lat;
        let lng_span = if self.crosses_antimeridian() {
            (180.0 - self.min_lng) + (self.max_lng + 180.0)
        } else {
            self.max_lng - self.min_lng
        };
        lat_span * lng_span
    }

    /// Half-perimeter (R*-tree margin heuristic).
    pub fn margin(&self) -> f64 {
        let lat_span = self.max_lat - self.min_lat;
        let lng_span = if self.crosses_antimeridian() {
            (180.0 - self.min_lng) + (self.max_lng + 180.0)
        } else {
            self.max_lng - self.min_lng
        };
        lat_span + lng_span
    }

    pub fn contains_point(&self, lng: f64, lat: f64) -> bool {
        if lat < self.min_lat || lat > self.max_lat {
            return false;
        }
        if self.crosses_antimeridian() {
            lng >= self.min_lng || lng <= self.max_lng
        } else {
            lng >= self.min_lng && lng <= self.max_lng
        }
    }

    pub fn contains_bbox(&self, other: &BoundingBox) -> bool {
        if other.min_lat < self.min_lat || other.max_lat > self.max_lat {
            return false;
        }
        match (self.crosses_antimeridian(), other.crosses_antimeridian()) {
            (false, false) => other.min_lng >= self.min_lng && other.max_lng <= self.max_lng,
            (true, false) => other.min_lng >= self.min_lng || other.max_lng <= self.max_lng,
            (false, true) => false,
            (true, true) => other.min_lng >= self.min_lng && other.max_lng <= self.max_lng,
        }
    }

    pub fn intersects(&self, other: &BoundingBox) -> bool {
        if self.max_lat < other.min_lat || self.min_lat > other.max_lat {
            return false;
        }
        match (self.crosses_antimeridian(), other.crosses_antimeridian()) {
            (false, false) => self.min_lng <= other.max_lng && self.max_lng >= other.min_lng,
            (true, false) => other.max_lng >= self.min_lng || other.min_lng <= self.max_lng,
            (false, true) => self.max_lng >= other.min_lng || self.min_lng <= other.max_lng,
            (true, true) => true,
        }
    }

    /// Union in geographic space. Same as `union_in(other, BboxSpace::Geographic)`.
    pub fn union(&self, other: &BoundingBox) -> BoundingBox {
        self.union_in(other, BboxSpace::Geographic)
    }

    /// Smallest box covering both, in `space`.
    ///
    /// Geographic space picks the shorter of the direct and the
    /// anti-meridian-wrapping longitude span. Planar space takes plain
    /// per-axis min / max, so two disjoint metre boxes never wrap.
    pub fn union_in(&self, other: &BoundingBox, space: BboxSpace) -> BoundingBox {
        let min_lat = self.min_lat.min(other.min_lat);
        let max_lat = self.max_lat.max(other.max_lat);
        let (min_lng, max_lng) = match space {
            BboxSpace::Geographic => {
                union_longitude(self.min_lng, self.max_lng, other.min_lng, other.max_lng)
            }
            BboxSpace::Planar => (
                self.min_lng.min(other.min_lng),
                self.max_lng.max(other.max_lng),
            ),
        };
        BoundingBox {
            min_lng,
            min_lat,
            max_lng,
            max_lat,
        }
    }

    pub fn extend_point(&mut self, lng: f64, lat: f64) {
        self.min_lat = self.min_lat.min(lat);
        self.max_lat = self.max_lat.max(lat);
        if self.crosses_antimeridian() {
            let in_east = lng >= self.min_lng;
            let in_west = lng <= self.max_lng;
            if !in_east && !in_west {
                if self.min_lng - lng < lng - self.max_lng {
                    self.min_lng = lng;
                } else {
                    self.max_lng = lng;
                }
            }
        } else {
            self.min_lng = self.min_lng.min(lng);
            self.max_lng = self.max_lng.max(lng);
        }
    }

    pub fn overlap_area(&self, other: &BoundingBox) -> f64 {
        if !self.intersects(other) {
            return 0.0;
        }
        let lat_overlap = self.max_lat.min(other.max_lat) - self.min_lat.max(other.min_lat);
        if lat_overlap <= 0.0 {
            return 0.0;
        }
        let lng_overlap =
            lng_overlap_span(self.min_lng, self.max_lng, other.min_lng, other.max_lng);
        lat_overlap * lng_overlap
    }

    /// Enlargement in geographic space. Same as
    /// `enlargement_in(other, BboxSpace::Geographic)`.
    pub fn enlargement(&self, other: &BoundingBox) -> f64 {
        self.enlargement_in(other, BboxSpace::Geographic)
    }

    /// Area added by growing `self` to cover `other`, in `space`.
    pub fn enlargement_in(&self, other: &BoundingBox, space: BboxSpace) -> f64 {
        self.union_in(other, space).area() - self.area()
    }
}

fn lng_overlap_span(a_min: f64, a_max: f64, b_min: f64, b_max: f64) -> f64 {
    let a_wraps = a_min > a_max;
    let b_wraps = b_min > b_max;
    match (a_wraps, b_wraps) {
        (false, false) => (a_max.min(b_max) - a_min.max(b_min)).max(0.0),
        (true, false) => {
            let east = (180.0_f64.min(b_max) - a_min.max(b_min)).max(0.0);
            let west = (a_max.min(b_max) - (-180.0_f64).max(b_min)).max(0.0);
            east + west
        }
        (false, true) => lng_overlap_span(b_min, b_max, a_min, a_max),
        (true, true) => {
            let east = (180.0 - a_min.max(b_min)).max(0.0);
            let west = (a_max.min(b_max) + 180.0).max(0.0);
            east + west
        }
    }
}

fn union_longitude(a_min: f64, a_max: f64, b_min: f64, b_max: f64) -> (f64, f64) {
    let a_wraps = a_min > a_max;
    let b_wraps = b_min > b_max;
    match (a_wraps, b_wraps) {
        (false, false) => {
            let simple_min = a_min.min(b_min);
            let simple_max = a_max.max(b_max);
            let simple_span = simple_max - simple_min;
            let gap = (b_min - a_max).max(0.0).min((a_min - b_max).max(0.0));
            if gap <= 0.0 {
                return (simple_min, simple_max);
            }
            let wrap_min = a_min.max(b_min);
            let wrap_max = a_max.min(b_max);
            let wrap_span = (180.0 - wrap_min) + (wrap_max + 180.0);
            if wrap_span < simple_span {
                (wrap_min, wrap_max)
            } else {
                (simple_min, simple_max)
            }
        }
        (true, false) => {
            if b_min >= a_min || b_max <= a_max {
                (a_min, a_max)
            } else {
                let extend_east = a_min - b_min;
                let extend_west = b_max - a_max;
                if extend_east <= extend_west {
                    (b_min, a_max)
                } else {
                    (a_min, b_max)
                }
            }
        }
        (false, true) => union_longitude(b_min, b_max, a_min, a_max),
        (true, true) => (a_min.min(b_min), a_max.max(b_max)),
    }
}

/// Compute bounding box for any Geometry in geographic space. Same as
/// `geometry_bbox_in(geom, BboxSpace::Geographic)`.
pub fn geometry_bbox(geom: &Geometry) -> BoundingBox {
    geometry_bbox_in(geom, BboxSpace::Geographic)
}

/// Compute bounding box for any Geometry in `space`.
///
/// `space` decides how the members of a GeometryCollection combine.
pub fn geometry_bbox_in(geom: &Geometry, space: BboxSpace) -> BoundingBox {
    match geom {
        Geometry::Point { coordinates } => BoundingBox::from_point(coordinates[0], coordinates[1]),
        Geometry::LineString { coordinates } => bbox_from_coords(coordinates),
        Geometry::Polygon { coordinates } => {
            let all: Vec<[f64; 2]> = coordinates.iter().flat_map(|r| r.iter().copied()).collect();
            bbox_from_coords(&all)
        }
        Geometry::MultiPoint { coordinates } => bbox_from_coords(coordinates),
        Geometry::MultiLineString { coordinates } => {
            let all: Vec<[f64; 2]> = coordinates
                .iter()
                .flat_map(|ls| ls.iter().copied())
                .collect();
            bbox_from_coords(&all)
        }
        Geometry::MultiPolygon { coordinates } => {
            let all: Vec<[f64; 2]> = coordinates
                .iter()
                .flat_map(|poly| poly.iter().flat_map(|ring| ring.iter().copied()))
                .collect();
            bbox_from_coords(&all)
        }
        Geometry::GeometryCollection { geometries } => {
            if geometries.is_empty() {
                return BoundingBox::new(0.0, 0.0, 0.0, 0.0);
            }
            let mut result = geometry_bbox_in(&geometries[0], space);
            for geom in &geometries[1..] {
                result = result.union_in(&geometry_bbox_in(geom, space), space);
            }
            result
        }
    }
}

fn bbox_from_coords(coords: &[[f64; 2]]) -> BoundingBox {
    if coords.is_empty() {
        return BoundingBox::new(0.0, 0.0, 0.0, 0.0);
    }
    let mut min_lng = coords[0][0];
    let mut min_lat = coords[0][1];
    let mut max_lng = coords[0][0];
    let mut max_lat = coords[0][1];
    for c in &coords[1..] {
        min_lng = min_lng.min(c[0]);
        min_lat = min_lat.min(c[1]);
        max_lng = max_lng.max(c[0]);
        max_lat = max_lat.max(c[1]);
    }
    BoundingBox {
        min_lng,
        min_lat,
        max_lng,
        max_lat,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_bbox() {
        let bb = geometry_bbox(&Geometry::point(10.0, 20.0));
        assert_eq!(bb.min_lng, 10.0);
        assert_eq!(bb.max_lng, 10.0);
        assert_eq!(bb.area(), 0.0);
    }

    #[test]
    fn polygon_bbox() {
        let poly = Geometry::polygon(vec![vec![
            [-10.0, -5.0],
            [10.0, -5.0],
            [10.0, 5.0],
            [-10.0, 5.0],
            [-10.0, -5.0],
        ]]);
        let bb = geometry_bbox(&poly);
        assert_eq!(bb.min_lng, -10.0);
        assert_eq!(bb.max_lng, 10.0);
    }

    #[test]
    fn contains_point_normal() {
        let bb = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        assert!(bb.contains_point(5.0, 5.0));
        assert!(!bb.contains_point(11.0, 5.0));
    }

    #[test]
    fn contains_point_antimeridian() {
        let bb = BoundingBox::new(170.0, -10.0, -170.0, 10.0);
        assert!(bb.crosses_antimeridian());
        assert!(bb.contains_point(175.0, 0.0));
        assert!(bb.contains_point(-175.0, 0.0));
        assert!(!bb.contains_point(0.0, 0.0));
    }

    #[test]
    fn intersects_normal() {
        let a = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        let b = BoundingBox::new(5.0, 5.0, 15.0, 15.0);
        assert!(a.intersects(&b));
        let c = BoundingBox::new(20.0, 20.0, 30.0, 30.0);
        assert!(!a.intersects(&c));
    }

    #[test]
    fn intersects_antimeridian() {
        let a = BoundingBox::new(170.0, -5.0, -170.0, 5.0);
        let b = BoundingBox::new(175.0, 0.0, -175.0, 3.0);
        assert!(a.intersects(&b));
        let c = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        assert!(!a.intersects(&c));
    }

    #[test]
    fn union_normal() {
        let a = BoundingBox::new(0.0, 0.0, 5.0, 5.0);
        let b = BoundingBox::new(3.0, 3.0, 10.0, 10.0);
        let u = a.union(&b);
        assert_eq!(u.min_lng, 0.0);
        assert_eq!(u.max_lng, 10.0);
    }

    /// Two disjoint metre boxes far past ±180 must not be read as longitude
    /// and wrapped across the anti-meridian.
    #[test]
    fn planar_union_of_disjoint_metre_boxes_does_not_wrap() {
        let a = BoundingBox::new(1e5, 0.0, 2e5, 10.0);
        let b = BoundingBox::new(7e5, 0.0, 8e5, 10.0);
        let u = a.union_in(&b, BboxSpace::Planar);
        assert_eq!((u.min_lng, u.max_lng), (1e5, 8e5));
        assert!(!u.crosses_antimeridian());
        assert!((a.enlargement_in(&b, BboxSpace::Planar) - (7e5 - 1e5) * 10.0).abs() < 1e-6);
    }

    #[test]
    fn overlap_area_partial() {
        let a = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        let b = BoundingBox::new(5.0, 5.0, 15.0, 15.0);
        assert!((a.overlap_area(&b) - 25.0).abs() < 1e-10);
    }

    #[test]
    fn geometry_collection_bbox() {
        let gc = Geometry::GeometryCollection {
            geometries: vec![Geometry::point(0.0, 0.0), Geometry::point(10.0, 10.0)],
        };
        let bb = geometry_bbox(&gc);
        assert_eq!(bb.min_lng, 0.0);
        assert_eq!(bb.max_lng, 10.0);
    }
}
