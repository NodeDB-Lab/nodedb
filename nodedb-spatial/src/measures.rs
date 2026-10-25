// SPDX-License-Identifier: Apache-2.0

//! Centroid of a geometry.
//!
//! The result is the centroid of the highest-dimension components present,
//! so a `GeometryCollection` of a polygon and a stray point returns the
//! polygon's centroid, not a blend of the two. Components weigh by their
//! area or length in the CRS's [`Metric`]: geodesic on a geographic CRS,
//! planar on a projected one.

use nodedb_types::geometry::Geometry;

use crate::metric::Metric;

/// Centroid of a geometry, as a `Point`.
///
/// Follows OGC dimension precedence: areal components win over linear, which
/// win over punctual. Returns `None` only when the geometry carries no
/// vertices at all. A winning dimension whose every component is degenerate
/// (zero area or zero length) falls back to the next dimension down.
pub fn st_centroid(geom: &Geometry, metric: &Metric) -> Option<Geometry> {
    let [x, y] = centroid_coords(geom, metric)?;
    Some(Geometry::point(x, y))
}

/// Unsigned area of a ring, square metres.
fn ring_weight(ring: &[[f64; 2]], metric: &Metric) -> f64 {
    metric.ring_area(ring)
}

/// Length of one segment, metres.
fn segment_weight(a: [f64; 2], b: [f64; 2], metric: &Metric) -> f64 {
    metric.segment_length(a, b)
}

/// A weighted point accumulator: `(Σ w·lng, Σ w·lat, Σ w)`.
#[derive(Default, Clone, Copy)]
struct Weighted {
    lng: f64,
    lat: f64,
    weight: f64,
}

impl Weighted {
    fn add(&mut self, point: [f64; 2], weight: f64) {
        self.lng += point[0] * weight;
        self.lat += point[1] * weight;
        self.weight += weight;
    }

    fn merge(&mut self, other: Self) {
        self.lng += other.lng;
        self.lat += other.lat;
        self.weight += other.weight;
    }

    fn resolve(self) -> Option<[f64; 2]> {
        if self.weight > 0.0 {
            Some([self.lng / self.weight, self.lat / self.weight])
        } else {
            None
        }
    }
}

fn centroid_coords(geom: &Geometry, metric: &Metric) -> Option<[f64; 2]> {
    areal_centroid(geom, metric)
        .and_then(Weighted::resolve)
        .or_else(|| linear_centroid(geom, metric).and_then(Weighted::resolve))
        .or_else(|| punctual_centroid(geom).and_then(Weighted::resolve))
}

/// Area-weighted centroid of every areal component, or `None` if there are no
/// areal components at all.
fn areal_centroid(geom: &Geometry, metric: &Metric) -> Option<Weighted> {
    match geom {
        Geometry::Polygon { coordinates } => Some(polygon_centroid(coordinates, metric)),
        Geometry::MultiPolygon { coordinates } => {
            let mut acc = Weighted::default();
            for rings in coordinates {
                acc.merge(polygon_centroid(rings, metric));
            }
            Some(acc)
        }
        Geometry::GeometryCollection { geometries } => collect(geometries, metric, areal_centroid),
        Geometry::Point { .. }
        | Geometry::LineString { .. }
        | Geometry::MultiPoint { .. }
        | Geometry::MultiLineString { .. } => None,
    }
}

/// Length-weighted centroid of every linear component.
fn linear_centroid(geom: &Geometry, metric: &Metric) -> Option<Weighted> {
    match geom {
        Geometry::LineString { coordinates } => Some(line_centroid(coordinates, metric)),
        Geometry::MultiLineString { coordinates } => {
            let mut acc = Weighted::default();
            for line in coordinates {
                acc.merge(line_centroid(line, metric));
            }
            Some(acc)
        }
        Geometry::GeometryCollection { geometries } => collect(geometries, metric, linear_centroid),
        Geometry::Point { .. }
        | Geometry::MultiPoint { .. }
        | Geometry::Polygon { .. }
        | Geometry::MultiPolygon { .. } => None,
    }
}

/// Arithmetic mean of every vertex, the last-resort dimension. Every geometry
/// contributes here, so a degenerate polygon still yields a centroid.
fn punctual_centroid(geom: &Geometry) -> Option<Weighted> {
    let mut acc = Weighted::default();
    for point in vertices(geom) {
        acc.add(point, 1.0);
    }
    Some(acc)
}

/// Merge the per-component results of `f` across a collection, yielding `None`
/// when no member contributed at that dimension.
fn collect(
    geometries: &[Geometry],
    metric: &Metric,
    f: fn(&Geometry, &Metric) -> Option<Weighted>,
) -> Option<Weighted> {
    let mut acc = Weighted::default();
    let mut any = false;
    for member in geometries {
        if let Some(part) = f(member, metric) {
            acc.merge(part);
            any = true;
        }
    }
    any.then_some(acc)
}

/// Centroid of one polygon, weighting the exterior ring positively and each
/// hole negatively so the hole's mass is removed from the result.
fn polygon_centroid(rings: &[Vec<[f64; 2]>], metric: &Metric) -> Weighted {
    let mut acc = Weighted::default();
    let mut iter = rings.iter();
    let Some(exterior) = iter.next() else {
        return acc;
    };
    if let Some(point) = ring_centroid(exterior) {
        acc.add(point, ring_weight(exterior, metric));
    }
    for hole in iter {
        if let Some(point) = ring_centroid(hole) {
            acc.add(point, -ring_weight(hole, metric));
        }
    }
    // Holes cancel at most the whole exterior. A negative total weight
    // means malformed input (holes larger than the shell), and it must not
    // flip the centroid to the far side of the polygon.
    if acc.weight <= 0.0 {
        return Weighted::default();
    }
    acc
}

/// Area-weighted centroid of a closed ring via the shoelace formula. Falls
/// back to the vertex mean when the ring encloses no area (collinear points).
fn ring_centroid(ring: &[[f64; 2]]) -> Option<[f64; 2]> {
    if ring.is_empty() {
        return None;
    }
    let mut twice_area = 0.0;
    let mut lng = 0.0;
    let mut lat = 0.0;
    for i in 0..ring.len() {
        let a = ring[i];
        let b = ring[(i + 1) % ring.len()];
        let cross = a[0] * b[1] - b[0] * a[1];
        twice_area += cross;
        lng += (a[0] + b[0]) * cross;
        lat += (a[1] + b[1]) * cross;
    }
    if twice_area.abs() < f64::EPSILON {
        let n = ring.len() as f64;
        return Some([
            ring.iter().map(|c| c[0]).sum::<f64>() / n,
            ring.iter().map(|c| c[1]).sum::<f64>() / n,
        ]);
    }
    Some([lng / (3.0 * twice_area), lat / (3.0 * twice_area)])
}

/// Length-weighted centroid of a polyline: each segment contributes its
/// midpoint weighted by its length in `metric`.
fn line_centroid(coords: &[[f64; 2]], metric: &Metric) -> Weighted {
    let mut acc = Weighted::default();
    for pair in coords.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        let length = segment_weight(a, b, metric);
        acc.add([(a[0] + b[0]) / 2.0, (a[1] + b[1]) / 2.0], length);
    }
    acc
}

/// Every vertex of a geometry, in document order.
fn vertices(geom: &Geometry) -> impl Iterator<Item = [f64; 2]> + '_ {
    geom.coords()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st_centroid(geom: &Geometry) -> Option<Geometry> {
        super::st_centroid(geom, &Metric::wgs84())
    }

    #[test]
    fn centroid_of_point_is_itself() {
        let p = Geometry::point(10.0, 20.0);
        assert_eq!(st_centroid(&p), Some(Geometry::point(10.0, 20.0)));
    }

    #[test]
    fn centroid_of_square_is_its_center() {
        let square = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [2.0, 0.0],
            [2.0, 2.0],
            [0.0, 2.0],
            [0.0, 0.0],
        ]]);
        let Some(Geometry::Point { coordinates }) = st_centroid(&square) else {
            panic!("expected a Point centroid");
        };
        assert!((coordinates[0] - 1.0).abs() < 1e-9);
        assert!((coordinates[1] - 1.0).abs() < 1e-9);
    }

    /// An L-shape's centroid sits at the area-weighted center, which the
    /// vertex mean misses.
    #[test]
    fn centroid_is_area_weighted_not_vertex_mean() {
        let l_shape = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [4.0, 0.0],
            [4.0, 1.0],
            [1.0, 1.0],
            [1.0, 4.0],
            [0.0, 4.0],
            [0.0, 0.0],
        ]]);
        let Some(Geometry::Point { coordinates }) = st_centroid(&l_shape) else {
            panic!("expected a Point centroid");
        };
        // A 4x1 arm (area 4, centroid (2, 0.5)) plus a 1x3 arm (area 3,
        // centroid (0.5, 2.5)) gives 9.5/7 ≈ 1.357 on both axes. The vertex
        // mean is 10/6 ≈ 1.667, so this discriminates the two.
        let expected = 9.5 / 7.0;
        assert!(
            (coordinates[0] - expected).abs() < 0.01 && (coordinates[1] - expected).abs() < 0.01,
            "expected ~({expected}, {expected}), got {coordinates:?}"
        );
    }

    /// A collection holding both a polygon and a distant point must report the
    /// polygon's centroid — areal dimension outranks punctual.
    #[test]
    fn centroid_of_collection_prefers_areal_components() {
        let collection = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::polygon(vec![vec![
                    [0.0, 0.0],
                    [2.0, 0.0],
                    [2.0, 2.0],
                    [0.0, 2.0],
                    [0.0, 0.0],
                ]]),
                Geometry::point(100.0, 100.0),
            ],
        };
        let Some(Geometry::Point { coordinates }) = st_centroid(&collection) else {
            panic!("expected a Point centroid");
        };
        assert!(
            (coordinates[0] - 1.0).abs() < 1e-6,
            "stray point must not pull the centroid, got {coordinates:?}"
        );
    }

    /// A zero-area polygon has no areal mass; the centroid must fall back
    /// rather than divide by zero and vanish.
    #[test]
    fn centroid_of_degenerate_polygon_falls_back() {
        let collapsed =
            Geometry::polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [0.0, 0.0]]]);
        let centroid = st_centroid(&collapsed);
        assert!(
            centroid.is_some(),
            "a collinear ring must still yield a centroid"
        );
    }

    #[test]
    fn centroid_of_linestring_is_length_weighted() {
        // A long western segment and a short eastern one: the centroid must
        // sit inside the long segment, not at the vertex mean.
        let line = Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0], [11.0, 0.0]]);
        let Some(Geometry::Point { coordinates }) = st_centroid(&line) else {
            panic!("expected a Point centroid");
        };
        assert!(
            coordinates[0] < 6.0,
            "length weighting must bias toward the long segment, got {coordinates:?}"
        );
    }

    #[test]
    fn centroid_of_empty_geometry_is_none() {
        let empty = Geometry::GeometryCollection { geometries: vec![] };
        assert_eq!(st_centroid(&empty), None);
    }
}
