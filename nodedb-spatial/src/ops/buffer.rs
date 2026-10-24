// SPDX-License-Identifier: Apache-2.0

//! `ST_Buffer` for every geometry kind, by a distance in metres.
//!
//! A projected geometry buffers in its grid, the distance converted to the
//! CRS unit. A geographic geometry buffers in a local projection on its own
//! ellipsoid, then maps back to degrees:
//!
//! - an extent of at most 12° of longitude, away from the poles: Transverse
//!   Mercator centred on the extent (scale 1 on the central meridian, error
//!   under 0.6 % at its edge);
//! - an extent reaching past 80° latitude: polar Lambert azimuthal
//!   equal-area;
//! - any other extent: each component buffers in its own local projection
//!   and the pieces union in degrees.
//!
//! Curves use round joins and caps with `segments` sides per full circle.

use geo::algorithm::buffer::{BufferStyle, LineCap, LineJoin};
use geo::{Buffer, MultiPolygon};
use nodedb_crs::method::{LaeaParams, ProjectionDef, Projector, TmParams};
use nodedb_crs::{CrsDef, CrsError, CrsKind, DomainReason, Srid};
use nodedb_types::geometry::{Geometry, GeometryKind};

use super::convert::{from_geo, multipolygon, simplest, to_geo};
use crate::error::SpatialError;

/// Widest longitude extent, degrees, buffered in one Transverse Mercator.
const MAX_TM_SPAN_DEG: f64 = 12.0;

/// Latitude, degrees, past which the polar projection is used.
const POLAR_LAT_DEG: f64 = 80.0;

/// Metres per degree of latitude, rounded down, to widen an extent by the
/// buffer distance.
const METRES_PER_DEGREE_MIN: f64 = 110_000.0;

/// Buffer `geom` (coordinates in `def`) by `metres`. A negative distance
/// shrinks polygons and erases points and lines. An EMPTY input or result
/// is `POLYGON EMPTY`.
pub fn buffer(
    geom: &Geometry,
    metres: f64,
    segments: usize,
    def: &CrsDef,
) -> Result<Geometry, SpatialError> {
    if geom.is_empty() {
        return Ok(Geometry::empty(GeometryKind::Polygon));
    }
    let style = || {
        let angle = std::f64::consts::TAU / segments.max(3) as f64;
        move |distance: f64| {
            BufferStyle::new(distance)
                .line_join(LineJoin::Round(angle))
                .line_cap(LineCap::Round(angle))
        }
    };
    let out = match def.kind {
        CrsKind::Projected => {
            let units = metres / def.unit.metres_per_unit;
            to_geo(geom).buffer_with_style(style()(units))
        }
        CrsKind::Geographic => geographic(geom, metres, def, &style())?,
    };
    Ok(finish(&out))
}

fn finish(out: &MultiPolygon<f64>) -> Geometry {
    let g = multipolygon(out);
    if g.is_empty() {
        Geometry::empty(GeometryKind::Polygon)
    } else {
        simplest(g)
    }
}

fn geographic(
    geom: &Geometry,
    metres: f64,
    def: &CrsDef,
    style: &dyn Fn(f64) -> BufferStyle<f64>,
) -> Result<MultiPolygon<f64>, SpatialError> {
    let extent = Extent::of(geom, metres);
    if let Some(local) = Local::choose(&extent, def) {
        return local.buffer(geom, metres, style);
    }
    let mut pieces: Vec<geo::Polygon<f64>> = Vec::new();
    for component in components(geom) {
        let extent = Extent::of(&component, metres);
        let local = Local::choose(&extent, def).unwrap_or_else(|| Local::laea(&extent, def));
        pieces.extend(local.buffer(&component, metres, style)?.0);
    }
    Ok(geo::unary_union(&pieces))
}

/// Every point, line and polygon of a geometry.
fn components(geom: &Geometry) -> Vec<Geometry> {
    match geom {
        Geometry::MultiPoint { coordinates } => coordinates
            .iter()
            .map(|c| Geometry::point(c[0], c[1]))
            .collect(),
        Geometry::MultiLineString { coordinates } => coordinates
            .iter()
            .map(|l| Geometry::line_string(l.clone()))
            .collect(),
        Geometry::MultiPolygon { coordinates } => coordinates
            .iter()
            .map(|p| Geometry::polygon(p.clone()))
            .collect(),
        Geometry::GeometryCollection { geometries } => {
            geometries.iter().flat_map(components).collect()
        }
        other if other.is_empty() => Vec::new(),
        other => vec![other.clone()],
    }
}

/// Longitude extent unwrapped around the first vertex, and latitude
/// extent, both widened by the buffer distance.
struct Extent {
    min_lon: f64,
    max_lon: f64,
    min_lat: f64,
    max_lat: f64,
}

impl Extent {
    fn of(geom: &Geometry, metres: f64) -> Self {
        let mut coords = geom.coords();
        let first = coords.next().unwrap_or([0.0, 0.0]);
        let mut e = Extent {
            min_lon: first[0],
            max_lon: first[0],
            min_lat: first[1],
            max_lat: first[1],
        };
        for [lon, lat] in coords {
            let lon = unwrap(lon, first[0]);
            e.min_lon = e.min_lon.min(lon);
            e.max_lon = e.max_lon.max(lon);
            e.min_lat = e.min_lat.min(lat);
            e.max_lat = e.max_lat.max(lat);
        }
        let pad = metres.abs() / METRES_PER_DEGREE_MIN;
        e.min_lat = (e.min_lat - pad).max(-90.0);
        e.max_lat = (e.max_lat + pad).min(90.0);
        let poleward = e.min_lat.abs().max(e.max_lat.abs()).min(89.0);
        let lon_pad = pad / poleward.to_radians().cos();
        e.min_lon -= lon_pad;
        e.max_lon += lon_pad;
        e
    }

    fn centre(&self) -> [f64; 2] {
        [
            (self.min_lon + self.max_lon) / 2.0,
            (self.min_lat + self.max_lat) / 2.0,
        ]
    }
}

/// `lon` moved by whole turns into `reference ± 180°`.
fn unwrap(lon: f64, reference: f64) -> f64 {
    lon - 360.0 * ((lon - reference) / 360.0).round()
}

/// A local projection on the CRS ellipsoid.
struct Local {
    projector: Projector,
    /// Longitude output is unwrapped around this, so a buffer across the
    /// antimeridian stays one continuous ring.
    centre_lon: f64,
    srid: Srid,
}

impl Local {
    /// The projection for `extent`, or `None` when it is too wide for one.
    fn choose(extent: &Extent, def: &CrsDef) -> Option<Self> {
        if extent.max_lat > POLAR_LAT_DEG || extent.min_lat < -POLAR_LAT_DEG {
            let pole = if extent.max_lat > POLAR_LAT_DEG {
                90.0
            } else {
                -90.0
            };
            let both = extent.max_lat > POLAR_LAT_DEG && extent.min_lat < -POLAR_LAT_DEG;
            return (!both).then(|| Self::polar(pole, extent, def));
        }
        if extent.max_lon - extent.min_lon > MAX_TM_SPAN_DEG {
            return None;
        }
        let [lon0, _] = extent.centre();
        let params = TmParams {
            lat0_deg: 0.0,
            lon0_deg: normalize(lon0),
            k0: 1.0,
            false_easting: 0.0,
            false_northing: 0.0,
        };
        Some(Self::new(
            ProjectionDef::TransverseMercator(params),
            lon0,
            def,
        ))
    }

    fn polar(lat0: f64, extent: &Extent, def: &CrsDef) -> Self {
        let [lon0, _] = extent.centre();
        let params = LaeaParams {
            lat0_deg: lat0,
            lon0_deg: normalize(lon0),
            false_easting: 0.0,
            false_northing: 0.0,
        };
        Self::new(ProjectionDef::LambertAzimuthalEqualArea(params), lon0, def)
    }

    /// Lambert azimuthal equal-area centred on `extent`, for a component
    /// too wide for Transverse Mercator.
    fn laea(extent: &Extent, def: &CrsDef) -> Self {
        let [lon0, lat0] = extent.centre();
        let params = LaeaParams {
            lat0_deg: lat0,
            lon0_deg: normalize(lon0),
            false_easting: 0.0,
            false_northing: 0.0,
        };
        Self::new(ProjectionDef::LambertAzimuthalEqualArea(params), lon0, def)
    }

    fn new(projection: ProjectionDef, centre_lon: f64, def: &CrsDef) -> Self {
        Self {
            projector: projection.prepare(&def.datum.ellipsoid),
            centre_lon,
            srid: def.srid,
        }
    }

    fn buffer(
        &self,
        geom: &Geometry,
        metres: f64,
        style: &dyn Fn(f64) -> BufferStyle<f64>,
    ) -> Result<MultiPolygon<f64>, SpatialError> {
        let projected = geom.try_map_coords(&mut |[x, y]| {
            self.projector
                .forward(x.to_radians(), y.to_radians())
                .map(|(e, n)| [e, n])
                .map_err(|reason| self.fault(x, y, reason))
        })?;
        let buffered = from_geo(&geo::Geometry::MultiPolygon(
            to_geo(&projected).buffer_with_style(style(metres)),
        ));
        let back = buffered.try_map_coords(&mut |[e, n]| {
            self.projector
                .inverse(e, n)
                .map(|(lam, phi)| [unwrap(lam.to_degrees(), self.centre_lon), phi.to_degrees()])
                .map_err(|reason| self.fault(e, n, reason))
        })?;
        let Geometry::MultiPolygon { coordinates } = back else {
            return Ok(MultiPolygon::new(Vec::new()));
        };
        Ok(MultiPolygon::new(
            coordinates
                .iter()
                .map(|p| super::convert::polygon(p))
                .collect(),
        ))
    }

    fn fault(&self, x: f64, y: f64, reason: DomainReason) -> SpatialError {
        SpatialError::Crs(CrsError::OutOfDomain {
            srid: self.srid,
            x,
            y,
            reason,
        })
    }
}

/// Longitude in [−180°, 180°).
fn normalize(lon: f64) -> f64 {
    (lon + 180.0).rem_euclid(360.0) - 180.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric::Metric;
    use nodedb_crs::CrsContext;

    fn def(code: u32) -> std::sync::Arc<CrsDef> {
        CrsContext::builtin_only()
            .resolve(Srid::new(code).unwrap())
            .unwrap()
    }

    /// A geographic point buffer is a geodesic circle: every vertex lies
    /// the buffer distance from the centre, measured on the ellipsoid. A
    /// 64-gon vertex sits at most 0.13 % off the circle.
    #[test]
    fn geographic_point_buffer_is_a_geodesic_circle() {
        let wgs84 = def(4326);
        let out = buffer(&Geometry::point(101.7, 3.1), 1_000.0, 64, &wgs84).unwrap();
        let Geometry::Polygon { coordinates } = &out else {
            panic!("{out:?}");
        };
        let m = Metric::wgs84();
        for c in &coordinates[0] {
            let d = m.segment_length([101.7, 3.1], *c);
            assert!((d - 1_000.0).abs() < 2.0, "{d}");
        }
    }

    #[test]
    fn multi_geometries_buffer_every_member() {
        let wgs84 = def(4326);
        let far = Geometry::MultiPoint {
            coordinates: vec![[0.0, 0.0], [100.0, 10.0]],
        };
        let out = buffer(&far, 500.0, 32, &wgs84).unwrap();
        assert!(
            matches!(&out, Geometry::MultiPolygon { coordinates } if coordinates.len() == 2),
            "{out:?}"
        );
        let collection = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::point(0.0, 0.0),
                Geometry::line_string(vec![[0.01, 0.0], [0.02, 0.0]]),
            ],
        };
        assert!(!buffer(&collection, 100.0, 32, &wgs84).unwrap().is_empty());
    }

    #[test]
    fn polar_point_buffers_in_a_polar_projection() {
        let wgs84 = def(4326);
        let out = buffer(&Geometry::point(0.0, 89.9), 1_000.0, 32, &wgs84).unwrap();
        let m = Metric::wgs84();
        let Geometry::Polygon { coordinates } = &out else {
            panic!("{out:?}");
        };
        for c in &coordinates[0] {
            let d = m.segment_length([0.0, 89.9], *c);
            assert!((d - 1_000.0).abs() < 10.0, "{d}");
        }
    }

    #[test]
    fn projected_buffer_uses_the_crs_unit() {
        let feet = def(2263);
        let out = buffer(&Geometry::point(1_000_000.0, 200_000.0), 100.0, 64, &feet).unwrap();
        let Geometry::Polygon { coordinates } = &out else {
            panic!("{out:?}");
        };
        let r = (coordinates[0][0][0] - 1_000_000.0).hypot(coordinates[0][0][1] - 200_000.0);
        assert!((r - 100.0 / (1200.0 / 3937.0)).abs() < 1.0, "{r}");
    }

    #[test]
    fn negative_distance_erases_a_point() {
        let out = buffer(&Geometry::point(0.0, 0.0), -10.0, 32, &def(4326)).unwrap();
        assert!(out.is_empty());
    }
}
