// SPDX-License-Identifier: Apache-2.0

//! A geometry tagged with its spatial reference system and its Z / M
//! ordinates.

use nodedb_srid::{SridResolver, UnknownSrid};

use super::super::ordinates::{Dimension, OrdinateError, Ordinates, Position};
use super::super::shape::Geometry;
use super::super::srid::Srid;
use super::crs_member::srid_from_crs;
use super::geojson_read::{GeoJsonError, decode_stored_tree, parse_geojson, read_geojson_value};
use crate::bbox::{BboxSpace, BoundingBox, geometry_bbox_in};

/// A geometry, the SRID its coordinates are in, and its Z / M ordinates.
///
/// The shape and the ordinates are private so they always agree: Z and M
/// hold one finite value per XY coordinate, in traversal order.
#[derive(Debug, Clone, PartialEq)]
pub struct SridGeometry {
    pub srid: Srid,
    geometry: Geometry,
    ordinates: Ordinates,
}

impl SridGeometry {
    /// An XY geometry.
    pub fn new(srid: Srid, geometry: Geometry) -> Self {
        Self {
            srid,
            geometry,
            ordinates: Ordinates::xy(),
        }
    }

    /// A geometry with Z and / or M. Fails when an array does not hold one
    /// finite value per coordinate.
    pub fn with_ordinates(
        srid: Srid,
        geometry: Geometry,
        ordinates: Ordinates,
    ) -> Result<Self, OrdinateError> {
        ordinates.check(geometry.coord_count())?;
        Ok(Self {
            srid,
            geometry,
            ordinates,
        })
    }

    /// An XY geometry in WGS 84, the SRID of untagged GeoJSON.
    pub fn wgs84(geometry: Geometry) -> Self {
        Self::new(Srid::WGS84, geometry)
    }

    pub fn geometry(&self) -> &Geometry {
        &self.geometry
    }

    pub fn ordinates(&self) -> &Ordinates {
        &self.ordinates
    }

    pub fn dimension(&self) -> Dimension {
        self.ordinates.dimension()
    }

    pub fn has_z(&self) -> bool {
        self.dimension().has_z()
    }

    pub fn has_m(&self) -> bool {
        self.dimension().has_m()
    }

    /// Whether the geometry has no coordinates (OGC EMPTY).
    pub fn is_empty(&self) -> bool {
        self.geometry.is_empty()
    }

    pub fn into_parts(self) -> (Srid, Geometry, Ordinates) {
        (self.srid, self.geometry, self.ordinates)
    }

    /// The same geometry under another SRID label.
    pub fn with_srid(mut self, srid: Srid) -> Self {
        self.srid = srid;
        self
    }

    /// The same SRID around a derived XY shape. A derived shape (buffer,
    /// hull, envelope) has no Z or M of its own.
    pub fn with_geometry(&self, geometry: Geometry) -> Self {
        Self::new(self.srid, geometry)
    }

    /// The same shape with Z and M dropped (`ST_Force2D`).
    pub fn force_2d(&self) -> Self {
        Self::new(self.srid, self.geometry.clone())
    }

    /// The same shape in `dimension`. An ordinate the geometry lacks is
    /// filled with `fill_z` or `fill_m`. An ordinate `dimension` lacks is
    /// dropped. Fails when a fill value is not finite.
    pub fn force_dimension(
        &self,
        dimension: Dimension,
        fill_z: f64,
        fill_m: f64,
    ) -> Result<Self, OrdinateError> {
        let n = self.geometry.coord_count();
        let pick = |want: bool, have: Option<&[f64]>, fill: f64| {
            want.then(|| have.map_or_else(|| vec![fill; n], <[f64]>::to_vec))
        };
        let ordinates = Ordinates::new(
            pick(dimension.has_z(), self.ordinates.z(), fill_z),
            pick(dimension.has_m(), self.ordinates.m(), fill_m),
        );
        Self::with_ordinates(self.srid, self.geometry.clone(), ordinates)
    }

    /// Every coordinate with its Z and M, in traversal order.
    pub fn positions(&self) -> impl Iterator<Item = Position> + '_ {
        self.geometry.coords().enumerate().map(|(index, [x, y])| {
            let (z, m) = self.ordinates.at(index);
            Position { x, y, z, m }
        })
    }

    /// Map every position through `f`, keeping the shape, the SRID and the
    /// dimension. A Z or M `f` returns as `None` keeps the input value.
    ///
    /// Stops at the first position `f` rejects and returns its error.
    pub fn try_map_positions<E>(
        &self,
        mut f: impl FnMut(Position) -> Result<Position, E>,
    ) -> Result<SridGeometry, E> {
        let dimension = self.dimension();
        let n = self.geometry.coord_count();
        let mut z_out = dimension.has_z().then(|| Vec::with_capacity(n));
        let mut m_out = dimension.has_m().then(|| Vec::with_capacity(n));
        let mut index = 0usize;
        let ordinates = &self.ordinates;
        let geometry = self.geometry.try_map_coords(&mut |[x, y]| {
            let (z, m) = ordinates.at(index);
            index += 1;
            let p = f(Position { x, y, z, m })?;
            if let (Some(out), Some(value)) = (z_out.as_mut(), p.z.or(z)) {
                out.push(value);
            }
            if let (Some(out), Some(value)) = (m_out.as_mut(), p.m.or(m)) {
                out.push(value);
            }
            Ok([p.x, p.y])
        })?;
        Ok(Self {
            srid: self.srid,
            geometry,
            ordinates: Ordinates::new(z_out, m_out),
        })
    }

    /// Bounding box in the coordinate space of this SRID. Fails when `ctx`
    /// does not define the SRID.
    pub fn bbox(&self, ctx: &dyn SridResolver) -> Result<BoundingBox, UnknownSrid> {
        let space = BboxSpace::of(ctx, self.srid)?;
        Ok(geometry_bbox_in(&self.geometry, space))
    }

    /// Read a geometry out of a stored document field value.
    ///
    /// Accepts the three stored forms: a native `Value::Geometry`, a GeoJSON
    /// string, and a GeoJSON object decoded from a document. A value that is
    /// not a well-formed geometry gives `Ok(None)`. A geometry whose `crs`
    /// member is not a supported form, or names an SRID `ctx` does not
    /// define, gives `Err`.
    pub fn from_value(
        value: &crate::Value,
        ctx: &dyn SridResolver,
    ) -> Result<Option<SridGeometry>, UnknownSrid> {
        let parsed = match value {
            crate::Value::Geometry(g) => {
                ctx.resolve_u32(g.srid.code())?;
                return Ok(Some(g.clone()));
            }
            crate::Value::String(s) => parse_geojson(s, ctx),
            crate::Value::Object(_) => geojson_object(value, ctx),
            _ => return Ok(None),
        };
        match parsed {
            Ok(g) => Ok(Some(g)),
            Err(GeoJsonError::Srid(e)) => Err(e),
            Err(
                GeoJsonError::NotGeometry
                | GeoJsonError::Malformed { .. }
                | GeoJsonError::Ordinates(_),
            ) => Ok(None),
        }
    }
}

/// Parse a GeoJSON object held in a document `Value`. The `crs` member
/// must resolve in `ctx`.
pub fn geojson_object(
    value: &crate::Value,
    ctx: &dyn SridResolver,
) -> Result<SridGeometry, GeoJsonError> {
    let json = serde_json::Value::from(value.clone());
    let parts = read_geojson_value(&json)?;
    let srid = srid_from_crs(parts.crs.as_ref(), ctx)?;
    Ok(SridGeometry::with_ordinates(
        srid,
        parts.geometry,
        parts.ordinates,
    )?)
}

/// Decode a stored GeoJSON object held in a document `Value`. The `crs`
/// name is checked for form and code range only.
pub fn stored_geojson_object(value: &crate::Value) -> Result<SridGeometry, GeoJsonError> {
    decode_stored_tree(&serde_json::Value::from(value.clone()))
}

impl From<Geometry> for SridGeometry {
    fn from(geometry: Geometry) -> Self {
        Self::wgs84(geometry)
    }
}

/// A total, deterministic text form. A WGS 84 geometry renders as its
/// shape. Any other SRID gets an EWKT-style `SRID=<code>;` prefix. Z and M
/// follow the shape when present.
impl std::fmt::Display for SridGeometry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.srid != Srid::WGS84 {
            write!(f, "SRID={};", self.srid.code())?;
        }
        write!(f, "{:?}", self.geometry)?;
        if let Some(z) = self.ordinates.z() {
            write!(f, " Z={z:?}")?;
        }
        if let Some(m) = self.ordinates.m() {
            write!(f, " M={m:?}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::ordinates::Ordinate;
    use super::*;

    fn srid(code: u32) -> Srid {
        Srid::new(code).unwrap()
    }

    fn ctx() -> nodedb_crs::CrsContext<'static> {
        nodedb_crs::CrsContext::builtin_only()
    }

    fn line_z() -> SridGeometry {
        SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]),
            Ordinates::new(Some(vec![10.0, 20.0]), None),
        )
        .unwrap()
    }

    #[test]
    fn with_ordinates_checks_lengths() {
        let err = SridGeometry::with_ordinates(
            Srid::WGS84,
            Geometry::point(1.0, 2.0),
            Ordinates::new(Some(vec![1.0, 2.0]), None),
        );
        assert_eq!(
            err,
            Err(OrdinateError::LengthMismatch {
                ordinate: Ordinate::Z,
                expected: 1,
                actual: 2
            })
        );
    }

    #[test]
    fn positions_pair_xy_with_z_and_m() {
        let g = line_z();
        let p: Vec<Position> = g.positions().collect();
        assert_eq!(p[1].z, Some(20.0));
        assert_eq!(p[1].m, None);
        assert_eq!(g.dimension(), Dimension::Xyz);
    }

    #[test]
    fn derived_shape_drops_z() {
        let g = line_z();
        assert_eq!(
            g.with_geometry(Geometry::point(0.0, 0.0)).dimension(),
            Dimension::Xy
        );
        assert_eq!(g.force_2d().dimension(), Dimension::Xy);
    }

    #[test]
    fn force_dimension_fills_and_drops() {
        let g = line_z();
        let zm = g.force_dimension(Dimension::Xyzm, 0.0, 5.0).unwrap();
        assert_eq!(zm.ordinates().z(), Some(&[10.0, 20.0][..]));
        assert_eq!(zm.ordinates().m(), Some(&[5.0, 5.0][..]));
        let m = g.force_dimension(Dimension::Xym, 0.0, 0.0).unwrap();
        assert_eq!(m.ordinates().z(), None);
    }

    #[test]
    fn try_map_positions_keeps_dimension() {
        let g = line_z();
        let moved = g
            .try_map_positions(|p| {
                Ok::<_, ()>(Position {
                    x: p.x + 1.0,
                    z: p.z.map(|z| z * 2.0),
                    ..p
                })
            })
            .unwrap();
        assert_eq!(moved.ordinates().z(), Some(&[20.0, 40.0][..]));
        assert_eq!(
            moved.geometry(),
            &Geometry::line_string(vec![[1.0, 0.0], [2.0, 1.0]])
        );
    }

    #[test]
    fn from_value_reads_every_stored_form() {
        let ctx = ctx();
        let g = SridGeometry::new(srid(32647), Geometry::point(1.0, 2.0));
        let text = sonic_rs::to_string(&g).unwrap();
        assert_eq!(
            SridGeometry::from_value(&crate::Value::Geometry(g.clone()), &ctx),
            Ok(Some(g.clone()))
        );
        assert_eq!(
            SridGeometry::from_value(&crate::Value::String(text.clone()), &ctx),
            Ok(Some(g.clone()))
        );
        let json: serde_json::Value = sonic_rs::from_str(&text).unwrap();
        let obj = crate::Value::from(json);
        assert_eq!(SridGeometry::from_value(&obj, &ctx), Ok(Some(g)));
        assert_eq!(
            SridGeometry::from_value(&crate::Value::Integer(1), &ctx),
            Ok(None)
        );
    }

    #[test]
    fn from_value_refuses_an_undefined_srid() {
        let g = SridGeometry::new(srid(999), Geometry::point(1.0, 2.0));
        assert_eq!(
            SridGeometry::from_value(&crate::Value::Geometry(g), &ctx()),
            Err(UnknownSrid::Undefined { code: 999 })
        );
    }

    /// A geometry with an unsupported `crs` member is an error, never
    /// "not a geometry".
    #[test]
    fn from_value_refuses_an_unsupported_crs_member() {
        let ctx = ctx();
        let linked = r#"{"type":"Point","coordinates":[1.0,2.0],"crs":{"type":"link","properties":{"href":"x"}}}"#;
        assert!(matches!(
            SridGeometry::from_value(&crate::Value::String(linked.into()), &ctx),
            Err(UnknownSrid::InvalidCrsMember { .. })
        ));
        assert_eq!(
            SridGeometry::from_value(&crate::Value::String("not json".into()), &ctx),
            Ok(None)
        );
    }

    #[test]
    fn bbox_space_follows_the_crs_kind() {
        let ctx = ctx();
        let projected = SridGeometry::new(srid(32650), Geometry::point(5e5, 6e5));
        assert!(projected.bbox(&ctx).is_ok());
        let undefined = SridGeometry::new(srid(999), Geometry::point(1.0, 2.0));
        assert_eq!(
            undefined.bbox(&ctx),
            Err(UnknownSrid::Undefined { code: 999 })
        );
    }

    #[test]
    fn display_prefixes_non_wgs84_and_appends_z() {
        let g = SridGeometry::new(srid(32648), Geometry::point(1.0, 2.0));
        assert_eq!(
            g.to_string(),
            "SRID=32648;Point { coordinates: [1.0, 2.0] }"
        );
        assert_eq!(
            SridGeometry::wgs84(Geometry::point(1.0, 2.0)).to_string(),
            "Point { coordinates: [1.0, 2.0] }"
        );
        assert!(line_z().to_string().ends_with(" Z=[10.0, 20.0]"));
    }
}
