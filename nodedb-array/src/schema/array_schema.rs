// SPDX-License-Identifier: Apache-2.0

//! Top-level array schema.
//!
//! `ArraySchema` is the canonical descriptor every layer of the engine
//! agrees on: storage uses it for tile layout, the planner uses it for
//! slice/aggregate validation, and SQL surfaces it through DDL. It is
//! constructed via [`super::ArraySchemaBuilder`] so all invariants
//! (dim/tile-extent arity, non-empty attrs, unique names) are enforced
//! at one site.

use serde::{Deserialize, Serialize};

use super::attr_meta::AttrMeta;
use super::attr_spec::{AttrSpec, AttrType};
use super::cell_order::{CellOrder, TileOrder};
use super::dim_spec::DimSpec;
use super::georef::ArrayGeoref;
use super::validation;
use crate::error::{ArrayError, ArrayResult};

/// Full array schema.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArraySchema {
    pub name: String,
    pub dims: Vec<DimSpec>,
    pub attrs: Vec<AttrSpec>,
    /// One tile-extent per dim, same order as `dims`. The product of
    /// extents (clamped to domain size) is the cells-per-tile budget.
    pub tile_extents: Vec<u64>,
    pub cell_order: CellOrder,
    pub tile_order: TileOrder,
    /// Georeference of the array. Not part of [`Self::content_msgpack`], so
    /// it never changes `schema_hash`.
    pub georef: Option<ArrayGeoref>,
}

impl ArraySchema {
    /// This schema with `georef` in place of its current one, validated
    /// against the dims.
    pub fn with_georef(self, georef: Option<ArrayGeoref>) -> ArrayResult<Self> {
        validation::georef::check(&self.name, &self.dims, georef.as_ref())?;
        Ok(Self { georef, ..self })
    }

    pub fn arity(&self) -> usize {
        self.dims.len()
    }

    pub fn dim(&self, name: &str) -> Option<&DimSpec> {
        self.dims.iter().find(|d| d.name == name)
    }

    pub fn attr(&self, name: &str) -> Option<&AttrSpec> {
        self.attrs.iter().find(|a| a.name == name)
    }

    /// This schema with `meta` in place of attr `attr`'s metadata, checked
    /// against the attr type.
    pub fn with_attr_meta(mut self, attr: &str, meta: AttrMeta) -> ArrayResult<Self> {
        let array = self.name.clone();
        let spec = self
            .attrs
            .iter_mut()
            .find(|a| a.name == attr)
            .ok_or_else(|| ArrayError::InvalidAttr {
                array: array.clone(),
                attr: attr.to_string(),
                detail: "no such attribute".to_string(),
            })?;
        meta.check(spec.dtype)
            .map_err(|e| ArrayError::InvalidAttrMeta {
                array,
                attr: attr.to_string(),
                detail: e.to_string(),
            })?;
        spec.meta = meta;
        Ok(self)
    }

    /// MessagePack encoding of the schema's *content* fields — the parts
    /// that determine compatibility for cross-array element-wise ops.
    /// Excludes the array `name`, the georef, and every attr's metadata, so
    /// two structurally identical schemas produce identical bytes (and
    /// therefore identical `schema_hash`).
    pub fn content_msgpack(&self) -> ArrayResult<Vec<u8>> {
        let attrs: Vec<AttrContent<'_>> = self
            .attrs
            .iter()
            .map(|a| AttrContent {
                name: &a.name,
                dtype: a.dtype,
                nullable: a.nullable,
            })
            .collect();
        let payload = SchemaContent {
            dims: &self.dims,
            attrs,
            tile_extents: &self.tile_extents,
            cell_order: self.cell_order,
            tile_order: self.tile_order,
        };
        zerompk::to_msgpack_vec(&payload).map_err(|e| ArrayError::SchemaEncode {
            array: self.name.clone(),
            detail: e.to_string(),
        })
    }
}

/// Borrowed view of the `ArraySchema` fields that participate in
/// content-only hashing.
#[derive(zerompk::ToMessagePack)]
struct SchemaContent<'a> {
    dims: &'a Vec<DimSpec>,
    attrs: Vec<AttrContent<'a>>,
    tile_extents: &'a Vec<u64>,
    cell_order: CellOrder,
    tile_order: TileOrder,
}

/// The content of one attr: everything but its metadata.
#[derive(zerompk::ToMessagePack)]
struct AttrContent<'a> {
    name: &'a String,
    dtype: AttrType,
    nullable: bool,
}

#[cfg(test)]
mod tests {
    use super::super::attr_spec::AttrType;
    use super::super::dim_spec::DimType;
    use super::*;
    use crate::types::domain::{Domain, DomainBound};

    #[test]
    fn content_msgpack_excludes_name() {
        // Two distinct-named schemas with identical structural content
        // produce equal content_msgpack — this is the property
        // ARRAY_ELEMENTWISE relies on to compare arrays by shape.
        let mk = |name: &str| ArraySchema {
            name: name.into(),
            dims: vec![DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            )],
            attrs: vec![AttrSpec::new("v", AttrType::Float64, true)],
            tile_extents: vec![4],
            cell_order: CellOrder::Hilbert,
            tile_order: TileOrder::Hilbert,
            georef: None,
        };
        let a = mk("alpha");
        let b = mk("beta");
        assert_ne!(a.name, b.name);
        assert_eq!(a.content_msgpack().unwrap(), b.content_msgpack().unwrap());
    }

    #[test]
    fn schema_arity_matches_dim_count() {
        let s = ArraySchema {
            name: "g".into(),
            dims: vec![
                DimSpec::new(
                    "chrom",
                    DimType::Int64,
                    Domain::new(DomainBound::Int64(0), DomainBound::Int64(24)),
                ),
                DimSpec::new(
                    "pos",
                    DimType::Int64,
                    Domain::new(DomainBound::Int64(0), DomainBound::Int64(300_000_000)),
                ),
            ],
            attrs: vec![AttrSpec::new("variant", AttrType::String, false)],
            tile_extents: vec![1, 1_000_000],
            cell_order: CellOrder::Hilbert,
            tile_order: TileOrder::Hilbert,
            georef: None,
        };
        assert_eq!(s.arity(), 2);
        assert!(s.dim("chrom").is_some());
        assert!(s.attr("variant").is_some());
        assert!(s.dim("missing").is_none());
    }

    fn raster() -> ArraySchema {
        let dim = |name: &str| {
            DimSpec::new(
                name,
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1023)),
            )
        };
        ArraySchema {
            name: "scene".into(),
            dims: vec![dim("row"), dim("col")],
            attrs: vec![AttrSpec::new("b1", AttrType::UInt16, true)],
            tile_extents: vec![256, 256],
            cell_order: CellOrder::RowMajor,
            tile_order: TileOrder::RowMajor,
            georef: None,
        }
    }

    fn pixel_georef() -> ArrayGeoref {
        ArrayGeoref {
            srid: nodedb_types::geometry::Srid::new(32647).unwrap(),
            x_dim: 1,
            y_dim: 0,
            transform: Some(super::super::georef::GeoTransform([
                600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0,
            ])),
        }
    }

    #[test]
    fn content_msgpack_ignores_georef() {
        let plain = raster();
        let geo = raster().with_georef(Some(pixel_georef())).unwrap();
        assert_ne!(plain, geo);
        assert_eq!(
            plain.content_msgpack().unwrap(),
            geo.content_msgpack().unwrap()
        );
    }

    fn landsat_meta() -> AttrMeta {
        AttrMeta {
            nodata: Some(0.0),
            scale: Some(0.0000275),
            offset: Some(-0.2),
            units: Some("reflectance".into()),
        }
    }

    #[test]
    fn content_msgpack_ignores_attr_meta() {
        let plain = raster();
        let with_meta = raster().with_attr_meta("b1", landsat_meta()).unwrap();
        assert_ne!(plain, with_meta);
        assert_eq!(
            plain.content_msgpack().unwrap(),
            with_meta.content_msgpack().unwrap()
        );
        let dropped = with_meta.with_attr_meta("b1", AttrMeta::default()).unwrap();
        assert_eq!(dropped, plain);
    }

    #[test]
    fn attr_meta_round_trips_and_is_checked() {
        let with_meta = raster().with_attr_meta("b1", landsat_meta()).unwrap();
        let bytes = zerompk::to_msgpack_vec(&with_meta).unwrap();
        let back: ArraySchema = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back.attrs[0].meta, landsat_meta());
        let bad = AttrMeta {
            nodata: Some(-1.0),
            ..AttrMeta::default()
        };
        assert!(matches!(
            raster().with_attr_meta("b1", bad),
            Err(ArrayError::InvalidAttrMeta { .. })
        ));
        assert!(matches!(
            raster().with_attr_meta("b9", landsat_meta()),
            Err(ArrayError::InvalidAttr { .. })
        ));
    }

    #[test]
    fn content_change_changes_the_bytes() {
        let mut narrowed = raster();
        narrowed.attrs[0] = AttrSpec::new("b1", AttrType::Int8, true);
        assert_ne!(
            raster().content_msgpack().unwrap(),
            narrowed.content_msgpack().unwrap()
        );
    }

    #[test]
    fn schema_with_georef_msgpack_round_trip() {
        let geo = raster().with_georef(Some(pixel_georef())).unwrap();
        let bytes = zerompk::to_msgpack_vec(&geo).unwrap();
        let back: ArraySchema = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, geo);
        assert_eq!(back.georef, Some(pixel_georef()));
    }

    #[test]
    fn with_georef_validates_and_clears() {
        let mut bad = pixel_georef();
        bad.y_dim = 1;
        assert!(raster().with_georef(Some(bad)).is_err());
        let geo = raster().with_georef(Some(pixel_georef())).unwrap();
        let cleared = geo.with_georef(None).unwrap();
        assert_eq!(cleared, raster());
    }
}
