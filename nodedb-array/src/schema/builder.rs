// SPDX-License-Identifier: Apache-2.0

//! Schema construction with full validation.
//!
//! Every `ArraySchema` in the system passes through `ArraySchemaBuilder::build()`
//! so the per-domain validation rules (dim arity, tile-extent arity,
//! non-empty attrs, unique names, well-formed bounds) live in one
//! place.

use super::array_schema::ArraySchema;
use super::attr_spec::AttrSpec;
use super::cell_order::{CellOrder, TileOrder};
use super::dim_spec::DimSpec;
use super::georef::ArrayGeoref;
use super::validation;
use crate::error::ArrayResult;

/// Builder for [`ArraySchema`]. Build is fallible.
#[derive(Debug, Clone)]
pub struct ArraySchemaBuilder {
    name: String,
    dims: Vec<DimSpec>,
    attrs: Vec<AttrSpec>,
    tile_extents: Vec<u64>,
    cell_order: CellOrder,
    tile_order: TileOrder,
    georef: Option<ArrayGeoref>,
}

impl ArraySchemaBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            dims: Vec::new(),
            attrs: Vec::new(),
            tile_extents: Vec::new(),
            cell_order: CellOrder::default(),
            tile_order: TileOrder::default(),
            georef: None,
        }
    }

    pub fn georef(mut self, georef: ArrayGeoref) -> Self {
        self.georef = Some(georef);
        self
    }

    pub fn dim(mut self, dim: DimSpec) -> Self {
        self.dims.push(dim);
        self
    }

    pub fn attr(mut self, attr: AttrSpec) -> Self {
        self.attrs.push(attr);
        self
    }

    pub fn tile_extents(mut self, extents: Vec<u64>) -> Self {
        self.tile_extents = extents;
        self
    }

    pub fn cell_order(mut self, order: CellOrder) -> Self {
        self.cell_order = order;
        self
    }

    pub fn tile_order(mut self, order: TileOrder) -> Self {
        self.tile_order = order;
        self
    }

    pub fn build(self) -> ArrayResult<ArraySchema> {
        validation::dims::check(&self.name, &self.dims)?;
        validation::attrs::check(&self.name, &self.attrs)?;
        validation::tiles::check(&self.name, &self.dims, &self.tile_extents)?;
        validation::georef::check(&self.name, &self.dims, self.georef.as_ref())?;
        Ok(ArraySchema {
            name: self.name,
            dims: self.dims,
            attrs: self.attrs,
            tile_extents: self.tile_extents,
            cell_order: self.cell_order,
            tile_order: self.tile_order,
            georef: self.georef,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::attr_spec::AttrType;
    use super::super::dim_spec::DimType;
    use super::*;
    use crate::types::domain::{Domain, DomainBound};

    fn int64_dim(name: &str, hi: i64) -> DimSpec {
        DimSpec::new(
            name,
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(hi)),
        )
    }

    #[test]
    fn build_succeeds_for_well_formed_schema() {
        let s = ArraySchemaBuilder::new("g")
            .dim(int64_dim("chrom", 24))
            .dim(int64_dim("pos", 300_000_000))
            .attr(AttrSpec::new("variant", AttrType::String, false))
            .tile_extents(vec![1, 1_000_000])
            .build()
            .unwrap();
        assert_eq!(s.arity(), 2);
        assert_eq!(s.cell_order, CellOrder::Hilbert);
    }

    #[test]
    fn build_rejects_no_dims() {
        let r = ArraySchemaBuilder::new("g")
            .attr(AttrSpec::new("v", AttrType::Int64, false))
            .tile_extents(vec![])
            .build();
        assert!(r.is_err());
    }

    #[test]
    fn build_rejects_no_attrs() {
        let r = ArraySchemaBuilder::new("g")
            .dim(int64_dim("d", 10))
            .tile_extents(vec![1])
            .build();
        assert!(r.is_err());
    }

    fn float_dim(name: &str) -> DimSpec {
        DimSpec::new(
            name,
            DimType::Float64,
            Domain::new(DomainBound::Float64(-180.0), DomainBound::Float64(180.0)),
        )
    }

    fn geo_builder(x: DimSpec, y: DimSpec) -> ArraySchemaBuilder {
        ArraySchemaBuilder::new("scene")
            .dim(y)
            .dim(x)
            .attr(AttrSpec::new("b1", AttrType::UInt16, true))
            .tile_extents(vec![256, 256])
    }

    fn georef(transform: Option<[f64; 6]>) -> ArrayGeoref {
        ArrayGeoref {
            srid: nodedb_types::geometry::Srid::WGS84,
            x_dim: 1,
            y_dim: 0,
            transform: transform.map(super::super::georef::GeoTransform),
        }
    }

    const GT_4326: [f64; 6] = [
        100.5,
        8.983152841195214e-05,
        0.0,
        6.5,
        0.0,
        -8.983152841195214e-05,
    ];

    #[test]
    fn build_accepts_pixel_georef() {
        let s = geo_builder(int64_dim("col", 1023), int64_dim("row", 1023))
            .georef(georef(Some(GT_4326)))
            .build()
            .unwrap();
        assert_eq!(s.georef, Some(georef(Some(GT_4326))));
    }

    #[test]
    fn build_accepts_world_georef() {
        let s = geo_builder(float_dim("lon"), float_dim("lat"))
            .georef(georef(None))
            .build()
            .unwrap();
        assert!(s.georef.is_some());
    }

    #[test]
    fn build_refuses_each_georef_rule() {
        // Pixel mode on FLOAT64 dims.
        assert!(
            geo_builder(float_dim("lon"), float_dim("lat"))
                .georef(georef(Some(GT_4326)))
                .build()
                .is_err()
        );
        // World mode on a STRING dim.
        let s_dim = DimSpec::new(
            "s",
            DimType::String,
            Domain::new(
                DomainBound::String(String::new()),
                DomainBound::String("z".into()),
            ),
        );
        assert!(
            geo_builder(s_dim, float_dim("lat"))
                .georef(georef(None))
                .build()
                .is_err()
        );
        // Out-of-range dim index.
        let mut missing = georef(None);
        missing.x_dim = 2;
        assert!(
            geo_builder(float_dim("lon"), float_dim("lat"))
                .georef(missing)
                .build()
                .is_err()
        );
        // Same dim for both axes.
        let mut same = georef(None);
        same.y_dim = 1;
        assert!(
            geo_builder(float_dim("lon"), float_dim("lat"))
                .georef(same)
                .build()
                .is_err()
        );
        // Non-finite coefficient and singular transform.
        let mut nan = GT_4326;
        nan[0] = f64::NAN;
        for gt in [nan, [0.0, 1.0, 1.0, 0.0, 1.0, 1.0]] {
            assert!(
                geo_builder(int64_dim("col", 9), int64_dim("row", 9))
                    .georef(georef(Some(gt)))
                    .build()
                    .is_err()
            );
        }
    }

    #[test]
    fn build_rejects_extent_arity_mismatch() {
        let r = ArraySchemaBuilder::new("g")
            .dim(int64_dim("d", 10))
            .attr(AttrSpec::new("v", AttrType::Int64, false))
            .tile_extents(vec![1, 2])
            .build();
        assert!(r.is_err());
    }
}
