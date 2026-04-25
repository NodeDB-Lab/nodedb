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
        }
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
        Ok(ArraySchema {
            name: self.name,
            dims: self.dims,
            attrs: self.attrs,
            tile_extents: self.tile_extents,
            cell_order: self.cell_order,
            tile_order: self.tile_order,
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
