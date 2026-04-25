//! Top-level array schema.
//!
//! `ArraySchema` is the canonical descriptor every layer of the engine
//! agrees on: storage uses it for tile layout, the planner uses it for
//! slice/aggregate validation, and SQL surfaces it through DDL. It is
//! constructed via [`super::ArraySchemaBuilder`] so all invariants
//! (dim/tile-extent arity, non-empty attrs, unique names) are enforced
//! at one site.

use serde::{Deserialize, Serialize};

use super::attr_spec::AttrSpec;
use super::cell_order::{CellOrder, TileOrder};
use super::dim_spec::DimSpec;

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
}

impl ArraySchema {
    pub fn arity(&self) -> usize {
        self.dims.len()
    }

    pub fn dim(&self, name: &str) -> Option<&DimSpec> {
        self.dims.iter().find(|d| d.name == name)
    }

    pub fn attr(&self, name: &str) -> Option<&AttrSpec> {
        self.attrs.iter().find(|a| a.name == name)
    }

    /// MessagePack encoding of the schema's *content* fields — the parts
    /// that determine compatibility for cross-array element-wise ops.
    /// Excludes the array `name` so two structurally-identical schemas
    /// with different names produce identical bytes (and therefore
    /// identical `schema_hash`).
    pub fn content_msgpack(&self) -> Vec<u8> {
        // Encode tuple of the structural fields. zerompk derives all
        // cover the inner types.
        let payload = SchemaContent {
            dims: &self.dims,
            attrs: &self.attrs,
            tile_extents: &self.tile_extents,
            cell_order: self.cell_order,
            tile_order: self.tile_order,
        };
        zerompk::to_msgpack_vec(&payload).unwrap_or_default()
    }
}

/// Borrowed view of the `ArraySchema` fields that participate in
/// content-only hashing (everything except `name`).
#[derive(zerompk::ToMessagePack)]
struct SchemaContent<'a> {
    dims: &'a Vec<DimSpec>,
    attrs: &'a Vec<AttrSpec>,
    tile_extents: &'a Vec<u64>,
    cell_order: CellOrder,
    tile_order: TileOrder,
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
        // NDARRAY_ELEMENTWISE relies on to compare arrays by shape.
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
        };
        let a = mk("alpha");
        let b = mk("beta");
        assert_ne!(a.name, b.name);
        assert_eq!(a.content_msgpack(), b.content_msgpack());
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
        };
        assert_eq!(s.arity(), 2);
        assert!(s.dim("chrom").is_some());
        assert!(s.attr("variant").is_some());
        assert!(s.dim("missing").is_none());
    }
}
