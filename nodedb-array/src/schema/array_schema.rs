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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

#[cfg(test)]
mod tests {
    use super::super::attr_spec::AttrType;
    use super::super::dim_spec::DimType;
    use super::*;
    use crate::types::domain::{Domain, DomainBound};

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
