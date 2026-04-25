//! Sparse tile payload — coordinate list + per-attribute columns.
//!
//! Storage layout is column-major so Tier 2 can apply per-attribute
//! codecs (`nodedb-codec`) without a transpose. Dimension values are
//! dictionary-encoded inline: rare in genomic / single-cell workloads
//! that have many repeats per tile.

use serde::{Deserialize, Serialize};

use super::mbr::{MbrBuilder, TileMBR};
use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

/// Per-dim dictionary: distinct values seen, and one index per cell
/// pointing into the dictionary. Index width is selected by callers
/// (Tier 2) when picking the codec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DimDict {
    pub values: Vec<CoordValue>,
    pub indices: Vec<u32>,
}

impl DimDict {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            indices: Vec::new(),
        }
    }

    fn push(&mut self, v: &CoordValue) {
        if let Some(idx) = self.values.iter().position(|x| x == v) {
            self.indices.push(idx as u32);
        } else {
            self.indices.push(self.values.len() as u32);
            self.values.push(v.clone());
        }
    }

    pub fn cardinality(&self) -> usize {
        self.values.len()
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

/// Sparse tile payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SparseTile {
    /// One dictionary per schema dim, parallel to `schema.dims`.
    pub dim_dicts: Vec<DimDict>,
    /// One column per schema attr, parallel to `schema.attrs`.
    pub attr_cols: Vec<Vec<CellValue>>,
    pub mbr: TileMBR,
}

impl SparseTile {
    /// Empty tile sized for the given schema.
    pub fn empty(schema: &ArraySchema) -> Self {
        Self {
            dim_dicts: (0..schema.arity()).map(|_| DimDict::new()).collect(),
            attr_cols: (0..schema.attrs.len()).map(|_| Vec::new()).collect(),
            mbr: TileMBR::new(schema.arity(), schema.attrs.len()),
        }
    }

    pub fn nnz(&self) -> u32 {
        self.mbr.nnz
    }
}

/// Streaming builder. Folds `(coord, attrs)` pairs into the tile body
/// and the MBR in one pass.
pub struct SparseTileBuilder<'a> {
    schema: &'a ArraySchema,
    dim_dicts: Vec<DimDict>,
    attr_cols: Vec<Vec<CellValue>>,
    mbr: MbrBuilder,
}

impl<'a> SparseTileBuilder<'a> {
    pub fn new(schema: &'a ArraySchema) -> Self {
        Self {
            schema,
            dim_dicts: (0..schema.arity()).map(|_| DimDict::new()).collect(),
            attr_cols: (0..schema.attrs.len()).map(|_| Vec::new()).collect(),
            mbr: MbrBuilder::new(schema.arity(), schema.attrs.len()),
        }
    }

    pub fn push(&mut self, coord: &[CoordValue], attrs: &[CellValue]) -> ArrayResult<()> {
        if coord.len() != self.schema.arity() {
            return Err(ArrayError::CoordArityMismatch {
                array: self.schema.name.clone(),
                expected: self.schema.arity(),
                got: coord.len(),
            });
        }
        if attrs.len() != self.schema.attrs.len() {
            return Err(ArrayError::CellTypeMismatch {
                array: self.schema.name.clone(),
                attr: "<all>".to_string(),
                detail: format!(
                    "expected {} attr columns, got {}",
                    self.schema.attrs.len(),
                    attrs.len()
                ),
            });
        }
        for (i, c) in coord.iter().enumerate() {
            self.dim_dicts[i].push(c);
        }
        for (i, a) in attrs.iter().enumerate() {
            self.attr_cols[i].push(a.clone());
        }
        self.mbr.fold(coord, attrs);
        Ok(())
    }

    pub fn build(self) -> SparseTile {
        SparseTile {
            dim_dicts: self.dim_dicts,
            attr_cols: self.attr_cols,
            mbr: self.mbr.build(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "chrom",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(24)),
            ))
            .dim(DimSpec::new(
                "pos",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1_000_000)),
            ))
            .attr(AttrSpec::new("variant", AttrType::String, false))
            .attr(AttrSpec::new("qual", AttrType::Float64, true))
            .tile_extents(vec![1, 1_000_000])
            .build()
            .unwrap()
    }

    #[test]
    fn sparse_tile_collects_columns_in_order() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(100)],
            &[CellValue::String("ALT".into()), CellValue::Float64(40.0)],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(200)],
            &[CellValue::String("REF".into()), CellValue::Float64(50.0)],
        )
        .unwrap();
        let t = b.build();
        assert_eq!(t.nnz(), 2);
        assert_eq!(t.attr_cols.len(), 2);
        assert_eq!(t.attr_cols[0].len(), 2);
        assert_eq!(t.attr_cols[1].len(), 2);
    }

    #[test]
    fn dim_dict_dedups_repeated_values() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(100)],
            &[CellValue::String("ALT".into()), CellValue::Float64(40.0)],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(200)],
            &[CellValue::String("REF".into()), CellValue::Float64(50.0)],
        )
        .unwrap();
        let t = b.build();
        // chrom dictionary should have one entry (1 repeated).
        assert_eq!(t.dim_dicts[0].cardinality(), 1);
        assert_eq!(t.dim_dicts[0].len(), 2);
    }

    #[test]
    fn sparse_tile_rejects_bad_arity() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let r = b.push(
            &[CoordValue::Int64(1)],
            &[CellValue::String("ALT".into()), CellValue::Float64(0.0)],
        );
        assert!(r.is_err());
    }

    #[test]
    fn sparse_tile_rejects_bad_attr_count() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let r = b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(0)],
            &[CellValue::String("ALT".into())],
        );
        assert!(r.is_err());
    }

    #[test]
    fn empty_sparse_tile_has_zero_nnz() {
        let s = schema();
        let t = SparseTile::empty(&s);
        assert_eq!(t.nnz(), 0);
        assert_eq!(t.dim_dicts.len(), 2);
        assert_eq!(t.attr_cols.len(), 2);
    }
}
