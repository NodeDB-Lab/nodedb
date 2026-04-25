//! Per-tile Minimum Bounding Region + per-attr statistics.
//!
//! The MBR is recorded in the tile footer and indexed at the segment
//! level (Tier 2) via R\*-tree, so a slice query can prune entire tiles
//! before any payload is decoded.

use serde::{Deserialize, Serialize};

use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;
use crate::types::domain::DomainBound;

/// Per-attribute summary used for predicate pushdown.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AttrStats {
    /// Numeric (`Int64` / `Float64`) min + max + null count.
    Numeric { min: f64, max: f64, null_count: u32 },
    /// String / bytes — only cardinality + null count, no min/max.
    Categorical { distinct: u32, null_count: u32 },
    /// All cells in this tile carry `CellValue::Null` for this attr.
    AllNull { null_count: u32 },
}

/// Tile-level MBR. `dim_mins` / `dim_maxs` carry one bound per schema
/// dimension; `nnz` is the count of populated (non-tombstoned) cells;
/// `attr_stats` is parallel to the schema's `attrs`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TileMBR {
    pub dim_mins: Vec<DomainBound>,
    pub dim_maxs: Vec<DomainBound>,
    pub nnz: u32,
    pub attr_stats: Vec<AttrStats>,
}

impl TileMBR {
    pub fn new(arity: usize, n_attrs: usize) -> Self {
        Self {
            dim_mins: Vec::with_capacity(arity),
            dim_maxs: Vec::with_capacity(arity),
            nnz: 0,
            attr_stats: Vec::with_capacity(n_attrs),
        }
    }
}

/// Builder that folds cells into running per-dim min/max and per-attr
/// stats. Use one builder per tile during write-side accumulation.
pub struct MbrBuilder {
    arity: usize,
    n_attrs: usize,
    dim_mins: Vec<Option<CoordValue>>,
    dim_maxs: Vec<Option<CoordValue>>,
    attr_min: Vec<Option<f64>>,
    attr_max: Vec<Option<f64>>,
    attr_distinct: Vec<std::collections::HashSet<Vec<u8>>>,
    attr_nulls: Vec<u32>,
    attr_is_categorical: Vec<bool>,
    attr_total: Vec<u32>,
    nnz: u32,
}

impl MbrBuilder {
    pub fn new(arity: usize, n_attrs: usize) -> Self {
        Self {
            arity,
            n_attrs,
            dim_mins: vec![None; arity],
            dim_maxs: vec![None; arity],
            attr_min: vec![None; n_attrs],
            attr_max: vec![None; n_attrs],
            attr_distinct: vec![Default::default(); n_attrs],
            attr_nulls: vec![0; n_attrs],
            attr_is_categorical: vec![false; n_attrs],
            attr_total: vec![0; n_attrs],
            nnz: 0,
        }
    }

    pub fn fold(&mut self, coord: &[CoordValue], attrs: &[CellValue]) {
        for (i, c) in coord.iter().take(self.arity).enumerate() {
            let lo_replaces = self.dim_mins[i].as_ref().is_none_or(|cur| coord_lt(c, cur));
            if lo_replaces {
                self.dim_mins[i] = Some(c.clone());
            }
            let hi_replaces = self.dim_maxs[i].as_ref().is_none_or(|cur| coord_lt(cur, c));
            if hi_replaces {
                self.dim_maxs[i] = Some(c.clone());
            }
        }
        for (i, a) in attrs.iter().take(self.n_attrs).enumerate() {
            self.attr_total[i] += 1;
            match a {
                CellValue::Null => self.attr_nulls[i] += 1,
                CellValue::Int64(v) => {
                    fold_numeric(&mut self.attr_min[i], &mut self.attr_max[i], *v as f64)
                }
                CellValue::Float64(v) => {
                    fold_numeric(&mut self.attr_min[i], &mut self.attr_max[i], *v)
                }
                CellValue::String(s) => {
                    self.attr_is_categorical[i] = true;
                    self.attr_distinct[i].insert(s.as_bytes().to_vec());
                }
                CellValue::Bytes(b) => {
                    self.attr_is_categorical[i] = true;
                    self.attr_distinct[i].insert(b.clone());
                }
            }
        }
        self.nnz += 1;
    }

    pub fn build(self) -> TileMBR {
        let dim_mins = self.dim_mins.iter().map(coord_to_bound).collect();
        let dim_maxs = self.dim_maxs.iter().map(coord_to_bound).collect();
        let mut attr_stats = Vec::with_capacity(self.n_attrs);
        for i in 0..self.n_attrs {
            let s = if self.attr_total[i] == self.attr_nulls[i] {
                AttrStats::AllNull {
                    null_count: self.attr_nulls[i],
                }
            } else if self.attr_is_categorical[i] {
                AttrStats::Categorical {
                    distinct: self.attr_distinct[i].len() as u32,
                    null_count: self.attr_nulls[i],
                }
            } else {
                // Numeric branch is reached only when at least one
                // non-null numeric cell was folded (the if/else above
                // excludes all-null and categorical), so attr_min /
                // attr_max are populated.
                AttrStats::Numeric {
                    min: self.attr_min[i].expect("numeric branch implies min is set"),
                    max: self.attr_max[i].expect("numeric branch implies max is set"),
                    null_count: self.attr_nulls[i],
                }
            };
            attr_stats.push(s);
        }
        TileMBR {
            dim_mins,
            dim_maxs,
            nnz: self.nnz,
            attr_stats,
        }
    }
}

fn coord_lt(a: &CoordValue, b: &CoordValue) -> bool {
    a.partial_cmp(b)
        .is_some_and(|o| o == std::cmp::Ordering::Less)
}

fn fold_numeric(min: &mut Option<f64>, max: &mut Option<f64>, v: f64) {
    *min = Some(min.map_or(v, |m| m.min(v)));
    *max = Some(max.map_or(v, |m| m.max(v)));
}

fn coord_to_bound(c: &Option<CoordValue>) -> DomainBound {
    match c {
        Some(CoordValue::Int64(v)) => DomainBound::Int64(*v),
        Some(CoordValue::Float64(v)) => DomainBound::Float64(*v),
        Some(CoordValue::TimestampMs(v)) => DomainBound::TimestampMs(*v),
        Some(CoordValue::String(v)) => DomainBound::String(v.clone()),
        None => DomainBound::Int64(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mbr_tracks_min_max_per_dim() {
        let mut b = MbrBuilder::new(2, 1);
        b.fold(
            &[CoordValue::Int64(5), CoordValue::Int64(10)],
            &[CellValue::Int64(1)],
        );
        b.fold(
            &[CoordValue::Int64(2), CoordValue::Int64(20)],
            &[CellValue::Int64(2)],
        );
        b.fold(
            &[CoordValue::Int64(8), CoordValue::Int64(15)],
            &[CellValue::Int64(3)],
        );
        let m = b.build();
        assert_eq!(m.nnz, 3);
        assert_eq!(m.dim_mins[0], DomainBound::Int64(2));
        assert_eq!(m.dim_maxs[0], DomainBound::Int64(8));
        assert_eq!(m.dim_mins[1], DomainBound::Int64(10));
        assert_eq!(m.dim_maxs[1], DomainBound::Int64(20));
    }

    #[test]
    fn mbr_numeric_attr_stats() {
        let mut b = MbrBuilder::new(1, 1);
        b.fold(&[CoordValue::Int64(0)], &[CellValue::Float64(1.5)]);
        b.fold(&[CoordValue::Int64(1)], &[CellValue::Float64(3.5)]);
        b.fold(&[CoordValue::Int64(2)], &[CellValue::Null]);
        let m = b.build();
        match &m.attr_stats[0] {
            AttrStats::Numeric {
                min,
                max,
                null_count,
            } => {
                assert_eq!(*min, 1.5);
                assert_eq!(*max, 3.5);
                assert_eq!(*null_count, 1);
            }
            other => panic!("expected Numeric, got {other:?}"),
        }
    }

    #[test]
    fn mbr_all_null_attr() {
        let mut b = MbrBuilder::new(1, 1);
        b.fold(&[CoordValue::Int64(0)], &[CellValue::Null]);
        b.fold(&[CoordValue::Int64(1)], &[CellValue::Null]);
        let m = b.build();
        assert!(matches!(
            m.attr_stats[0],
            AttrStats::AllNull { null_count: 2 }
        ));
    }

    #[test]
    fn mbr_categorical_attr_stats() {
        let mut b = MbrBuilder::new(1, 1);
        b.fold(&[CoordValue::Int64(0)], &[CellValue::String("a".into())]);
        b.fold(&[CoordValue::Int64(1)], &[CellValue::String("b".into())]);
        b.fold(&[CoordValue::Int64(2)], &[CellValue::String("a".into())]);
        let m = b.build();
        match &m.attr_stats[0] {
            AttrStats::Categorical {
                distinct,
                null_count,
            } => {
                assert_eq!(*distinct, 2);
                assert_eq!(*null_count, 0);
            }
            other => panic!("expected Categorical, got {other:?}"),
        }
    }
}
