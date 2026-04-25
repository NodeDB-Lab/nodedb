//! Reductions over a sparse tile.
//!
//! Five reducers (Sum / Count / Min / Max / Mean) operate on a single
//! attribute column. The shape is partial-friendly: each tile produces
//! an [`AggregateResult`] which the executor merges across tiles via
//! [`AggregateResult::merge`]. Mean carries (sum, count) so merges are
//! exact rather than averaging averages.
//!
//! Group-by ([`group_by_dim`]) buckets cells by one dim's values and
//! returns a `Vec<(CoordValue, AggregateResult)>` ordered by first
//! appearance. Sort/order is the caller's job — keeping insertion
//! order avoids forcing `Ord` on `CoordValue`.

use std::collections::HashMap;

use crate::tile::sparse_tile::SparseTile;
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reducer {
    Sum,
    Count,
    Min,
    Max,
    Mean,
}

/// Partial result for one reducer over one (group of) cell(s).
/// `Mean` retains `(sum, count)` so partials merge exactly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateResult {
    Sum {
        value: f64,
        count: u64,
    },
    Count {
        count: u64,
    },
    Min {
        value: f64,
        count: u64,
    },
    Max {
        value: f64,
        count: u64,
    },
    Mean {
        sum: f64,
        count: u64,
    },
    /// No non-null cells observed yet for this reducer.
    Empty(Reducer),
}

impl AggregateResult {
    /// Merge two partials of the same reducer kind. Mismatched
    /// reducers produce a left-biased result (the executor only ever
    /// merges same-kind partials, so this is a programming-error path).
    pub fn merge(self, other: AggregateResult) -> AggregateResult {
        use AggregateResult::*;
        match (self, other) {
            (Empty(_), x) | (x, Empty(_)) => x,
            (
                Sum {
                    value: a,
                    count: ca,
                },
                Sum {
                    value: b,
                    count: cb,
                },
            ) => Sum {
                value: a + b,
                count: ca + cb,
            },
            (Count { count: a }, Count { count: b }) => Count { count: a + b },
            (
                Min {
                    value: a,
                    count: ca,
                },
                Min {
                    value: b,
                    count: cb,
                },
            ) => Min {
                value: if a <= b { a } else { b },
                count: ca + cb,
            },
            (
                Max {
                    value: a,
                    count: ca,
                },
                Max {
                    value: b,
                    count: cb,
                },
            ) => Max {
                value: if a >= b { a } else { b },
                count: ca + cb,
            },
            (Mean { sum: a, count: ca }, Mean { sum: b, count: cb }) => Mean {
                sum: a + b,
                count: ca + cb,
            },
            (lhs, _) => lhs,
        }
    }

    /// Final scalar — useful for tests and the planner's last fold.
    pub fn finalize(self) -> Option<f64> {
        match self {
            AggregateResult::Sum { value, .. } => Some(value),
            AggregateResult::Count { count } => Some(count as f64),
            AggregateResult::Min { value, .. } | AggregateResult::Max { value, .. } => Some(value),
            AggregateResult::Mean { sum, count } if count > 0 => Some(sum / count as f64),
            _ => None,
        }
    }
}

fn empty(r: Reducer) -> AggregateResult {
    AggregateResult::Empty(r)
}

/// Reduce one attr column over all rows of a tile. `attr_idx` must be
/// in range; non-numeric / null cells are skipped (Count includes
/// non-null cells regardless of dtype, since "count of cells with this
/// attribute populated" is a sensible fold across all attr types).
pub fn aggregate_attr(tile: &SparseTile, attr_idx: usize, reducer: Reducer) -> AggregateResult {
    let Some(col) = tile.attr_cols.get(attr_idx) else {
        return empty(reducer);
    };
    let mut acc = empty(reducer);
    for v in col {
        let one = single_cell(v, reducer);
        acc = acc.merge(one);
    }
    acc
}

fn single_cell(v: &CellValue, reducer: Reducer) -> AggregateResult {
    use AggregateResult::*;
    let n = match v {
        CellValue::Int64(x) => Some(*x as f64),
        CellValue::Float64(x) => Some(*x),
        CellValue::String(_) | CellValue::Bytes(_) | CellValue::Null => None,
    };
    match (reducer, n) {
        (Reducer::Count, _) if !v.is_null() => Count { count: 1 },
        (Reducer::Count, _) => Empty(Reducer::Count),
        (Reducer::Sum, Some(x)) => Sum { value: x, count: 1 },
        (Reducer::Min, Some(x)) => Min { value: x, count: 1 },
        (Reducer::Max, Some(x)) => Max { value: x, count: 1 },
        (Reducer::Mean, Some(x)) => Mean { sum: x, count: 1 },
        (r, None) => Empty(r),
    }
}

/// One bucket of a group-by aggregate. The key is the dim value the
/// rows share; `result` is the reducer's running partial over that
/// bucket.
#[derive(Debug, Clone)]
pub struct GroupAggregate {
    pub key: CoordValue,
    pub result: AggregateResult,
}

/// Group rows by one dim's values and reduce `attr_idx` per group.
/// Returns groups in first-seen order.
pub fn group_by_dim(
    tile: &SparseTile,
    dim_idx: usize,
    attr_idx: usize,
    reducer: Reducer,
) -> Vec<GroupAggregate> {
    let n = tile.nnz() as usize;
    let Some(dict) = tile.dim_dicts.get(dim_idx) else {
        return Vec::new();
    };
    let Some(col) = tile.attr_cols.get(attr_idx) else {
        return Vec::new();
    };
    let mut order: Vec<CoordValue> = Vec::new();
    let mut by_key: HashMap<CoordValue, AggregateResult> = HashMap::new();
    for (row, cell) in col.iter().enumerate().take(n) {
        let key = dict.values[dict.indices[row] as usize].clone();
        let one = single_cell(cell, reducer);
        match by_key.get_mut(&key) {
            Some(slot) => *slot = slot.merge(one),
            None => {
                order.push(key.clone());
                by_key.insert(key, empty(reducer).merge(one));
            }
        }
    }
    order
        .into_iter()
        .map(|k| GroupAggregate {
            result: by_key.remove(&k).unwrap_or(empty(reducer)),
            key: k,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchema;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::sparse_tile::SparseTileBuilder;
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Float64, true))
            .tile_extents(vec![16])
            .build()
            .unwrap()
    }

    fn tile(rows: &[(i64, Option<f64>)]) -> SparseTile {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        for (k, v) in rows {
            let cv = v.map(CellValue::Float64).unwrap_or(CellValue::Null);
            b.push(&[CoordValue::Int64(*k)], &[cv]).unwrap();
        }
        b.build()
    }

    #[test]
    fn sum_skips_nulls() {
        let t = tile(&[(0, Some(1.0)), (1, None), (2, Some(3.0))]);
        let r = aggregate_attr(&t, 0, Reducer::Sum);
        assert_eq!(r.finalize(), Some(4.0));
    }

    #[test]
    fn count_includes_nonnull_only() {
        let t = tile(&[(0, Some(1.0)), (1, None), (2, Some(3.0))]);
        let r = aggregate_attr(&t, 0, Reducer::Count);
        assert_eq!(r.finalize(), Some(2.0));
    }

    #[test]
    fn min_max_mean() {
        let t = tile(&[(0, Some(1.0)), (1, Some(5.0)), (2, Some(3.0))]);
        assert_eq!(aggregate_attr(&t, 0, Reducer::Min).finalize(), Some(1.0));
        assert_eq!(aggregate_attr(&t, 0, Reducer::Max).finalize(), Some(5.0));
        assert_eq!(aggregate_attr(&t, 0, Reducer::Mean).finalize(), Some(3.0));
    }

    #[test]
    fn merge_combines_partials_exactly() {
        let a = AggregateResult::Mean {
            sum: 10.0,
            count: 4,
        };
        let b = AggregateResult::Mean { sum: 6.0, count: 2 };
        // (10 + 6) / (4 + 2) = 16/6
        assert_eq!(a.merge(b).finalize(), Some(16.0 / 6.0));
    }

    #[test]
    fn empty_reducer_finalizes_to_none() {
        let t = tile(&[(0, None)]);
        let r = aggregate_attr(&t, 0, Reducer::Sum);
        assert_eq!(r.finalize(), None);
    }

    #[test]
    fn group_by_buckets_by_dim_value() {
        let t = tile(&[
            (0, Some(1.0)),
            (1, Some(2.0)),
            (0, Some(3.0)),
            (1, Some(4.0)),
        ]);
        let g = group_by_dim(&t, 0, 0, Reducer::Sum);
        assert_eq!(g.len(), 2);
        // first-seen order: 0, then 1
        assert_eq!(g[0].key, CoordValue::Int64(0));
        assert_eq!(g[0].result.finalize(), Some(4.0));
        assert_eq!(g[1].key, CoordValue::Int64(1));
        assert_eq!(g[1].result.finalize(), Some(6.0));
    }
}
