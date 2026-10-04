// SPDX-License-Identifier: BUSL-1.1

//! Partial aggregate state of one `(bucket, group_key)`.
//!
//! The bucket keeps its row count for `COUNT` and one [`ColumnPartial`] per
//! source column in the aggregate's [`ColumnLayout`]. Finalizing an
//! expression gives the value the ad-hoc aggregate gives over the same rows:
//! `COUNT` is an `Integer`, SUM is exact (`Integer`, `Decimal`, or `Float`),
//! AVG is a `Float` from the exact total, and MIN / MAX / FIRST / LAST are
//! the original cells.

use nodedb_query::EvalError;
use nodedb_types::Value;

use super::super::definition::{AggFunction, AggregateExpr};
use super::column::ColumnPartial;
use super::layout::ColumnLayout;

/// Partial aggregate state for a single `(bucket, group_key)` combination.
#[derive(Debug)]
pub struct PartialAggregate {
    pub bucket_ts: i64,
    /// Symbol IDs for GROUP BY columns.
    pub group_key: Vec<u32>,
    /// Rows aggregated into this bucket.
    pub count: u64,
    /// One state per column slot of the aggregate's layout.
    pub columns: Vec<ColumnPartial>,
}

impl PartialAggregate {
    /// An empty bucket shaped by `layout`.
    pub fn new(bucket_ts: i64, group_key: Vec<u32>, layout: &ColumnLayout) -> Self {
        Self {
            bucket_ts,
            group_key,
            count: 0,
            columns: (0..layout.len())
                .map(|slot| ColumnPartial::with_sketches(layout.sketches(slot)))
                .collect(),
        }
    }

    /// Fold `other`, a bucket of the same layout, into this one.
    pub fn merge(&mut self, other: &PartialAggregate) {
        self.count += other.count;
        for (mine, theirs) in self.columns.iter_mut().zip(&other.columns) {
            mine.merge(theirs);
        }
    }

    /// Fold `other`, a bucket of another layout, into this one. `map[i]` is
    /// the slot of `other` that feeds this bucket's slot `i`. A slot mapped to
    /// `None` takes nothing.
    pub fn merge_mapped(&mut self, other: &PartialAggregate, map: &[Option<usize>]) {
        self.count += other.count;
        for (mine, source) in self.columns.iter_mut().zip(map) {
            if let Some(theirs) = source.and_then(|slot| other.columns.get(slot)) {
                mine.merge(theirs);
            }
        }
    }

    /// The final value of `expr` over this bucket. `layout` is the layout
    /// this bucket was built with. A column the bucket has no numeric cell
    /// for finalizes to NULL, except `COUNT`, which counts rows.
    pub fn finalize(
        &self,
        expr: &AggregateExpr,
        layout: &ColumnLayout,
    ) -> Result<Value, EvalError> {
        if expr.function == AggFunction::Count {
            return i64::try_from(self.count)
                .map(Value::Integer)
                .map_err(|_| EvalError::NumericOverflow { function: "count" });
        }
        let Some(column) = layout
            .slot(&expr.source_column)
            .and_then(|slot| self.columns.get(slot))
        else {
            return Ok(Value::Null);
        };
        let cell = |v: Option<&Value>| v.cloned().unwrap_or(Value::Null);
        Ok(match &expr.function {
            AggFunction::Sum => column.sum.sum()?,
            AggFunction::Avg => column.sum.avg()?,
            AggFunction::Min => cell(column.min.as_ref()),
            AggFunction::Max => cell(column.max.as_ref()),
            AggFunction::First => cell(column.first.as_ref().map(|f| &f.value)),
            AggFunction::Last => cell(column.last.as_ref().map(|l| &l.value)),
            AggFunction::CountDistinct => column
                .hll
                .as_ref()
                .filter(|_| column.sum.count() > 0)
                .map_or(Value::Null, |h| Value::Float(h.estimate())),
            AggFunction::Percentile(q) => column
                .tdigest
                .as_ref()
                .filter(|_| column.sum.count() > 0)
                .map_or(Value::Null, |td| Value::Float(td.quantile(*q))),
            AggFunction::TopK(_) => match &column.topk {
                Some(ss) => i64::try_from(ss.top_k().len())
                    .map(Value::Integer)
                    .map_err(|_| EvalError::NumericOverflow { function: "topk" })?,
                None => Value::Null,
            },
            // `Count` returned above. `AggFunction` is non-exhaustive: a
            // function this bucket keeps no state for has no value.
            _ => Value::Null,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::definition::{
        ContinuousAggregateDef, RefreshPolicy,
    };

    fn expr(function: AggFunction, column: &str) -> AggregateExpr {
        AggregateExpr {
            function,
            source_column: column.into(),
            output_column: String::new(),
        }
    }

    fn layout(aggregates: Vec<AggregateExpr>) -> ColumnLayout {
        ColumnLayout::of(&ContinuousAggregateDef {
            database_id: 0,
            name: "a".into(),
            source: "s".into(),
            bucket_interval: "1m".into(),
            bucket_interval_ms: 60_000,
            group_by: Vec::new(),
            aggregates,
            refresh_policy: RefreshPolicy::OnFlush,
            retention_period_ms: 0,
            stale: false,
        })
    }

    #[test]
    fn finalize_keeps_integer_types() {
        let exprs = vec![
            expr(AggFunction::Count, "*"),
            expr(AggFunction::Sum, "v"),
            expr(AggFunction::Min, "v"),
            expr(AggFunction::Max, "v"),
            expr(AggFunction::Avg, "v"),
        ];
        let layout = layout(exprs.clone());
        let mut p = PartialAggregate::new(0, Vec::new(), &layout);
        for (ts, v) in [i64::MAX, i64::MAX].iter().enumerate() {
            p.count += 1;
            p.columns[0].add_int(ts as i64, *v);
        }
        let out: Vec<Value> = exprs
            .iter()
            .map(|e| p.finalize(e, &layout).unwrap())
            .collect();
        assert_eq!(out[0], Value::Integer(2));
        assert_eq!(
            out[1],
            Value::Decimal(rust_decimal::Decimal::from_i128_with_scale(
                2 * i128::from(i64::MAX),
                0
            ))
        );
        assert_eq!(out[2], Value::Integer(i64::MAX));
        assert_eq!(out[3], Value::Integer(i64::MAX));
        assert_eq!(out[4], Value::Float(i64::MAX as f64));
    }

    #[test]
    fn column_without_cells_is_null() {
        let exprs = vec![expr(AggFunction::Sum, "v"), expr(AggFunction::Min, "v")];
        let layout = layout(exprs.clone());
        let mut p = PartialAggregate::new(0, Vec::new(), &layout);
        p.count = 3;
        for e in &exprs {
            assert_eq!(p.finalize(e, &layout).unwrap(), Value::Null);
        }
        assert_eq!(
            p.finalize(&expr(AggFunction::Count, "v"), &layout).unwrap(),
            Value::Integer(3)
        );
    }

    #[test]
    fn merge_mapped_takes_matching_columns() {
        let up_layout = layout(vec![
            expr(AggFunction::Sum, "a"),
            expr(AggFunction::Sum, "b"),
        ]);
        let down_layout = layout(vec![expr(AggFunction::Sum, "b")]);
        let mut up = PartialAggregate::new(0, Vec::new(), &up_layout);
        up.count = 1;
        up.columns[0].add_int(0, 5);
        up.columns[1].add_int(0, 7);
        let mut down = PartialAggregate::new(0, Vec::new(), &down_layout);
        down.merge_mapped(&up, &down_layout.map_from(&up_layout));
        assert_eq!(down.count, 1);
        assert_eq!(
            down.finalize(&expr(AggFunction::Sum, "b"), &down_layout)
                .unwrap(),
            Value::Integer(7)
        );
    }
}
