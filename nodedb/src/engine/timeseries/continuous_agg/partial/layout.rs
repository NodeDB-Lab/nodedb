// SPDX-License-Identifier: BUSL-1.1

//! Column layout of an aggregate's partial state.
//!
//! A partial bucket keeps one [`super::ColumnPartial`] per distinct source
//! column the aggregate reads. `COUNT(*)` reads no column and uses the
//! bucket's row count. Every aggregate expression on one column shares that
//! column's state, so SUM, MIN, AVG and the rest of one column all derive
//! from the same inputs.

use super::super::definition::{AggFunction, ContinuousAggregateDef};

/// The source columns of an aggregate, in first-use order, and the sketch
/// functions each column feeds.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnLayout {
    columns: Vec<String>,
    sketches: Vec<Vec<AggFunction>>,
}

impl ColumnLayout {
    /// The layout of `def`.
    pub fn of(def: &ContinuousAggregateDef) -> Self {
        let mut layout = Self {
            columns: Vec::new(),
            sketches: Vec::new(),
        };
        for expr in &def.aggregates {
            if expr.source_column == "*" {
                continue;
            }
            let slot = match layout.slot(&expr.source_column) {
                Some(slot) => slot,
                None => {
                    layout.columns.push(expr.source_column.clone());
                    layout.sketches.push(Vec::new());
                    layout.columns.len() - 1
                }
            };
            if expr.function.uses_sketch() {
                layout.sketches[slot].push(expr.function.clone());
            }
        }
        layout
    }

    /// The slot of `column`, or `None` when the aggregate does not read it.
    pub fn slot(&self, column: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == column)
    }

    /// The source column names, in slot order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// The sketch functions fed by the column in `slot`.
    pub fn sketches(&self, slot: usize) -> &[AggFunction] {
        self.sketches.get(slot).map_or(&[], Vec::as_slice)
    }

    /// Number of column slots.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the aggregate reads no column.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// For each slot of `self`, the slot of the same column in `upstream`.
    /// A column `upstream` does not read maps to `None`.
    pub fn map_from(&self, upstream: &ColumnLayout) -> Vec<Option<usize>> {
        self.columns.iter().map(|c| upstream.slot(c)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::continuous_agg::definition::{AggregateExpr, RefreshPolicy};

    fn expr(function: AggFunction, column: &str) -> AggregateExpr {
        AggregateExpr {
            function,
            source_column: column.into(),
            output_column: String::new(),
        }
    }

    fn def(aggregates: Vec<AggregateExpr>) -> ContinuousAggregateDef {
        ContinuousAggregateDef {
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
        }
    }

    #[test]
    fn one_slot_per_distinct_column() {
        let layout = ColumnLayout::of(&def(vec![
            expr(AggFunction::Count, "*"),
            expr(AggFunction::Sum, "v"),
            expr(AggFunction::Max, "w"),
            expr(AggFunction::CountDistinct, "v"),
        ]));
        assert_eq!(layout.columns(), ["v".to_string(), "w".to_string()]);
        assert_eq!(layout.sketches(0), [AggFunction::CountDistinct]);
        assert!(layout.sketches(1).is_empty());
        assert_eq!(layout.slot("*"), None);
    }

    #[test]
    fn map_from_matches_columns_by_name() {
        let up = ColumnLayout::of(&def(vec![
            expr(AggFunction::Sum, "a"),
            expr(AggFunction::Sum, "b"),
        ]));
        let down = ColumnLayout::of(&def(vec![
            expr(AggFunction::Sum, "b"),
            expr(AggFunction::Sum, "c"),
        ]));
        assert_eq!(down.map_from(&up), vec![Some(1), None]);
    }
}
