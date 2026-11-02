// SPDX-License-Identifier: BUSL-1.1

//! Shared types for grouped aggregation: result container, schema
//! resolution, per-row accumulation, and bitmask iteration.

use std::collections::HashMap;

use super::super::columnar_agg::AggAccum;
use super::super::columnar_memtable::{ColumnData, ColumnType, NULL_SYMBOL_ID};
use super::group_key::GroupKey;

/// Result of a grouped aggregation across one or more sources.
#[derive(Debug, Clone, Default)]
pub struct GroupedAggResult {
    pub groups: HashMap<GroupKey, Vec<AggAccum>>,
    pub num_aggs: usize,
}

impl GroupedAggResult {
    pub fn new(num_aggs: usize) -> Self {
        Self {
            groups: HashMap::new(),
            num_aggs,
        }
    }

    pub fn merge(&mut self, other: &GroupedAggResult) {
        for (key, other_accums) in &other.groups {
            self.merge_group(key.clone(), other_accums);
        }
    }

    /// Merge one group's accumulators into this result, creating the group's
    /// entry if it is not already present.
    pub(super) fn merge_group(&mut self, key: GroupKey, accums: &[AggAccum]) {
        let entry = self
            .groups
            .entry(key)
            .or_insert_with(|| (0..self.num_aggs).map(|_| AggAccum::default()).collect());
        for (i, a) in accums.iter().enumerate() {
            if i < entry.len() {
                entry[i].merge(a);
            }
        }
    }
}

/// Pre-resolved column indices for a schema.
pub(super) struct ResolvedSchema {
    pub group_cols: Vec<(usize, ColumnType)>,
    pub agg_cols: Vec<AggColInfo>,
    pub ts_idx: usize,
}

pub(super) enum AggColInfo {
    CountStar,
    /// `COUNT(col)`: counts the rows whose cell at this index is not NULL.
    CountField(usize),
    Numeric(usize),
    Skip,
}

/// Whether row `row_idx` of `column` holds SQL NULL: NaN for a float, the
/// NULL symbol id for a symbol. Integer and time cells are never NULL.
#[inline]
fn cell_is_null(column: Option<&ColumnData>, row_idx: usize) -> bool {
    match column {
        Some(ColumnData::Float64(v)) => v.get(row_idx).is_none_or(|f| f.is_nan()),
        Some(ColumnData::Symbol(ids)) | Some(ColumnData::DictEncoded { ids, .. }) => {
            ids.get(row_idx).is_none_or(|id| *id == NULL_SYMBOL_ID)
        }
        Some(ColumnData::Int64(_)) | Some(ColumnData::Timestamp(_)) => false,
        None => true,
    }
}

/// Resolve the column indices an aggregate pass needs.
///
/// `ts_idx` is the schema's own designated time column — passed in, never
/// looked up by name. The time column is whatever the collection declared as
/// its `TIME_KEY`, so searching for a conventional name would fail on every
/// collection that named it something else and silently collapse the whole
/// aggregate to an empty result.
pub(super) fn resolve_schema(
    schema: &[(String, ColumnType)],
    ts_idx: usize,
    group_by: &[String],
    aggregates: &[(String, String)],
) -> Option<ResolvedSchema> {
    if ts_idx >= schema.len() {
        return None;
    }
    let group_cols: Vec<_> = group_by
        .iter()
        .map(|name| {
            schema
                .iter()
                .enumerate()
                .find(|(_, (n, _))| n == name)
                .map(|(i, (_, ty))| (i, *ty))
        })
        .collect::<Option<Vec<_>>>()?;

    let agg_cols: Vec<_> = aggregates
        .iter()
        .map(|(op, field)| {
            if field == "*" {
                AggColInfo::CountStar
            } else if let Some((idx, (_, ty))) =
                schema.iter().enumerate().find(|(_, (n, _))| n == field)
            {
                if op == "count" {
                    AggColInfo::CountField(idx)
                } else {
                    match ty {
                        ColumnType::Float64 | ColumnType::Int64 | ColumnType::Timestamp(_) => {
                            AggColInfo::Numeric(idx)
                        }
                        ColumnType::Symbol | ColumnType::Text => AggColInfo::Skip,
                    }
                }
            } else if op == "count" {
                AggColInfo::CountStar
            } else {
                AggColInfo::Skip
            }
        })
        .collect();

    Some(ResolvedSchema {
        group_cols,
        agg_cols,
        ts_idx,
    })
}

/// Accumulate one row into an accum slice using pre-resolved column info.
#[inline]
pub(super) fn accumulate_row(
    accums: &mut [AggAccum],
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    row_idx: usize,
) {
    for (agg_idx, info) in resolved.agg_cols.iter().enumerate() {
        match info {
            AggColInfo::CountStar => accums[agg_idx].feed_count_only(),
            AggColInfo::CountField(col_idx) => {
                if !cell_is_null(columns[*col_idx], row_idx) {
                    accums[agg_idx].feed_count_only();
                }
            }
            AggColInfo::Numeric(col_idx) => {
                // An integer cell feeds exactly: no rounding through `f64`.
                // A NULL float (NaN) feeds nothing, as SQL aggregates skip
                // NULL inputs.
                match columns[*col_idx] {
                    Some(ColumnData::Float64(v)) if !v[row_idx].is_nan() => {
                        accums[agg_idx].feed(v[row_idx])
                    }
                    Some(ColumnData::Int64(v)) | Some(ColumnData::Timestamp(v)) => {
                        accums[agg_idx].feed_int(v[row_idx])
                    }
                    _ => {}
                }
            }
            AggColInfo::Skip => {}
        }
    }
}

/// Iterate set bits in a bitmask, calling `f` for each row index.
#[inline]
pub(super) fn for_each_set_bit(mask: &[u64], row_count: usize, mut f: impl FnMut(usize)) {
    for (word_idx, &mask_word) in mask.iter().enumerate() {
        let mut word = mask_word;
        while word != 0 {
            let bit = word.trailing_zeros() as usize;
            let row_idx = word_idx * 64 + bit;
            if row_idx < row_count {
                f(row_idx);
            }
            word &= word - 1;
        }
    }
}
