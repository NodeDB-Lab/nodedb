// SPDX-License-Identifier: BUSL-1.1

//! Rebuild a memtable's per-series row counts from the rows it holds.
//!
//! Every row stores its series id in `_ts_series`, so the recount reads it.
//! A schema without that column derives each row's key as ingest does
//! (`ilp_ingest::row_series_key`): the measurement and every non-NULL
//! `Symbol` cell the row stores. A `Text` field never joins the key. The
//! measurement is the one the known keys carry.

use std::collections::HashMap;

use nodedb_types::timeseries::{SeriesId, SeriesKey};

use super::super::types::{ColumnData, ColumnType, NULL_SYMBOL_ID};
use super::table::ColumnarMemtable;

impl ColumnarMemtable {
    /// The series this memtable counts rows for.
    pub fn series_ids(&self) -> Vec<SeriesId> {
        self.series_row_counts.keys().copied().collect()
    }

    /// Rebuild the per-series counts from the rows held now.
    ///
    /// `known` is each series the rows can belong to with its catalog key: the
    /// series this memtable counted before rows left it. It serves only a
    /// schema without `_ts_series`. A row whose key matches no known key
    /// counts under the id its key hashes to, so the number of series stays
    /// exact.
    pub fn recount_series(&mut self, known: &[(SeriesId, SeriesKey)]) {
        let mut counts: HashMap<SeriesId, u64> = HashMap::new();
        let rows = self.row_count as usize;
        if let Some(ColumnData::Int64(ids)) = self
            .schema
            .ts_series_idx()
            .and_then(|idx| self.columns.get(idx))
        {
            for &id in ids.iter().take(rows) {
                *counts
                    .entry(SeriesId::from_ne_bytes(id.to_ne_bytes()))
                    .or_insert(0) += 1;
            }
            self.series_row_counts = counts;
            return;
        }
        let symbol_columns: Vec<(usize, &str)> = self
            .schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, (_, ty))| *ty == ColumnType::Symbol)
            .map(|(index, (name, _))| (index, name.as_str()))
            .collect();
        let by_key: HashMap<&SeriesKey, SeriesId> =
            known.iter().map(|(id, key)| (key, *id)).collect();
        let metric = known.first().map_or("", |(_, key)| key.metric.as_str());
        for row in 0..rows {
            let tags: Vec<(String, String)> = symbol_columns
                .iter()
                .filter_map(|&(column, name)| {
                    self.tag_value(column, row)
                        .map(|value| (name.to_string(), value))
                })
                .collect();
            let key = SeriesKey::new(metric, tags);
            let id = by_key
                .get(&key)
                .copied()
                .unwrap_or_else(|| key.to_series_id(0));
            *counts.entry(id).or_insert(0) += 1;
        }
        self.series_row_counts = counts;
    }

    /// The tag value of `row` in symbol column `column`, or `None` for a null
    /// cell.
    fn tag_value(&self, column: usize, row: usize) -> Option<String> {
        match self.columns.get(column)? {
            ColumnData::Symbol(ids) => {
                let id = *ids.get(row)?;
                if id == NULL_SYMBOL_ID {
                    return None;
                }
                self.symbol_dicts.get(&column)?.get(id).map(str::to_string)
            }
            ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            } => {
                if !valid.get(row).copied().unwrap_or(false) {
                    return None;
                }
                let id = usize::try_from(*ids.get(row)?).ok()?;
                dictionary.get(id).cloned()
            }
            ColumnData::Timestamp(_) | ColumnData::Float64(_) | ColumnData::Int64(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::types::{
        ColumnValue, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
    };
    use super::*;

    fn schema() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("host".into(), ColumnType::Symbol),
                ("note".into(), ColumnType::Text),
                ("value".into(), ColumnType::Float64),
                (
                    nodedb_types::columnar::schema::TS_SERIES.into(),
                    ColumnType::Int64,
                ),
            ],
            timestamp_idx: 0,
            codecs: Vec::new(),
        }
    }

    fn key(host: &str) -> SeriesKey {
        SeriesKey::new("cpu", vec![("host".into(), host.into())])
    }

    #[test]
    fn a_recount_counts_the_series_of_the_rows_held() {
        let mut mt = ColumnarMemtable::new(schema(), ColumnarMemtableConfig::default());
        let known = vec![(11, key("a")), (22, key("b"))];
        for (series, host, note) in [(11, "a", "x"), (22, "b", "y"), (11, "a", "z")] {
            mt.ingest_row(
                series,
                &[
                    ColumnValue::Timestamp(1_000),
                    ColumnValue::Symbol(host.into()),
                    ColumnValue::Symbol(note.into()),
                    ColumnValue::Float64(1.0),
                    ColumnValue::Int64(0),
                ],
            )
            .expect("ingest");
        }
        assert_eq!(mt.series_count(), 2);
        mt.recount_series(&known);
        assert_eq!(mt.series_count(), 2, "the stored series ids are counted");
        let mut ids = mt.series_ids();
        ids.sort_unstable();
        assert_eq!(ids, vec![11, 22]);
    }

    /// Without `_ts_series`, the key is the measurement and the `Symbol`
    /// cells. A `Text` field with a distinct value per row adds no series.
    #[test]
    fn a_recount_without_series_ids_keys_on_symbol_cells_only() {
        let mut no_ids = schema();
        no_ids
            .columns
            .retain(|(name, _)| name != nodedb_types::columnar::schema::TS_SERIES);
        let mut mt = ColumnarMemtable::new(no_ids, ColumnarMemtableConfig::default());
        let known = vec![(11, key("a")), (22, key("b"))];
        for (series, host, note) in [(11, "a", "x"), (22, "b", "y"), (11, "a", "z")] {
            mt.ingest_row(
                series,
                &[
                    ColumnValue::Timestamp(1_000),
                    ColumnValue::Symbol(host.into()),
                    ColumnValue::Symbol(note.into()),
                    ColumnValue::Float64(1.0),
                ],
            )
            .expect("ingest");
        }
        mt.recount_series(&known);
        let mut ids = mt.series_ids();
        ids.sort_unstable();
        assert_eq!(ids, vec![11, 22], "the note field adds no series");
    }
}
