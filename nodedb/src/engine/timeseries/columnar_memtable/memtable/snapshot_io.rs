// SPDX-License-Identifier: BUSL-1.1

//! Lossless snapshot export and import for a [`ColumnarMemtable`].

use std::collections::HashMap;

use nodedb_types::timeseries::{SeriesId, SymbolDictionary};

use super::super::snapshot::{MemtableSnapshot, column_to_snapshot, rebuild_columns};
use super::super::types::{ColumnarMemtableConfig, ColumnarSchema};
use super::table::ColumnarMemtable;

impl ColumnarMemtable {
    /// Restore the pre-transaction resident-byte accounting after replacing
    /// the memtable from a logical snapshot.
    ///
    /// A snapshot preserves values and dictionaries, but rebuilding its vectors
    /// intentionally does not preserve their spare capacity. Transaction undo
    /// retains the original governor reservation, so it must also reinstate the
    /// original reported footprint rather than silently undercounting it.
    pub(crate) fn restore_memory_bytes_for_undo(&mut self, memory_bytes: usize) {
        self.memory_bytes = memory_bytes;
    }

    /// Export a lossless snapshot (carries column types + symbol dicts). INFALLIBLE.
    pub fn export_snapshot(&self) -> MemtableSnapshot {
        let columns: Vec<_> = self.columns.iter().map(column_to_snapshot).collect();

        // Stable ordering for deterministic snapshots.
        let mut symbol_dicts: Vec<(usize, SymbolDictionary)> = self
            .symbol_dicts
            .iter()
            .map(|(&idx, dict)| (idx, dict.clone()))
            .collect();
        symbol_dicts.sort_by_key(|(idx, _)| *idx);

        let mut series_row_counts: Vec<(SeriesId, u64)> = self
            .series_row_counts
            .iter()
            .map(|(&k, &v)| (k, v))
            .collect();
        series_row_counts.sort_by_key(|(id, _)| *id);

        MemtableSnapshot {
            schema_columns: self.schema.columns.clone(),
            timestamp_idx: self.schema.timestamp_idx,
            columns,
            symbol_dicts,
            series_row_counts,
            row_count: self.row_count,
            min_ts: self.min_ts,
            max_ts: self.max_ts,
        }
    }

    /// Reconstruct a memtable from a snapshot. Returns a typed error on any
    /// schema/row-count mismatch.
    ///
    /// `config` is not carried in the snapshot — it is operator tuning, not
    /// data — so callers pass the live `ColumnarMemtableConfig::from_tuning`
    /// value. A memtable keeps its limits for its whole life, so restoring with
    /// compiled defaults would silently ignore the operator's configuration.
    pub fn from_snapshot(
        snap: MemtableSnapshot,
        config: ColumnarMemtableConfig,
    ) -> crate::Result<Self> {
        if snap.timestamp_idx >= snap.schema_columns.len() {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "snapshot timestamp_idx {} out of range for {} columns",
                    snap.timestamp_idx,
                    snap.schema_columns.len(),
                ),
            });
        }

        let columns = rebuild_columns(snap.columns, &snap.schema_columns, snap.row_count)?;
        let n = snap.schema_columns.len();
        // Codecs are ephemeral — not serialized; rebuild as Auto.
        let schema = ColumnarSchema {
            columns: snap.schema_columns,
            timestamp_idx: snap.timestamp_idx,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; n],
        };

        let symbol_dicts: HashMap<usize, SymbolDictionary> =
            snap.symbol_dicts.into_iter().collect();
        let series_row_counts: HashMap<SeriesId, u64> =
            snap.series_row_counts.into_iter().collect();
        let memory_bytes: usize =
            columns.iter().map(|c| c.memory_bytes()).sum::<usize>() + symbol_dicts.len() * 256;

        Ok(Self {
            schema,
            columns,
            series_row_counts,
            symbol_dicts,
            row_count: snap.row_count,
            memory_bytes,
            config,
            min_ts: snap.min_ts,
            max_ts: snap.max_ts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::snapshot::ColumnSnapshot;
    use super::super::super::types::{ColumnData, ColumnType, ColumnValue, TimeKind};
    use super::*;

    fn default_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 1024 * 1024,
            hard_memory_limit: 2 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    #[test]
    fn snapshot_roundtrip_empty_memtable() {
        let mt = ColumnarMemtable::new_metric(default_config());
        let snap = mt.export_snapshot();
        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: MemtableSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
        let mt2 = ColumnarMemtable::from_snapshot(snap2, default_config()).expect("from_snapshot");
        assert_eq!(mt2.row_count(), 0);
        assert!(mt2.is_empty());
        assert_eq!(mt2.schema().columns.len(), 2);
        assert_eq!(mt2.schema().timestamp_idx, 0);
    }

    #[test]
    fn snapshot_roundtrip_multi_column_with_symbol_tags() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let mut mt = ColumnarMemtable::new(schema, default_config());

        for i in 0..5u64 {
            mt.ingest_row(
                i,
                &[
                    ColumnValue::Timestamp(1000 + i as i64),
                    ColumnValue::Float64(i as f64 * 1.5),
                    ColumnValue::Symbol(format!("host-{i}")),
                ],
            )
            .expect("ingest");
        }
        // Also re-use an existing host to verify symbol dict cardinality.
        mt.ingest_row(
            99,
            &[
                ColumnValue::Timestamp(2000),
                ColumnValue::Float64(99.0),
                ColumnValue::Symbol("host-0".to_string()),
            ],
        )
        .expect("ingest existing host");

        let expected_row_count = mt.row_count();
        let expected_min_ts = mt.min_ts();
        let expected_max_ts = mt.max_ts();
        let expected_schema_cols: Vec<(String, ColumnType)> = mt.schema().columns.clone();
        let expected_ts_idx = mt.schema().timestamp_idx;
        let expected_dict_len = mt.symbol_dict(2).map(|d| d.len()).unwrap_or(0);

        let snap = mt.export_snapshot();
        let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
        let snap2: MemtableSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
        let mt2 = ColumnarMemtable::from_snapshot(snap2, default_config()).expect("from_snapshot");

        assert_eq!(mt2.row_count(), expected_row_count);
        assert_eq!(mt2.min_ts(), expected_min_ts);
        assert_eq!(mt2.max_ts(), expected_max_ts);
        assert_eq!(mt2.schema().columns, expected_schema_cols);
        assert_eq!(mt2.schema().timestamp_idx, expected_ts_idx);

        // Verify symbol dict was faithfully restored.
        let dict2 = mt2.symbol_dict(2).expect("host dict present");
        assert_eq!(dict2.len(), expected_dict_len);
        assert_eq!(dict2.get(0), Some("host-0"));

        // Verify column data lengths match.
        for i in 0..3 {
            assert_eq!(mt2.column(i).len(), expected_row_count as usize);
        }
    }

    /// The time kind survives the snapshot round trip for every kind.
    #[test]
    fn snapshot_roundtrip_keeps_time_kind() {
        for kind in [
            TimeKind::Millis,
            TimeKind::Instant(nodedb_types::InstantKind::Naive),
            TimeKind::Instant(nodedb_types::InstantKind::Utc),
        ] {
            let schema = ColumnarSchema {
                columns: vec![
                    ("ts".into(), ColumnType::Timestamp(kind)),
                    ("value".into(), ColumnType::Float64),
                ],
                timestamp_idx: 0,
                codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
            };
            let mt = ColumnarMemtable::new(schema, default_config());
            let snap = mt.export_snapshot();
            let bytes = zerompk::to_msgpack_vec(&snap).expect("serialize");
            let snap2: MemtableSnapshot = zerompk::from_msgpack(&bytes).expect("deserialize");
            let mt2 =
                ColumnarMemtable::from_snapshot(snap2, default_config()).expect("from_snapshot");
            assert_eq!(mt2.schema().columns[0].1, ColumnType::Timestamp(kind));
        }
    }

    #[test]
    fn snapshot_roundtrip_dict_encoded_column_rebuilds_reverse() {
        // Construct a DictEncoded snapshot directly (bypass ingest path).
        let snap = MemtableSnapshot {
            schema_columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("tag".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            columns: vec![
                ColumnSnapshot::Timestamp(vec![1000, 2000, 3000]),
                ColumnSnapshot::DictEncoded {
                    ids: vec![0, 1, 0],
                    dictionary: vec!["alpha".to_string(), "beta".to_string()],
                    valid: vec![true, true, true],
                },
            ],
            symbol_dicts: vec![],
            series_row_counts: vec![],
            row_count: 3,
            min_ts: 1000,
            max_ts: 3000,
        };

        let mt = ColumnarMemtable::from_snapshot(snap, default_config()).expect("from_snapshot");
        assert_eq!(mt.row_count(), 3);
        // Verify the DictEncoded column has correct data.
        match mt.column(1) {
            ColumnData::DictEncoded {
                ids,
                dictionary,
                reverse,
                valid,
            } => {
                assert_eq!(ids, &[0u32, 1, 0]);
                assert_eq!(dictionary, &["alpha", "beta"]);
                assert_eq!(reverse.get("alpha"), Some(&0u32));
                assert_eq!(reverse.get("beta"), Some(&1u32));
                assert_eq!(valid, &[true, true, true]);
            }
            other => panic!("expected DictEncoded, got {:?}", other),
        }
    }

    #[test]
    fn snapshot_from_invalid_column_lengths_returns_error() {
        // row_count says 3 but timestamp column has only 2 rows → mismatch.
        let snap = MemtableSnapshot {
            schema_columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            columns: vec![
                ColumnSnapshot::Timestamp(vec![1000, 2000]),  // 2 rows
                ColumnSnapshot::Float64(vec![1.0, 2.0, 3.0]), // 3 rows — mismatch
            ],
            symbol_dicts: vec![],
            series_row_counts: vec![],
            row_count: 3,
            min_ts: 1000,
            max_ts: 2000,
        };
        let result = ColumnarMemtable::from_snapshot(snap, default_config());
        assert!(
            matches!(result, Err(crate::Error::BadRequest { .. })),
            "expected BadRequest error on length mismatch"
        );
    }
}
