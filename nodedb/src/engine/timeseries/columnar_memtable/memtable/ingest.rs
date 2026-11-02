// SPDX-License-Identifier: BUSL-1.1

//! Row ingest into a [`ColumnarMemtable`].

use nodedb_types::timeseries::{IngestResult, SeriesId};

use super::super::types::{ColumnData, ColumnType, ColumnValue, NULL_SYMBOL_ID};
use super::table::ColumnarMemtable;

impl ColumnarMemtable {
    /// Ingest a row with explicit column values.
    ///
    /// `values` must match the schema length. Tag string values are resolved
    /// to symbol IDs via the per-column dictionary.
    ///
    /// ## Why this does NOT enforce `hard_memory_limit`
    ///
    /// Every row reaching here belongs to a WAL record that has ALREADY
    /// COMMITTED, so refusing it is not backpressure — it is silent loss of a
    /// durable write. A refusal rescued by flushing and re-ingesting
    /// mid-record stamps the flushed partition with the PREVIOUS record's LSN
    /// while it holds part of the current one; replay, gated on that stamp,
    /// re-appends the whole record on top.
    ///
    /// The ceiling lives at the record boundary instead (the ingest handler's
    /// admission gate flushes BEFORE a record when the memtable is at or over
    /// it), so a flush always lands on a whole-record prefix and the
    /// partition's stamp is true of every row in it. Errors returned here are
    /// therefore genuine per-row data faults — bad arity, type mismatch,
    /// exhausted tag dictionary — never "come back later".
    pub fn ingest_row(
        &mut self,
        series_id: SeriesId,
        values: &[ColumnValue],
    ) -> crate::Result<IngestResult> {
        let col_types: Vec<(String, ColumnType)> = self.schema.columns.clone();

        if values.len() != col_types.len() {
            return Err(crate::Error::BadRequest {
                detail: format!("expected {} columns, got {}", col_types.len(), values.len()),
            });
        }

        let mut ts = 0i64;
        let mut row_bytes = 0usize;
        let max_card = self.config.max_tag_cardinality;

        for (i, (val, (col_name, col_type))) in values.iter().zip(col_types.iter()).enumerate() {
            match (val, col_type) {
                (ColumnValue::Timestamp(t), ColumnType::Timestamp(_)) => {
                    if let ColumnData::Timestamp(ref mut v) = self.columns[i] {
                        v.push(*t);
                    }
                    ts = *t;
                    row_bytes += 8;
                }
                (ColumnValue::Float64(f), ColumnType::Float64) => {
                    if let ColumnData::Float64(ref mut v) = self.columns[i] {
                        v.push(*f);
                    }
                    row_bytes += 8;
                }
                // `_ts_series` stores the row's series id, whatever value
                // the row named: the id is the catalog's, never the line's.
                (_, ColumnType::Int64)
                    if col_name.as_str() == nodedb_types::columnar::schema::TS_SERIES =>
                {
                    if let ColumnData::Int64(ref mut v) = self.columns[i] {
                        v.push(i64::from_ne_bytes(series_id.to_ne_bytes()));
                    }
                    row_bytes += 8;
                }
                (ColumnValue::Int64(n), ColumnType::Int64) => {
                    if let ColumnData::Int64(ref mut v) = self.columns[i] {
                        v.push(*n);
                    }
                    row_bytes += 8;
                }
                (ColumnValue::Symbol(s), ColumnType::Symbol | ColumnType::Text) => {
                    // A tag column is held to the cardinality cap. A text
                    // field is not: its cap is the dictionary id space, which
                    // stops short of the NULL id.
                    let cap = if *col_type == ColumnType::Symbol {
                        max_card
                    } else {
                        NULL_SYMBOL_ID
                    };
                    let dict =
                        self.symbol_dicts
                            .get_mut(&i)
                            .ok_or_else(|| crate::Error::BadRequest {
                                detail: format!(
                                    "internal error: symbol dict missing for column {i}"
                                ),
                            })?;
                    let known = dict.len();
                    match dict.resolve(s, cap) {
                        Some(sym_id) => {
                            if let ColumnData::Symbol(ref mut v) = self.columns[i] {
                                v.push(sym_id);
                            }
                            // A new dictionary entry holds the string's bytes.
                            if dict.len() > known {
                                row_bytes += s.len();
                            }
                        }
                        None => {
                            self.rollback_partial_row(i);
                            let limit = if *col_type == ColumnType::Symbol {
                                "tag cardinality"
                            } else {
                                "text dictionary"
                            };
                            return Err(crate::Error::BadRequest {
                                detail: format!(
                                    "{limit} limit ({cap}) exceeded for column '{col_name}'"
                                ),
                            });
                        }
                    }
                    row_bytes += 4;
                }
                // A NULL symbol takes no dictionary entry.
                (ColumnValue::Null, ColumnType::Symbol | ColumnType::Text) => {
                    if let ColumnData::Symbol(ref mut v) = self.columns[i] {
                        v.push(NULL_SYMBOL_ID);
                    }
                    row_bytes += 4;
                }
                (ColumnValue::Null, ColumnType::Float64) => {
                    if let ColumnData::Float64(ref mut v) = self.columns[i] {
                        v.push(f64::NAN);
                    }
                    row_bytes += 8;
                }
                _ => {
                    self.rollback_partial_row(i);
                    return Err(crate::Error::BadRequest {
                        detail: format!("type mismatch at column {i}: expected {col_type:?}"),
                    });
                }
            }
        }

        self.update_stats(series_id, ts, row_bytes);
        Ok(self.check_flush_state())
    }

    /// Roll back a partially written row (called on error during `ingest_row`).
    fn rollback_partial_row(&mut self, columns_written: usize) {
        for col in self.columns.iter_mut().take(columns_written) {
            match col {
                ColumnData::Timestamp(v) => {
                    v.pop();
                }
                ColumnData::Float64(v) => {
                    v.pop();
                }
                ColumnData::Int64(v) => {
                    v.pop();
                }
                ColumnData::Symbol(v) => {
                    v.pop();
                }
                ColumnData::DictEncoded { ids, valid, .. } => {
                    ids.pop();
                    valid.pop();
                }
            }
        }
    }

    fn update_stats(&mut self, series_id: SeriesId, ts: i64, row_bytes: usize) {
        *self.series_row_counts.entry(series_id).or_insert(0) += 1;
        self.row_count += 1;
        self.memory_bytes += row_bytes;
        if ts < self.min_ts {
            self.min_ts = ts;
        }
        if ts > self.max_ts {
            self.max_ts = ts;
        }
    }

    fn check_flush_state(&self) -> IngestResult {
        if self.memory_bytes >= self.config.max_memory_bytes {
            IngestResult::FlushNeeded
        } else {
            IngestResult::Ok
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::types::{ColumnarMemtableConfig, ColumnarSchema, TimeKind};
    use super::*;

    fn default_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 1024 * 1024,
            hard_memory_limit: 2 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    /// Ingest one `(timestamp, value)` row into a metric-layout memtable.
    fn ingest_sample(
        mt: &mut ColumnarMemtable,
        series_id: SeriesId,
        ts: i64,
        value: f64,
    ) -> IngestResult {
        mt.ingest_row(
            series_id,
            &[ColumnValue::Timestamp(ts), ColumnValue::Float64(value)],
        )
        .expect("a metric row fits the metric layout")
    }

    #[test]
    fn ingest_simple_metric() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        let result = ingest_sample(&mut mt, 1, 1000, 42.5);
        assert_eq!(result, IngestResult::Ok);
        assert_eq!(mt.row_count(), 1);
        assert_eq!(mt.min_ts(), 1000);
        assert_eq!(mt.max_ts(), 1000);

        let ts_col = mt.column(0).as_timestamps();
        assert_eq!(ts_col, &[1000]);
        let val_col = mt.column(1).as_f64();
        assert!((val_col[0] - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn ingest_multiple_metrics() {
        let mut mt = ColumnarMemtable::new_metric(default_config());
        for i in 0..100 {
            ingest_sample(&mut mt, i % 10, 1000 + i as i64, i as f64);
        }
        assert_eq!(mt.row_count(), 100);
        assert_eq!(mt.series_count(), 10);
        assert_eq!(mt.min_ts(), 1000);
        assert_eq!(mt.max_ts(), 1099);
    }

    #[test]
    fn ingest_row_with_tags() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
                ("host".into(), ColumnType::Symbol),
                ("dc".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 4],
        };
        let mut mt = ColumnarMemtable::new(schema, default_config());

        let result = mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(5000),
                ColumnValue::Float64(99.9),
                ColumnValue::Symbol("prod-1".to_string()),
                ColumnValue::Symbol("us-east".to_string()),
            ],
        );
        assert!(result.is_ok());
        assert_eq!(mt.row_count(), 1);

        // Verify symbol dictionaries were populated.
        let host_dict = mt.symbol_dict(2).unwrap();
        assert_eq!(host_dict.len(), 1);
        assert_eq!(host_dict.get(0), Some("prod-1"));

        let dc_dict = mt.symbol_dict(3).unwrap();
        assert_eq!(dc_dict.get(0), Some("us-east"));
    }

    #[test]
    fn tag_cardinality_breaker() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
                ("tag".into(), ColumnType::Symbol),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
        };
        let config = ColumnarMemtableConfig {
            max_tag_cardinality: 5,
            ..default_config()
        };
        let mut mt = ColumnarMemtable::new(schema, config);

        // First 5 unique tags work.
        for i in 0..5 {
            let tag = format!("val-{i}");
            let r = mt.ingest_row(
                i as u64,
                &[
                    ColumnValue::Timestamp(1000 + i as i64),
                    ColumnValue::Float64(1.0),
                    ColumnValue::Symbol(tag.clone()),
                ],
            );
            assert!(r.is_ok());
        }
        assert_eq!(mt.row_count(), 5);

        // 6th unique tag is rejected.
        let r = mt.ingest_row(
            99,
            &[
                ColumnValue::Timestamp(2000),
                ColumnValue::Float64(1.0),
                ColumnValue::Symbol("one-too-many".to_string()),
            ],
        );
        assert!(r.is_err());
        // Row count didn't increase (rolled back).
        assert_eq!(mt.row_count(), 5);
    }

    #[test]
    fn ingest_row_accepts_past_hard_limit() {
        // A row reaching the memtable belongs to an already-committed WAL
        // record. Refusing it would drop a durable write on replay. So
        // `ingest_row` accepts every row regardless of the ceiling. It never
        // returns `Rejected`, and the resident footprint overshoots the hard
        // limit.
        let config = ColumnarMemtableConfig {
            max_memory_bytes: 100,
            hard_memory_limit: 200,
            max_tag_cardinality: 1000,
        };
        let mut mt = ColumnarMemtable::new_metric(config);

        for i in 0..1000 {
            let r = ingest_sample(&mut mt, 1, i, 1.0);
            assert_ne!(
                r,
                IngestResult::Rejected,
                "row {i} must not be rejected — that would drop a durable record on replay"
            );
        }
        assert_eq!(mt.row_count(), 1000, "every row past the limit is retained");
        assert!(
            mt.memory_bytes() >= 200,
            "the footprint is allowed to overshoot the hard limit"
        );
    }

    #[test]
    fn flush_needed_signal() {
        let config = ColumnarMemtableConfig {
            max_memory_bytes: 100,
            hard_memory_limit: 200,
            max_tag_cardinality: 1000,
        };
        let mut mt = ColumnarMemtable::new_metric(config);

        let mut flush_signaled = false;
        for i in 0..100 {
            let r = ingest_sample(&mut mt, 1, i, 1.0);
            if r == IngestResult::FlushNeeded {
                flush_signaled = true;
                break;
            }
        }
        assert!(flush_signaled);
    }

    #[test]
    fn type_mismatch_rejected() {
        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("value".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        let mut mt = ColumnarMemtable::new(schema, default_config());

        let r = mt.ingest_row(
            1,
            &[
                ColumnValue::Timestamp(1000),
                ColumnValue::Int64(42), // Wrong: schema says Float64
            ],
        );
        assert!(r.is_err());
        assert_eq!(mt.row_count(), 0); // Rolled back.
    }
}
