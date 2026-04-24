//! Timeseries ILP ingest handler.
//!
//! msgpack / JSON row ingests that normalize into ILP text live in the
//! sibling `ingest_formats` module.

use std::collections::HashMap;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnarMemtable, ColumnarMemtableConfig,
};
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;

impl CoreLoop {
    /// Execute a timeseries ingest.
    ///
    /// `wal_lsn` is set by the WAL catch-up task to enable deduplication:
    /// if the record has already been ingested (LSN <= max ingested) or
    /// flushed to disk (LSN <= max flushed), the ingest is skipped.
    pub(in crate::data::executor) fn execute_timeseries_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        payload: &[u8],
        format: &str,
        wal_lsn: Option<u64>,
    ) -> Response {
        let key = (tid, collection.to_string());
        // LSN-based deduplication: only skip records that are provably
        // already flushed to sealed disk partitions.
        if let Some(lsn) = wal_lsn
            && let Some(registry) = self.ts_registries.get(&key)
        {
            let max_flushed = registry
                .iter()
                .map(|(_, e)| e.meta.last_flushed_wal_lsn)
                .max()
                .unwrap_or(0);
            if max_flushed > 0 && lsn <= max_flushed {
                let result = serde_json::json!({
                    "accepted": 0,
                    "rejected": 0,
                    "collection": collection,
                    "dedup_skipped": true,
                });
                let json = match response_codec::encode_json(&result) {
                    Ok(b) => b,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                };
                return Response {
                    request_id: task.request.request_id,
                    status: Status::Ok,
                    attempt: 1,
                    partial: false,
                    payload: Payload::from_vec(json),
                    watermark_lsn: self.watermark,
                    error_code: None,
                };
            }
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        match format {
            "ilp" => self.execute_ilp_ingest(task, tid, collection, payload, wal_lsn, now_ms),
            "json" => self.execute_json_ingest(task, tid, collection, payload, wal_lsn, now_ms),
            "msgpack" => {
                self.execute_msgpack_ingest(task, tid, collection, payload, wal_lsn, now_ms)
            }
            _ => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("unknown ingest format: {format}"),
                },
            ),
        }
    }

    pub(super) fn execute_ilp_ingest(
        &mut self,
        task: &ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        payload: &[u8],
        wal_lsn: Option<u64>,
        now_ms: i64,
    ) -> Response {
        let key = (tid, collection.to_string());
        let input = match std::str::from_utf8(payload) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid UTF-8 in ILP: {e}"),
                    },
                );
            }
        };

        let lines: Vec<_> = ilp::parse_batch(input)
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        if lines.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "no valid ILP lines in payload".into(),
                },
            );
        }

        let bitemporal = self.is_bitemporal(tid.as_u32(), collection);
        // Ensure memtable exists (auto-create on first write).
        let is_new_memtable = !self.columnar_memtables.contains_key(&key);
        if is_new_memtable {
            let mut schema = ilp_ingest::infer_schema(&lines);
            if bitemporal {
                ilp_ingest::ensure_bitemporal_columns(&mut schema);
            }
            let config = ColumnarMemtableConfig {
                max_memory_bytes: 64 * 1024 * 1024,
                hard_memory_limit: 80 * 1024 * 1024,
                max_tag_cardinality: 100_000,
            };
            let mt = ColumnarMemtable::new(schema, config);
            self.columnar_memtables.insert(key.clone(), mt);
        }

        // Schema evolution: detect new fields and expand memtable schema.
        let cols_before = if !is_new_memtable {
            self.columnar_memtables
                .get(&key)
                .map(|mt| mt.schema().columns.len())
                .unwrap_or(0)
        } else {
            0
        };
        if !is_new_memtable && let Some(mt) = self.columnar_memtables.get_mut(&key) {
            ilp_ingest::evolve_schema(mt, &lines);
        }
        let schema_changed = !is_new_memtable
            && self
                .columnar_memtables
                .get(&key)
                .is_some_and(|mt| mt.schema().columns.len() != cols_before);

        // Pre-flush: flush BEFORE ingesting if memtable is at the soft limit
        // OR if the timeseries engine budget is exhausted (governor pressure).
        let governor_pressure = self
            .governor
            .as_ref()
            .is_some_and(|g| g.try_reserve(nodedb_mem::EngineId::Timeseries, 0).is_err());
        if let Some(mt) = self.columnar_memtables.get(&key)
            && (mt.memory_bytes() >= 64 * 1024 * 1024 || governor_pressure)
        {
            self.flush_ts_collection(tid, collection, now_ms);
        }

        let Some(mt) = self.columnar_memtables.get_mut(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after init: {collection}"),
                },
            );
        };
        // Reserve memory budget for this batch (~24 bytes per ILP line estimate).
        let batch_estimate = lines.len() * 24;
        if let Some(ref gov) = self.governor {
            let _ = gov.try_reserve(nodedb_mem::EngineId::Timeseries, batch_estimate);
        }

        let stamps = if bitemporal {
            Some(ilp_ingest::BitempStamps { system_ms: now_ms })
        } else {
            None
        };
        let lvc = self.ts_last_value_caches.get_mut(&key);
        let mut series_keys = HashMap::new();
        let (mut accepted, rejected) =
            ilp_ingest::ingest_batch_with_lvc(mt, &lines, &mut series_keys, now_ms, lvc, stamps);

        // If rows were rejected (memtable hit hard limit), flush and re-ingest.
        if rejected > 0 {
            tracing::warn!(
                collection,
                accepted,
                rejected,
                "ILP batch rows rejected by hard limit, flushing and retrying"
            );
            self.flush_ts_collection(tid, collection, now_ms);
            if let Some(mt) = self.columnar_memtables.get_mut(&key) {
                let mut retry_keys = HashMap::new();
                let retry_lines = &lines[accepted..];
                let retry_lvc = self.ts_last_value_caches.get_mut(&key);
                let (retry_accepted, _) = ilp_ingest::ingest_batch_with_lvc(
                    mt,
                    retry_lines,
                    &mut retry_keys,
                    now_ms,
                    retry_lvc,
                    stamps,
                );
                accepted += retry_accepted;
            }
        }

        // Post-flush: standard 64MB threshold check.
        let Some(mt) = self.columnar_memtables.get(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after ingest: {collection}"),
                },
            );
        };
        if mt.memory_bytes() >= 64 * 1024 * 1024 {
            self.flush_ts_collection(tid, collection, now_ms);
        }

        // Track WAL LSN and last ingest time for dedup + idle flush.
        if accepted > 0 {
            if let Some(lsn) = wal_lsn {
                let entry = self.ts_max_ingested_lsn.entry(key.clone()).or_insert(0);
                *entry = (*entry).max(lsn);
            }
            self.last_ts_ingest = Some(std::time::Instant::now());
        }

        self.checkpoint_coordinator
            .mark_dirty("timeseries", accepted);

        // Include schema_columns when schema is new OR evolved.
        let include_schema = is_new_memtable || schema_changed;
        let result = if include_schema && let Some(mt) = self.columnar_memtables.get(&key) {
            let schema_columns: Vec<serde_json::Value> = mt
                .schema()
                .columns
                .iter()
                .map(|(name, col_type)| {
                    let type_str = match col_type {
                        ColumnType::Timestamp => "TIMESTAMP",
                        ColumnType::Float64 => "FLOAT",
                        ColumnType::Int64 => "BIGINT",
                        ColumnType::Symbol => "VARCHAR",
                    };
                    serde_json::json!([name, type_str])
                })
                .collect();
            serde_json::json!({
                "accepted": accepted,
                "rejected": rejected,
                "collection": collection,
                "schema_columns": schema_columns,
            })
        } else {
            serde_json::json!({
                "accepted": accepted,
                "rejected": rejected,
                "collection": collection,
            })
        };
        let json = match response_codec::encode_json(&result) {
            Ok(b) => b,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        Response {
            request_id: task.request.request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(json),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }
}
