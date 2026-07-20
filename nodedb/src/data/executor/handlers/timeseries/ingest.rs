// SPDX-License-Identifier: BUSL-1.1

//! Timeseries ILP ingest handler.
//!
//! Every ingest format funnels through here: msgpack / JSON row ingests
//! normalize into ILP text in the sibling `ingest_formats` module and then call
//! `execute_ilp_ingest`, so the record-boundary admission gate below covers
//! them all. The checks the gate runs live in the sibling `admission` module.

use std::collections::HashMap;

use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnarMemtable, ColumnarMemtableConfig,
};
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;

use super::admission;

/// Parameters for a timeseries ingest operation on the Data Plane.
///
/// Bundles the non-`self` arguments to `execute_timeseries_ingest` so the
/// method stays within the argument-count limit.
pub(in crate::data::executor) struct TimeseriesIngestExec<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    pub wal_lsn: Option<u64>,
    pub provenance: Option<&'a SyncProvenance>,
}

impl CoreLoop {
    /// Execute a timeseries ingest.
    ///
    /// When `provenance` is `Some`, the sync idempotency gate runs first:
    /// - Duplicate / Fenced / Gap → return `SyncAckResult` via `response_with_payload`
    ///   without re-applying engine state.
    /// - Apply → continue; after the memtable write call `sync_commit` to
    ///   advance the HWM, then return `SyncAckResult{Applied}` via payload.
    ///
    /// When `provenance` is `None` (SQL / ILP paths), behave exactly as
    /// before: no gate, no `SyncAckResult` in the payload.
    ///
    /// `wal_lsn` deduplication (last-flushed skip) is preserved on the Apply
    /// branch: if the record is already on disk the memtable write is skipped,
    /// but `sync_commit` still advances the HWM because the record WAS
    /// applied (durably flushed to a segment).
    pub(in crate::data::executor) fn execute_timeseries_ingest(
        &mut self,
        args: TimeseriesIngestExec<'_>,
    ) -> Response {
        let TimeseriesIngestExec {
            task,
            tid,
            collection,
            payload,
            format,
            wal_lsn,
            provenance,
        } = args;
        // ── Sync idempotency gate (Data-Plane side) ──────────────────────────
        if let Some(prov) = provenance {
            let admit = self.sync_admit(prov);
            match admit {
                SyncAdmit::Apply => {
                    // Fall through to the ingest path below.
                    // sync_commit is called AFTER the memtable write.
                }
                non_apply => {
                    let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                    return self.sync_ack_response(
                        task,
                        ack_status_from_admit(&non_apply),
                        current_hwm,
                    );
                }
            }
        }

        let key = (task.request.database_id, tid, collection.to_string());

        // ── LSN-based deduplication (last-flushed skip) ──────────────────────
        // Skip memtable re-apply if the record was already flushed to disk.
        // On the sync path we still advance the HWM after this check because
        // the record IS durably applied (on the flushed segment).
        let already_flushed = if let Some(lsn) = wal_lsn
            && let Some(registry) = self.ts_registries.get(&key)
        {
            let max_flushed = registry
                .iter()
                .map(|(_, e)| e.meta.last_flushed_wal_lsn)
                .max()
                .unwrap_or(0);
            max_flushed > 0 && lsn <= max_flushed
        } else {
            false
        };

        if already_flushed {
            // Advance the HWM even though the memtable write is skipped — the
            // record is durable on disk, so the seq counts as applied.
            if let Some(prov) = provenance {
                self.sync_commit(prov);
                let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
            }

            // Non-sync path: return original dedup_skipped JSON shape.
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
                read_set_valid: None,
                read_version_lsn: crate::types::Lsn::ZERO,
                write_set: Vec::new(),
            };
        }

        // Use the epoch's deterministic timestamp when executing inside a Calvin
        // txn; fall back to wall clock for single-shard (non-Calvin) paths.
        let now_ms: i64 = self.epoch_system_ms.unwrap_or_else(|| {
            // no-determinism: fallback only reached outside Calvin path; epoch_system_ms is set for Calvin
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });

        let ingest_response = match format {
            "ilp" => self.execute_ilp_ingest(task, tid, collection, payload, wal_lsn, now_ms),
            "json" => self.execute_json_ingest(task, tid, collection, payload, wal_lsn, now_ms),
            "msgpack" => {
                self.execute_msgpack_ingest(task, tid, collection, payload, wal_lsn, now_ms)
            }
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("unknown ingest format: {format}"),
                    },
                );
            }
        };

        // On the sync path, advance the HWM after a successful ingest and
        // return a SyncAckResult payload instead of the normal JSON body.
        if let Some(prov) = provenance
            && ingest_response.status == Status::Ok
        {
            self.sync_commit(prov);
            let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
            return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
        }

        // Advance the collection floor for this committed timeseries write.
        if ingest_response.status == Status::Ok {
            self.note_collection_write_lsn(task, collection);
        }

        // Either no provenance, or ingest failed on the Apply path — surface
        // the response as-is; the HWM is NOT advanced (record not applied).
        ingest_response
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
        let key = (task.request.database_id, tid, collection.to_string());
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

        let bitemporal =
            self.is_bitemporal(task.request.database_id.as_u64(), tid.as_u64(), collection);
        // Ensure memtable exists (auto-create on first write).
        let is_new_memtable = !self.columnar_memtables.contains_key(&key);
        if is_new_memtable {
            let mut schema = ilp_ingest::infer_schema(&lines);
            if bitemporal {
                ilp_ingest::ensure_bitemporal_columns(&mut schema);
            }
            let config = ColumnarMemtableConfig::from_tuning(&self.ts_tuning);
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

        // ── Record-boundary admission gate ───────────────────────────────────
        //
        // By the time the Data Plane sees this record, the WAL has ALREADY
        // COMMITTED it. Refusing its rows for memory is therefore not
        // backpressure — it is silent loss of a durable write. The memtable
        // MUST take the record whole, so everything that could otherwise stop
        // it partway through is resolved HERE, before the first row lands:
        //
        //   * soft budget reached — flush now rather than mid-record;
        //   * governor pressure — the timeseries engine budget is exhausted;
        //   * hard ceiling reached — the memtable would previously have started
        //     refusing rows somewhere inside this record;
        //   * tag-dictionary headroom — the dictionaries cannot absorb this
        //     batch's distinct symbols and would start failing rows INTERLEAVED
        //     with rows that still resolve.
        //
        // Both limits are tested because nothing orders them: a config with a
        // hard ceiling below the soft budget makes the hard term the binding
        // one, and vice versa.
        //
        // Gating here is what makes `flush_ts_collection`'s stamp true. That
        // flush labels its partition with `ts_max_ingested_lsn`, which is this
        // record's PREDECESSOR (this record's LSN is recorded only once it is
        // fully ingested, below), and boot replay skips every record at or
        // below the highest stamp it finds. Flushing between two rows of this
        // record writes a partition holding part of it but stamped with its
        // predecessor — replay then does not skip the record and re-appends all
        // of it on top of the rows already in that partition. Stamping this
        // record's LSN instead would be worse: replay would skip it and lose
        // the rows that had not been flushed.
        //
        // The accepted cost is a bounded overshoot. After a pre-flush the
        // memtable is empty and takes the whole record regardless of its size,
        // so this call can end above `hard_memory_limit` by up to one record's
        // decoded payload — bounded by the WAL's own
        // `MAX_WAL_PAYLOAD_SIZE` (64 MiB) cap on a record, times this
        // collection's decoded-bytes-per-payload-byte ratio. The post-ingest
        // flush below then drains it immediately, so the overshoot is transient
        // rather than a new resident ceiling. Overshooting for one record beats
        // duplicating or losing rows the WAL has already promised the client.
        let governor_pressure = self.governor.as_ref().is_some_and(|g| {
            g.try_reserve(
                task.request.database_id,
                tid,
                nodedb_mem::EngineId::Timeseries,
                0,
            )
            .is_err()
        });
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        let hard_limit = self.ts_tuning.memtable_hard_limit_bytes;
        let max_tag_cardinality = self.ts_tuning.max_tag_cardinality;
        let needs_flush = self.columnar_memtables.get(&key).is_some_and(|mt| {
            let resident = mt.memory_bytes();
            resident >= soft_limit
                || resident >= hard_limit
                || governor_pressure
                || !admission::has_tag_headroom(mt, &lines, max_tag_cardinality)
        });
        if needs_flush
            && let Err(e) =
                self.flush_ts_collection(tid, task.request.database_id, collection, now_ms)
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("pre-ingest ts flush failed: {e}"),
                },
            );
        }

        let Some(mt) = self.columnar_memtables.get_mut(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after init: {collection}"),
                },
            );
        };

        let stamps = if bitemporal {
            Some(ilp_ingest::BitempStamps { system_ms: now_ms })
        } else {
            None
        };
        let lvc = self.ts_last_value_caches.get_mut(&key);
        let mut series_keys = HashMap::new();
        let (accepted, rejected) =
            ilp_ingest::ingest_batch_with_lvc(mt, &lines, &mut series_keys, now_ms, lvc, stamps);

        // A rejection here is a genuine per-row data fault (bad arity, type
        // mismatch, or a batch whose own distinct-symbol count exceeds
        // `max_tag_cardinality` and so fits in no generation). It is reported
        // as `rejected` in the response and NOTHING else: there is deliberately
        // no flush-and-retry.
        //
        // The retry that used to live here re-ingested `lines[accepted..]` on
        // the assumption that rejections were a strict suffix. Cardinality
        // rejections are not — once a dictionary is full, lines carrying new
        // tag values fail while lines reusing existing ones still succeed — so
        // `accepted` under-counted the consumed prefix and the retry re-ingested
        // lines already in the memtable. The flush that preceded it reset the
        // dictionaries, so the retry SUCCEEDED, duplicating them on the spot.
        // The admission gate above removes the reason to retry at all: the
        // memtable was made able to take the record whole before the first row
        // went in.
        if rejected > 0 {
            tracing::warn!(
                collection,
                accepted,
                rejected,
                "ILP batch rows rejected as invalid rows"
            );
        }

        // Track this record's WAL LSN BEFORE the post-ingest flush below, and
        // only now that the record is FULLY ingested.
        //
        // The order is load-bearing. `flush_ts_collection` stamps the partition
        // it writes with `ts_max_ingested_lsn`, and replay skips every record at
        // or below the highest stamp it finds. Stamping after the flush — as
        // this did — labelled a partition holding record L's rows with L-1, so a
        // restart replayed L back on top of the partition that already contained
        // it and every one of its rows appeared twice. Timeseries ingest is an
        // APPEND: nothing masks the duplicate.
        if accepted > 0
            && let Some(lsn) = wal_lsn
        {
            let entry = self.ts_max_ingested_lsn.entry(key.clone()).or_insert(0);
            *entry = (*entry).max(lsn);
        }

        // Post-flush: soft-budget check. Correct to run here because the record
        // is now fully ingested and its LSN recorded, so the partition this
        // writes is stamped with an LSN that covers every row in it — including
        // this record's. It is also what makes the admission gate's one-record
        // overshoot transient: a record that carried the memtable past the
        // budget is drained again before the next one arrives.
        let Some(mt) = self.columnar_memtables.get(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after ingest: {collection}"),
                },
            );
        };
        let needs_flush = mt.memory_bytes() >= soft_limit;
        if needs_flush
            && let Err(e) =
                self.flush_ts_collection(tid, task.request.database_id, collection, now_ms)
        {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("post-ingest ts flush failed: {e}"),
                },
            );
        }

        if accepted > 0 {
            // no-determinism: last_ts_ingest is a flush-trigger timer, not Calvin row data
            self.last_ts_ingest = Some(std::time::Instant::now());
        }

        self.checkpoint_coordinator
            .mark_dirty("timeseries", accepted);

        // Re-charge the engine memory budget to the memtable's current
        // resident footprint. The reservation is held (in
        // `columnar_memtable_mem`) until the memtable is drained on flush,
        // so the Timeseries budget reflects what the memtable holds and the
        // flush release is balanced — never `release()`-ing bytes that were
        // never reserved.
        self.recharge_ts_memtable_budget(tid, task.request.database_id, collection);

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
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }
}
