// SPDX-License-Identifier: BUSL-1.1

//! Timeseries ILP ingest handler.
//!
//! Every ingest format funnels through here: msgpack / JSON row ingests
//! normalize into ILP text in the sibling `ingest_formats` module and then call
//! `execute_ilp_ingest`, so the record-boundary admission gate below covers
//! them all. The checks the gate runs live in the sibling `admission` module.
//!
//! That funnel is also why the row-level-security write gate sits here rather
//! than at each format's entry point: a transport that builds its own ingest
//! task — the raw line-protocol listener does exactly that — still reaches this
//! one place, so the policy cannot be routed around by adding a caller.

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::engine::timeseries::columnar_memtable::{ColumnarMemtable, ColumnarMemtableConfig};
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;

use super::admission;
use super::ingest_dispatch::{TimeseriesApplyMode, TimeseriesIngestParams};
use super::rls_gate;

impl CoreLoop {
    /// Check every condition that could reject a staged ILP ingest before the
    /// transaction is allowed to commit. The simulation is
    /// deliberately isolated from live state: schema evolution and dictionary
    /// probes run against an exact snapshot clone, so this cannot publish a
    /// schema, consume tag IDs, or create a memtable.
    pub(in crate::data::executor) fn prevalidate_deferred_ilp_ingest(
        &self,
        task: &crate::data::executor::task::ExecutionTask,
        tid: crate::types::TenantId,
        collection: &str,
        lines: &[ilp::IlpLine<'_>],
    ) -> Result<(), ErrorCode> {
        let key = (task.request.database_id, tid, collection.to_string());
        let bitemporal =
            self.is_bitemporal(task.request.database_id.as_u64(), tid.as_u64(), collection);
        let existing = self.columnar_memtables.get(&key);
        let live_resident = existing.map(ColumnarMemtable::memory_bytes);
        let mut simulation = match existing {
            Some(memtable) => {
                ColumnarMemtable::from_snapshot(memtable.export_snapshot(), memtable.config())
                    .map_err(|error| ErrorCode::Internal {
                        detail: format!(
                            "failed to clone timeseries memtable for admission: {error}"
                        ),
                    })?
            }
            None => {
                let mut schema = self.initial_ts_schema(task, tid, collection, lines);
                if bitemporal {
                    ilp_ingest::ensure_bitemporal_columns(&mut schema);
                }
                ColumnarMemtable::new(schema, ColumnarMemtableConfig::from_tuning(&self.ts_tuning))
            }
        };
        // Snapshots retain rows and dictionaries but not spare vector capacity,
        // so their baseline footprint can be lower than the live memtable.
        // Simulate only the schema change, then apply that delta to the live
        // resident bytes; otherwise a full live memtable could vote yes.
        let simulation_baseline = simulation.memory_bytes();
        if existing.is_some() {
            ilp_ingest::evolve_schema(&mut simulation, lines);
        }

        if !admission::has_tag_headroom(&simulation, lines, self.ts_tuning.max_tag_cardinality) {
            return Err(ErrorCode::RejectedPrevalidation {
                reason: "transactional timeseries ingest exceeds tag dictionary headroom".into(),
            });
        }

        let governor_pressure = self
            .governor
            .try_reserve(
                task.request.database_id,
                tid,
                nodedb_mem::EngineId::Timeseries,
                0,
            )
            .is_err();
        let resident = match live_resident {
            Some(live) => live.saturating_add(
                simulation
                    .memory_bytes()
                    .saturating_sub(simulation_baseline),
            ),
            None => simulation.memory_bytes(),
        };
        if resident >= self.ts_tuning.memtable_budget_bytes
            || resident >= self.ts_tuning.memtable_hard_limit_bytes
            || governor_pressure
        {
            return Err(ErrorCode::RejectedPrevalidation {
                reason: "transactional timeseries ingest requires a flush before mutation".into(),
            });
        }
        Ok(())
    }

    pub(super) fn execute_ilp_ingest(&mut self, params: TimeseriesIngestParams<'_>) -> Response {
        let TimeseriesIngestParams {
            task,
            tid,
            collection,
            payload,
            wal_lsn,
            now_ms,
            mode,
            rls_write_check,
            returning,
            rls_filters,
        } = params;
        let key = (task.request.database_id, tid, collection.to_string());
        let input = match std::str::from_utf8(payload) {
            Ok(s) => s,
            Err(_) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: "ILP payload is not valid UTF-8".into(),
                    },
                );
            }
        };

        let lines = match ilp::parse_batch(input) {
            Ok(batch) => batch.into_lines(),
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: error.to_string(),
                    },
                );
            }
        };

        if lines.is_empty() {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: "no ILP data lines in payload".into(),
                },
            );
        }
        if lines
            .iter()
            .any(|line| line.measurement.as_ref() != collection)
        {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: "ILP measurements must match the routed collection".into(),
                },
            );
        }

        // The write policy decides the whole batch before anything else in this
        // handler runs: ahead of the memtable being created, the schema being
        // evolved, and the first row being appended. A refusal therefore leaves
        // no trace at all, rather than a schema published or a prefix of the
        // batch made durable.
        let time_key = self
            .declared_ts_time_key(task.request.database_id, tid, collection)
            .map(str::to_string);
        if let Err(error) = rls_gate::admit_ilp_lines(
            rls_write_check,
            &lines,
            time_key.as_deref(),
            now_ms,
            tid.as_u64(),
            collection,
        ) {
            return self.response_error(task, error);
        }

        // A `RETURNING` ingest must take every row or none, because its
        // row set has no place to report a rejected row. The tag ceiling is
        // the one rejection a flush cannot clear, so it is decided here,
        // before the first write.
        if returning.is_some()
            && admission::exceeds_tag_ceiling(
                &self.ts_symbol_columns_for_ingest(
                    task.request.database_id,
                    tid,
                    collection,
                    &lines,
                ),
                &lines,
                self.ts_tuning.max_tag_cardinality,
            )
        {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: format!(
                        "timeseries ingest with RETURNING carries more distinct tag values \
                         than the tag cardinality limit ({}) allows",
                        self.ts_tuning.max_tag_cardinality
                    ),
                },
            );
        }

        if mode == TimeseriesApplyMode::RedoInstall {
            self.record_redo_ts_pre_image(task.request.database_id, tid, collection);
        }

        let bitemporal =
            self.is_bitemporal(task.request.database_id.as_u64(), tid.as_u64(), collection);
        let is_new_memtable = !self.columnar_memtables.contains_key(&key);
        if is_new_memtable {
            let mut schema = self.initial_ts_schema(task, tid, collection, &lines);
            if bitemporal {
                ilp_ingest::ensure_bitemporal_columns(&mut schema);
            }
            let config = ColumnarMemtableConfig::from_tuning(&self.ts_tuning);
            let mt = ColumnarMemtable::new(schema, config);
            self.columnar_memtables.insert(key.clone(), mt);
        }

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

        // The WAL has already committed this record, so the admission gate
        // resolves every possible mid-record stop before the first row lands.
        // A replayed or installed sub-record never flushes here: the replay
        // arm flushed before the record's first sub-record (`group_flush`),
        // and a flush between two sub-records would split the record.
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        if mode == TimeseriesApplyMode::Immediate
            && self.ts_ingest_needs_flush(&key, &lines)
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
        let catalog = self.ts_series_catalogs.entry(key.clone()).or_default();
        let outcome = ilp_ingest::ingest_batch_with_lvc(ilp_ingest::IngestBatchArgs {
            memtable: mt,
            lines: &lines,
            catalog,
            default_timestamp_ms: now_ms,
            lvc,
            bitemporal: stamps,
            collect_row_indices: returning.is_some(),
        });
        let accepted = outcome.accepted;
        let rejected = outcome.rejected;
        // The record's rows landed, whatever the response below says. A
        // flush from here on names the record; the pre-ingest flush above ran
        // before it and does not. A committed-redo install is noted by the
        // apply once the whole record installed.
        if mode != TimeseriesApplyMode::RedoInstall
            && let Some(lsn) = wal_lsn
        {
            self.note_ts_record_applied(lsn);
        }

        if rejected > 0 {
            tracing::warn!(
                collection,
                accepted,
                rejected,
                "ILP batch rows rejected as invalid rows"
            );
        }

        // A rejected row is a FAILURE, not a requested skip. The count
        // response below reports `rejected`, but a `RETURNING` row set has no
        // place for that number. The tag-ceiling check above refuses every
        // rejection it can foresee before any row lands. A rejection that
        // still reaches here follows accepted rows, so the error is
        // `Internal`: a refusal code would claim nothing applied.
        if returning.is_some() && rejected > 0 {
            let reason = outcome
                .first_rejection
                .unwrap_or_else(|| "no reason recorded".to_string());
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "timeseries ingest with RETURNING rejected {rejected} of {} rows and \
                         cannot report them alongside a row set; first rejection: {reason}",
                        accepted + rejected
                    ),
                },
            );
        }

        // Read the stored rows back through the ORDINARY scan projection, at
        // the indices they landed at, before any flush below can drain the
        // memtable out from under those indices. Reusing `emit_memtable_row`
        // is what makes `RETURNING` agree with `SELECT` by construction: a
        // missing float field is stored as NaN and both paths render it as SQL
        // NULL, which a hand-written projection over the ingest values would
        // have printed as "NaN".
        let returned_rows: Vec<rmpv::Value> = match returning {
            Some(_) => match self.columnar_memtables.get(&key) {
                Some(mt) => {
                    match super::raw_scan::emit_memtable_rows_at(mt, &outcome.accepted_row_indices)
                    {
                        Ok(rows) => rows,
                        Err(e) => return self.response_error(task, e),
                    }
                }
                None => Vec::new(),
            },
            None => Vec::new(),
        };

        let Some(mt) = self.columnar_memtables.get(&key) else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("memtable missing after ingest: {collection}"),
                },
            );
        };
        let needs_flush = mt.memory_bytes() >= soft_limit;
        if mode != TimeseriesApplyMode::RedoInstall {
            if mode == TimeseriesApplyMode::Immediate
                && needs_flush
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
                // no-determinism: Instant::now runs only for the operational idle/checkpoint timer, outside a committed-redo install.
                self.last_ts_ingest = Some(std::time::Instant::now());
            }

            self.checkpoint_coordinator
                .mark_dirty("timeseries", accepted);
            self.recharge_ts_memtable_budget(tid, task.request.database_id, collection);
        }

        // Answered only once every flush above has succeeded, so a statement
        // that fails after the rows landed reports the failure rather than a
        // row set. The rows themselves were captured before those flushes,
        // because a flush drains the memtable and invalidates the indices.
        if let Some(spec) = returning {
            return self.timeseries_stored_returning_response(
                task,
                spec,
                rls_filters,
                &returned_rows,
            );
        }

        let include_schema = is_new_memtable || schema_changed;
        let result = if include_schema && let Some(mt) = self.columnar_memtables.get(&key) {
            let schema_columns: Vec<serde_json::Value> = mt
                .schema()
                .columns
                .iter()
                .map(|(name, col_type)| serde_json::json!([name, col_type.ddl_type_name()]))
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
        let json = match response_codec::encode_json_as_msgpack(&result) {
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
