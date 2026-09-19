// SPDX-License-Identifier: BUSL-1.1

//! Timeseries ingest dispatch and side-effect mode types.

use nodedb_physical::physical_plan::document::ReturningSpec;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;

/// Side-effect policy for a timeseries ingest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::data::executor) enum TimeseriesApplyMode {
    Immediate,
    CommitDeferred,
}

/// Parameters for a timeseries ingest operation on the Data Plane.
pub(in crate::data::executor) struct TimeseriesIngestExec<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub format: &'a str,
    pub wal_lsn: Option<u64>,
    pub provenance: Option<&'a SyncProvenance>,
    pub mode: TimeseriesApplyMode,
    /// Compiled row-level-security WRITE predicate carried by the plan.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Projection for a `RETURNING` clause, when the statement carried one.
    /// The ILP listener and Prometheus remote-write build this op directly with
    /// no SQL statement behind them, so they leave it `None`.
    pub returning: Option<&'a ReturningSpec>,
    /// Compiled row-level-security READ predicate gating the rows `returning`
    /// emits — a separate gate from `rls_write_check`, which decides whether
    /// the write happens at all.
    pub rls_filters: &'a [u8],
}

/// Borrowed inputs shared by every timeseries payload decoder.
pub(in crate::data::executor) struct TimeseriesIngestParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: crate::types::TenantId,
    pub collection: &'a str,
    pub payload: &'a [u8],
    pub wal_lsn: Option<u64>,
    pub now_ms: i64,
    pub mode: TimeseriesApplyMode,
    /// Compiled row-level-security WRITE predicate, decided against every
    /// parsed row in `execute_ilp_ingest` — the one point every payload format
    /// funnels through.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Carried through the format decoders unchanged so the projection is
    /// resolved in `execute_ilp_ingest`, on the far side of every format's
    /// normalization into ILP. Projecting in a decoder instead would report the
    /// submitted values rather than the stored point.
    pub returning: Option<&'a ReturningSpec>,
    /// Read gate for the rows `returning` emits. See `TimeseriesIngestExec`.
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    /// The wall-clock instant a timeseries ingest reads, in epoch milliseconds.
    ///
    /// It is the default timestamp for a row that carries none, so anything
    /// reasoning about the row an ingest will store — the statement-time
    /// row-level-security gate in particular — has to read the same clock this
    /// one does, or the two disagree about the time column. `epoch_system_ms`
    /// is the Calvin-supplied deterministic override; only its absence falls
    /// back to the host clock.
    pub(in crate::data::executor) fn ingest_now_ms(&self) -> i64 {
        self.epoch_system_ms.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        })
    }

    /// Execute a timeseries ingest, applying the sync gate and dispatching the
    /// payload to the format-specific implementation.
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
            mode,
            rls_write_check,
            returning,
            rls_filters,
        } = args;
        if let Some(prov) = provenance {
            let admit = self.sync_admit(prov);
            if !matches!(admit, SyncAdmit::Apply) {
                let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                return self.sync_ack_response(task, ack_status_from_admit(&admit), current_hwm);
            }
        }

        let key = (task.request.database_id, tid, collection.to_string());
        if mode == TimeseriesApplyMode::CommitDeferred {
            let governor_pressure = self
                .governor
                .try_reserve(
                    task.request.database_id,
                    tid,
                    nodedb_mem::EngineId::Timeseries,
                    0,
                )
                .is_err();
            let needs_flush = self.columnar_memtables.get(&key).is_some_and(|memtable| {
                memtable.memory_bytes() >= self.ts_tuning.memtable_budget_bytes
                    || memtable.memory_bytes() >= self.ts_tuning.memtable_hard_limit_bytes
                    || governor_pressure
            });
            if needs_flush {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: "transactional timeseries ingest requires a flush before mutation"
                            .into(),
                    },
                );
            }
        }

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
        // A record written before a later truncate describes rows the
        // truncate removed: the same "nothing to write" answer as a record
        // already on disk.
        let already_flushed = already_flushed
            || wal_lsn.is_some_and(|lsn| {
                self.ts_truncate_floors
                    .get(&key)
                    .is_some_and(|floor| lsn <= *floor)
            });

        if already_flushed {
            if let Some(prov) = provenance
                && mode == TimeseriesApplyMode::Immediate
            {
                self.sync_commit(prov);
                let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
                return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
            }
            // This record was already ingested and flushed, so nothing was
            // written now and there is no row to hand back. A projecting
            // statement still owes the client a ROW SET, not the count payload
            // below — the response shaper decodes the two differently, and
            // handing it the wrong one would surface as a decode failure rather
            // than as the empty result this actually is.
            if let Some(spec) = returning {
                return self.timeseries_stored_returning_response(task, spec, rls_filters, &[]);
            }
            let result = serde_json::json!({
                "accepted": 0,
                "rejected": 0,
                "collection": collection,
                "dedup_skipped": true,
            });
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

        let now_ms = self.ingest_now_ms();

        let ingest_response = match format {
            "ilp" => self.execute_ilp_ingest(TimeseriesIngestParams {
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
            }),
            "ilp-msgpack" => self.execute_ilp_msgpack_ingest(TimeseriesIngestParams {
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
            }),
            "json" => self.execute_json_ingest(TimeseriesIngestParams {
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
            }),
            "msgpack" => self.execute_msgpack_ingest(TimeseriesIngestParams {
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
            }),
            _ => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("unknown ingest format: {format}"),
                    },
                );
            }
        };

        if let Some(prov) = provenance
            && ingest_response.status == Status::Ok
            && mode == TimeseriesApplyMode::Immediate
        {
            self.sync_commit(prov);
            let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
            return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
        }
        if ingest_response.status == Status::Ok && mode == TimeseriesApplyMode::Immediate {
            self.note_collection_write_lsn(task, collection);
        }
        ingest_response
    }
}
