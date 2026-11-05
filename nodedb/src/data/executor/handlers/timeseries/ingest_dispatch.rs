// SPDX-License-Identifier: BUSL-1.1

//! Timeseries ingest dispatch and side-effect mode types.

use nodedb_physical::physical_plan::document::ReturningSpec;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::sync_gate::{SyncAdmit, ack_status_from_admit};
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::resolved_ingest::RESOLVED_INGEST_FORMAT;

/// Side-effect policy for a timeseries ingest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::data::executor) enum TimeseriesApplyMode {
    /// A live write: one record, one sub-record. The memtable flushes before
    /// the rows land when it cannot take them whole, and after they land
    /// when it is over its budget.
    Immediate,
    /// Restart replay of one sub-record. The replay arm flushed before the
    /// record's first sub-record when the memtable could not take the whole
    /// record, and settles it once the last sub-record landed
    /// (`group_flush`). No flush runs here: it would split the record.
    Replay,
    /// The install pass of a committed redo record. The replay arm flushed
    /// before the record's first sub-record, as in `Replay`; the ingest
    /// records the pre-image its undo restores. No flush, budget recharge or
    /// timer update runs after the rows land: the apply settles those once
    /// the whole record installed (`settle_redo_timeseries`).
    RedoInstall,
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

    /// The ingest instant of a timeseries batch, in epoch milliseconds: the
    /// default timestamp of its untimed rows.
    ///
    /// It is the millisecond of an ordinal of the shared clock documents and
    /// edges stamp from: the Calvin epoch's, the replicated entry's, or a
    /// fresh one. The rows' system time is [`Self::ts_batch_ordinal`].
    pub(in crate::data::executor) fn ingest_stamp_ms(&self) -> i64 {
        match self.epoch_system_ms {
            Some(ms) => ms,
            None => nodedb_types::ordinal_to_ms(self.bitemporal_ordinal()),
        }
    }

    /// The system-time ordinal every row of one timeseries batch stores in
    /// `_ts_system`. A row is visible at a cut `S` iff its ordinal is at or
    /// below `S`. Rows are append-only, so a batch writes one version of each
    /// row it lands.
    ///
    /// A Calvin stage or resolve stamps the transaction's ordinal. An apply
    /// whose WAL record carries an ordinal stamps it
    /// (`ApplyScope::replicated_ordinal`): the one a replicated entry's
    /// leader assigned, or the one the local append reserved. A replay of the
    /// record stamps the same one. A live apply with no carried ordinal
    /// stamps a fresh ordinal of the shared clock, strictly above every
    /// pinned cut the clock folded in, so a read pinned before the apply
    /// never sees the batch. A replay, a redo install, and a Calvin epoch
    /// apply with no carried ordinal stamp the last ordinal of `carried_ms`,
    /// the instant the record carries, so every replay of the record stamps
    /// alike. The stamp folds into the shared clock.
    pub(in crate::data::executor) fn ts_batch_ordinal(
        &self,
        mode: TimeseriesApplyMode,
        carried_ms: i64,
    ) -> i64 {
        let ordinal = match (
            self.apply_scope.calvin_txn_ordinal,
            self.apply_scope.replicated_ordinal,
        ) {
            (Some(ordinal), _) | (None, Some(ordinal)) => ordinal,
            (None, None)
                if mode == TimeseriesApplyMode::Immediate && self.epoch_system_ms.is_none() =>
            {
                self.hlc.next_ordinal()
            }
            (None, None) => nodedb_types::ms_to_ordinal_upper(carried_ms),
        };
        self.observe_bitemporal_ordinal(ordinal);
        ordinal
    }

    /// Fold an ingest stamp a record or a resolved batch carries into the
    /// shared clock, so a later batch or version stamps at or past it.
    pub(in crate::data::executor) fn observe_ingest_stamp_ms(&self, stamp_ms: i64) {
        self.observe_bitemporal_ordinal(stamp_ms.saturating_mul(nodedb_types::NANOS_PER_MS));
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
        // A record the collection's replay stamp names is already in a
        // partition, or a truncate removed its rows: nothing to write. Every
        // other record applies, including one below the highest named LSN,
        // which was still on its way when that flush or truncate ran. A
        // committed-redo apply installs its record once, whatever the stamp
        // names (`replay_watermark_skips`).
        let already_flushed = wal_lsn.is_some_and(|lsn| self.ts_replay_skips(&key, lsn));

        if already_flushed {
            if let Some(prov) = provenance
                && mode != TimeseriesApplyMode::RedoInstall
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
                return self.timeseries_stored_returning_response(
                    task,
                    spec,
                    rls_filters,
                    &[],
                    None,
                );
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
                    return self.response_error(task, ErrorCode::from(e));
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
                stage_vote: None,
                read_vshards: crate::types::ReadShards::new(),
                write_set: Vec::new(),
            };
        }

        // The ingest stamp the write funnel resolved and the WAL record
        // carries, so live apply and replay stamp alike. It folds into the
        // shared clock.
        let now_ms = task
            .resolved_now_ms()
            .and_then(|ms| i64::try_from(ms).ok())
            .unwrap_or_else(|| self.ingest_stamp_ms());
        self.observe_ingest_stamp_ms(now_ms);

        let ingest_response = match format {
            RESOLVED_INGEST_FORMAT => self.execute_resolved_ingest(TimeseriesIngestParams {
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
        {
            self.sync_commit(prov);
            let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
            return self.sync_ack_response(task, AckStatus::Applied, applied_seq);
        }
        if ingest_response.status == Status::Ok
            && let Err(e) = self.note_collection_write(task, collection)
        {
            return self.response_error(task, e);
        }
        ingest_response
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{SystemTimeScope, ms_to_ordinal_upper};

    use super::TimeseriesApplyMode;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    /// A live batch applied after a pinned cut stamps past it, as a
    /// document written after the cut does, so a read pinned at the cut
    /// hides both. A batch and a document before the pin stay visible to it,
    /// including a batch in the cut's own millisecond.
    #[test]
    fn a_batch_after_a_pinned_cut_reads_like_a_document_written_after_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let batch_before = core.ts_batch_ordinal(TimeseriesApplyMode::Immediate, 0);
        let doc_before = core.bitemporal_ordinal();

        // A cut taken now, folded into the clock as a serving node folds a
        // pinned cut. Its millisecond still has ordinals above it.
        let cut = core.hlc.peek();
        core.observe_bitemporal_ordinal(cut);
        let batch_after = core.ts_batch_ordinal(TimeseriesApplyMode::Immediate, 0);
        let doc_after = core.bitemporal_ordinal();

        let scope = SystemTimeScope::Pinned {
            ordinal: cut,
            cut_marker: 0,
        };
        let cutoff = scope.as_of_ordinal().expect("a pinned cut");
        assert!(batch_before <= cutoff, "an earlier batch reads at the cut");
        assert!(doc_before <= cutoff, "an earlier document reads at the cut");
        assert!(
            batch_after > cutoff,
            "a later batch stamps past the cut: {batch_after} vs {cutoff}"
        );
        assert!(doc_after > cutoff, "a later document stamps past the cut");
    }

    /// A replicated entry's apply stamps the entry's ordinal. A replay with
    /// no carried ordinal stamps the last ordinal of the carried instant, on
    /// every replay alike, and folds it into the clock.
    #[test]
    fn a_replayed_batch_stamps_alike_on_every_replay() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        core.apply_scope.replicated_ordinal = Some(77);
        assert_eq!(core.ts_batch_ordinal(TimeseriesApplyMode::Immediate, 5), 77);
        core.apply_scope.replicated_ordinal = None;

        let carried = core.ingest_stamp_ms() + 60_000;
        let first = core.ts_batch_ordinal(TimeseriesApplyMode::Replay, carried);
        let second = core.ts_batch_ordinal(TimeseriesApplyMode::Replay, carried);
        assert_eq!(first, ms_to_ordinal_upper(carried));
        assert_eq!(first, second);
        assert!(core.bitemporal_ordinal() > first, "the stamp folded in");
    }

    /// An ingest stamp a record carries folds into the clock, so the next
    /// batch stamps at or past it.
    #[test]
    fn a_carried_ingest_stamp_folds_into_the_clock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (core, _tx, _rx): (CoreLoop, _, _) = make_core_with_dir(dir.path());
        let carried = core.ingest_stamp_ms() + 60_000;
        core.observe_ingest_stamp_ms(carried);
        assert!(core.ingest_stamp_ms() >= carried);
    }
}
