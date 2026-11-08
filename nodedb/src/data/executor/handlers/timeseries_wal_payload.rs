// SPDX-License-Identifier: BUSL-1.1

//! Apply one decoded `TimeseriesBatch` record: a timeseries ingest into the
//! collection's memtable, or a plain columnar insert.

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::{TimeseriesApplyMode, TimeseriesIngestExec};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp};

use super::timeseries_wal_decode::{ColumnarReplayArgs, TimeseriesReplayArgs};

/// The payload formats the timeseries ingest handler accepts.
const INGEST_FORMATS: [&str; 5] = [
    crate::engine::timeseries::resolved_ingest::RESOLVED_INGEST_FORMAT,
    "ilp",
    "ilp-msgpack",
    "json",
    "msgpack",
];

struct BatchReplayContext<'a> {
    engine: &'a str,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &'a str,
    record_lsn: u64,
    grouped: bool,
    provenance: Option<&'a nodedb_types::sync::wire::SyncProvenance>,
}

impl CoreLoop {
    /// Replay one timeseries ingest record through the live ingest handler.
    ///
    /// Every timeseries ingest record names its payload format. A record
    /// that names no format, or one the handler does not accept, is not a
    /// record shape any writer produces. Restart replay halts on it, and a
    /// committed-redo apply fails.
    pub(super) fn replay_timeseries_payload(
        &mut self,
        tid: crate::types::TenantId,
        db_id: DatabaseId,
        args: TimeseriesReplayArgs<'_>,
    ) -> usize {
        let TimeseriesReplayArgs {
            collection,
            payload,
            record_lsn,
            grouped,
            provenance,
            rls_write_check,
            format,
            default_timestamp_ms,
            system_ordinal,
        } = args;
        let Some(format) = format.filter(|format| INGEST_FORMATS.contains(format)) else {
            self.replay_record_unapplied(
                "timeseries",
                "format",
                record_lsn,
                &format!(
                    "timeseries record for '{collection}' names no known ingest format \
                     ({format:?})"
                ),
            );
            return 0;
        };
        let Some(vshard) = self.replay_vshard("timeseries", record_lsn, db_id, collection) else {
            return 0;
        };
        let Some(qualified) =
            self.replay_qualified("timeseries", record_lsn, db_id, collection.to_string())
        else {
            return 0;
        };
        let mut task = Self::replay_task(
            tid,
            db_id,
            vshard,
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: qualified,
                payload: payload.to_vec(),
                format: format.to_string(),
                wal_lsn: Some(record_lsn),
                surrogates: Vec::new(),
                provenance: provenance.clone(),
                rls_write_check: rls_write_check.clone(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            Some(crate::types::Lsn::new(record_lsn)),
        );
        // Untimed rows take the instant the record carries, the one the live
        // apply stored them with.
        task.resolved_now_ms = default_timestamp_ms.and_then(|ms| u64::try_from(ms).ok());
        let installing = self.recording_redo_undo();
        if installing {
            let hwm = self.capture_sync_hwm_undo(provenance.as_ref());
            self.record_redo_undo(hwm);
        }
        let mode = if installing {
            TimeseriesApplyMode::RedoInstall
        } else {
            TimeseriesApplyMode::Replay
        };
        // The batch stamps the ordinal the record carries, the one its live
        // apply stamped. A record that carries none keeps the scope the
        // install or replay already holds.
        let outer_ordinal = self.apply_scope.replicated_ordinal;
        if system_ordinal.is_some() {
            self.apply_scope.replicated_ordinal = system_ordinal;
        }
        let prior_hwm = (grouped && !self.applying_committed_redo())
            .then(|| self.capture_sync_hwm_undo(provenance.as_ref()))
            .flatten();
        let response = self.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid,
            collection,
            payload,
            format,
            wal_lsn: Some(record_lsn),
            provenance: provenance.as_ref(),
            mode,
            rls_write_check,
            // Replay rebuilds stored state at boot; there is no client waiting
            // on a row set, and no identity whose reads would need gating. The
            // projection belongs to the originating request, which was answered
            // before the process restarted.
            returning: None,
            rls_filters: &[],
        });
        self.apply_scope.replicated_ordinal = outer_ordinal;
        if !self.replay_batch_verdict(
            response,
            BatchReplayContext {
                engine: "timeseries",
                database_id: db_id,
                tenant_id: tid.as_u64(),
                collection,
                record_lsn,
                grouped,
                provenance: provenance.as_ref(),
            },
            prior_hwm,
        ) {
            return 0;
        }
        if installing {
            self.note_redo_timeseries_written((db_id, tid, collection.to_string()));
        }
        if format == "ilp-msgpack" {
            return zerompk::from_msgpack::<Vec<String>>(payload).map_or(0, |rows| rows.len());
        }
        if format == crate::engine::timeseries::resolved_ingest::RESOLVED_INGEST_FORMAT {
            return crate::engine::timeseries::resolved_ingest::ResolvedTsBatch::from_bytes(
                payload,
            )
            .map_or(0, |batch| batch.rows.len());
        }
        match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(rows)) => rows.len(),
            Ok(nodedb_types::Value::Object(_)) => 1,
            _ => 0,
        }
    }

    pub(super) fn replay_columnar_payload(
        &mut self,
        tid: crate::types::TenantId,
        db_id: DatabaseId,
        args: ColumnarReplayArgs<'_>,
    ) -> usize {
        let ColumnarReplayArgs {
            collection,
            payload,
            record_lsn,
            grouped,
            provenance,
            surrogates,
            conflict_policy,
            system_from,
            restored_versions,
        } = args;
        // Each row whose key already exists is skipped, merged or replaced the
        // way the live insert decided it.
        let conflict_policy = match crate::wal::ColumnarConflictPolicy::decode(&conflict_policy) {
            Ok(policy) => policy,
            Err(error) => {
                self.replay_record_unapplied(
                    "columnar",
                    "conflict_policy",
                    record_lsn,
                    &format!("columnar insert into '{collection}': {error}"),
                );
                return 0;
            }
        };
        // `execute_columnar_insert` reads only `task.request.{database_id,
        // tenant_id, request_id}` — it never inspects the embedded plan.
        // Embed empty vecs for the plan-level surrogates/provenance to avoid
        // cloning the owned values we need to pass as explicit args below.
        let Some(vshard) = self.replay_vshard("columnar", record_lsn, db_id, collection) else {
            return 0;
        };
        let Some(qualified) =
            self.replay_qualified("columnar", record_lsn, db_id, collection.to_string())
        else {
            return 0;
        };
        let task = Self::replay_task(
            tid,
            db_id,
            vshard,
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: qualified,
                payload: payload.to_vec(),
                format: "msgpack".into(),
                intent: conflict_policy.intent,
                on_conflict_updates: conflict_policy.on_conflict_updates.clone(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: Some(record_lsn),
                system_from,
                // The handler reads the stamps from the params below, never
                // from the embedded plan.
                restored_versions: Vec::new(),
                rls_write_check: conflict_policy.rls_write_check.clone(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            Some(crate::types::Lsn::new(record_lsn)),
        );
        // Restore the persisted per-row surrogates so `execute_columnar_insert`
        // rebinds the exact same cross-engine identity via
        // `insert_with_surrogate`. An empty slice leaves the rows unbound,
        // exactly as the live insert that carried no surrogates did.
        let prior_hwm = (grouped && !self.applying_committed_redo())
            .then(|| self.capture_sync_hwm_undo(provenance.as_ref()))
            .flatten();
        if self.recording_redo_undo() {
            let hwm = self.capture_sync_hwm_undo(provenance.as_ref());
            self.record_redo_undo(hwm);
        }
        let response = self.execute_columnar_insert(
            &task,
            crate::data::executor::handlers::columnar_write::ColumnarInsertParams {
                collection,
                payload,
                format: "msgpack",
                intent: conflict_policy.intent,
                on_conflict_updates: &conflict_policy.on_conflict_updates,
                surrogates: &surrogates,
                schema_bytes: &[],
                provenance: provenance.as_ref(),
                system_from,
                restored_versions: &restored_versions,
                rls_write_check: &conflict_policy.rls_write_check,
                // WAL replay reconstructs stored state; there is no client
                // waiting on a projection, and no identity to gate reads for.
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        );
        if !self.replay_batch_verdict(
            response,
            BatchReplayContext {
                engine: "columnar",
                database_id: db_id,
                tenant_id: tid.as_u64(),
                collection,
                record_lsn,
                grouped,
                provenance: provenance.as_ref(),
            },
            prior_hwm,
        ) {
            return 0;
        }
        match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(rows)) => rows.len(),
            Ok(nodedb_types::Value::Object(_)) => 1,
            _ => 0,
        }
    }
    /// Standalone refused intents consume their sequence. Grouped errors undo the frontier and halt.
    fn replay_batch_verdict(
        &mut self,
        response: Response,
        context: BatchReplayContext<'_>,
        prior_hwm: Option<UndoEntry>,
    ) -> bool {
        if response.status == crate::bridge::envelope::Status::Ok {
            return true;
        }
        self.restore_grouped_replay_sync_hwm(
            prior_hwm,
            context.database_id.as_u64(),
            context.tenant_id,
            context.record_lsn,
        );
        if !context.grouped
            && !self.applying_committed_redo()
            && matches!(response.error_code.as_deref(), Some(ErrorCode::SyncRejected { provenance, applied_seq, .. })
                if Some(provenance) == context.provenance && provenance.producer_id != 0 && *applied_seq >= provenance.seq)
        {
            return false;
        }
        if self.applying_committed_redo() {
            self.replay_record_rejected(
                context.engine,
                context.record_lsn,
                response.error_code,
                &format!(
                    "{} batch into '{}' failed",
                    context.engine, context.collection
                ),
            );
        } else {
            self.replay_record_unapplied(
                context.engine,
                "batch_handler",
                context.record_lsn,
                &format!(
                    "{} batch into '{}' failed: {:?}",
                    context.engine, context.collection, response.error_code
                ),
            );
        }
        false
    }
}
