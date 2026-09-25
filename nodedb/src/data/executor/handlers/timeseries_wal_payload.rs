// SPDX-License-Identifier: BUSL-1.1

//! Apply one decoded `TimeseriesBatch` record: a timeseries ingest into the
//! collection's memtable, or a plain columnar insert.

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::{TimeseriesApplyMode, TimeseriesIngestExec};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::engine::timeseries::columnar_memtable::{
    ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp};
use nodedb_types::timeseries::MetricSample;

use super::timeseries_wal_decode::{ColumnarReplayArgs, TimeseriesReplayArgs};

impl CoreLoop {
    /// Ensure a timeseries memtable exists for the given collection, creating if needed.
    ///
    /// Uses the same operator tuning the live ingest path does. A memtable keeps
    /// the limits it was built with for its whole life, so seeding replay with
    /// hardcoded defaults would leave a restarted node running budgets the
    /// operator did not configure until every collection happened to flush.
    fn ensure_columnar_memtable(
        &mut self,
        key: (DatabaseId, crate::types::TenantId, String),
        schema: ColumnarSchema,
    ) {
        let config = ColumnarMemtableConfig::from_tuning(&self.ts_tuning);
        self.columnar_memtables
            .entry(key)
            .or_insert_with(|| ColumnarMemtable::new(schema, config));
    }

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
            provenance,
            format,
            default_timestamp_ms,
        } = args;
        if let Ok(batch) =
            zerompk::from_msgpack::<nodedb_types::timeseries::TimeseriesWalBatch>(payload)
        {
            let key = (db_id, tid, collection.to_string());
            if self.recording_redo_undo() {
                let undo = UndoEntry::TimeseriesIngest(self.capture_timeseries_ingest_undo(&key));
                self.record_redo_undo([undo]);
            }
            self.ensure_columnar_memtable(key.clone(), ColumnarSchema::metric_default());

            let Some(mt) = self.columnar_memtables.get_mut(&key) else {
                return 0;
            };
            for (series_id, timestamp_ms, value) in &batch.samples {
                mt.ingest_metric(
                    *series_id,
                    MetricSample {
                        timestamp_ms: *timestamp_ms,
                        value: *value,
                    },
                );
            }
            let sample_count = batch.samples.len();
            if self.recording_redo_undo() {
                // The install settles the budget once the whole record landed.
                self.note_redo_timeseries_written(key);
            } else {
                // Re-charge the engine memory budget to the memtable's
                // resident footprint after replaying these samples. The
                // reservation is held until the memtable is drained on flush,
                // so a replay-driven flush balances its release instead of
                // over-releasing.
                self.recharge_ts_memtable_budget(tid, db_id, collection);
            }
            return sample_count;
        }

        let format = format.unwrap_or_else(|| {
            if std::str::from_utf8(payload).is_ok() {
                "ilp"
            } else {
                "msgpack"
            }
        });
        let mut task = Self::replay_task(
            tid,
            db_id,
            crate::types::VShardId::from_collection_in_database(db_id, collection),
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
                payload: payload.to_vec(),
                format: format.to_string(),
                wal_lsn: Some(record_lsn),
                surrogates: Vec::new(),
                provenance: provenance.clone(),
                rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
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
        let response = self.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid,
            collection,
            payload,
            format,
            wal_lsn: Some(record_lsn),
            provenance: provenance.as_ref(),
            mode,
            // Replay re-applies a record the policy already decided when it was
            // written, and the identity that wrote it is not present at boot to
            // resolve `$auth.*` against. A refused write never reaches replay:
            // its record is cancelled before the refusal is acknowledged.
            rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            // Replay rebuilds stored state at boot; there is no client waiting
            // on a row set, and no identity whose reads would need gating. The
            // projection belongs to the originating request, which was answered
            // before the process restarted.
            returning: None,
            rls_filters: &[],
        });
        if response.status != crate::bridge::envelope::Status::Ok {
            self.replay_record_rejected(
                "timeseries",
                record_lsn,
                response.error_code,
                &format!("timeseries ingest into '{collection}' failed"),
            );
            return 0;
        }
        if installing {
            self.note_redo_timeseries_written((db_id, tid, collection.to_string()));
        }
        if format == "ilp-msgpack" {
            return zerompk::from_msgpack::<Vec<String>>(payload).map_or(0, |rows| rows.len());
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
            provenance,
            surrogates,
            conflict_policy,
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
        let task = Self::replay_task(
            tid,
            db_id,
            crate::types::VShardId::from_collection_in_database(db_id, collection),
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
                payload: payload.to_vec(),
                format: "msgpack".into(),
                intent: conflict_policy.intent,
                on_conflict_updates: conflict_policy.on_conflict_updates.clone(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: Some(record_lsn),
                rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            Some(crate::types::Lsn::new(record_lsn)),
        );
        // Restore the persisted per-row surrogates so `execute_columnar_insert`
        // rebinds the exact same cross-engine identity via
        // `insert_with_surrogate`. An empty slice (legacy records / sync path)
        // falls back to fresh allocation as before.
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
                rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                // WAL replay reconstructs stored state; there is no client
                // waiting on a projection, and no identity to gate reads for.
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        );
        if response.status != crate::bridge::envelope::Status::Ok {
            self.replay_record_rejected(
                "columnar",
                record_lsn,
                response.error_code,
                &format!("columnar insert into '{collection}' failed"),
            );
            return 0;
        }
        match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(rows)) => rows.len(),
            Ok(nodedb_types::Value::Object(_)) => 1,
            _ => 0,
        }
    }
}
