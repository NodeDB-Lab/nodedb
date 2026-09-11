// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for MetaOp variants (WAL, snapshots, retention, continuous aggregates).

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::MetaOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::control::calvin::CalvinExecCtx;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_meta(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &MetaOp,
    ) -> Response {
        match op {
            MetaOp::WalAppend { payload } => self.execute_wal_append(task, payload),

            MetaOp::Cancel { target_request_id } => self.execute_cancel(task, *target_request_id),

            MetaOp::TransactionBatch { plans, txn_id } => {
                self.execute_transaction_batch(task, tid, plans, &[], *txn_id)
            }

            MetaOp::CreateSnapshot => self.execute_create_snapshot(task),
            MetaOp::Compact => self.execute_compact(task),
            MetaOp::Checkpoint => self.execute_checkpoint(task),

            MetaOp::RegisterContinuousAggregate { def } => {
                self.continuous_agg_mgr.register(def.clone());
                tracing::info!(
                    name = def.name,
                    source = def.source,
                    interval = def.bucket_interval,
                    "continuous aggregate registered"
                );
                self.response_ok(task)
            }

            MetaOp::UnregisterContinuousAggregate { name } => {
                self.continuous_agg_mgr
                    .unregister(task.request.database_id.as_u64(), name);
                tracing::info!(name, "continuous aggregate unregistered");
                self.response_ok(task)
            }

            MetaOp::ListContinuousAggregates => {
                let infos = self.continuous_agg_mgr.list_aggregates();
                match response_codec::encode_serde(&infos) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }

            MetaOp::CreateTenantSnapshot { tenant_id } => {
                self.execute_create_tenant_snapshot(task, *tenant_id)
            }

            MetaOp::RestoreTenantSnapshot {
                tenant_id,
                snapshot,
                replace_mode,
                clear_vshards,
                collections_to_clear,
            } => self.execute_restore_tenant_snapshot(
                task,
                *tenant_id,
                snapshot,
                *replace_mode,
                clear_vshards,
                collections_to_clear,
            ),

            MetaOp::ConvertCollection {
                collection,
                target_type,
                schema_json,
                source_storage_mode,
            } => self.execute_convert_collection(
                task,
                tid,
                collection.as_str(),
                target_type,
                schema_json,
                source_storage_mode,
            ),

            MetaOp::PurgeTenant { tenant_id } => self.execute_purge_tenant(task, *tenant_id),

            MetaOp::UnregisterCollection {
                tenant_id,
                name,
                purge_lsn,
                reclaim_l1_files,
            } => self.execute_unregister_collection(
                task,
                *tenant_id,
                name,
                *purge_lsn,
                *reclaim_l1_files,
            ),

            MetaOp::UnregisterMaterializedView { tenant_id, name } => {
                self.execute_unregister_materialized_view(task, *tenant_id, name)
            }

            MetaOp::QueryCollectionSize { tenant_id, name } => {
                self.execute_query_collection_size(task, *tenant_id, name)
            }

            // Retention / purge / continuous-agg / last-value bodies live in
            // `dispatch/meta_retention/`; the arms below are one-line delegations
            // so the Meta match stays exhaustive.
            MetaOp::EnforceTimeseriesRetention {
                collection,
                max_age_ms,
            } => self.meta_enforce_timeseries_retention(task, collection.as_str(), *max_age_ms),
            MetaOp::ApplyContinuousAggRetention => self.meta_apply_continuous_agg_retention(task),
            MetaOp::QueryAggregateWatermark { aggregate_name } => {
                self.meta_query_aggregate_watermark(task, aggregate_name)
            }
            MetaOp::QueryLastValues { collection } => {
                self.meta_query_last_values(task, collection.as_str())
            }
            MetaOp::QueryLastValue {
                collection,
                series_id,
            } => self.meta_query_last_value(task, collection.as_str(), *series_id),

            MetaOp::AlterArray {
                audit_retain_ms, ..
            } => {
                // All catalog + registry mutations are performed on the Control
                // Plane before this op is dispatched. The Data Plane simply echoes
                // an 8-byte LE u64 acknowledgement (the new audit_retain_ms, or 0
                // when set to NULL).
                let ack: u64 = (*audit_retain_ms)
                    .and_then(|inner| inner)
                    .map(|ms| ms as u64)
                    .unwrap_or(0);
                self.response_with_payload(task, ack.to_le_bytes().to_vec())
            }

            op @ (MetaOp::TemporalPurgeEdgeStore { .. }
            | MetaOp::TemporalPurgeDocumentStrict { .. }
            | MetaOp::TemporalPurgeColumnar { .. }
            | MetaOp::TemporalPurgeCrdt { .. }
            | MetaOp::TemporalPurgeArray { .. }) => self.dispatch_temporal_purge(task, op),

            MetaOp::CalvinExecuteStatic {
                epoch,
                position,
                tenant_id,
                plans,
                epoch_system_ms,
                is_group_leader,
                versioned_reads,
            } => self.execute_calvin_execute_static(
                task,
                CalvinExecCtx {
                    epoch: *epoch,
                    position: *position,
                    epoch_system_ms: *epoch_system_ms,
                    is_group_leader: *is_group_leader,
                },
                tenant_id,
                plans,
                versioned_reads,
            ),

            MetaOp::CalvinExecutePassive {
                epoch,
                position,
                tenant_id,
                keys_to_read,
            } => self.execute_calvin_execute_passive(
                task,
                *epoch,
                *position,
                tenant_id,
                keys_to_read,
            ),

            MetaOp::CalvinExecuteActive {
                epoch,
                position,
                tenant_id,
                plans,
                injected_reads,
                epoch_system_ms,
                is_group_leader,
            } => self.execute_calvin_execute_active(
                task,
                CalvinExecCtx {
                    epoch: *epoch,
                    position: *position,
                    epoch_system_ms: *epoch_system_ms,
                    is_group_leader: *is_group_leader,
                },
                tenant_id,
                plans,
                injected_reads,
            ),

            MetaOp::RebuildIndex {
                collection,
                index_name,
                concurrent,
            } => self.execute_rebuild_index(
                task,
                tid,
                collection.as_str(),
                index_name.as_deref(),
                *concurrent,
            ),

            MetaOp::PutSynonymGroup {
                tenant_id,
                record_json,
            } => self.execute_put_synonym_group(task, *tenant_id, record_json),

            MetaOp::DeleteSynonymGroup { tenant_id, name } => {
                self.execute_delete_synonym_group(task, *tenant_id, name)
            }

            MetaOp::RenameCollection {
                tenant_id,
                old_database_id,
                new_database_id,
                old_collection,
                new_collection,
            } => self.execute_rename_collection(
                task,
                crate::data::executor::handlers::control::move_tenant::RenameCollectionParams {
                    tenant_id: *tenant_id,
                    old_database_id: *old_database_id,
                    new_database_id: *new_database_id,
                    old_collection: old_collection.as_str(),
                    new_collection: new_collection.as_str(),
                },
            ),

            MetaOp::RecordCalvinWriteVersions {
                tenant_id,
                plans,
                epoch,
                position,
            } => {
                // The Calvin apply already committed; this records the write
                // version of every key it wrote at the CalvinApplied WAL LSN the
                // scheduler threaded onto the request envelope, reusing the same
                // recorder the single-shard fast-path commit funnels through. A
                // no-op when the envelope carries no LSN.
                self.record_batch_write_versions(task, tenant_id.as_u64(), plans);
                // Drain the per-index value tuples the distributed flush staged
                // for this batch and record them at the same applied LSN.
                if let Some(lsn) = task.wal_lsn() {
                    self.record_staged_calvin_index_values(
                        task.request.database_id,
                        *tenant_id,
                        *epoch,
                        *position,
                        task.request.vshard_id.as_u32(),
                        lsn,
                    );
                }
                self.response_ok(task)
            }

            MetaOp::CalvinFlush { epoch, position } => {
                self.execute_calvin_flush(task, *epoch, *position)
            }

            MetaOp::CalvinDrop { epoch, position } => {
                self.execute_calvin_drop(task, *epoch, *position)
            }

            // Resolve a committing transaction's staged post-images into one
            // `RedoRecord` and return its bytes. Reads the overlay by `&`; never
            // mutates base (the redo record is installed separately).
            MetaOp::ResolveTxn { txn_id, plans } => {
                self.execute_resolve_txn(task, tid, *txn_id, plans)
            }

            // Same shape as `ResolveTxn` above, but sourced from Calvin's own
            // staging state (`commit_pending` + the synthetic-`TxnId` overlay)
            // instead of a session transaction's.
            MetaOp::CalvinResolve { epoch, position } => {
                self.execute_calvin_resolve(task, *epoch, *position)
            }

            MetaOp::StageWrite { plan } => self.execute_stage_write(task, tid, plan),

            // Release the staging overlay once a transaction resolves (commit
            // or rollback). `HashMap::remove` on an absent key is a no-op, so
            // this is safe even when no overlay was ever populated. The GRAPH
            // overlay is a parallel, independent structure (see
            // `GraphTxnOverlay`) and is dropped in lockstep.
            //
            // Columnar engines this transaction auto-created during staging
            // (`stage_columnar_insert` -> `ensure_columnar_engine_schema`) are
            // dropped here too, but ONLY if still empty. On ROLLBACK the
            // staged rows never left the overlay, so the engine's memtable is
            // still empty and gets dropped -- no phantom empty engine survives
            // the rollback. On COMMIT, `TransactionBatch` has already replayed
            // the insert through `execute_columnar_insert` (populating the
            // memtable) before this dispatches, so the empty-check fails and
            // the engine correctly stays registered with its committed rows.
            MetaOp::DropTxnOverlay { txn_id } => {
                // Behaviour-preserving delegation to the shared teardown, which
                // the lease reaper also calls (see `CoreLoop::drop_overlay_entry`).
                self.drop_overlay_entry(*txn_id);
                self.response_ok(task)
            }

            // Return a composite savepoint marker spanning BOTH overlays: the
            // value/TTL overlay's undo-journal length followed by the parallel
            // GRAPH overlay's, each an 8-byte LE u64 (16 bytes total). An
            // absent overlay (no staged write of that kind yet) reports 0.
            MetaOp::MarkSavepoint { txn_id } => {
                // A savepoint marks an active transaction — refresh its lease.
                self.touch_overlay(*txn_id);
                let value_marker = self
                    .txn_overlays
                    .get(txn_id)
                    .map(|overlay| overlay.journal_len())
                    .unwrap_or(0) as u64;
                let graph_marker = self
                    .graph_txn_overlays
                    .get(txn_id)
                    .map(|overlay| overlay.journal_len())
                    .unwrap_or(0) as u64;
                let mut payload = Vec::with_capacity(16);
                payload.extend_from_slice(&value_marker.to_le_bytes());
                payload.extend_from_slice(&graph_marker.to_le_bytes());
                self.response_with_payload(task, payload)
            }

            // Rewind BOTH the value/TTL overlay and the GRAPH overlay to their
            // marked journal lengths. An absent overlay is a no-op (nothing of
            // that kind was staged).
            MetaOp::RollbackToSavepoint {
                txn_id,
                value_marker,
                graph_marker,
            } => {
                // Rewinding a savepoint is transaction activity — refresh lease.
                self.touch_overlay(*txn_id);
                if let Some(overlay) = self.txn_overlays.get_mut(txn_id) {
                    overlay.rollback_to(*value_marker as usize);
                }
                if let Some(overlay) = self.graph_txn_overlays.get_mut(txn_id) {
                    overlay.rollback_to(*graph_marker as usize);
                }
                self.response_ok(task)
            }
        }
    }
}

#[cfg(test)]
mod txn_created_columnar_engine_tests {
    //! `MetaOp::DropTxnOverlay` must reap a columnar engine that a transaction
    //! auto-created purely via statement-time staging (its rows never left the
    //! per-txn overlay for the engine's memtable) — but ONLY while that engine
    //! is still empty. On ROLLBACK the memtable is empty, so the phantom engine
    //! is dropped; on COMMIT the memtable has already been populated by the
    //! `TransactionBatch` replay, so the engine (and its rows) survive.
    //!
    //! Observed directly on `CoreLoop::columnar_engines` membership — the field
    //! the fix mutates — because a leaked empty engine is invisible to ordinary
    //! SELECTs (`execute_columnar_scan` returns an empty result identically for
    //! an absent key and a present-but-empty engine).

    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::MetaOp;
    use nodedb_types::Surrogate;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    use nodedb_types::value::Value;

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::handlers::transaction::stage_write::StageColumnarInsertParams;
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{
        DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, TxnId, VShardId,
    };

    const TID: u64 = 1;

    fn make_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open");
        (core, dir)
    }

    fn make_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Compact),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    /// Deterministic 2-column schema (`id` Int64 PK, `v` Float64) so the
    /// engine's column order is fixed regardless of row `HashMap` iteration.
    fn schema_bytes() -> Vec<u8> {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("v", ColumnType::Float64),
        ])
        .expect("valid schema");
        zerompk::to_msgpack_vec(&schema).expect("encode schema")
    }

    /// One-row payload `[{"id": id, "v": val}]` in the staging wire shape
    /// `stage_columnar_insert` decodes.
    fn payload(id: i64, val: f64) -> Vec<u8> {
        let mut obj = HashMap::new();
        obj.insert("id".to_string(), Value::Integer(id));
        obj.insert("v".to_string(), Value::Float(val));
        nodedb_types::value_to_msgpack(&Value::Array(vec![Value::Object(obj)])).expect("encode row")
    }

    /// Drive the real staging path: stage one columnar INSERT into a NEW
    /// collection under `txn_id`. Returns the engine key it auto-registers.
    fn stage_new_collection(
        core: &mut CoreLoop,
        task: &ExecutionTask,
        txn_id: TxnId,
        collection: &str,
    ) -> (DatabaseId, TenantId, String) {
        let sb = schema_bytes();
        let pl = payload(1, 1.0);
        let surrogates = [Surrogate::new(1)];
        let no_policy = nodedb_types::RlsWriteCheck::NoPolicyApplies;
        let resp = core.stage_columnar_insert(StageColumnarInsertParams {
            task,
            tid: TID,
            txn_id,
            collection,
            payload: &pl,
            surrogates: &surrogates,
            schema_bytes: &sb,
            on_conflict_updates: &[],
            rls_write_check: &no_policy,
        });
        assert_eq!(
            resp.status,
            Status::Ok,
            "staged columnar insert into a new collection must succeed: {:?}",
            resp.error_code
        );
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
        )
    }

    #[test]
    fn rollback_drops_the_empty_txn_created_columnar_engine() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn_id = TxnId::new(42);

        let key = stage_new_collection(&mut core, &task, txn_id, "rolled_back");

        // Staging auto-created the engine and recorded it as txn-created; the
        // staged row lives only in the overlay, so the memtable is empty.
        assert!(
            core.columnar_engines.contains_key(&key),
            "staging must auto-register the columnar engine"
        );
        assert!(
            core.txn_created_columnar_engines
                .get(&txn_id)
                .is_some_and(|s| s.contains(&key)),
            "the newly-created engine must be tracked for this txn"
        );
        assert!(
            core.columnar_engines[&key].memtable().is_empty(),
            "staged rows go to the overlay, not the memtable — memtable stays empty"
        );

        // ROLLBACK path: DropTxnOverlay must reap the still-empty phantom engine.
        let resp = core.dispatch_meta(&task, TID, &MetaOp::DropTxnOverlay { txn_id });
        assert_eq!(resp.status, Status::Ok);

        // The core assertion. Pre-fix, `columnar_engines` still contains `key`
        // here (DropTxnOverlay only cleared the overlays) — so this FAILS on the
        // pre-fix tree and passes only once the empty engine is dropped.
        assert!(
            !core.columnar_engines.contains_key(&key),
            "a rolled-back txn must NOT leave a phantom empty columnar engine registered"
        );
        assert!(
            !core.txn_created_columnar_engines.contains_key(&txn_id),
            "per-txn created-engine tracking must be cleared on resolution"
        );
    }

    #[test]
    fn commit_keeps_the_populated_txn_created_columnar_engine() {
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn_id = TxnId::new(7);

        let key = stage_new_collection(&mut core, &task, txn_id, "committed");
        assert!(core.columnar_engines.contains_key(&key));

        // Mimic COMMIT: the `TransactionBatch` replay applies the buffered
        // insert to the engine's memtable BEFORE DropTxnOverlay dispatches.
        core.columnar_engines
            .get_mut(&key)
            .expect("engine present")
            .insert(&[Value::Integer(1), Value::Float(1.0)])
            .expect("apply committed row to memtable");
        assert!(
            !core.columnar_engines[&key].memtable().is_empty(),
            "commit replay must populate the memtable"
        );

        let resp = core.dispatch_meta(&task, TID, &MetaOp::DropTxnOverlay { txn_id });
        assert_eq!(resp.status, Status::Ok);

        // Guards against an over-eager fix that drops every txn-created engine
        // unconditionally: a populated engine must survive COMMIT.
        assert!(
            core.columnar_engines.contains_key(&key),
            "a committed txn's populated columnar engine must stay registered"
        );
        assert!(
            !core.txn_created_columnar_engines.contains_key(&txn_id),
            "per-txn created-engine tracking must be cleared on commit too"
        );
    }

    #[test]
    fn drop_does_not_touch_a_preexisting_engine_staged_into() {
        // An engine that already existed before the txn must never be tracked
        // or dropped, even if a staged insert routes through it.
        let (mut core, _dir) = make_core();
        let task = make_task();
        let txn_id = TxnId::new(9);
        let collection = "preexisting";

        // First txn creates + commits (memtable populated) — the engine is now
        // pre-existing committed state.
        let key = stage_new_collection(&mut core, &task, txn_id, collection);
        core.columnar_engines
            .get_mut(&key)
            .expect("engine present")
            .insert(&[Value::Integer(1), Value::Float(1.0)])
            .expect("apply committed row");
        let resp = core.dispatch_meta(&task, TID, &MetaOp::DropTxnOverlay { txn_id });
        assert_eq!(resp.status, Status::Ok);
        assert!(core.columnar_engines.contains_key(&key));

        // Second txn stages into the SAME (now pre-existing) collection.
        let txn2 = TxnId::new(10);
        let sb = schema_bytes();
        let pl = payload(2, 2.0);
        let surrogates = [Surrogate::new(2)];
        let no_policy = nodedb_types::RlsWriteCheck::NoPolicyApplies;
        let resp = core.stage_columnar_insert(StageColumnarInsertParams {
            task: &task,
            tid: TID,
            txn_id: txn2,
            collection,
            payload: &pl,
            surrogates: &surrogates,
            schema_bytes: &sb,
            on_conflict_updates: &[],
            rls_write_check: &no_policy,
        });
        assert_eq!(resp.status, Status::Ok);
        assert!(
            !core
                .txn_created_columnar_engines
                .get(&txn2)
                .is_some_and(|s| s.contains(&key)),
            "a pre-existing engine must NOT be tracked as txn-created"
        );

        // Rolling back the second txn must leave the pre-existing engine alone.
        let resp = core.dispatch_meta(&task, TID, &MetaOp::DropTxnOverlay { txn_id: txn2 });
        assert_eq!(resp.status, Status::Ok);
        assert!(
            core.columnar_engines.contains_key(&key),
            "rolling back a staged insert into a pre-existing engine must not drop it"
        );
    }

    #[test]
    fn active_txn_overlays_gauge_tracks_overlay_lifecycle() {
        let (mut core, _dir) = make_core();
        let metrics = std::sync::Arc::new(crate::control::metrics::SystemMetrics::new());
        core.metrics = Some(metrics.clone());
        let txn_id = TxnId::new(99);

        let gauge = || {
            metrics
                .active_txn_overlays
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        // Idle: nothing staged, gauge sits at zero.
        assert_eq!(gauge(), 0, "gauge must start at zero");

        // First materialization of the value/TTL overlay bumps the gauge to 1.
        let _ = core.txn_overlay_mut(txn_id);
        assert_eq!(gauge(), 1, "first overlay creation must bump the gauge");

        // A second access to the SAME transaction's overlay must NOT double-count.
        let _ = core.txn_overlay_mut(txn_id);
        assert_eq!(gauge(), 1, "re-accessing an existing overlay must not bump");

        // The parallel GRAPH overlay for the same txn is a distinct entry: 2.
        let _ = core.graph_txn_overlay_mut(txn_id);
        assert_eq!(gauge(), 2, "the graph overlay is a distinct tracked entry");

        // DropTxnOverlay removes both entries and decrements by the exact count.
        let task = make_task();
        let resp = core.dispatch_meta(&task, TID, &MetaOp::DropTxnOverlay { txn_id });
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(
            gauge(),
            0,
            "dropping both overlays must return the gauge to zero"
        );
    }

    // ── Overlay lease GC (reap of abandoned per-txn staging overlays) ────
    //
    // Mirrors the reservation lease-GC pattern: a still-active transaction
    // refreshes its stamp on every write AND every read, so only genuinely
    // abandoned overlays (past `OVERLAY_LEASE_NS`) are reclaimed. Fully
    // deterministic via the logical `OrdinalClock` — no sleeps.

    use crate::data::executor::handlers::transaction::overlay_reap::OVERLAY_LEASE_NS;

    #[test]
    fn reap_reclaims_past_lease_overlay_and_spares_active_one() {
        let (mut core, _dir) = make_core();
        let metrics = std::sync::Arc::new(crate::control::metrics::SystemMetrics::new());
        core.metrics = Some(metrics.clone());
        let task = make_task();
        let gauge = || {
            metrics
                .active_txn_overlays
                .load(std::sync::atomic::Ordering::Relaxed)
        };

        let txn_a = TxnId::new(1001);
        let txn_b = TxnId::new(1002);

        // txn_A: value overlay (+ an auto-created, still-empty columnar engine)
        // plus a parallel graph overlay — all three leaking maps populated.
        let key_a = stage_new_collection(&mut core, &task, txn_a, "reap_a");
        core.graph_txn_overlay_mut(txn_a);
        assert!(core.txn_overlays.contains_key(&txn_a));
        assert!(core.graph_txn_overlays.contains_key(&txn_a));
        assert!(core.txn_created_columnar_engines.contains_key(&txn_a));

        // Freeze txn_A's newest stamp; everything staged after is strictly newer.
        let a_stamp = core.hlc.peek();

        // txn_B stages later, so its stamp is strictly greater than a_stamp.
        let _key_b = stage_new_collection(&mut core, &task, txn_b, "reap_b");
        core.graph_txn_overlay_mut(txn_b);
        assert_eq!(gauge(), 4, "two overlays each for txn_A and txn_B");

        // Advance the clock so the threshold (peek - LEASE) lands strictly above
        // txn_A's stamp but at-or-below txn_B's: a_stamp < threshold <= b_stamp.
        core.hlc.update_from_remote(a_stamp + OVERLAY_LEASE_NS + 1);

        core.reap_expired_overlays();

        // txn_A is past-lease: all three maps cleared, its empty auto-created
        // engine dropped, gauge decremented by its two overlays.
        assert!(
            !core.txn_overlays.contains_key(&txn_a),
            "past-lease txn_A value overlay must be reaped"
        );
        assert!(
            !core.graph_txn_overlays.contains_key(&txn_a),
            "past-lease txn_A graph overlay must be reaped"
        );
        assert!(
            !core.txn_created_columnar_engines.contains_key(&txn_a),
            "past-lease txn_A columnar tracking must be reaped"
        );
        assert!(
            !core.columnar_engines.contains_key(&key_a),
            "txn_A's still-empty auto-created engine must be dropped on reap"
        );
        assert_eq!(gauge(), 2, "gauge decremented by txn_A's two overlays");

        // txn_B is still active (stamp above threshold): spared entirely — the
        // active-txn-not-reaped safety half.
        assert!(
            core.txn_overlays.contains_key(&txn_b),
            "active txn_B value overlay must survive"
        );
        assert!(
            core.graph_txn_overlays.contains_key(&txn_b),
            "active txn_B graph overlay must survive"
        );
    }

    #[test]
    fn read_your_own_write_refresh_spares_past_lease_overlay() {
        let (mut core, _dir) = make_core();
        let task = make_task();

        let txn_a = TxnId::new(2001);
        let key_a = stage_new_collection(&mut core, &task, txn_a, "refresh_a");
        let a_stamp = core.hlc.peek();

        // Advance so txn_A is nominally past-lease before the read.
        core.hlc.update_from_remote(a_stamp + OVERLAY_LEASE_NS + 1);

        // A real in-transaction READ (read-your-own-write scan merge) refreshes
        // the lease via the instrumented `touch_overlay` on the read path.
        let coll_key = (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "refresh_a".to_string(),
        );
        let mut rows: Vec<(String, Vec<u8>)> = Vec::new();
        core.merge_overlay_into_scan(txn_a, &coll_key, &mut rows, &|_, _| true);

        core.reap_expired_overlays();

        // The read refreshed txn_A's stamp to the current clock, so it survives
        // — proving refresh-on-access keeps a live read-only txn alive.
        assert!(
            core.txn_overlays.contains_key(&txn_a),
            "a txn refreshed by an in-txn read must NOT be reaped"
        );
        assert!(
            core.txn_created_columnar_engines.contains_key(&txn_a),
            "read-refreshed txn_A columnar tracking must be retained"
        );
        assert!(
            core.columnar_engines.contains_key(&key_a),
            "read-refreshed txn_A engine must be retained"
        );
    }
}
