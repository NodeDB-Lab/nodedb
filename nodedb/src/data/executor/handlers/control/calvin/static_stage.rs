// SPDX-License-Identifier: BUSL-1.1

//! Static-set multi-shard Calvin transaction staging.

use std::panic::{AssertUnwindSafe, catch_unwind};

use tracing::{debug, info_span};

use nodedb_types::calvin::VersionedReadEntry;

use crate::bridge::envelope::{ErrorCode, Payload, Response, StageVote, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::commit_pending::PendingCommit;
use crate::data::executor::handlers::control::calvin_reply::{CalvinReply, CalvinStaging};
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;
use nodedb_physical::physical_plan::PhysicalPlan;

use crate::data::executor::handlers::control::calvin_txn_id::calvin_synthetic_txn_id;
use crate::data::panic_payload::panic_payload_to_string;

use super::shared::CalvinExecCtx;

impl CoreLoop {
    /// Validate a static-set Calvin transaction and stage it for commit.
    ///
    /// Computes the local commit vote by checking whether this participant's
    /// slice of the transaction's LSN-versioned read-set is still current
    /// against the per-core write versions, then STAGES the write plans into
    /// the synthetic overlay and the commit-pending buffer keyed by
    /// `(epoch, position)`. It performs NO base mutation and fires NO side
    /// effects. Nothing is observable until the apply of the slice's stamped
    /// redo entry installs it, or [`CoreLoop::execute_calvin_drop`] discards
    /// the staged state. The response carries the vote on `stage_vote`. Staging runs
    /// under the epoch's deterministic time anchor, which the pending entry
    /// keeps for resolve. `body_plans` indexes the plans a trigger body
    /// buffered: they stage under `Trigger`, so their rows fire no trigger.
    pub(in crate::data::executor) fn execute_calvin_execute_static(
        &mut self,
        task: &ExecutionTask,
        ctx: CalvinExecCtx,
        tenant_id: &TenantId,
        plans: &[PhysicalPlan],
        versioned_reads: &[VersionedReadEntry],
        body_plans: &[u32],
    ) -> Response {
        let CalvinExecCtx {
            epoch,
            position,
            epoch_system_ms,
        } = ctx;
        let vshard_id = task.request.vshard_id.as_u32();
        debug!(
            core = self.core_id,
            epoch,
            position,
            epoch_system_ms,
            vshard_id,
            plan_count = plans.len(),
            read_count = versioned_reads.len(),
            "calvin stage for commit"
        );
        let _stage_span = info_span!(
            "executor_stage",
            epoch,
            position,
            vshard = vshard_id,
            tenant_id = tenant_id.as_u64(),
            trace_id = ?task.request.trace_id,
        )
        .entered();

        // Derive the synthetic transaction identity before ANY mutation. A
        // representational failure must not leave either a pending buffer or an
        // overlay behind for a transaction that cannot later be resolved.
        let synthetic_txn_id = match calvin_synthetic_txn_id(epoch, position, vshard_id) {
            Ok(id) => id,
            Err(error) => {
                return self.calvin_stage_failure(task, epoch, position, vshard_id, error);
            }
        };

        // Stage every plan before publishing `PendingCommit`. Panic isolation
        // cleans both staging representations after any failure. Staging reads
        // the epoch's time anchor, so every replica stages the same images.
        let prev_epoch_ms = self.epoch_system_ms;
        self.epoch_system_ms = Some(epoch_system_ms);
        let stage_result = catch_unwind(AssertUnwindSafe(|| {
            let mut staging = CalvinStaging::default();
            for (index, plan) in plans.iter().enumerate() {
                let body = u32::try_from(index).is_ok_and(|index| body_plans.contains(&index));
                self.begin_calvin_plan_staging(task, synthetic_txn_id, *tenant_id, plan, body);
                self.stage_calvin_plan(task, synthetic_txn_id, *tenant_id, plan, &mut staging)?;
                // Test-only fault boundary after a potentially-mutating stage.
                crate::fail_point!("calvin_static::during_overlay_stage");
            }
            Ok::<CalvinReply, ErrorCode>(staging.reply)
        }));
        self.epoch_system_ms = prev_epoch_ms;
        let reply = match stage_result {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) => {
                return self.calvin_stage_failure(task, epoch, position, vshard_id, error);
            }
            Err(payload) => {
                return self.calvin_stage_failure(
                    task,
                    epoch,
                    position,
                    vshard_id,
                    ErrorCode::Internal {
                        detail: format!(
                            "panic while staging static Calvin transaction: {}",
                            panic_payload_to_string(payload.as_ref())
                        ),
                    },
                );
            }
        };

        // Local commit vote: is this participant's slice of the read-set still
        // current against the local write versions? Empty read-set is vacuously
        // current. Read-only — no base mutation here. A stale-read false vote
        // retains the fully staged state until the durable global verdict.
        let vote = StageVote::of_read_set(self.read_set_still_current(
            task,
            tenant_id.as_u64(),
            versioned_reads,
        ));

        // Publish only a fully staged transaction. Resolve reads these plans
        // against the overlay; a global abort drops both.
        self.calvin.commit_pending.insert(
            (epoch, position, vshard_id),
            PendingCommit {
                plans: plans.to_vec(),
                tenant_id: *tenant_id,
                epoch_system_ms,
                reply,
            },
        );

        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            error_code: None,
            stage_vote: Some(vote),
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }
}

impl CoreLoop {
    /// Tell the transaction overlay the source `plan` stages under: `Trigger`
    /// for a body's plan, else the transaction's own.
    fn begin_calvin_plan_staging(
        &mut self,
        task: &ExecutionTask,
        txn_id: crate::types::TxnId,
        tenant_id: TenantId,
        plan: &PhysicalPlan,
        body: bool,
    ) {
        let source = if body {
            crate::event::EventSource::Trigger
        } else {
            task.request.event_source
        };
        let database_id = task.request.database_id;
        let collections =
            crate::control::wal_replication::transaction_redo::collections::written_collections(
                std::slice::from_ref(plan),
            );
        self.txn_overlay_mut(txn_id).begin_staging(
            source,
            collections
                .into_iter()
                .map(|collection| (database_id, tenant_id, collection)),
        );
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::TimeseriesOp;
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::doc_format;
    use crate::data::executor::handlers::transaction::overlay::Staged;
    use crate::types::DatabaseId;

    use super::super::shared::test_support::{
        bulk_delete_plan, canonical_ilp_plan, doc_value, make_task, point_insert_plan,
    };

    #[test]
    fn calvin_execute_static_stages_point_insert_into_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let task = make_task();
        let tenant_id = TenantId::new(1);
        let plans = vec![point_insert_plan("orders", "o1", 7)];
        let ctx = CalvinExecCtx {
            epoch: 1,
            position: 0,
            epoch_system_ms: 0,
        };

        let resp = core.execute_calvin_execute_static(&task, ctx, &tenant_id, &plans, &[], &[]);
        assert_eq!(resp.status, Status::Ok);
        // An empty read-set is current.
        assert_eq!(resp.stage_vote, Some(StageVote::Commit));

        let vshard_id = task.request.vshard_id.as_u32();

        // `commit_pending` holds the staged plans resolve reads.
        assert!(
            core.calvin.commit_pending.contains_key(&(1, 0, vshard_id)),
            "commit_pending must hold the staged transaction"
        );

        // The synthetic overlay entry additionally holds the resolved
        // post-image for the concrete point-write plan.
        let synthetic = calvin_synthetic_txn_id(1, 0, vshard_id).unwrap();
        let coll_key = (DatabaseId::DEFAULT, tenant_id, "orders".to_string());
        let expected_body = doc_format::canonicalize_document_for_storage(&doc_value("a", "1"));
        assert_eq!(
            core.txn_overlays
                .get(&synthetic)
                .and_then(|o| o.get(&coll_key, 7)),
            Some(&Staged::Put(expected_body)),
            "the Calvin write plan must be staged into the synthetic-TxnId overlay"
        );
    }

    /// A predicate read of `collection` on vShard 0, taken at `read_lsn`.
    fn homed_read(collection: &str, read_lsn: u64) -> VersionedReadEntry {
        VersionedReadEntry {
            engine: nodedb_types::calvin::EngineTag::Document,
            collection: collection.to_string(),
            key: nodedb_types::calvin::ReadKeyIdent::Predicate,
            read_lsn: crate::types::Lsn::new(read_lsn),
            home_vshard: Some(0),
            served_by: 0,
        }
    }

    #[test]
    fn static_stage_with_a_superseded_read_votes_serialization_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.note_write_lsn(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "orders",
            None,
            crate::types::Lsn::new(20),
        );
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let ctx = CalvinExecCtx {
            epoch: 3,
            position: 0,
            epoch_system_ms: 0,
        };

        let resp = core.execute_calvin_execute_static(
            &task,
            ctx,
            &tenant_id,
            &[point_insert_plan("orders", "o1", 7)],
            &[homed_read("orders", 10)],
            &[],
        );

        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.stage_vote, Some(StageVote::SerializationConflict));
        // A stale slice stays staged until the global verdict drops it.
        let vshard = task.request.vshard_id.as_u32();
        assert!(core.calvin.commit_pending.contains_key(&(3, 0, vshard)));
    }

    #[test]
    fn static_stage_with_a_current_read_votes_commit() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.note_write_lsn(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            "orders",
            None,
            crate::types::Lsn::new(20),
        );
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let ctx = CalvinExecCtx {
            epoch: 3,
            position: 1,
            epoch_system_ms: 0,
        };

        let resp = core.execute_calvin_execute_static(
            &task,
            ctx,
            &tenant_id,
            &[point_insert_plan("orders", "o1", 7)],
            &[homed_read("orders", 20)],
            &[],
        );

        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.stage_vote, Some(StageVote::Commit));
    }

    #[test]
    fn synthetic_id_failure_leaves_no_pending_or_overlay_state() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let epoch_outside_synthetic_range = 1_u64 << 33;

        let response = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: epoch_outside_synthetic_range,
                position: 0,
                epoch_system_ms: 0,
            },
            &tenant_id,
            &[point_insert_plan("orders", "o1", 7)],
            &[],
            &[],
        );

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.stage_vote, Some(StageVote::ParticipantError));
        assert!(core.calvin.commit_pending.is_empty());
        assert!(core.txn_overlays.is_empty());
        assert!(core.graph_txn_overlays.is_empty());
    }

    #[test]
    fn static_stage_error_cleans_all_prior_overlay_state_before_voting_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let metrics = std::sync::Arc::new(crate::control::metrics::SystemMetrics::new());
        core.metrics = Some(metrics.clone());
        let gauge = || {
            metrics
                .active_txn_overlays
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        let baseline = gauge();
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let ctx = CalvinExecCtx {
            epoch: 9,
            position: 3,
            epoch_system_ms: 0,
        };
        let vshard = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(9, 3, vshard).unwrap();
        core.graph_txn_overlay_mut(synthetic);
        assert_eq!(
            gauge(),
            baseline + 1,
            "graph staging must increment the gauge"
        );
        // The first plan adds a document overlay; the second is invalid without
        // an OLLP prediction and must clean both overlays atomically.
        let plans = vec![
            point_insert_plan("orders", "o1", 7),
            bulk_delete_plan("orders", None),
        ];

        let response = core.execute_calvin_execute_static(&task, ctx, &tenant_id, &plans, &[], &[]);

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.stage_vote, Some(StageVote::ParticipantError));
        assert!(!core.calvin.commit_pending.contains_key(&(9, 3, vshard)));
        assert!(!core.txn_overlays.contains_key(&synthetic));
        assert!(!core.graph_txn_overlays.contains_key(&synthetic));
        assert_eq!(
            gauge(),
            baseline,
            "failed staging must restore the overlay gauge"
        );
    }

    #[test]
    fn static_stage_error_also_cleans_existing_graph_overlay_for_synthetic_id() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let vshard = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(9, 5, vshard).unwrap();
        // Use the graph-overlay choke point so this regression also covers its
        // gauge accounting without requiring a graph catalog fixture.
        core.graph_txn_overlay_mut(synthetic);

        let response = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 9,
                position: 5,
                epoch_system_ms: 0,
            },
            &tenant_id,
            &[bulk_delete_plan("orders", None)],
            &[],
            &[],
        );

        assert_eq!(response.stage_vote, Some(StageVote::ParticipantError));
        assert!(!core.graph_txn_overlays.contains_key(&synthetic));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn static_stage_panic_cleans_prior_overlay_state_before_voting_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let metrics = std::sync::Arc::new(crate::control::metrics::SystemMetrics::new());
        core.metrics = Some(metrics.clone());
        let gauge = || {
            metrics
                .active_txn_overlays
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        let baseline = gauge();
        let task = make_task();
        let tenant_id = TenantId::new(1);
        let ctx = CalvinExecCtx {
            epoch: 9,
            position: 4,
            epoch_system_ms: 0,
        };
        let vshard = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(9, 4, vshard).unwrap();
        core.graph_txn_overlay_mut(synthetic);
        assert_eq!(
            gauge(),
            baseline + 1,
            "graph staging must increment the gauge"
        );
        let _fail = crate::fail_point::FailGuard::install(
            "calvin_static::during_overlay_stage",
            crate::fail_point::FailAction::Panic,
        );

        let response = core.execute_calvin_execute_static(
            &task,
            ctx,
            &tenant_id,
            &[point_insert_plan("orders", "o1", 7)],
            &[],
            &[],
        );

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.stage_vote, Some(StageVote::ParticipantError));
        assert!(!core.calvin.commit_pending.contains_key(&(9, 4, vshard)));
        assert!(!core.txn_overlays.contains_key(&synthetic));
        assert!(!core.graph_txn_overlays.contains_key(&synthetic));
        assert_eq!(
            gauge(),
            baseline,
            "panic cleanup must restore the overlay gauge"
        );
    }

    #[test]
    fn static_calvin_ilp_staging_is_all_or_abort() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();
        let tenant = TenantId::new(1);
        let vshard = task.request.vshard_id.as_u32();
        let synthetic = calvin_synthetic_txn_id(21, 1, vshard).expect("synthetic transaction id");

        let malformed = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cpu"),
            payload: vec![0xc1],
            format: "ilp-msgpack".to_owned(),
            wal_lsn: None,
            surrogates: vec![Surrogate::new(1)],
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let failed = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 21,
                position: 1,
                epoch_system_ms: 0,
            },
            &tenant,
            &[malformed],
            &[],
            &[],
        );
        assert_eq!(failed.stage_vote, Some(StageVote::ParticipantError));
        assert!(matches!(
            failed.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ));
        assert!(!core.calvin.commit_pending.contains_key(&(21, 1, vshard)));
        assert!(!core.txn_overlays.contains_key(&synthetic));

        let valid = canonical_ilp_plan("cpu", vec!["cpu value=1i", "cpu value=2i"], vec![1, 2]);
        let staged = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 21,
                position: 1,
                epoch_system_ms: 0,
            },
            &tenant,
            &[valid],
            &[],
            &[],
        );
        assert_eq!(staged.stage_vote, Some(StageVote::Commit));
        assert_eq!(
            core.txn_overlays
                .get(&synthetic)
                .map(|overlay| overlay.len()),
            Some(2)
        );

        let mismatch = canonical_ilp_plan("cpu", vec!["memory value=1i"], vec![3]);
        let failed = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 21,
                position: 2,
                epoch_system_ms: 0,
            },
            &tenant,
            &[mismatch],
            &[],
            &[],
        );
        let mismatch_id = calvin_synthetic_txn_id(21, 2, vshard).expect("synthetic transaction id");
        assert_eq!(failed.stage_vote, Some(StageVote::ParticipantError));
        assert!(!core.calvin.commit_pending.contains_key(&(21, 2, vshard)));
        assert!(!core.txn_overlays.contains_key(&mismatch_id));

        core.ts_tuning.max_tag_cardinality = 1;
        let overflow = canonical_ilp_plan(
            "cpu",
            vec!["cpu,host=a value=1i", "cpu,host=b value=2i"],
            vec![4, 5],
        );
        let failed = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 21,
                position: 3,
                epoch_system_ms: 0,
            },
            &tenant,
            &[overflow],
            &[],
            &[],
        );
        let overflow_id = calvin_synthetic_txn_id(21, 3, vshard).expect("synthetic transaction id");
        assert_eq!(failed.status, Status::Error);
        assert_eq!(failed.stage_vote, Some(StageVote::ParticipantError));
        assert!(matches!(
            failed.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ));
        assert!(
            !core.calvin.commit_pending.contains_key(&(21, 3, vshard)),
            "a rejected stage cannot reach the TransactionRedo-producing flush path"
        );
        assert!(!core.txn_overlays.contains_key(&overflow_id));
    }
}
