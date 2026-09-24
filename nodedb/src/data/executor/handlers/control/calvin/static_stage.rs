// SPDX-License-Identifier: BUSL-1.1

//! Static-set multi-shard Calvin transaction staging.

use std::panic::{AssertUnwindSafe, catch_unwind};

use tracing::{debug, info_span};

use nodedb_types::calvin::VersionedReadEntry;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::commit_pending::PendingCommit;
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
    /// the commit-pending buffer keyed by `(epoch, position)`. It performs NO
    /// base mutation and fires NO side effects — nothing is observable until a
    /// subsequent [`CoreLoop::execute_calvin_flush`] replays the staged plans
    /// (or [`CoreLoop::execute_calvin_drop`] discards them). The response
    /// carries the vote on `read_set_valid`; the deterministic time anchor and
    /// leadership scope are captured with the staged plans and restored at
    /// flush time (when the actual apply — and any time-dependent writes — run).
    pub(in crate::data::executor) fn execute_calvin_execute_static(
        &mut self,
        task: &ExecutionTask,
        ctx: CalvinExecCtx,
        tenant_id: &TenantId,
        plans: &[PhysicalPlan],
        versioned_reads: &[VersionedReadEntry],
    ) -> Response {
        let CalvinExecCtx {
            epoch,
            position,
            epoch_system_ms,
            is_group_leader,
        } = ctx;
        let vshard_id = task.request.vshard_id.as_u32();
        debug!(
            core = self.core_id,
            epoch,
            position,
            epoch_system_ms,
            vshard_id,
            is_group_leader,
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
            for plan in plans {
                self.stage_calvin_overlay(task, synthetic_txn_id, *tenant_id, plan)?;
                // Test-only fault boundary after a potentially-mutating stage.
                crate::fail_point!("calvin_static::during_overlay_stage");
            }
            Ok::<(), ErrorCode>(())
        }));
        self.epoch_system_ms = prev_epoch_ms;
        match stage_result {
            Ok(Ok(())) => {}
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
        }

        // Local commit vote: is this participant's slice of the read-set still
        // current against the local write versions? Empty read-set is vacuously
        // current. Read-only — no base mutation here. A stale-read false vote
        // retains the fully staged state until the durable global verdict.
        let vote = self.read_set_still_current(task, tenant_id.as_u64(), versioned_reads);

        // Publish only a fully staged transaction. The verdict-driven flush
        // replays this raw plan buffer; a global abort drops it and its overlay.
        self.commit_pending.insert(
            (epoch, position, vshard_id),
            PendingCommit {
                plans: plans.to_vec(),
                tenant_id: *tenant_id,
                epoch_system_ms,
                is_group_leader,
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
            read_set_valid: Some(vote),
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
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
            is_group_leader: true,
        };

        let resp = core.execute_calvin_execute_static(&task, ctx, &tenant_id, &plans, &[]);
        assert_eq!(resp.status, Status::Ok);

        let vshard_id = task.request.vshard_id.as_u32();

        // `commit_pending` is unchanged -- it still holds the raw plans that
        // drive the base install at flush time.
        assert!(
            core.commit_pending.contains_key(&(1, 0, vshard_id)),
            "commit_pending must still be populated exactly as before this unit"
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
                is_group_leader: true,
            },
            &tenant_id,
            &[point_insert_plan("orders", "o1", 7)],
            &[],
        );

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.read_set_valid, Some(false));
        assert!(core.commit_pending.is_empty());
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
            is_group_leader: true,
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

        let response = core.execute_calvin_execute_static(&task, ctx, &tenant_id, &plans, &[]);

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.read_set_valid, Some(false));
        assert!(!core.commit_pending.contains_key(&(9, 3, vshard)));
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
                is_group_leader: true,
            },
            &tenant_id,
            &[bulk_delete_plan("orders", None)],
            &[],
        );

        assert_eq!(response.read_set_valid, Some(false));
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
            is_group_leader: true,
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
        );

        assert_eq!(response.status, Status::Error);
        assert_eq!(response.read_set_valid, Some(false));
        assert!(!core.commit_pending.contains_key(&(9, 4, vshard)));
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
                is_group_leader: true,
            },
            &tenant,
            &[malformed],
            &[],
        );
        assert_eq!(failed.read_set_valid, Some(false));
        assert!(matches!(
            failed.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ));
        assert!(!core.commit_pending.contains_key(&(21, 1, vshard)));
        assert!(!core.txn_overlays.contains_key(&synthetic));

        let valid = canonical_ilp_plan("cpu", vec!["cpu value=1i", "cpu value=2i"], vec![1, 2]);
        let staged = core.execute_calvin_execute_static(
            &task,
            CalvinExecCtx {
                epoch: 21,
                position: 1,
                epoch_system_ms: 0,
                is_group_leader: true,
            },
            &tenant,
            &[valid],
            &[],
        );
        assert_eq!(staged.read_set_valid, Some(true));
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
                is_group_leader: true,
            },
            &tenant,
            &[mismatch],
            &[],
        );
        let mismatch_id = calvin_synthetic_txn_id(21, 2, vshard).expect("synthetic transaction id");
        assert_eq!(failed.read_set_valid, Some(false));
        assert!(!core.commit_pending.contains_key(&(21, 2, vshard)));
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
                is_group_leader: true,
            },
            &tenant,
            &[overflow],
            &[],
        );
        let overflow_id = calvin_synthetic_txn_id(21, 3, vshard).expect("synthetic transaction id");
        assert_eq!(failed.status, Status::Error);
        assert_eq!(failed.read_set_valid, Some(false));
        assert!(matches!(
            failed.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { .. })
        ));
        assert!(
            !core.commit_pending.contains_key(&(21, 3, vshard)),
            "a rejected stage cannot reach the TransactionRedo-producing flush path"
        );
        assert!(!core.txn_overlays.contains_key(&overflow_id));
    }
}
