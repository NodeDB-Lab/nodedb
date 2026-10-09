// SPDX-License-Identifier: BUSL-1.1

//! Post-apply write-version recording for committed Calvin transactions.
//!
//! A Calvin apply's committed WAL LSN is the LSN of its `TransactionRedo`
//! record, or of the `CalvinApplied` marker a transaction that wrote nothing
//! here appends. The install records the collection floors and index-value
//! versions of the record at its LSN. The per-key versions of the local write
//! plans are recorded here: the scheduler dispatches a one-way, record-only op
//! back to the same core, which funnels the plans through the shared
//! write-version recorder at that LSN — the same shard-local WAL-LSN space
//! the single-shard fast path and read watermarks use.

use super::super::types::CommitState;
use super::deferred::{DispatchOutcome, DispatchStep};
use super::halt::{HaltReason, HaltStep};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::types::Lsn;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

/// Result of [`Scheduler::record_calvin_write_versions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) enum VersionRecord {
    /// The dispatcher accepted the record.
    Sent,
    /// The record waits for capacity. The txn stays pending until it is sent.
    Parked,
    /// The record cannot be built or sent. The scheduler halted, and the txn
    /// stays pending and unapplied.
    Halted,
}

impl Scheduler {
    /// Record the per-key write versions of a just-committed Calvin
    /// transaction's locally-applied write plans at its committed WAL
    /// `applied_lsn`.
    ///
    /// Dispatches a record-only [`MetaOp::RecordCalvinWriteVersions`] op back to
    /// this vShard's core with `applied_lsn` stamped on the request envelope's
    /// `wal_lsn`; the core funnels the plans through the shared write-version
    /// recorder at that LSN. The recorded version therefore lands in the same
    /// WAL-LSN space as fast-path writes.
    ///
    /// The record feeds read-set validation. A later txn's stage checks each
    /// read against these versions. A missing version reports a stale read as
    /// valid. So the txn keeps its locks until the record is sent. The core
    /// applies requests in dispatch order, so the record lands before the
    /// stage of any txn that waits on these locks. The response is drained
    /// and discarded.
    ///
    /// A record refused at capacity is parked and re-sent once capacity
    /// frees. A decode or routing error, or a terminal dispatch refusal,
    /// halts the scheduler.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn record_calvin_write_versions(
        &mut self,
        txn_id: TxnId,
        applied_lsn: Lsn,
    ) -> VersionRecord {
        let epoch = txn_id.epoch;
        let position = txn_id.position;

        let Some(pending) = self.pending.get(&txn_id) else {
            return self.halt_version_record(txn_id, "the txn has no pending entry".to_string());
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let plans = match super::super::helpers::decode_plans(&pending.txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => return self.halt_version_record(txn_id, format!("plan decode: {e}")),
        };
        // The locally-applied slice this vShard committed. Empty for a
        // validate-only READ participant (no local writes) — the recorder then
        // has nothing to record, which is correct. The recorder no-ops any plan
        // without a per-key or collection version, so no gate on plan kind is
        // applied here — gating on a narrower write predicate would silently
        // skip recordable writes (e.g. a CRDT apply) and reopen the version gap.
        let local = match self.local_calvin_plans(plans, database_id, epoch, position) {
            Ok(p) => p,
            Err(e) => {
                return self.halt_version_record(txn_id, format!("local plan routing: {e}"));
            }
        };

        let request_id = self.next_request_id();
        let plan = PhysicalPlan::Meta(MetaOp::RecordCalvinWriteVersions {
            tenant_id,
            plans: local,
        });
        // The committed write-LSN for this Calvin apply — recorded against
        // every key the plans wrote, in the same WAL-LSN space as fast-path.
        let request =
            self.build_exempt_request(request_id, tenant_id, database_id, plan, Some(applied_lsn));

        match self.dispatch_sequenced(txn_id, DispatchStep::WriteVersionRecord, request) {
            DispatchOutcome::Sent => VersionRecord::Sent,
            DispatchOutcome::Deferred => VersionRecord::Parked,
            DispatchOutcome::Failed(error) => {
                self.fail_dispatch_step(txn_id, DispatchStep::WriteVersionRecord, error);
                VersionRecord::Halted
            }
        }
    }

    /// Complete `txn_id` once its parked write-version record is sent, when
    /// its commit tail already ran.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn complete_after_version_record(
        &mut self,
        txn_id: TxnId,
    ) {
        let held = self
            .pending
            .get(&txn_id)
            .is_some_and(|pending| pending.commit_state == CommitState::AwaitingVersionRecord);
        if held {
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
        }
    }

    /// Halt on a write-version record that cannot be built.
    fn halt_version_record(&mut self, txn_id: TxnId, error: String) -> VersionRecord {
        self.halt_apply(
            txn_id,
            HaltReason::WriteVersionRecordFailed,
            HaltStep::WriteVersionRecord,
            error,
        );
        VersionRecord::Halted
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::calvin::CalvinCompletionRegistry;
    use nodedb_types::TenantId;

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::intake::IntakeClosure;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        await_data_plane_request, begin_data_plane_drain, build_test_scheduler,
        build_test_scheduler_with_data_side, fill_tenant_inflight, make_validate_only_txn,
        release_filler, spawn_scheduler_loop, staged_pending, test_coll_vshard,
    };

    /// Once a Data Plane response frees tenant capacity, a write-version
    /// record refused at capacity reaches the Data Plane.
    #[tokio::test]
    async fn refused_write_version_record_reaches_data_plane_after_capacity_frees() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let txn_id = TxnId::new(21, 0);
        scheduler.pending.insert(
            txn_id,
            staged_pending(make_validate_only_txn(21, 0), txn_id),
        );
        let shared = Arc::clone(&scheduler.shared);
        let fillers = fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));

        assert_eq!(
            scheduler.record_calvin_write_versions(txn_id, Lsn::new(42)),
            VersionRecord::Parked
        );
        let running = spawn_scheduler_loop(scheduler);
        release_filler(&shared, &mut data_side, fillers[0]);

        let arrived = await_data_plane_request(&mut data_side, |plan| {
            matches!(
                plan,
                PhysicalPlan::Meta(MetaOp::RecordCalvinWriteVersions { .. })
            )
        })
        .await;
        running.stop().await;

        assert!(
            arrived,
            "the refused write-version record must reach the Data Plane once capacity frees"
        );
    }

    /// A write-version record refused terminally halts the scheduler. The
    /// txn stays pending and unapplied, and intake closes.
    #[tokio::test]
    async fn terminal_write_version_record_refusal_halts_and_holds_the_txn() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, _data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let txn_id = TxnId::new(22, 0);
        scheduler.pending.insert(
            txn_id,
            staged_pending(make_validate_only_txn(22, 0), txn_id),
        );
        begin_data_plane_drain(&scheduler.shared);

        assert_eq!(
            scheduler.record_calvin_write_versions(txn_id, Lsn::new(42)),
            VersionRecord::Halted
        );

        assert!(!scheduler.applied.is_applied(22, 0));
        assert!(scheduler.pending.contains_key(&txn_id));
        assert_eq!(
            scheduler.apply_halt().map(|h| h.report.step),
            Some("write_version_record")
        );
        assert_eq!(scheduler.intake_closure(), Some(IntakeClosure::ApplyHalted));
    }

    /// A parked write-version record refused terminally on re-send halts the
    /// scheduler, and its held txn stays pending and unapplied.
    #[tokio::test]
    async fn parked_write_version_record_refused_on_resend_holds_the_txn() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let txn_id = TxnId::new(23, 0);
        scheduler.pending.insert(
            txn_id,
            staged_pending(make_validate_only_txn(23, 0), txn_id),
        );
        let shared = Arc::clone(&scheduler.shared);
        fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        assert_eq!(
            scheduler.record_calvin_write_versions(txn_id, Lsn::new(42)),
            VersionRecord::Parked
        );
        if let Some(pending) = scheduler.pending.get_mut(&txn_id) {
            pending.commit_state = CommitState::AwaitingVersionRecord;
        }

        begin_data_plane_drain(&shared);
        scheduler.redispatch_deferred();

        assert!(!scheduler.applied.is_applied(23, 0));
        assert!(scheduler.pending.contains_key(&txn_id));
        assert_eq!(
            scheduler.apply_halt().map(|h| h.report.step),
            Some("write_version_record")
        );
        assert!(!scheduler.resends_deferred());
    }

    /// A txn held for its parked write-version record completes once the
    /// record is sent.
    #[tokio::test]
    async fn held_txn_completes_once_its_write_version_record_is_sent() {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let txn_id = TxnId::new(24, 0);
        scheduler.pending.insert(
            txn_id,
            staged_pending(make_validate_only_txn(24, 0), txn_id),
        );
        let shared = Arc::clone(&scheduler.shared);
        let fillers = fill_tenant_inflight(&shared, &mut data_side, TenantId::new(1));
        assert_eq!(
            scheduler.record_calvin_write_versions(txn_id, Lsn::new(42)),
            VersionRecord::Parked
        );
        if let Some(pending) = scheduler.pending.get_mut(&txn_id) {
            pending.commit_state = CommitState::AwaitingVersionRecord;
        }
        assert!(
            !scheduler.applied.is_applied(24, 0),
            "a parked record holds the position unapplied"
        );

        release_filler(&shared, &mut data_side, fillers[0]);
        scheduler.redispatch_deferred();

        assert!(!scheduler.has_deferred_dispatch());
        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(scheduler.applied.is_applied(24, 0));
        assert!(!scheduler.is_apply_halted());
    }

    /// Plans that do not decode leave no record to send. The scheduler
    /// halts, and the txn stays pending and unapplied.
    #[tokio::test]
    async fn undecodable_plans_halt_the_write_version_record() {
        let (mut scheduler, _dir) = build_test_scheduler(test_coll_vshard());
        let txn_id = TxnId::new(25, 0);
        let mut pending = staged_pending(make_validate_only_txn(25, 0), txn_id);
        // 0xc1 is a reserved MessagePack marker, so no plan batch decodes.
        pending.txn.tx_class.plans = vec![0xc1];
        scheduler.pending.insert(txn_id, pending);

        assert_eq!(
            scheduler.record_calvin_write_versions(txn_id, Lsn::new(42)),
            VersionRecord::Halted
        );

        assert!(!scheduler.applied.is_applied(25, 0));
        assert!(scheduler.pending.contains_key(&txn_id));
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::WriteVersionRecordFailed)
        );
    }
}
