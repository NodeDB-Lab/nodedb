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

use super::deferred::{DispatchOutcome, DispatchStep};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::types::Lsn;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

impl Scheduler {
    /// Record the per-key write versions of a just-committed Calvin
    /// transaction's locally-applied write plans at its committed WAL
    /// `applied_lsn`.
    ///
    /// Dispatches a record-only [`MetaOp::RecordCalvinWriteVersions`] op back to
    /// this vShard's core with `applied_lsn` stamped on the request envelope's
    /// `wal_lsn`; the core funnels the plans through the shared write-version
    /// recorder at that LSN. The recorded version therefore lands in the same
    /// WAL-LSN space as fast-path writes, so a later read-set validation against
    /// these keys is not a false-Valid serializability hole.
    ///
    /// Fire-and-forget: the recorded version is not needed to complete the
    /// transaction, so the response is drained and discarded. A brief index-lag
    /// window before the record op lands is harmless — nothing enforces read-set
    /// validation against these versions yet. A record refused at capacity is
    /// parked and re-sent once capacity frees, never dropped. A decode or
    /// routing failure, or a terminal dispatch refusal, leaves the version
    /// unrecorded and never blocks the commit.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn record_calvin_write_versions(
        &mut self,
        txn_id: TxnId,
        applied_lsn: Lsn,
    ) {
        let epoch = txn_id.epoch;
        let position = txn_id.position;

        let Some(pending) = self.pending.get(&txn_id) else {
            return;
        };
        let tenant_id = pending.txn.tx_class.tenant_id;
        let database_id = pending.txn.tx_class.database_id;
        let plans = match super::super::helpers::decode_plans(&pending.txn.tx_class.plans) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin: write-version recording skipped — plan decode failed"
                );
                return;
            }
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
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin: write-version recording skipped — local plan routing failed"
                );
                return;
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

        // The request carries everything a re-send needs, so a parked record
        // outlives the txn's `pending` entry.
        if let DispatchOutcome::Failed(error) =
            self.dispatch_sequenced(txn_id, DispatchStep::WriteVersionRecord, request)
        {
            self.fail_dispatch_step(txn_id, DispatchStep::WriteVersionRecord, error);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::calvin::CalvinCompletionRegistry;
    use nodedb_types::TenantId;

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        await_data_plane_request, build_test_scheduler_with_data_side, fill_tenant_inflight,
        make_validate_only_txn, release_filler, spawn_scheduler_loop, staged_pending,
        test_coll_vshard,
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

        scheduler.record_calvin_write_versions(txn_id, Lsn::new(42));
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
}
