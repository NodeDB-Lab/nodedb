// SPDX-License-Identifier: BUSL-1.1

//! Post-apply write-version recording for committed Calvin transactions.
//!
//! A Calvin apply's committed WAL LSN is allocated only AFTER the apply
//! succeeds — the `CalvinApplied` WAL record is appended on the Control Plane
//! once the executor response returns — so the apply itself carries no
//! committed LSN and the per-core write-version index cannot be advanced in
//! place (the dispatch stamps `wal_lsn: None`). Once the scheduler has that
//! LSN it dispatches a one-way, record-only op back to the same core, which
//! funnels the transaction's locally-applied write plans through the shared
//! write-version recorder at that LSN — the same shard-local WAL-LSN space the
//! single-shard fast path and read watermarks use.

use std::sync::atomic::Ordering;

use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::types::Lsn;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

impl Scheduler {
    /// Record the per-key write versions of a just-committed Calvin
    /// transaction's locally-applied write plans at its CalvinApplied WAL
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
    /// validation against these versions yet. A dropped record (decode failure,
    /// no local write plan, or dispatch backpressure) simply leaves the version
    /// unrecorded and never blocks the commit.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn record_calvin_write_versions(
        &self,
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
            epoch,
            position,
        });
        // The committed write-LSN for this Calvin apply — recorded against
        // every key the plans wrote, in the same WAL-LSN space as fast-path.
        let request =
            self.build_exempt_request(request_id, tenant_id, database_id, plan, Some(applied_lsn));

        // Register so the response routes to a real receiver (not the
        // unknown-request warning path), then discard it — the recording is
        // one-way.
        let resp_rx = self.shared.tracker.register(request_id);
        let dispatch_result = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };
        if let Err(e) = dispatch_result {
            self.shared.tracker.cancel(&request_id);
            if self.note_dispatch_busy_shared(&e) {
                // Retryable capacity condition (issue 352): counted + paced.
                tracing::debug!(
                    vshard_id = self.vshard_id,
                    epoch,
                    position,
                    error = %e,
                    "calvin: write-version record dispatch deferred (capacity busy)"
                );
                return;
            }
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch,
                position,
                error = %e,
                "calvin: write-version record dispatch failed"
            );
            return;
        }
        tokio::spawn(async move {
            let mut rx = resp_rx;
            let _ = rx.recv().await;
        });
        self.shared
            .calvin_counters
            .write_versions_recorded
            .fetch_add(1, Ordering::Relaxed);
    }
}
