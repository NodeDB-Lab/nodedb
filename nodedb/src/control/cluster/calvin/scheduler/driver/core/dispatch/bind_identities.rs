// SPDX-License-Identifier: BUSL-1.1

//! Install the pk → surrogate identities a local Calvin write slice carries.
//!
//! The coordinator assigned each surrogate at plan time in its own catalog.
//! Every participant — group leader and follower alike — applies the slice
//! to its Data Plane, so every participant installs the same binding here,
//! or a later point read by primary key resolves nothing on this node.

use nodedb_physical::physical_plan::PhysicalPlan;

use super::super::halt::{HaltReason, HaltStep};
use super::super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::surrogate::bind_plan_identities;
use crate::types::{DatabaseId, TenantId};

impl Scheduler {
    /// Bind every identity in `plans` first-wins and rewrite each surrogate
    /// slot with the authoritative value, the same walk the replicated-write
    /// decoder runs.
    ///
    /// A catalog error is local to this replica, and its peers apply the
    /// slice. So the scheduler halts: the txn is not yet in `pending`, its
    /// locks stay held under its lock owner, and its position stays
    /// unapplied. Returns `false` after the halt; the caller returns at once.
    pub(super) fn bind_local_identities(
        &mut self,
        plans: &mut [PhysicalPlan],
        database_id: DatabaseId,
        tenant_id: TenantId,
        txn_id: TxnId,
    ) -> bool {
        let assigner = &self.shared.surrogate_assigner;
        let bound = plans
            .iter_mut()
            .try_for_each(|plan| bind_plan_identities(assigner, database_id, tenant_id, plan));
        match bound {
            Ok(()) => true,
            Err(e) => {
                self.halt_apply(
                    txn_id,
                    HaltReason::IdentityBindFailed,
                    HaltStep::IdentityBind,
                    format!("surrogate binding failed: {e}"),
                );
                false
            }
        }
    }
}
