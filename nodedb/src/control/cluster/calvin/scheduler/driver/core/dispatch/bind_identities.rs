// SPDX-License-Identifier: BUSL-1.1

//! Install the pk → surrogate identities a local Calvin write slice carries.
//!
//! The coordinator assigned each surrogate at plan time in its own catalog.
//! The data-group leader binds them here before it stages the slice, and
//! carries them in the slice's redo entry. Every other replica binds the
//! same identities before the redo installs, so a later point read by
//! primary key resolves the row on every node.

use nodedb_physical::physical_plan::PhysicalPlan;

use super::super::halt::{HaltReason, HaltStep};
use super::super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::surrogate::{CarriedIdentity, bind_plan_identities, collect_plan_identities};
use crate::types::{DatabaseId, TenantId};

impl Scheduler {
    /// Bind every identity in `plans` first-wins, rewrite each surrogate
    /// slot with the authoritative value, the same walk the replicated-write
    /// decoder runs, and return the identities the slice's redo carries.
    ///
    /// A catalog error is local to this node, and its peers apply the slice.
    /// So the scheduler halts: the txn is not yet in `pending`, its locks
    /// stay held under its lock owner, and its position stays unapplied.
    /// Returns `None` after the halt; the caller returns at once.
    pub(super) fn bind_local_identities(
        &mut self,
        plans: &mut [PhysicalPlan],
        database_id: DatabaseId,
        tenant_id: TenantId,
        txn_id: TxnId,
    ) -> Option<Vec<CarriedIdentity>> {
        let assigner = &self.shared.surrogate_assigner;
        let rewritten = plans
            .iter_mut()
            .try_for_each(|plan| bind_plan_identities(assigner, database_id, tenant_id, plan));
        let bound = rewritten
            .and_then(|()| collect_plan_identities(assigner, database_id, tenant_id, plans));
        match bound {
            Ok(identities) => Some(identities),
            Err(e) => {
                self.halt_apply(
                    txn_id,
                    HaltReason::IdentityBindFailed,
                    HaltStep::IdentityBind,
                    format!("surrogate binding failed: {e}"),
                );
                None
            }
        }
    }
}
