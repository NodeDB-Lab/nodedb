// SPDX-License-Identifier: BUSL-1.1

//! COMMIT-time `TRUNCATE ... RESTART IDENTITY`.
//!
//! Inside a transaction the truncate is staged, so its sequences restart only
//! once COMMIT has made the truncate durable. A `ROLLBACK` leaves every
//! sequence where it was. Autocommit restarts them right after dispatch
//! instead (`pgwire::handler::routing::dispatch_loop`).

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Reset the sequences of every collection a buffered
/// `TRUNCATE ... RESTART IDENTITY` names. Called after every abort branch
/// of the commit has returned.
pub(super) fn restart_truncated_identities(
    state: &SharedState,
    tenant_id: TenantId,
    buffered: &[PhysicalTask],
) {
    for task in buffered {
        if let PhysicalPlan::Document(DocumentOp::Truncate {
            collection,
            restart_identity: true,
            ..
        }) = &task.plan
        {
            state.sequence_registry.restart_sequences_for_collection(
                task.database_id.as_u64(),
                tenant_id.as_u64(),
                collection.as_str(),
            );
        }
    }
}
