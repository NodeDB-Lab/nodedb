// SPDX-License-Identifier: BUSL-1.1

//! Which task sets take the recon coordinator.

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::PhysicalTask;

use crate::Error;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::is_dependent_predicate;

/// Detect whether `tasks` carry a dependent predicate or a TRUNCATE on an
/// implicit-edge-bearing collection, requiring the edge recon path.
///
/// Returns `Some((collection, database_id))` of the FIRST dependent-predicate
/// task (`BulkUpdate`/`BulkDelete`) or TRUNCATE whose target collection has
/// `has_implicit_edges` set in the catalog, else `None`.
///
/// A genuine catalog READ error propagates as a typed [`crate::Error`]:
/// misrouting a delete on a real I/O fault will silently skip edge cleanup
/// (dangling edges). An ABSENT catalog (`None`) or absent collection row
/// (`Ok(None)`) is treated as non-edge-bearing and yields `None`.
///
/// This does NOT check the not-in-transaction-block or registry-available
/// guards — those differ per protocol / are session-state concerns and stay at
/// the call sites.
pub fn plan_needs_implicit_edge_recon(
    state: &SharedState,
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
) -> crate::Result<Option<(String, DatabaseId)>> {
    let Some(dep_task) = tasks.iter().find(|t| is_edge_recon_plan(&t.plan)) else {
        return Ok(None);
    };
    // Every edge recon plan names its database-qualified collection.
    let coll = dep_task
        .plan
        .collection()
        .ok_or_else(|| Error::Internal {
            detail: "internal invariant break: an edge recon plan names no collection".into(),
        })?
        .to_owned();
    let db = dep_task.database_id;
    let edge_bearing = {
        // The plan names the collection database-qualified. The catalog keys
        // collections by the bare name.
        let bare = nodedb_types::CollectionKey::from_qualified_str(db, &coll)?.name();
        let catalog = state.credentials.catalog();
        catalog
            .get_collection(db, tenant_id.as_u64(), bare)?
            .is_some_and(|c| c.has_implicit_edges)
    };
    if edge_bearing {
        Ok(Some((coll, db)))
    } else {
        Ok(None)
    }
}

/// Whether `plan` takes the edge recon path when its collection is
/// edge-bearing: a dependent predicate write, a CRDT document delete, or a
/// TRUNCATE.
pub fn is_edge_recon_plan(plan: &PhysicalPlan) -> bool {
    is_dependent_predicate(plan)
        || super::dependent_recon_crdt::is_crdt_doc_delete(plan)
        || super::edge_truncate::is_truncate(plan)
}
