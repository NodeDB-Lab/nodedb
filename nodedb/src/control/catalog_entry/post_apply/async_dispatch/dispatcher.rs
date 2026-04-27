//! Post-apply side-effect dispatcher.
//!
//! Dispatches per-variant side effects for `CatalogEntry` mutations on
//! **every node** (leader and followers). The match is exhaustive by design —
//! adding a new `CatalogEntry` variant without wiring a branch (even if that
//! branch is `()`) is a compile error.
//!
//! ## Applied-index contract for `PutCollection`
//!
//! `DocumentOp::Register` MUST complete before `apply` returns and before the
//! applied-index watcher bumps. Correctness depends on this: any subsequent
//! `DocumentOp::Scan` on the same node must find the collection registered in
//! `doc_configs` so Binary Tuple (strict) documents decode correctly.
//!
//! `tokio::task::block_in_place` is used for the Register dispatch so it runs
//! synchronously on the calling tokio worker thread. The raft tick loop always
//! runs on a tokio worker thread, so `block_in_place` is valid here.
//!
//! All other variants (purge, MV delete, etc.) are fire-and-forget and are
//! spawned as background tasks — their correctness does not depend on
//! completing before the watcher bumps.

use std::sync::Arc;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;

use super::collection;

/// Dispatch post-apply side effects of `entry`. Runs on every node (leader
/// and followers) so each node's local Data Plane observes catalog mutations
/// symmetrically.
pub fn spawn_post_apply_async_side_effects(
    entry: CatalogEntry,
    shared: Arc<SharedState>,
    raft_index: u64,
) {
    match entry {
        CatalogEntry::PutCollection(stored) => {
            // SYNCHRONOUS: Register must complete before the applied-index
            // watcher bumps so any subsequent scan on this node finds the
            // collection in doc_configs. block_in_place is valid because
            // the raft tick loop runs on a tokio worker thread.
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    collection::put_async(*stored, shared).await;
                });
            });
        }
        CatalogEntry::PurgeCollection { tenant_id, name } => {
            tokio::spawn(async move {
                collection::purge_async(tenant_id, name, raft_index, shared).await;
            });
        }
        // `DeleteMaterializedView` now has async follow-up: every
        // node dispatches `MetaOp::UnregisterMaterializedView` to its
        // local Data Plane so the MV's columnar segment files +
        // per-core in-memory state get reclaimed on followers, not
        // just on the leader. Runs on every node; idempotent.
        CatalogEntry::DeleteMaterializedView { tenant_id, name } => {
            tokio::spawn(async move {
                super::materialized_view::delete_async(tenant_id, name, shared).await;
            });
        }
        // ── Variants with no async side effect today ─────────────────────────
        // Listed explicitly (no `_ => {}`) so the compiler forces a decision
        // when a new variant is added. Note: `DeleteTrigger` and
        // `DeleteChangeStream` handle their per-node in-memory
        // teardown synchronously via `apply_post_apply_side_effects_sync`
        // (which also runs on every node); they have no additional
        // async work today.
        CatalogEntry::DeactivateCollection { .. }
        | CatalogEntry::PutSequence(_)
        | CatalogEntry::DeleteSequence { .. }
        | CatalogEntry::PutSequenceState(_)
        | CatalogEntry::PutTrigger(_)
        | CatalogEntry::DeleteTrigger { .. }
        | CatalogEntry::PutFunction(_)
        | CatalogEntry::DeleteFunction { .. }
        | CatalogEntry::PutProcedure(_)
        | CatalogEntry::DeleteProcedure { .. }
        | CatalogEntry::PutSchedule(_)
        | CatalogEntry::DeleteSchedule { .. }
        | CatalogEntry::PutChangeStream(_)
        | CatalogEntry::DeleteChangeStream { .. }
        | CatalogEntry::PutUser(_)
        | CatalogEntry::DeactivateUser { .. }
        | CatalogEntry::PutRole(_)
        | CatalogEntry::DeleteRole { .. }
        | CatalogEntry::PutApiKey(_)
        | CatalogEntry::RevokeApiKey { .. }
        | CatalogEntry::PutMaterializedView(_)
        | CatalogEntry::PutTenant(_)
        | CatalogEntry::DeleteTenant { .. }
        | CatalogEntry::PutRlsPolicy(_)
        | CatalogEntry::DeleteRlsPolicy { .. }
        | CatalogEntry::PutPermission(_)
        | CatalogEntry::DeletePermission { .. }
        | CatalogEntry::PutOwner(_)
        | CatalogEntry::DeleteOwner { .. } => {
            let _ = shared;
            let _ = raft_index;
        }
    }
}
