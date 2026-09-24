// SPDX-License-Identifier: BUSL-1.1

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
//! Collection purge and materialized-view deletion have the same ordering
//! requirement: all local Data Plane cores must reclaim the old incarnation
//! before the applied-index watcher advances, because a same-name re-CREATE may
//! immediately follow. Reclaim failure is fatal to the applying node; the
//! durable pending-reclaim record is drained on restart before stale state can
//! be served.
//!
//! ## Applied-index contract for the vector-index variants
//!
//! `VectorOp::SetParams` and `VectorOp::DropIndex` MUST complete before the
//! applied-index watcher bumps. A vector write that lands first materializes
//! the index with default build parameters, and `execute_set_vector_params`
//! then refuses to reconfigure a materialized index — so a late `SetParams`
//! never applies and the node serves the wrong index for good. The same
//! refusal makes a late `DropIndex` block the same-name re-CREATE that may
//! follow it.
//!
//! ## Applied-index contract for the synonym-group variants
//!
//! `MetaOp::PutSynonymGroup` and `MetaOp::DeleteSynonymGroup` MUST complete
//! before the applied-index watcher bumps. `propose_catalog_entry` returns to
//! the DDL caller once the local watermark reaches the entry, so the client's
//! next query runs immediately after. A group that installs after that query
//! expands nothing, and a group that is deleted after it keeps expanding —
//! both answer with the wrong row set and no error, which no later dispatch
//! makes the client aware of.
//!
//! ## Ordering for `CompactHistory`
//!
//! `CrdtOp::CompactAtVersion` has no such refusal: a compaction that lands
//! after a later read still discards the same oplog entries. It is spawned
//! fire-and-forget.
//!
//! Variants without a read-after-apply dependency remain fire-and-forget.

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
        CatalogEntry::PutCollectionIfAbsent(stored) => {
            // Register from the CANONICAL collection read back from the
            // catalog after apply — never from the carried entry. On the
            // no-op path (the collection already existed) the carried
            // `stored` may hold a divergent incoming config; the catalog
            // holds the authoritative pre-existing one. Post-apply the
            // collection always exists (created or pre-existing), so the
            // read-back is always Some; a None here would mean the redb
            // write silently failed, so warn and skip rather than register
            // a divergent config.
            let canonical = shared
                .credentials
                .catalog()
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten();
            match canonical {
                Some(canonical) => {
                    // SYNCHRONOUS: Register must complete before the
                    // applied-index watcher bumps so any subsequent scan on
                    // this node finds the collection in doc_configs.
                    // block_in_place is valid because the raft tick loop
                    // runs on a tokio worker thread.
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async move {
                            collection::put_async(canonical, shared).await;
                        });
                    });
                }
                None => {
                    tracing::warn!(
                        collection = %stored.name,
                        tenant = stored.tenant_id,
                        "PutCollectionIfAbsent post-apply: canonical collection not found in \
                         catalog after apply; skipping Data Plane register"
                    );
                }
            }
        }
        CatalogEntry::PurgeCollection {
            database_id,
            tenant_id,
            name,
        } => {
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    collection::reclaim_collection_storage(
                        &shared,
                        database_id,
                        tenant_id,
                        &name,
                        raft_index,
                        false,
                    )
                    .await
                })
            });
            if let Err(error) = result {
                panic!("collection post-apply reclaim failed: {error}");
            }
        }
        // SYNCHRONOUS: every node must clear the view target's per-core state
        // before its applied-index watcher advances. Otherwise a same-name
        // re-CREATE can observe cached aggregates from the dropped target.
        // A failure is fatal: the metadata deletion is already committed, so
        // continuing would serve an inconsistent catalog/Data Plane pair;
        // restart safely reconstructs the in-memory cache from empty state.
        CatalogEntry::DeleteMaterializedView {
            database_id,
            tenant_id,
            name,
        } => {
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    super::materialized_view::delete_async(
                        database_id,
                        tenant_id,
                        name,
                        raft_index,
                        shared,
                    )
                    .await
                })
            });
            if let Err(error) = result {
                panic!("materialized-view post-apply reclaim failed: {error}");
            }
        }
        // `PutContinuousAggregate` dispatches register to every core on
        // this node so the local `continuous_agg_mgr` picks up the new
        // definition after a raft commit without re-issuing DDL.
        CatalogEntry::PutContinuousAggregate(stored) => {
            let tenant_id = stored.tenant_id;
            let name = stored.name.clone();
            let def_bytes = stored.def_bytes.clone();
            tokio::spawn(async move {
                super::continuous_aggregate::put_async(tenant_id, name, def_bytes, shared).await;
            });
        }
        // SYNCHRONOUS: the build parameters must reach every core before the
        // applied-index watcher bumps, or a write racing ahead of them
        // materializes the index with defaults and pins it there.
        CatalogEntry::PutVectorIndexParams(stored) => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    super::vector::put_async(*stored, Arc::clone(&shared)).await;
                });
            });
        }
        // SYNCHRONOUS: a same-name re-CREATE may follow immediately, and
        // `SetParams` is refused while the dropped index is still materialized.
        CatalogEntry::DeleteVectorIndexParams {
            database_id,
            tenant_id,
            collection,
            field_name,
        } => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    super::vector::delete_async(
                        database_id,
                        tenant_id,
                        collection,
                        field_name,
                        Arc::clone(&shared),
                    )
                    .await;
                });
            });
        }
        // `CompactHistory` dispatches the oplog compaction to every core so
        // each node discards the same history the leader does. A late
        // compaction still succeeds, so this stays fire-and-forget.
        CatalogEntry::CompactHistory {
            tenant_id,
            collection,
            database_id,
            target_version_json,
            ..
        } => {
            tokio::spawn(async move {
                super::crdt_compact::compact_async(
                    database_id,
                    tenant_id,
                    &collection,
                    &target_version_json,
                    &shared,
                )
                .await;
            });
        }
        // `DeleteContinuousAggregate` dispatches unregister to every
        // core so per-node runtime state is reclaimed symmetrically.
        CatalogEntry::DeleteContinuousAggregate {
            database_id,
            tenant_id,
            name,
        } => {
            tokio::spawn(async move {
                super::continuous_aggregate::delete_async(database_id, tenant_id, name, shared)
                    .await;
            });
        }
        // SYNCHRONOUS: the group must reach every core's FTS backend before
        // the applied-index watcher bumps. A query that runs first expands
        // nothing and returns fewer rows with no error.
        CatalogEntry::PutSynonymGroup(stored) => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    super::synonym_group::put_async(*stored, &shared).await;
                });
            });
        }
        // SYNCHRONOUS: a query that runs before the removal lands keeps
        // expanding terms the statement already dropped.
        CatalogEntry::DeleteSynonymGroup {
            database_id,
            tenant_id,
            name,
        } => {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    super::synonym_group::delete_async(database_id, tenant_id, name, &shared).await;
                });
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
        | CatalogEntry::DropUser { .. }
        | CatalogEntry::PutRole(_)
        | CatalogEntry::DeleteRole { .. }
        | CatalogEntry::PutApiKey(_)
        | CatalogEntry::RevokeApiKey { .. }
        // The auth-user cache install is synchronous, in `sync.rs`.
        | CatalogEntry::PutAuthUser(_)
        | CatalogEntry::PutMaterializedView(_)
        | CatalogEntry::PutStreamingMaterializedView(_)
        | CatalogEntry::DeleteStreamingMaterializedView { .. }
        // PutContinuousAggregate / DeleteContinuousAggregate have their
        // own async branches above; they do not appear here.
        | CatalogEntry::PutTenant(_)
        | CatalogEntry::PutTenantWithAdmin { .. }
        | CatalogEntry::DeleteTenant { .. }
        | CatalogEntry::PutRlsPolicy(_)
        | CatalogEntry::DeleteRlsPolicy { .. }
        // Redaction policies: the real side effect happens in `sync.rs`.
        | CatalogEntry::PutRedactionPolicy(_)
        | CatalogEntry::DeleteRedactionPolicy { .. }
        | CatalogEntry::PutPermission(_)
        | CatalogEntry::DeletePermission { .. }
        // Scope grants: the store install happens in `sync.rs`.
        | CatalogEntry::PutScopeGrant(_)
        | CatalogEntry::DeleteScopeGrant { .. }
        | CatalogEntry::PutIndexRecord(_)
        | CatalogEntry::DeleteIndexRecord { .. }
        | CatalogEntry::PutOwner(_)
        | CatalogEntry::DeleteOwner { .. }
        // PutSynonymGroup / DeleteSynonymGroup have their own synchronous
        // branches above; they do not appear here. A custom type has no Data
        // Plane mirror — the registry install in `sync.rs` is the whole
        // per-node effect.
        | CatalogEntry::PutCustomType(_)
        | CatalogEntry::DeleteCustomType { .. }
        | CatalogEntry::PutDatabase(_)
        | CatalogEntry::DeleteDatabase { .. }
        | CatalogEntry::PutDatabaseGrant { .. }
        | CatalogEntry::DeleteDatabaseGrant { .. }
        | CatalogEntry::PutOidcProvider(_)
        | CatalogEntry::DeleteOidcProvider { .. }
        | CatalogEntry::RecordWalTombstone { .. }
        | CatalogEntry::CloneDatabase { .. }
        // Quota enforcement is installed synchronously, in `sync.rs`.
        | CatalogEntry::PutDatabaseQuota { .. }
        | CatalogEntry::DeleteDatabaseQuota { .. }
        | CatalogEntry::PutTenantQuota { .. }
        | CatalogEntry::DeleteTenantQuota { .. }
        | CatalogEntry::PutScopeQuota(_)
        | CatalogEntry::DeleteScopeQuota { .. }
        // Registry install happens in `sync.rs`.
        | CatalogEntry::PutRetentionPolicy(_)
        | CatalogEntry::DeleteRetentionPolicy { .. }
        // Registry install happens in `sync.rs`.
        | CatalogEntry::PutAlertRule(_)
        | CatalogEntry::DeleteAlertRule { .. }
        // Registry, CDC buffer, and offset teardown all happen in `sync.rs`.
        | CatalogEntry::CreateTopicIfAbsent(_)
        | CatalogEntry::DeleteTopicWithConsumerGroups { .. }
        | CatalogEntry::PutConsumerGroupIfAbsent(_)
        | CatalogEntry::DeleteConsumerGroup { .. }
        | CatalogEntry::MigrateConsumerGroupStream { .. }
        // Checkpoints have no in-memory mirror at all.
        // CompactHistory has its own async branch above; it does not appear
        // here.
        | CatalogEntry::PutCheckpoint(_)
        | CatalogEntry::DeleteCheckpoint { .. }
        // Vector model metadata has no in-memory mirror.
        // PutVectorIndexParams / DeleteVectorIndexParams have their own
        // async branches above; they do not appear here.
        | CatalogEntry::PutVectorModel(_)
        | CatalogEntry::DeleteVectorModel { .. }
        // Column statistics have no in-memory mirror.
        | CatalogEntry::PutColumnStats(_)
        | CatalogEntry::MoveTenantCutover { .. } => {
            let _ = shared;
            let _ = raft_index;
        }
    }
}
