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
//! [`run_post_apply_async_side_effects`] awaits the Register dispatch, and
//! the metadata applier awaits it before it advances past the entry.
//!
//! Collection purge, materialized-view deletion, and the MOVE TENANT cutover
//! have the same ordering requirement: all local Data Plane cores must reclaim
//! the old incarnation before the applied-index watcher advances, because a
//! same-name re-CREATE can immediately follow. A reclaim that queued a durable
//! `_system.pending_reclaim` retry counts as done: the pending-reclaim worker
//! and the boot drain own it. A reclaim that queued nothing returns `Err`, the
//! metadata applier stops the batch at the entry, and the re-delivered entry
//! retries the reclaim.
//!
//! ## Applied-index contract for the vector-index variants
//!
//! `VectorOp::SetParams` and `VectorOp::DropIndex` MUST complete before the
//! applied-index watcher bumps. A vector write that lands first materializes
//! the index with default build parameters, and `execute_set_vector_params`
//! then refuses to reconfigure a materialized index — so a late `SetParams`
//! never applies and the node serves the wrong index for good. The same
//! refusal makes a late `DropIndex` block the same-name re-CREATE that can
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
//! ## Durability for `CompactHistory`
//!
//! Apply records the owed compaction in `_system.pending_history_compaction`.
//! The fan-out is awaited, and the row is removed once every core compacted
//! and checkpointed. A failed fan-out keeps the row for the retry worker and
//! the boot drain, so it never stops the batch.
//!
//! Variants without a read-after-apply dependency remain fire-and-forget,
//! and only where boot rebuilds their effect from redb.

use std::sync::Arc;

use tracing::warn;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;

use super::collection::{self, ReclaimFailure};

/// Dispatch post-apply side effects of `entry`. Runs on every node (leader
/// and followers) so each node's local Data Plane observes catalog mutations
/// symmetrically.
///
/// A storage reclaim takes its purge boundary from this node's own WAL. Replay
/// compares the boundary against WAL record LSNs, so it must be a WAL LSN,
/// never a Raft log index. Every write of the reclaimed collection on this
/// node sits below the next LSN this WAL assigns.
///
/// `Err` means a reclaim failed with no durable retry queued. The caller must
/// not advance past the entry, so its re-delivery retries the reclaim.
///
/// A Register that a Data Plane core did not acknowledge is logged, and the
/// batch advances.
///
/// The future resolves once every awaited effect completed. A variant with
/// no awaited effect resolves on its first poll.
pub async fn run_post_apply_async_side_effects(
    entry: CatalogEntry,
    shared: Arc<SharedState>,
) -> crate::Result<()> {
    match entry {
        CatalogEntry::PutCollection(stored) => {
            // AWAITED: Register completes before the applied-index watcher
            // bumps, so any later scan on this node finds the collection in
            // doc_configs.
            //
            // The stamp carries version 1 only on a create, and the validator
            // admits a create only as a new incarnation. Its storage is
            // cleared first, so it starts empty.
            if stored.descriptor_version == 1 {
                collection::clear_before_recreate(
                    &shared,
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    &stored.name,
                )
                .await?;
            }
            let registered = collection::put_async(&stored, &shared).await;
            register_outcome(registered, &stored.name);
        }
        CatalogEntry::PutCollectionIfAbsent(stored) => {
            // Register from the CANONICAL collection read back from the
            // catalog after apply — never from the carried entry. On the
            // no-op path (the collection already existed) the carried
            // `stored` can hold a divergent incoming config; the catalog
            // holds the authoritative pre-existing one. Post-apply the
            // collection always exists (created or pre-existing), so the
            // read-back is always Some; a None here means the redb
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
                    // AWAITED: Register completes before the applied-index
                    // watcher bumps, so any later scan on this node finds the
                    // collection in doc_configs.
                    //
                    // The canonical row carries the entry's clock only when
                    // this entry created it: a new incarnation, cleared first.
                    if canonical.modification_hlc == stored.modification_hlc {
                        collection::clear_before_recreate(
                            &shared,
                            canonical.database_id.as_u64(),
                            canonical.tenant_id,
                            &canonical.name,
                        )
                        .await?;
                    }
                    let registered = collection::put_async(&canonical, &shared).await;
                    register_outcome(registered, &canonical.name);
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
            ..
        } => {
            let purge_lsn = shared.wal.next_lsn().as_u64();
            let result = collection::reclaim_collection_storage(
                &shared,
                database_id,
                tenant_id,
                &name,
                purge_lsn,
                false,
            )
            .await;
            reclaim_outcome(result, "collection purge")?;
        }
        // AWAITED: every node must clear the view target's per-core state
        // before its applied-index watcher advances. Otherwise a same-name
        // re-CREATE can observe cached aggregates from the dropped target.
        CatalogEntry::DeleteMaterializedView {
            database_id,
            tenant_id,
            name,
            ..
        } => {
            let purge_lsn = shared.wal.next_lsn().as_u64();
            let result = super::materialized_view::delete_async(
                database_id,
                tenant_id,
                name,
                purge_lsn,
                shared,
            )
            .await;
            reclaim_outcome(result, "materialized-view target reclaim")?;
        }
        // Fire-and-forget: boot re-registers every stored aggregate from redb,
        // so a register lost to a crash is rebuilt at the next boot.
        CatalogEntry::PutContinuousAggregate(stored) => {
            let tenant_id = stored.tenant_id;
            let name = stored.name.clone();
            let def_bytes = stored.def_bytes.clone();
            tokio::spawn(async move {
                super::continuous_aggregate::put_async(tenant_id, name, def_bytes, shared).await;
            });
        }
        // AWAITED: the build parameters must reach every core before the
        // applied-index watcher bumps, or a write racing ahead of them
        // materializes the index with defaults and pins it there.
        CatalogEntry::PutVectorIndexParams(stored) => {
            super::vector::put_async(*stored, shared).await;
        }
        // AWAITED: a same-name re-CREATE can follow immediately, and
        // `SetParams` is refused while the dropped index is still materialized.
        CatalogEntry::DeleteVectorIndexParams {
            database_id,
            tenant_id,
            collection,
            field_name,
            ..
        } => {
            super::vector::delete_async(database_id, tenant_id, collection, field_name, shared)
                .await;
        }
        // AWAITED: the owed-compaction row apply wrote is removed once every
        // core compacted durably. A failed fan-out leaves the row to the
        // retry worker and the boot drain.
        CatalogEntry::CompactHistory {
            tenant_id,
            collection,
            database_id,
            target_version_json,
            ..
        } => {
            let result = super::crdt_compact::compact_async(
                database_id,
                tenant_id,
                &collection,
                &target_version_json,
                &shared,
            )
            .await;
            if let Err(error) = result {
                warn!(
                    collection = %collection,
                    tenant = tenant_id,
                    error = %error,
                    "history compaction post-apply: still owed on this node; the retry worker \
                     re-drives it"
                );
            }
        }
        // Fire-and-forget: boot re-registers only the aggregates redb still
        // holds, so an unregister lost to a crash is rebuilt at the next boot.
        CatalogEntry::DeleteContinuousAggregate {
            database_id,
            tenant_id,
            name,
            ..
        } => {
            tokio::spawn(async move {
                super::continuous_aggregate::delete_async(database_id, tenant_id, name, shared)
                    .await;
            });
        }
        // AWAITED: the group must reach every core's FTS backend before the
        // applied-index watcher bumps. A query that runs first expands
        // nothing and returns fewer rows with no error.
        CatalogEntry::PutSynonymGroup(stored) => {
            super::synonym_group::put_async(*stored, &shared).await;
        }
        // AWAITED: a query that runs before the removal lands keeps expanding
        // terms the statement already dropped.
        CatalogEntry::DeleteSynonymGroup {
            database_id,
            tenant_id,
            name,
            ..
        } => {
            super::synonym_group::delete_async(database_id, tenant_id, name, &shared).await;
        }
        // AWAITED: every core must hold the new set before the applied-index
        // watcher bumps. A statement that runs first refuses the CRS it just
        // defined, or still resolves the one it just dropped. A core that
        // misses the set stops the batch, and the re-delivered entry resends
        // it.
        CatalogEntry::PutSpatialRefSys(_) => {
            super::spatial_ref_sys::install_on_every_core(&shared, "define").await?;
        }
        CatalogEntry::DropSpatialRefSys { .. } => {
            super::spatial_ref_sys::install_on_every_core(&shared, "drop").await?;
        }
        // No core holds a credential: a COPY reads the catalog row.
        CatalogEntry::PutCredential(_) | CatalogEntry::DropCredential { .. } => {}
        // AWAITED: the moved rows must leave the source key before the
        // applied-index watcher bumps. Otherwise a same-name CREATE in the
        // source database observes them.
        CatalogEntry::MoveTenantCutover {
            source_db_id,
            collections,
            ..
        } => {
            let result =
                super::move_tenant::reclaim_moved_sources(&shared, source_db_id, &collections)
                    .await;
            reclaim_outcome(result, "MOVE TENANT source reclaim")?;
        }
        // AWAITED: every shadow collection registers before the applied-index
        // watcher bumps, as a `PutCollection` does, so a write or scan on the
        // clone finds its config in doc_configs.
        CatalogEntry::CloneDatabase {
            target_descriptor, ..
        } => {
            let registered = collection::clone_shadows_async(target_descriptor.id, &shared).await;
            register_outcome(registered, &target_descriptor.name);
        }
        // AWAITED: every core opens the array before the applied-index
        // watcher advances, purging the tombstone of a prior incarnation, so
        // the next statement on this node reads the new incarnation.
        CatalogEntry::PutArray(stored) => {
            super::array::open_on_every_core(&shared, &stored).await?;
        }
        // AWAITED: a same-name CREATE can follow at once, and a moved store
        // must sit under its target key before the target's `PutArray` opens
        // it.
        CatalogEntry::DeleteArray {
            database_id,
            tenant_id,
            name,
            moved_to,
            ..
        } => {
            super::array::delete_on_every_core(
                &shared,
                database_id,
                tenant_id,
                &name,
                moved_to.map(|m| m.target_db_id),
            )
            .await?;
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
        // The teardown drops the database's arrays through their own
        // `DeleteArray` entries, ahead of this one.
        | CatalogEntry::DeleteDatabase { .. }
        | CatalogEntry::PutDatabaseGrant { .. }
        | CatalogEntry::DeleteDatabaseGrant { .. }
        | CatalogEntry::PutOidcProvider(_)
        | CatalogEntry::DeleteOidcProvider { .. }
        | CatalogEntry::RecordWalTombstone { .. }
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
        // Offset advance happens in `sync.rs`.
        | CatalogEntry::CommitConsumerOffsets(_)
        // A backup schedule mark is its catalog row alone.
        | CatalogEntry::PutBackupScheduleMark(_)
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
        // Clone copy-on-write rows have no in-memory mirror and no Data
        // Plane side effect.
        | CatalogEntry::PutCloneCopyup { .. }
        | CatalogEntry::PutCloneTombstone { .. }
        | CatalogEntry::PutKvCloneTombstone { .. }
        // Clone source drain claims are read only from the catalog by the
        // singleton worker's recovery.
        | CatalogEntry::PutCloneSourceDrain(_)
        | CatalogEntry::DeleteCloneSourceDrain { .. } => {
            let _ = shared;
        }
    }
    Ok(())
}

/// Log a Register that a Data Plane core did not acknowledge. The metadata
/// applier runs the post-apply lane, and no client waits on it. The batch
/// advances: the catalog row is durable, and boot seeds every core's config
/// from it.
fn register_outcome(result: crate::Result<()>, collection: &str) {
    if let Err(error) = result {
        tracing::error!(
            collection,
            error = %error,
            "catalog_entry: Register barrier failed — one or more Data Plane cores \
             did not acknowledge the schema update; this node may serve stale schema"
        );
    }
}

/// Map a reclaim result onto the post-apply contract.
///
/// A queued durable retry is owned by the pending-reclaim worker and the boot
/// drain, so it counts as done. Anything else is `Err`, which stops the apply
/// batch at this entry.
fn reclaim_outcome(result: Result<(), ReclaimFailure>, what: &str) -> crate::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(failure) if failure.retry_queued => {
            warn!(
                error = %failure.error,
                "{what} post-apply: reclaim failed on this node; the pending-reclaim worker \
                 owns the retry"
            );
            Ok(())
        }
        Err(failure) => Err(failure.error),
    }
}
