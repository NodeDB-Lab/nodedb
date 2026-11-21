// SPDX-License-Identifier: BUSL-1.1

//! Per-kind index teardown.
//!
//! Each index kind establishes different state at CREATE time, so each needs
//! its own removal:
//!
//! | kind      | durable state created                              |
//! |-----------|----------------------------------------------------|
//! | secondary | `StoredCollection.indexes` entry + sparse-engine index entries, or the KV engine's field index on a key-value collection |
//! | vector    | `_system.vector_index_params` row + Data Plane index + checkpoint |
//! | fulltext  | postings the drop's text rebuild replaces; its analyzer and fuzzy default travel on its record |
//! | spatial   | none beyond the registry + ownership rows           |
//! | sparse    | none beyond the registry + ownership rows           |
//! | sorted    | an order-statistic tree on the core holding the collection's rows |
//!
//! Every failure here propagates. A teardown that logs and continues will
//! report a successful drop over state that is still live — the same silent
//! success that made a vector index undroppable in the first place.
//!
//! The vector arm is the exception a replicated catalog forces: its WAL record
//! and Data Plane drop run on every node from the post-apply lane, which
//! cannot propagate and files a `Capture` instead.

use crate::control::security::catalog::{IndexKind, StoredIndexRecord};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::super::super::super::result::DdlError;
use crate::control::server::shared::session::ddl_buffer;
use crate::control::server::shared::session::ddl_effect::DeferredDdlEffect;

use super::commit::commit_collection_mutation;

/// Remove every piece of engine and catalog state belonging to `record`,
/// except the registry and ownership rows the caller removes afterwards.
pub(super) async fn teardown(
    state: &SharedState,
    record: &StoredIndexRecord,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    match record.kind {
        IndexKind::Secondary => secondary(state, record, database_id, tenant_id).await,
        IndexKind::Vector => vector(state, record, database_id, tenant_id).await,
        // A full-text index's analysis travels on its record. Once the record
        // is `Dropping` or gone, the collection's next Register derives its
        // analysis from the records that remain, on every node. The dropped
        // fields' postings leave in the drop's text rebuild.
        IndexKind::FullText => Ok(()),
        // A spatial or sparse index declares no durable engine state of its
        // own: `CREATE SPATIAL INDEX` / `CREATE SPARSE INDEX` register the
        // index and bind nothing in the Data Plane, and the R-tree / sparse
        // posting structures are built per collection from the rows
        // themselves. Removing the registry and ownership rows is the whole
        // teardown.
        IndexKind::Spatial | IndexKind::Sparse => Ok(()),
        // A sorted index owns an order-statistic tree on the core that holds
        // its collection's rows — the same core that maintains it on every
        // write — so its removal goes through the same route
        // `DROP SORTED INDEX` uses.
        IndexKind::Sorted => {
            let deferred = ddl_buffer::defer_effect(DeferredDdlEffect::SortedIndexDrop {
                tenant_id,
                database_id,
                collection: record.collection.clone(),
                index_name: record.name.clone(),
            });
            if deferred {
                return Ok(());
            }
            super::super::super::kv_sorted_index::drop_in_engine(
                state,
                &super::super::super::kv_sorted_index::SortedIndexTarget {
                    tenant_id,
                    database_id,
                    collection: &record.collection,
                },
                &record.name,
            )
            .await
        }
    }
}

/// Drop the `StoredIndex` entry from the owning collection and purge the
/// indexed path's entries: from the sparse engine, or from the KV engine on
/// a key-value collection.
async fn secondary(
    state: &SharedState,
    record: &StoredIndexRecord,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    let Some(mut coll) = catalog
        .get_collection(database_id, tenant_id.as_u64(), &record.collection)
        .map_err(|e| DdlError::from_error(&e))?
    else {
        // The registry outlived its collection — the collection teardown
        // path already reclaimed every engine surface, so there is nothing
        // left to remove here.
        return Ok(());
    };

    let dropped_field = coll
        .indexes
        .iter()
        .find(|i| i.name == record.name)
        .map(|i| i.field.clone());
    coll.indexes.retain(|i| i.name != record.name);
    commit_collection_mutation(state, &coll).await?;

    // Purge existing index entries from the sparse engine so stale rows
    // cannot leak into lookups on a re-created index of the same name.
    let Some(field) = dropped_field.or_else(|| record.fields.first().cloned()) else {
        return Ok(());
    };
    // A key-value collection's index lives in the KV engine.
    if coll.collection_type.is_kv() {
        let field = field.strip_prefix("$.").unwrap_or(&field).to_string();
        let deferred = ddl_buffer::defer_effect(DeferredDdlEffect::KvIndexDrop {
            tenant_id,
            database_id,
            collection: record.collection.clone(),
            field: field.clone(),
        });
        if deferred {
            return Ok(());
        }
        return super::kv_index::drop_kv_index(
            state,
            tenant_id,
            database_id,
            &record.collection,
            &field,
        )
        .await;
    }
    // The entries are keyed by the extraction path, which drops an array
    // path's `[]` suffix.
    let field = field.strip_suffix("[]").unwrap_or(&field).to_string();
    let plan = crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::DropIndex {
            collection: nodedb_types::QualifiedCollection::new(database_id, &record.collection),
            field,
            owner: nodedb_physical::physical_plan::DocIndexOwner::Named(record.name.clone()),
            // `dispatch` stamps the fence of the committed descriptor that
            // dropped the index, at once or at COMMIT.
            fence: nodedb_physical::physical_plan::DocIndexFence::default(),
        },
    );
    teardown_now_or_at_commit(state, tenant_id, database_id, &record.collection, plan).await
}

/// Remove the vector index's durable build parameters and its Data Plane
/// state (graph, config, declared dimension, checkpoint).
///
/// The catalog row is replicated, and the WAL drop record plus the Data Plane
/// drop are node-local physical state, so every node runs them from its own
/// post-apply lane, this one included, before the propose returns.
async fn vector(
    state: &SharedState,
    record: &StoredIndexRecord,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Result<(), DdlError> {
    super::super::super::vector_replicate::propose_delete_params(
        state,
        database_id.as_u64(),
        tenant_id.as_u64(),
        &record.collection,
        record.primary_field(),
    )
    .await
}

/// Run one teardown plan now, or at COMMIT inside an explicit transaction:
/// the catalog entry that drops the index is buffered, so its engine state
/// must survive a ROLLBACK.
async fn teardown_now_or_at_commit(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: crate::bridge::envelope::PhysicalPlan,
) -> Result<(), DdlError> {
    let deferred = ddl_buffer::defer_effect(DeferredDdlEffect::IndexTeardown {
        tenant_id,
        database_id,
        collection: collection.to_string(),
        plan: plan.clone(),
    });
    if deferred {
        return Ok(());
    }
    dispatch(state, tenant_id, database_id, collection, plan).await
}

/// Propose one teardown plan to every replica of the collection's vShard.
/// Each replica purges the entries at the plan's log position and appends
/// its WAL record, so a re-created index on any node starts from none.
///
/// A document index retire carries the fence of the committed descriptor
/// that dropped the index. That descriptor is committed here, so its fence
/// is above every descriptor that still held the index, and a build derived
/// from one of those changes nothing after this retire.
pub(crate) async fn dispatch(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    mut plan: crate::bridge::envelope::PhysicalPlan,
) -> Result<(), DdlError> {
    if let crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::DropIndex { fence, .. },
    ) = &mut plan
    {
        let committed = state
            .credentials
            .catalog()
            .get_committed_collection(database_id, tenant_id.as_u64(), collection)
            .map_err(|e| DdlError::from_error(&e))?;
        let Some(committed) = committed else {
            // The collection is gone. Its purge clears every core's set.
            return Ok(());
        };
        *fence = crate::control::doc_index_install::doc_index_fence(&committed);
    }
    super::engine_step::propose_index_step(
        state,
        tenant_id,
        database_id,
        collection,
        plan,
        "index teardown failed",
    )
    .await?;
    Ok(())
}
