// SPDX-License-Identifier: BUSL-1.1

//! Build a secondary index committed in the `Building` state: backfill it on
//! every replica through the collection's data group, then mark it `Ready`.
//!
//! An autocommit `CREATE INDEX` runs this at once. Inside an explicit
//! transaction the statement buffers the `Building` entry and defers this to
//! COMMIT, so a ROLLBACK leaves no index entry behind.

use crate::control::security::catalog::IndexBuildState;
use crate::control::server::shared::session::ddl_effect::SecondaryIndexBuild;
use crate::control::state::SharedState;

use super::super::super::super::result::DdlError;
use super::commit::commit_collection_mutation;

/// Backfill `build` on every replica and flip it to `Ready`.
///
/// A collection that no longer carries the index by name is skipped: the
/// same transaction dropped it before COMMIT. UNIQUE violations surface as
/// SQLSTATE 23505 and leave the index `Building`, so a later `DROP INDEX`
/// removes it after the data is fixed.
pub(crate) async fn build_secondary_index(
    state: &SharedState,
    build: &SecondaryIndexBuild,
) -> Result<(), DdlError> {
    let SecondaryIndexBuild {
        tenant_id,
        database_id,
        collection,
        index_name,
        extraction_path,
        is_array,
        unique,
        case_insensitive,
        predicate,
    } = build;
    let (tenant_id, database_id) = (*tenant_id, *database_id);
    let catalog = state.credentials.catalog();
    let Some(coll) = catalog
        .get_collection(database_id, tenant_id.as_u64(), collection)
        .map_err(|e| DdlError::from_error(&e))?
        .filter(|coll| coll.indexes.iter().any(|i| &i.name == index_name))
    else {
        return Ok(());
    };

    // A key-value collection's rows live in the KV engine, which keeps its
    // own secondary indexes: the index is built there, with a backfill.
    if coll.collection_type.is_kv() {
        let field = super::kv_index::kv_field(
            collection,
            &build_path(extraction_path, *is_array),
            &super::kv_index::KvIndexOptions {
                unique: *unique,
                case_insensitive: *case_insensitive,
                predicate: predicate.as_deref(),
            },
        )?;
        super::kv_index::register_kv_index(state, tenant_id, database_id, &coll, &field).await?;
        return mark_ready(state, build).await;
    }

    // The build proposal carries this node's metadata floor, so each replica
    // applies it only after the Building catalog entry. Every replica of the
    // collection's vShard builds from the rows it holds at the build's log
    // position, and adds the index to the paths it maintains in the same
    // transaction. Every write after the build maintains it. A UNIQUE
    // duplicate refuses the build on every replica with `23505`, and no
    // replica maintains the index.
    let backfill_plan = crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::BackfillIndex {
            collection: nodedb_types::QualifiedCollection::new(database_id, collection),
            path: extraction_path.clone(),
            is_array: *is_array,
            unique: *unique,
            case_insensitive: *case_insensitive,
            predicate: predicate.clone(),
            owner: nodedb_physical::physical_plan::DocIndexOwner::Named(index_name.clone()),
            // The `Building` descriptor the build derives from. A retire of
            // the same name committed before it carries a lower fence.
            fence: crate::control::doc_index_install::doc_index_fence(&coll),
        },
    );
    super::engine_step::propose_index_step(
        state,
        tenant_id,
        database_id,
        collection,
        backfill_plan,
        "index backfill",
    )
    .await?;

    mark_ready(state, build).await
}

/// The path as the statement named it: an array path keeps its `[]` suffix.
fn build_path(extraction_path: &str, is_array: bool) -> String {
    if is_array {
        format!("{extraction_path}[]")
    } else {
        extraction_path.to_string()
    }
}

/// Flip the built index to `Ready` on a fresh read, so a concurrent mutation
/// of the collection is folded in before the index vector is rewritten. A
/// collection dropped meanwhile has no index left to flip.
async fn mark_ready(state: &SharedState, build: &SecondaryIndexBuild) -> Result<(), DdlError> {
    let catalog = state.credentials.catalog();
    if let Some(mut ready_coll) = catalog
        .get_collection(
            build.database_id,
            build.tenant_id.as_u64(),
            &build.collection,
        )
        .map_err(|e| DdlError::from_error(&e))?
    {
        for idx in ready_coll.indexes.iter_mut() {
            if idx.name == build.index_name {
                idx.state = IndexBuildState::Ready;
            }
        }
        commit_collection_mutation(state, &ready_coll).await?;
    }
    Ok(())
}
