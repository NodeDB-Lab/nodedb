// SPDX-License-Identifier: BUSL-1.1

//! Build a secondary index committed in the `Building` state: backfill it on
//! every node, then mark it `Ready`.
//!
//! An autocommit `CREATE INDEX` runs this at once. Inside an explicit
//! transaction the statement buffers the `Building` entry and defers this to
//! COMMIT, so a ROLLBACK leaves no index entry behind.

use crate::control::security::catalog::IndexBuildState;
use crate::control::server::shared::session::ddl_effect::SecondaryIndexBuild;
use crate::control::state::SharedState;
use crate::types::TraceId;

use super::super::super::super::result::DdlError;
use super::commit::{commit_collection_mutation, err};

/// Backfill `build` on every node and flip it to `Ready`.
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
        .map_err(|e| err("XX000", e.to_string()))?
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

    // Register the Building index on this node before the backfill scans,
    // so a write that lands after the scan maintains it. The post-apply
    // register of a transaction's buffered entry runs asynchronously.
    super::super::dispatch_register_from_stored(state, &coll)
        .await
        .map_err(|e| err("XX000", e.to_string()))?;

    // The backfill runs on the local Data Plane (single node) or the leader
    // (cluster), vShard-local per core.
    let vshard = crate::types::VShardId::from_collection_in_database(database_id, collection);
    let backfill_plan = crate::bridge::envelope::PhysicalPlan::Document(
        nodedb_physical::physical_plan::DocumentOp::BackfillIndex {
            collection: nodedb_types::QualifiedCollection::new(database_id, collection),
            path: extraction_path.clone(),
            is_array: *is_array,
            unique: *unique,
            case_insensitive: *case_insensitive,
            predicate: predicate.clone(),
        },
    );
    let backfill_resp = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        database_id,
        vshard,
        backfill_plan,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| err("XX000", e.to_string()))?;

    if backfill_resp.status == crate::bridge::envelope::Status::Error {
        let detail = match backfill_resp.error_code.as_deref() {
            Some(crate::bridge::envelope::ErrorCode::Internal { detail, .. }) => detail.clone(),
            Some(other) => format!("{other:?}"),
            None => String::from_utf8_lossy(&backfill_resp.payload).into_owned(),
        };
        let code = if detail.to_lowercase().contains("unique") {
            "23505"
        } else {
            "XX000"
        };
        return Err(err(code, detail));
    }

    // Every other node backfills the rows it hosts. Single-node and peerless
    // clusters return at once.
    super::super::index_fanout::backfill_on_peers(
        state,
        super::super::index_fanout::PeerBackfill {
            tenant_id,
            database_id,
            collection,
            path: extraction_path,
            is_array: *is_array,
            unique: *unique,
            case_insensitive: *case_insensitive,
            predicate: predicate.as_deref(),
        },
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
        .map_err(|e| err("XX000", e.to_string()))?
    {
        for idx in ready_coll.indexes.iter_mut() {
            if idx.name == build.index_name {
                idx.state = IndexBuildState::Ready;
            }
        }
        commit_collection_mutation(state, &ready_coll, build.database_id).await?;
    }
    Ok(())
}
