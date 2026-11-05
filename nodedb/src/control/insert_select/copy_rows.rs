// SPDX-License-Identifier: BUSL-1.1

//! Shared row-copy machinery for `INSERT ... SELECT`, used by both the
//! autocommit orchestrator and the statement-time staged expander: assign a
//! fresh target-keyed surrogate to each shaped source row and emit
//! `(target_doc_id, value, surrogate)`.
//!
//! The rows arrive from `control::source_read` as standard msgpack maps,
//! already filtered, limited, and shaped into the target's columns.

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::control::state::SharedState;
use crate::control::target_identity::{TargetPk, assign_target_surrogates, resolve_target_pk};
use crate::engine::document::store::StorageKey;

/// Resolved, per-statement copy context shared across every source batch.
pub(crate) struct CopySpec {
    /// How the TARGET collection's primary key maps a copied row to a surrogate.
    pub target_pk: TargetPk,
}

/// Resolve the target PK for one `INSERT ... SELECT` statement.
///
/// `target_collection` is the db-qualified name as it appears in the
/// `DocumentOp::InsertSelect` plan.
pub(crate) fn resolve_copy_spec(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &str,
) -> crate::Result<CopySpec> {
    let target = state
        .credentials
        .catalog()
        .get_collection(
            database_id,
            tenant_id.as_u64(),
            nodedb_types::CollectionKey::from_qualified_str(database_id, target_collection)?.name(),
        )?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id,
            collection: target_collection.to_string(),
        })?;
    Ok(CopySpec {
        target_pk: resolve_target_pk(&target, "INSERT ... SELECT")?,
    })
}

/// Assign a fresh target surrogate to each shaped row of one source batch.
///
/// Returns the concrete `(target_doc_id, msgpack_value, fresh_surrogate)`
/// to write, in source order.
pub(crate) async fn assign_page_rows(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &str,
    spec: &CopySpec,
    rows: Vec<Vec<u8>>,
) -> crate::Result<Vec<(String, Vec<u8>, Surrogate)>> {
    // Every copied row's surrogate in one batch at the target's home.
    let bodies: Vec<&[u8]> = rows.iter().map(Vec::as_slice).collect();
    let surrogates = assign_target_surrogates(
        state,
        nodedb_types::CollectionKey::from_qualified_str(database_id, target_collection)?,
        tenant_id,
        &spec.target_pk,
        &bodies,
    )
    .await?;
    Ok(rows
        .into_iter()
        .zip(surrogates)
        .map(|(value, surrogate)| {
            (
                StorageKey::for_surrogate(surrogate).to_string(),
                value,
                surrogate,
            )
        })
        .collect())
}
