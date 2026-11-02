// SPDX-License-Identifier: BUSL-1.1

//! Copy-up write helper for cloned collections.
//!
//! When an UPDATE targets a row that exists only in the source of a
//! `Shadowed` clone, this module performs the copy-up:
//!
//! 1. Bind the target surrogate.
//! 2. Install the source row's version in the target shard through its
//!    apply log, so every replica holds it, at the source version's own
//!    system-time ordinal.
//! 3. Record `(target_collection, source_surrogate) → target_surrogate` in
//!    the `clone_copyups` table of every node, through the metadata log.
//!
//! The target row goes first. A clone read suppresses every source row that
//! has a mapping, so a mapping without its target row hides the row
//! entirely. A failure after the target write instead leaves an exact copy of
//! the source row with no mapping. A clone read also suppresses a source row
//! whose primary key the target holds, so the row still reads once, from the
//! target. A retry of the statement assigns the same target surrogate,
//! leaves the installed row as it stands, and then writes the mapping, so it
//! converges. A materializer copy installs only where the target holds no
//! version of the row, and materialization then drops the source side.

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::maintenance::clone_materializer::{SourceKey, SourceRow};
use crate::control::state::SharedState;

use super::copy::{CopyTarget, DocCopy, KvCopy, commit_doc_copies, commit_kv_copies};

/// Parameters for a KV copy-up operation.
pub(crate) struct KvCopyUpParams<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    pub target_db_id: DatabaseId,
    /// Plain (non-db_qualified) collection name.
    pub target_collection: &'a str,
    /// The source key's live version, with its stamps.
    pub source: SourceKey,
}

/// Perform a KV copy-up: install the source key's version into the target
/// shard on every replica, at the source version's ordinal and expiry, making
/// the row available for subsequent FieldSet or Delete operations in the
/// clone.
///
/// The copy is the same logical version as the source row. A snapshot that
/// read the key through the source reads the same row from the target, and a
/// transaction whose snapshot sits at or above the source version sees no
/// newer version of the key.
///
/// A KV copy-up records no mapping: a clone read hides a source key the
/// target holds, and the caller's tombstone follows. A failure before the
/// tombstone leaves a target row that already shadows the source, and a retry
/// leaves it as installed.
pub(crate) async fn perform_kv_clone_copyup(params: KvCopyUpParams<'_>) -> crate::Result<()> {
    let KvCopyUpParams {
        state,
        tenant_id,
        target_db_id,
        target_collection,
        source,
    } = params;

    let target_key = nodedb_types::CollectionKey::from_bare(target_db_id, target_collection);
    let surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
        state,
        target_key,
        tenant_id,
        &source.key,
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| crate::Error::Storage {
        engine: "clone_kv_copyup".into(),
        detail: format!("surrogate alloc failed: {e}"),
    })?;

    commit_kv_copies(
        state,
        CopyTarget {
            tenant_id,
            database_id: target_db_id,
            collection: target_collection,
        },
        vec![KvCopy {
            key: source.key,
            target_surrogate: surrogate,
            value: source.value,
            stamp: source.stamp,
        }],
    )
    .await
    .map_err(|e| crate::Error::Storage {
        engine: "clone_kv_copyup".into(),
        detail: format!("target write failed: {e}"),
    })
}

/// Parameters for a copy-up operation.
pub(crate) struct CopyUpParams<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    pub target_db_id: DatabaseId,
    /// Plain (non-db_qualified) collection name.
    pub target_collection: &'a str,
    /// The source surrogate to copy up.
    pub source_surrogate: Surrogate,
    /// The row's client identity text.
    pub source_doc_id: String,
    /// The source row's version as stored, with its stamps. The caller reads
    /// it at the scope the clone reads its source at.
    pub source_row: SourceRow,
}

/// Perform a copy-up: install the source row's version into the target shard
/// under a bound surrogate on every replica, at the source version's own
/// stamps, then record the mapping in `clone_copyups` on every node.
///
/// The copy is the same logical version as the source row, so no snapshot
/// sees the row change, vanish, or move above its snapshot.
///
/// Returns the target surrogate so the caller can apply the pending UPDATE
/// to it.
pub(crate) async fn perform_clone_copyup(params: CopyUpParams<'_>) -> crate::Result<Surrogate> {
    let CopyUpParams {
        state,
        tenant_id,
        target_db_id,
        target_collection,
        source_surrogate,
        source_doc_id,
        source_row,
    } = params;
    let copy_error = |detail: String| crate::Error::Storage {
        engine: "clone_copyup".into(),
        detail,
    };
    let stamp = source_row.stamp.ok_or_else(|| {
        copy_error(format!(
            "source row of document '{source_doc_id}' carries no version stamp"
        ))
    })?;

    // Bind the target surrogate using the (collection, doc_id) key.
    let target_key = nodedb_types::CollectionKey::from_bare(target_db_id, target_collection);
    let target_surrogate = crate::control::server::surrogate_exchange::assign_surrogate_routed(
        state,
        target_key,
        tenant_id,
        source_doc_id.as_bytes(),
        crate::types::TraceId::ZERO,
    )
    .await
    .map_err(|e| copy_error(format!("surrogate alloc failed: {e}")))?;

    let body = match state.credentials.catalog().get_collection(
        target_db_id,
        tenant_id.as_u64(),
        target_collection,
    )? {
        Some(coll) => super::identity::carry_identity(&coll, source_row.body, &source_doc_id),
        None => source_row.body,
    };
    commit_doc_copies(
        state,
        CopyTarget {
            tenant_id,
            database_id: target_db_id,
            collection: target_collection,
        },
        vec![DocCopy {
            document_id: source_doc_id.clone(),
            pk_bytes: source_doc_id.as_bytes().to_vec(),
            target_surrogate,
            body,
            stamp,
        }],
    )
    .await
    .map_err(|e| {
        copy_error(format!(
            "target write of document '{source_doc_id}' failed: {e}"
        ))
    })?;

    // Keyed by the TARGET collection: every reader looks the mapping up
    // under the clone it belongs to.
    let row = CopyupRow {
        database_id: target_db_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        collection: target_collection.to_string(),
        source_surrogate: source_surrogate.as_u32(),
    };
    super::cow_entry::replicate_async(state, &row.put(target_surrogate))
        .await
        .map_err(|e| {
            copy_error(format!(
                "mapping of document '{source_doc_id}' failed after its target write: {e}"
            ))
        })?;
    Ok(target_surrogate)
}

/// One copy-up mapping row, before it becomes a catalog entry.
struct CopyupRow {
    database_id: u64,
    tenant_id: u64,
    collection: String,
    source_surrogate: u32,
}

impl CopyupRow {
    fn put(self, target_surrogate: Surrogate) -> CatalogEntry {
        CatalogEntry::PutCloneCopyup {
            database_id: self.database_id,
            tenant_id: self.tenant_id,
            collection: self.collection,
            source_surrogate: self.source_surrogate,
            target_surrogate: target_surrogate.as_u32(),
        }
    }
}
