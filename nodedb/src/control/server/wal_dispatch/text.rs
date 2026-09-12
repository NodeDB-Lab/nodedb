// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Text(TextOp)`.

use nodedb_physical::physical_plan::TextOp;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;

use super::super::wal_dispatch_fts_spatial;

/// Append the WAL record for a single `TextOp`, returning the allocated LSN
/// for the FTS write variants (`Some`) or `None` for every read/search
/// variant, which carries no durable per-write effect.
///
/// `FtsIndexDoc` / `FtsDeleteDoc` are handled here so any call site that
/// reaches [`super::wal_append_if_write_with_creds`] with one of these
/// variants is durable by construction. The sync-inbound handler
/// (`sync/fts_handler.rs`) already calls `wal_append_fts_index` /
/// `wal_append_fts_delete` directly and dispatches straight to the Data
/// Plane via `dispatch_sync_payload` — it never reaches this function, so
/// this arm cannot double-append on that path today (mirrors
/// `VectorOp::DeleteBySurrogate`'s identical "sync path bypasses it, but log
/// here too" reasoning in `wal_dispatch/vector.rs`).
pub(crate) fn wal_append_text_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &TextOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        TextOp::FtsIndexDoc {
            collection,
            surrogate,
            text,
            provenance,
        } => {
            let doc_id =
                crate::engine::document::store::StorageKey::for_surrogate(*surrogate).to_string();
            let prov = provenance.clone().unwrap_or_default();
            let payload =
                nodedb_wal::record::FtsIndexPayload::new(prov, collection.as_str(), &doc_id, text);
            Some(wal_dispatch_fts_spatial::wal_append_fts_index(
                wal,
                tenant_id,
                vshard_id,
                database_id,
                &payload,
            )?)
        }
        TextOp::FtsDeleteDoc {
            collection,
            surrogate,
            provenance,
        } => {
            let doc_id =
                crate::engine::document::store::StorageKey::for_surrogate(*surrogate).to_string();
            let prov = provenance.clone().unwrap_or_default();
            let payload =
                nodedb_wal::record::FtsDeletePayload::new(prov, collection.as_str(), &doc_id);
            Some(wal_dispatch_fts_spatial::wal_append_fts_delete(
                wal,
                tenant_id,
                vshard_id,
                database_id,
                &payload,
            )?)
        }
        // Reads / scans / analyzer config: no durable effect.
        TextOp::Search { .. }
        | TextOp::BM25ScoreScan { .. }
        | TextOp::PhraseSearch { .. }
        | TextOp::HybridSearch { .. }
        | TextOp::HybridSearchTriple { .. }
        | TextOp::SetTextConfig { .. } => None,
    };
    Ok(appended)
}
