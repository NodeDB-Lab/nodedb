// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Text(TextOp)`.

use nodedb_physical::physical_plan::TextOp;
use nodedb_wal::record::RecordType;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;

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
    let Some((record_type, payload)) = encode_text_op_record(op)? else {
        return Ok(None);
    };
    let lsn = if record_type == RecordType::FtsIndex {
        wal.append_fts_index(tenant_id, vshard_id, database_id, &payload)?
    } else {
        wal.append_fts_delete(tenant_id, vshard_id, database_id, &payload)?
    };
    Ok(Some(lsn))
}

/// Encode the WAL record a single `TextOp` write journals as: its record type
/// (`FtsIndex` or `FtsDelete`) and payload. `None` for every read / search /
/// analyzer-config variant. Shared by the autocommit WAL append and the
/// transaction resolver, so an FTS write inside a transaction journals the
/// exact record its autocommit form does.
pub(crate) fn encode_text_op_record(op: &TextOp) -> crate::Result<Option<(RecordType, Vec<u8>)>> {
    let encoded = match op {
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
            Some((
                RecordType::FtsIndex,
                payload.to_bytes().map_err(crate::Error::Wal)?,
            ))
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
            Some((
                RecordType::FtsDelete,
                payload.to_bytes().map_err(crate::Error::Wal)?,
            ))
        }
        // Reads / scans / analyzer config: no durable effect.
        TextOp::Search { .. }
        | TextOp::BM25ScoreScan { .. }
        | TextOp::PhraseSearch { .. }
        | TextOp::HybridSearch { .. }
        | TextOp::HybridSearchTriple { .. }
        | TextOp::SetTextConfig { .. } => None,
    };
    Ok(encoded)
}
