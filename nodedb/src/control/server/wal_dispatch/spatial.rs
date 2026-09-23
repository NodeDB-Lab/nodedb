// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Spatial(SpatialOp)`.

use nodedb_physical::physical_plan::SpatialOp;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;

use super::super::wal_dispatch_fts_spatial;

/// Append the WAL record for a single `SpatialOp`, returning the allocated
/// LSN for the write variants (`Some`) or `None` for `Scan`, which carries no
/// durable per-write effect.
///
/// `Insert` / `Delete` are handled here so any call site that reaches
/// [`super::wal_append_if_write_with_creds`] with one of these variants is
/// durable by construction. The sync-inbound handler
/// (`sync/spatial_handler.rs`) already calls `wal_append_spatial_put` /
/// `wal_append_spatial_delete` directly and dispatches straight to the Data
/// Plane via `dispatch_sync_payload` — it never reaches this function, so
/// this arm cannot double-append on that path today (mirrors
/// `VectorOp::DeleteBySurrogate`'s identical "sync path bypasses it, but log
/// here too" reasoning in `wal_dispatch/vector.rs`).
pub(crate) fn wal_append_spatial_op(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &SpatialOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        SpatialOp::Insert {
            collection,
            field,
            surrogate,
            geometry,
            provenance,
        } => {
            let prov = provenance.clone().unwrap_or_default();
            let payload = wal_dispatch_fts_spatial::encode_spatial_put_payload(
                collection.as_str(),
                field,
                *surrogate,
                geometry,
                &prov,
            )?;
            Some(wal_dispatch_fts_spatial::wal_append_spatial_put(
                wal,
                tenant_id,
                vshard_id,
                database_id,
                &payload,
            )?)
        }
        SpatialOp::Delete {
            collection,
            field,
            surrogate,
            provenance,
        } => {
            let prov = provenance.clone().unwrap_or_default();
            let payload = wal_dispatch_fts_spatial::encode_spatial_delete_payload(
                collection.as_str(),
                field,
                *surrogate,
                &prov,
            );
            Some(wal_dispatch_fts_spatial::wal_append_spatial_delete(
                wal,
                tenant_id,
                vshard_id,
                database_id,
                &payload,
            )?)
        }
        // R-tree read: no durable effect.
        SpatialOp::Scan { .. } => None,
    };
    Ok(appended)
}
