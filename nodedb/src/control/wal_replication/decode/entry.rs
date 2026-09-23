// SPDX-License-Identifier: BUSL-1.1

//! Entry point: decode a committed `ReplicatedEntry` into a `PhysicalPlan`.
//!
//! `to_physical_plan` groups variants by `PhysicalPlan` family and delegates to
//! a per-engine `decode_arm`, exhaustively. Shared helpers live in [`super::ctx`].

use super::super::types::{ReplicatedEntry, ReplicatedWrite};
use super::ctx::DecodeCtx;
use super::{
    entry_array, entry_columnar_family, entry_crdt, entry_document, entry_graph, entry_kv, vector,
};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::surrogate::{SurrogateAssigner, bind_plan_identities};
use crate::types::{DatabaseId, TenantId, VShardId};

/// Decoded `(tenant, vshard, plan, resolved_now_ms)` for a committed entry.
/// `resolved_now_ms` is the instant the proposer read for the write, stamped
/// onto the request so every replica installs the same value: a TTL-bearing
/// KV write's `expire_at_ms`, and a timeseries ingest's untimed rows. `None`
/// for every other write.
pub type DecodedEntry = (TenantId, VShardId, PhysicalPlan, Option<u64>);

/// Returns `None` if the data is not a valid ReplicatedEntry (e.g., ConfChange or no-op).
/// `assigner`, when `Some`, installs every carried surrogate through
/// `bind_plan_identities`, never re-allocating — except a pre-surrogate CRDT
/// apply, which allocates loudly.
pub fn from_replicated_entry(
    data: &[u8],
    assigner: Option<&SurrogateAssigner>,
) -> crate::Result<Option<DecodedEntry>> {
    let entry = match ReplicatedEntry::from_bytes(data) {
        Some(e) => e,
        None => return Ok(None),
    };
    // Array CRDT variants are handled by the distributed applier before this call.
    match &entry.write {
        ReplicatedWrite::ArrayOp { .. } | ReplicatedWrite::ArraySchema { .. } => {
            return Ok(None);
        }
        _ => {}
    }
    let tenant_id = TenantId::new(entry.tenant_id);
    // `0` decodes to `DatabaseId::DEFAULT` (see `LegacyReplicatedEntry`).
    let database_id = DatabaseId::new(entry.database_id);
    let ctx = DecodeCtx {
        database_id,
        tenant_id,
    };
    let (mut plan, resolved_now_ms) = to_physical_plan(&entry.write, &ctx)?;
    if let Some(assigner) = assigner {
        bind_plan_identities(assigner, database_id, tenant_id, &mut plan)?;
    }
    Ok(Some((
        tenant_id,
        VShardId::new(entry.vshard_id),
        plan,
        resolved_now_ms,
    )))
}

/// Convert a ReplicatedWrite back into a PhysicalPlan, alongside `resolved_now_ms`
/// (see [`DecodedEntry`]).
fn to_physical_plan(
    write: &ReplicatedWrite,
    ctx: &DecodeCtx,
) -> crate::Result<(PhysicalPlan, Option<u64>)> {
    match write {
        // Document family (`PhysicalPlan::Document`).
        ReplicatedWrite::PointPut { .. }
        | ReplicatedWrite::PointInsert { .. }
        | ReplicatedWrite::PointDelete { .. }
        | ReplicatedWrite::PointUpdate { .. }
        | ReplicatedWrite::DocUpsert { .. }
        | ReplicatedWrite::DocBatchInsert { .. }
        | ReplicatedWrite::DocTruncate { .. }
        | ReplicatedWrite::BulkDml { .. }
        | ReplicatedWrite::InsertSelect { .. }
        | ReplicatedWrite::ApplyBalanceDelta { .. }
        | ReplicatedWrite::DocumentResolvedWrite { .. }
        | ReplicatedWrite::MergeApply { .. }
        | ReplicatedWrite::UpdateFromJoinApply { .. } => {
            Ok((entry_document::decode_arm(write)?, None))
        }
        // The full `Vector*` variant family dispatches to one helper — see
        // `vector::decode_arm`'s doc.
        ReplicatedWrite::VectorInsert { .. }
        | ReplicatedWrite::VectorBatchInsert { .. }
        | ReplicatedWrite::VectorDelete { .. }
        | ReplicatedWrite::SetVectorParams { .. }
        | ReplicatedWrite::DropVectorIndex { .. }
        | ReplicatedWrite::SparseInsert { .. }
        | ReplicatedWrite::SparseDelete { .. }
        | ReplicatedWrite::MultiVectorInsert { .. }
        | ReplicatedWrite::MultiVectorDelete { .. }
        | ReplicatedWrite::DeleteBySurrogate { .. }
        | ReplicatedWrite::DirectUpsert { .. }
        | ReplicatedWrite::VectorDirectDelete { .. }
        | ReplicatedWrite::VectorDirectTruncate { .. }
        | ReplicatedWrite::VectorDirectUpdate { .. }
        | ReplicatedWrite::VectorResolvedDirectWrite { .. } => {
            Ok((vector::decode_arm(write)?, None))
        }
        // CRDT family (`PhysicalPlan::Crdt`).
        ReplicatedWrite::CrdtApply { .. }
        | ReplicatedWrite::CrdtApplyFenced { .. }
        | ReplicatedWrite::CrdtApplyAuthenticated { .. }
        | ReplicatedWrite::CrdtImportCollection { .. }
        | ReplicatedWrite::CrdtListInsert { .. }
        | ReplicatedWrite::CrdtListDelete { .. }
        | ReplicatedWrite::CrdtListMove { .. }
        | ReplicatedWrite::CrdtDocUpsert { .. }
        | ReplicatedWrite::CrdtDocDelete { .. }
        | ReplicatedWrite::ConstraintChange { .. } => Ok((entry_crdt::decode_arm(write)?, None)),
        // Graph family (`PhysicalPlan::Graph`).
        ReplicatedWrite::EdgePut { .. }
        | ReplicatedWrite::EdgeDelete { .. }
        | ReplicatedWrite::SetNodeLabels { .. }
        | ReplicatedWrite::RemoveNodeLabels { .. }
        | ReplicatedWrite::EdgePutBatch { .. }
        | ReplicatedWrite::EdgeDeleteBatch { .. } => Ok((entry_graph::decode_arm(write)?, None)),
        // KV family: a TTL-bearing write carries `resolved_now_ms`.
        ReplicatedWrite::KvTruncate { .. }
        | ReplicatedWrite::KvPut { .. }
        | ReplicatedWrite::KvDelete { .. }
        | ReplicatedWrite::KvInsert { .. }
        | ReplicatedWrite::KvInsertIfAbsent { .. }
        | ReplicatedWrite::KvInsertOnConflictUpdate { .. }
        | ReplicatedWrite::KvBatchPut { .. }
        | ReplicatedWrite::KvExpire { .. }
        | ReplicatedWrite::KvPersist { .. }
        | ReplicatedWrite::KvIncr { .. }
        | ReplicatedWrite::KvIncrFloat { .. }
        | ReplicatedWrite::KvCas { .. }
        | ReplicatedWrite::KvGetSet { .. }
        | ReplicatedWrite::KvRegisterSortedIndex { .. }
        | ReplicatedWrite::KvDropSortedIndex { .. }
        | ReplicatedWrite::KvRegisterIndex { .. }
        | ReplicatedWrite::KvDropIndex { .. }
        | ReplicatedWrite::KvFieldSet { .. }
        | ReplicatedWrite::KvTransfer { .. }
        | ReplicatedWrite::KvTransferItem { .. }
        | ReplicatedWrite::KvResolvedWrite { .. }
        | ReplicatedWrite::KvPredicateUpdate { .. }
        | ReplicatedWrite::KvPredicateDelete { .. } => entry_kv::decode_arm(write),
        // A timeseries ingest carries the proposer's instant for its untimed
        // rows, installed on every replica as `resolved_now_ms`.
        ReplicatedWrite::TimeseriesIngest {
            default_timestamp_ms,
            ..
        } => Ok((
            entry_columnar_family::decode_arm(write)?,
            u64::try_from(*default_timestamp_ms).ok(),
        )),
        // Columnar-storage family + overlay sync engines.
        ReplicatedWrite::ColumnarIngest { .. }
        | ReplicatedWrite::FtsIndex { .. }
        | ReplicatedWrite::FtsDelete { .. }
        | ReplicatedWrite::SpatialInsert { .. }
        | ReplicatedWrite::SpatialDelete { .. }
        | ReplicatedWrite::ColumnarBulkDml { .. }
        | ReplicatedWrite::ColumnarBulkDmlResolved { .. }
        | ReplicatedWrite::ColumnarTruncate { .. }
        | ReplicatedWrite::TimeseriesTruncate { .. } => {
            Ok((entry_columnar_family::decode_arm(write)?, None))
        }
        // Raft-native array cell writes — the cluster SQL DML array path.
        // Distinct from the Lite-sync `ArrayOp` CRDT variant, intercepted below.
        ReplicatedWrite::ArrayCellPut { .. } | ReplicatedWrite::ArrayCellDelete { .. } => {
            Ok((entry_array::decode_arm(ctx, write)?, None))
        }
        // Intercepted upstream, never dispatched here; these arms exist only for exhaustiveness.
        ReplicatedWrite::ArrayOp { .. } => Err(crate::Error::Internal {
            detail: "ArrayOp reached to_physical_plan (should have been intercepted)".into(),
        }),
        ReplicatedWrite::ArraySchema { .. } => Err(crate::Error::Internal {
            detail: "ArraySchema reached to_physical_plan (should have been intercepted)".into(),
        }),
        ReplicatedWrite::CalvinReadResult { .. } => Err(crate::Error::Internal {
            detail: "CalvinReadResult reached to_physical_plan (should have been intercepted)"
                .into(),
        }),
        // The apply loop applies these through `transaction_redo`, which stamps
        // the redo with the entry's Raft coordinates.
        ReplicatedWrite::TransactionRedo { .. } => Err(crate::Error::Internal {
            detail: "TransactionRedo reached to_physical_plan (should have been intercepted)"
                .into(),
        }),
    }
}
