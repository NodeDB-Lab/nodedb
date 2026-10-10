// SPDX-License-Identifier: BUSL-1.1

//! The reconnaissance scan behind the predicate-driven materialized-sum
//! resolution.
//!
//! A `BulkUpdate` / `BulkDelete` / `TRUNCATE` names its rows by PREDICATE, not
//! by body: at plan time the Control Plane holds no row to read a join key off.
//! It reads them the same way the OLLP dependent-predicate path predicts its
//! write set — one scan of the same predicate, before execution — and resolves
//! the join values that scan surfaces.
//!
//! A `PointUpdate` / `PointDelete` names ONE row and carries no body either — an
//! update carries field assignments, a delete carries only a key — so its join
//! key is likewise only readable from the stored row.
//! [`recon_point_row`] reads that one row through the SAME routing, so there is
//! one way to read a source row at plan time rather than two that can disagree
//! about where the collection lives.
//!
//! Like the OLLP pre-execution scan, the read is routed through the gateway: a
//! bare local dispatch on a coordinator that does not host the collection's
//! vShard returns nothing, which will silently under-resolve and leave the
//! write with no target to address.
//!
//! # Transaction view
//!
//! Inside a transaction block the read carries the transaction's id. The Data
//! Plane then reads the transaction's staging overlay over committed base, as
//! an in-transaction `SELECT` does. A statement's write applies on top of the
//! rows the transaction's earlier statements staged, so its images are read
//! there too. A base-only read misses a row the transaction inserted and
//! returns the base value of a row it rewrote. The delta settled from such an
//! image is wrong by that row's staged contribution.
//!
//! # Plane discipline
//!
//! Runs on the coordinator's Control Plane (Tokio). The scan goes through the
//! gateway exactly as a `SELECT` does — no storage I/O and no io_uring here.

use nodedb_types::{Surrogate, TenantId};

use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TraceId, TxnId};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

/// What a plan-time reconnaissance read observed, and the version it observed
/// it at.
///
/// The version travels with the rows because a delta settled from them is only
/// as good as the images it was folded from: the caller stamps it onto a
/// read-set entry so the Calvin OCC check aborts the statement if the source
/// rows moved between this read and the apply. Rows without their version will
/// be a silently stale total.
pub(crate) struct ReconRead<T> {
    /// The decoded rows.
    pub rows: T,
    /// The source collection's write floor at read time — the comparand
    /// cross-shard OCC validation checks the read against.
    pub read_version_lsn: Lsn,
    /// The node that served the read. `read_version_lsn` is a position in
    /// that node's WAL, so the commit vote compares it only on that node.
    /// `0` when no one node is known to have served it.
    pub served_by: u64,
}

/// Scan `collection` for the rows `filters` matches, returning each row's full
/// decoded document.
///
/// Whole documents rather than a projection: the join column of every binding
/// the collection drives has to be readable, and so does every column an
/// expression assignment to a join column evaluates over. A projection will
/// have to enumerate all of them and will silently drop a value the assignment
/// depends on.
///
/// Empty `filters` means "no WHERE clause" — every row, which is what `TRUNCATE`
/// needs.
///
/// `txn_id` is the open transaction the statement runs in, `None` outside a
/// transaction block. See the module's transaction view.
pub(in crate::control::planner) async fn recon_scan_rows(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    txn_id: Option<TxnId>,
    collection: &str,
    filters: Vec<u8>,
) -> crate::Result<ReconRead<Vec<serde_json::Value>>> {
    let scan_plan = PhysicalPlan::Document(DocumentOp::Scan {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        filters,
        limit: usize::MAX,
        offset: 0,
        sort_keys: vec![],
        distinct: false,
        projection: vec![],
        computed_columns: vec![],
        window_functions: vec![],
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    });

    let read = execute_read(state, tenant_id, database_id, txn_id, collection, scan_plan).await?;
    let mut rows = Vec::new();
    for payload in &read.rows {
        rows.extend(decode_rows(payload.as_slice()));
    }
    Ok(ReconRead {
        rows,
        read_version_lsn: read.read_version_lsn,
        served_by: read.served_by,
    })
}

/// Read the ONE stored row `surrogate` addresses, or `None` when no such row
/// exists.
///
/// The point-shaped counterpart of [`recon_scan_rows`], for the write plans that
/// name a single row and carry no body: `PointUpdate` and `PointDelete` read
/// their join key off this image, and `PointPut` / `Upsert` read off it the join
/// key the row is ABOUT to leave, which the submitted body cannot report.
///
/// `None` is the ordinary answer, not a failure: an upsert that inserts, and an
/// update or delete whose primary key matches nothing, all rewrite no stored row
/// and so owe no target anything.
///
/// Identity is the surrogate, exactly as on the write path — `document_id` is
/// the user-facing primary key and carries no storage addressing.
///
/// `txn_id` is the open transaction the statement runs in, `None` outside a
/// transaction block. A row this transaction staged reads as staged, and a row
/// it staged a delete of reads as absent.
pub(crate) async fn recon_point_row(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    txn_id: Option<TxnId>,
    collection: &str,
    document_id: &str,
    surrogate: Surrogate,
) -> crate::Result<ReconRead<Option<serde_json::Value>>> {
    let get_plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        document_id: document_id.to_owned(),
        surrogate: Some(surrogate),
        pk_bytes: document_id.as_bytes().to_vec(),
        // No RLS filters, for the same reason the recon scan carries none: this
        // read decides which TOTAL a write moves, not what a principal can see.
        // Filtering it will let a row the caller cannot read leave its
        // contribution stranded on a target forever.
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });

    let read = execute_read(state, tenant_id, database_id, txn_id, collection, get_plan).await?;
    // A point get answers with the row's normalized MessagePack body, and with
    // an EMPTY payload when the row is absent.
    Ok(ReconRead {
        rows: read
            .rows
            .iter()
            .find(|payload| !payload.is_empty())
            .and_then(|payload| nodedb_types::json_from_msgpack(payload.as_slice()).ok()),
        read_version_lsn: read.read_version_lsn,
        served_by: read.served_by,
    })
}

/// Run one read plan through the gateway, returning the raw payloads.
///
/// A bare local dispatch on a coordinator that does not host the collection's
/// vShard returns nothing, which will silently under-resolve and leave the
/// write with no target to address — so every plan-time read routes through
/// the gateway.
///
/// The gateway notes the node that served each vShard it read. The read
/// validates on `collection`'s vShard, so that vShard's note names the node
/// whose WAL numbers `read_version_lsn`.
///
/// `read_version_lsn` is the collection's COMMITTED write floor whether or not
/// `txn_id` is set: a staged write appends no WAL record and moves no write
/// version. So an entry stamped with it still detects a concurrent commit to
/// the rows read, and never this transaction's own staged writes.
async fn execute_read(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    txn_id: Option<TxnId>,
    collection: &str,
    plan: PhysicalPlan,
) -> crate::Result<ReconRead<Vec<Vec<u8>>>> {
    let validation_vshard =
        nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?
            .vshard()
            .as_u32();
    let gateway = state.installed_gateway()?;
    let gw_ctx = crate::control::gateway::core::QueryContext {
        tenant_id,
        trace_id: TraceId::ZERO,
        database_id,
        // The transaction's staging overlay over committed base, as an
        // in-transaction read sees it.
        txn_id,
        linearizable: true,
    };
    // A shard verdict keeps its own typed error.
    let (payloads, _watermarks, read_version_lsn) = gateway
        .execute_internal_with_watermarks(&gw_ctx, plan)
        .await?;
    Ok(ReconRead {
        rows: payloads,
        read_version_lsn,
        served_by: crate::control::server::shared::session::read_set::serving_node(
            state,
            validation_vshard,
        ),
    })
}

/// Decode a document-scan payload into one document per row.
///
/// `decode_raw_scan_to_docs` is the shared reader for BOTH shapes a document
/// scan can come back in — the `{id, data}` raw-passthrough wrapper and the
/// plain per-row map — so the shape is not re-guessed here. A row body that will
/// not decode carries no readable column, so it contributes no join value; it is
/// left to the write path, which fails on the same body rather than silently
/// mis-accounting it.
fn decode_rows(payload: &[u8]) -> Vec<serde_json::Value> {
    crate::data::executor::response_codec::decode_raw_scan_to_docs(payload)
        .into_iter()
        .filter_map(|(_, body)| nodedb_types::json_from_msgpack(&body).ok())
        .collect()
}
