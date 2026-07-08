// SPDX-License-Identifier: BUSL-1.1

//! Top-level plan-shaped routing for the all-local-cores fan primitive, plus
//! the two generic gather variants shared by every plan that doesn't need a
//! bespoke single-blob merge (see `snapshot.rs`, `bsp.rs`, `wcc.rs`).

use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId};
use nodedb_physical::physical_plan::{GraphOp, MetaOp, PhysicalPlan};

use super::bsp::fan_bsp_all_cores;
use super::fanout::gather_graph_op_all_cores;
use super::snapshot::fan_tenant_snapshot_all_cores;
use super::wcc::fan_wcc_all_cores;

/// The canonical node-level result of fanning a plan across all local cores
/// and merging into the SAME payload shape a single core produces.
pub struct NodeLevelResult {
    pub payload: Vec<u8>,
    pub watermark_lsn: Lsn,
}

/// Fan `plan` across all local Data-Plane cores, merge per-core payloads, and
/// return a [`NodeLevelResult`] in the same shape the plan's single-core handler
/// produces.
///
/// Dispatch semantics are plan-dependent:
///
/// - **MATCH / MatchContinuation**: calls [`broadcast_match_to_all_cores`] and
///   re-encodes the `{rows, frontier}` envelope so the caller receives exactly
///   the shape a single-core MATCH handler returns.
/// - **BspSuperstep**: fans to all cores via the generic `gather_all_cores`
///   prologue, decodes each core's `BspSuperstepResult`, merges them by field
///   concatenation (owned-node sets are disjoint across cores), and re-encodes
///   the merged result.
/// - **Everything else**: delegates to [`gather_all_cores`] and wraps the
///   `merged_array` payload.
///
/// At 1 core/node every branch is behaviour-identical to the prior single-core
/// paths.
pub async fn execute_plan_all_local_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<NodeLevelResult> {
    match &plan {
        PhysicalPlan::Graph(g) => match g {
            // ── MATCH / MatchContinuation ─────────────────────────────────────
            GraphOp::Match { .. }
            | GraphOp::MatchContinuation { .. }
            | GraphOp::MatchVarLenResume { .. } => {
                use crate::control::server::graph_dispatch::match_broadcast::broadcast_match_to_all_cores;
                use crate::data::executor::handlers::graph_match::encode_match_envelope_raw;

                // Cross-node MATCH execution: this node received the plan from a
                // remote coordinator, so the transaction's staged overlay (which
                // lives on the origin node) is not reachable here — run
                // committed-CSR-only. Cross-node read-your-own-writes for MATCH
                // is a separate unit.
                let outcome = broadcast_match_to_all_cores(
                    state,
                    tenant_id,
                    database_id,
                    plan,
                    trace_id,
                    None,
                )
                .await?;

                // Carry the truncation resume cursor(s) onto the cross-node
                // wire inside the envelope bytes so a remote shard's truncation
                // lands in the coordinator instead of being silently dropped.
                let envelope = encode_match_envelope_raw(
                    outcome.rows_payload.as_ref(),
                    &outcome.frontier,
                    &outcome.resume,
                )?;

                Ok(NodeLevelResult {
                    payload: envelope,
                    watermark_lsn: Lsn::ZERO,
                })
            }

            // ── BspSuperstep ─────────────────────────────────────────────────
            GraphOp::BspSuperstep(_) => {
                fan_bsp_all_cores(state, tenant_id, database_id, plan, trace_id).await
            }

            // ── WccSuperstep ─────────────────────────────────────────────────
            GraphOp::WccSuperstep(_) => {
                fan_wcc_all_cores(state, tenant_id, database_id, plan, trace_id).await
            }

            // ── All other GraphOp variants → generic gather ───────────────────
            GraphOp::EdgePut { .. }
            | GraphOp::EdgePutBatch { .. }
            | GraphOp::EdgeDelete { .. }
            | GraphOp::EdgeDeleteBatch { .. }
            | GraphOp::Hop { .. }
            | GraphOp::Neighbors { .. }
            | GraphOp::NeighborsMulti { .. }
            | GraphOp::Path { .. }
            | GraphOp::Subgraph { .. }
            | GraphOp::RagFusion { .. }
            | GraphOp::Algo { .. }
            | GraphOp::SetNodeLabels { .. }
            | GraphOp::RemoveNodeLabels { .. }
            | GraphOp::TemporalNeighbors { .. }
            | GraphOp::TemporalAlgorithm { .. }
            | GraphOp::Stats { .. } => {
                generic_gather(state, tenant_id, database_id, plan, trace_id, txn_id).await
            }
        },

        // ── Meta ops: most return row arrays (→ generic gather); the
        // per-node snapshot ops return ONE opaque per-node blob and must NOT
        // be array-wrapped by the row gather/merge. ─────────────────────────
        PhysicalPlan::Meta(meta) => match meta {
            // A `CreateTenantSnapshot` response is a single `#[msgpack(map)]`
            // `TenantDataSnapshot` blob per core — NOT an array of rows. The
            // row gather (`encode_msgpack_array(extract_msgpack_elements(..))`)
            // would prepend a fixarray header to the map, corrupting the
            // section so restore's `from_msgpack::<TenantDataSnapshot>` fails.
            // Merge the per-core partial snapshots by typed field concatenation
            // (the same disjoint-per-core merge pattern as BSP/WCC) and return
            // one snapshot blob — identical in shape to the local
            // `snapshot_self`/`dispatch_async` path. At 1 core/node this is the
            // lone core's snapshot verbatim.
            MetaOp::CreateTenantSnapshot { .. } => {
                fan_tenant_snapshot_all_cores(state, tenant_id, database_id, plan, trace_id).await
            }
            // A `RestoreTenantSnapshot` response is a single JSON result object
            // (`{tenant_id, documents_restored, ...}`), not an array of rows.
            // Array-wrapping it is the same single-blob corruption class; return
            // the lone core's payload verbatim so the restore caller's
            // `success`-only check (and any future result inspection) sees the
            // unwrapped object.
            MetaOp::RestoreTenantSnapshot { .. } => {
                single_blob_gather(state, tenant_id, database_id, plan, trace_id).await
            }
            // Every other MetaOp either returns an array of rows / count payload
            // (→ generic gather is correct) or is a single-core control op whose
            // single-element wrap is harmless. Enumerated exhaustively (no
            // `_ =>`) so a NEW single-blob MetaOp forces a decision here.
            MetaOp::WalAppend { .. }
            | MetaOp::Cancel { .. }
            | MetaOp::TransactionBatch { .. }
            | MetaOp::CreateSnapshot
            | MetaOp::Compact
            | MetaOp::Checkpoint
            | MetaOp::RegisterContinuousAggregate { .. }
            | MetaOp::UnregisterContinuousAggregate { .. }
            | MetaOp::ListContinuousAggregates
            | MetaOp::ConvertCollection { .. }
            | MetaOp::PurgeTenant { .. }
            | MetaOp::UnregisterCollection { .. }
            | MetaOp::UnregisterMaterializedView { .. }
            | MetaOp::QueryCollectionSize { .. }
            | MetaOp::EnforceTimeseriesRetention { .. }
            | MetaOp::TemporalPurgeEdgeStore { .. }
            | MetaOp::TemporalPurgeDocumentStrict { .. }
            | MetaOp::TemporalPurgeColumnar { .. }
            | MetaOp::TemporalPurgeCrdt { .. }
            | MetaOp::TemporalPurgeArray { .. }
            | MetaOp::AlterArray { .. }
            | MetaOp::ApplyContinuousAggRetention
            | MetaOp::QueryAggregateWatermark { .. }
            | MetaOp::QueryLastValues { .. }
            | MetaOp::QueryLastValue { .. }
            | MetaOp::CalvinExecuteStatic { .. }
            | MetaOp::CalvinExecutePassive { .. }
            | MetaOp::CalvinExecuteActive { .. }
            | MetaOp::RebuildIndex { .. }
            | MetaOp::PutSynonymGroup { .. }
            | MetaOp::DeleteSynonymGroup { .. }
            | MetaOp::RenameCollection { .. }
            | MetaOp::StageWrite { .. }
            | MetaOp::DropTxnOverlay { .. }
            | MetaOp::MarkSavepoint { .. }
            | MetaOp::RollbackToSavepoint { .. } => {
                generic_gather(state, tenant_id, database_id, plan, trace_id, txn_id).await
            }
        },

        // ── All other PhysicalPlan variants → generic gather ──────────────────
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_) => {
            generic_gather(state, tenant_id, database_id, plan, trace_id, txn_id).await
        }
    }
}

/// Generic gather path: delegate to [`gather_all_cores`] and wrap.
async fn generic_gather(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<crate::types::TxnId>,
) -> crate::Result<NodeLevelResult> {
    use crate::control::server::exchange::gather::gather_all_cores;

    let outcome = gather_all_cores(state, tenant_id, database_id, plan, trace_id, txn_id).await?;
    Ok(NodeLevelResult {
        payload: outcome.merged_array,
        watermark_lsn: outcome.watermark_lsn,
    })
}

/// Single-blob gather: fan the plan across all local cores but return the lone
/// non-empty core's payload VERBATIM, with no row array-wrap.
///
/// For a Meta op that returns one opaque per-node blob (e.g. a JSON result
/// object), routing through the row gather would prepend a msgpack array header
/// and corrupt the blob. The local single-core dispatch path returns the bytes
/// unchanged; this mirrors that. At 1 core/node exactly one core responds and
/// its payload is returned as-is; if more than one core returns a non-empty
/// payload (a single-core control op fanned to many cores), the first is kept —
/// these ops produce an identical per-core acknowledgement, so any one is the
/// canonical node-level result.
async fn single_blob_gather(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<NodeLevelResult> {
    let responses =
        gather_graph_op_all_cores(state, tenant_id, database_id, plan, trace_id, "single-blob")
            .await?;

    let mut watermark_lsn = Lsn::ZERO;
    let mut payload: Option<Vec<u8>> = None;
    for resp in responses {
        if resp.watermark_lsn > watermark_lsn {
            watermark_lsn = resp.watermark_lsn;
        }
        if payload.is_none() && !resp.payload.is_empty() {
            payload = Some(resp.payload.as_ref().to_vec());
        }
    }

    Ok(NodeLevelResult {
        payload: payload.unwrap_or_default(),
        watermark_lsn,
    })
}
