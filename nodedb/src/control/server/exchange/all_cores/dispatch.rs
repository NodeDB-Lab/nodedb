// SPDX-License-Identifier: BUSL-1.1

//! Top-level plan-shaped routing for the all-local-cores fan primitive, plus
//! the two generic gather variants shared by every plan that doesn't need a
//! bespoke single-blob merge (see `snapshot.rs`, `bsp.rs`, `wcc.rs`).

use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId};
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
    /// Max per-collection read-version LSN of the fanned read. `Lsn::ZERO` for
    /// non-read results. Distinct from `watermark_lsn` — the comparand for
    /// cross-shard OCC read validation.
    pub read_version_lsn: Lsn,
}

/// Fan `plan` across all local Data-Plane cores, merge per-core payloads, and
/// return a [`NodeLevelResult`] in the same shape a single-core handler produces.
///
/// `txn_id` stamps each core's request to resolve the staged overlay; `None` for
/// autocommit. Graph-analytics fan-out is not transaction-scoped.
pub(crate) async fn execute_plan_all_local_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<NodeLevelResult> {
    match &plan {
        PhysicalPlan::Graph(g) => match g {
            // ── MATCH / MatchContinuation ─────────────────────────────────────
            GraphOp::Match { .. }
            | GraphOp::MatchContinuation { .. }
            | GraphOp::MatchVarLenResume { .. } => {
                use crate::control::server::graph_dispatch::match_broadcast::broadcast_match_to_all_cores;
                use crate::data::executor::handlers::graph_match::encode_match_envelope_raw;

                // Forwarded `txn_id` resolves the staged overlay once present on this node.
                let outcome = broadcast_match_to_all_cores(
                    state,
                    tenant_id,
                    database_id,
                    plan,
                    trace_id,
                    txn_id,
                )
                .await?;

                // Carries truncation resume cursor(s) so a remote shard's truncation
                // reaches the coordinator instead of being silently dropped.
                let envelope = encode_match_envelope_raw(
                    outcome.rows_payload.as_ref(),
                    &outcome.frontier,
                    &outcome.resume,
                )?;

                Ok(NodeLevelResult {
                    payload: envelope,
                    watermark_lsn: Lsn::ZERO,
                    read_version_lsn: Lsn::ZERO,
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
            | GraphOp::ResolveEdgeDelete(_)
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

        // Most Meta ops return row arrays (generic gather); per-node snapshot ops
        // return one opaque per-node blob and must not be array-wrapped.
        PhysicalPlan::Meta(meta) => match meta {
            // Single `TenantDataSnapshot` map per core — the row gather would prepend a
            // fixarray header, breaking restore's decode.
            MetaOp::CreateTenantSnapshot { .. } => {
                fan_tenant_snapshot_all_cores(state, tenant_id, database_id, plan, trace_id).await
            }
            // Single JSON result object, not an array — same single-blob corruption
            // class; return the lone core's payload verbatim.
            MetaOp::RestoreTenantSnapshot { .. } => {
                single_blob_gather(state, tenant_id, database_id, plan, trace_id, None).await
            }
            // Single `RedoRecord` blob, never actually fans across cores, but routed
            // through `single_blob_gather` so the payload returns verbatim.
            MetaOp::ResolveTxn { .. } => {
                single_blob_gather(state, tenant_id, database_id, plan, trace_id, None).await
            }
            // Same single `RedoRecord` blob shape as `ResolveTxn` (reuses it internally).
            MetaOp::CalvinResolve { .. } => {
                single_blob_gather(state, tenant_id, database_id, plan, trace_id, None).await
            }

            // Row gather would array-wrap the affected-count blob, corrupting extraction.
            MetaOp::StageWrite { .. } | MetaOp::DropTxnOverlay { .. } => {
                single_blob_gather(state, tenant_id, database_id, plan, trace_id, txn_id).await
            }
            // Enumerated exhaustively (no `_ =>`) so a new single-blob MetaOp forces a decision.
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
            | MetaOp::MarkSavepoint { .. }
            | MetaOp::RollbackToSavepoint { .. }
            | MetaOp::RecordCalvinWriteVersions { .. }
            | MetaOp::CalvinFlush { .. }
            | MetaOp::CalvinDrop { .. }
            | MetaOp::ApplyTransactionRedo { .. } => {
                generic_gather(state, tenant_id, database_id, plan, trace_id, txn_id).await
            }
        },

        PhysicalPlan::ClusterEvent(_) => Err(crate::Error::Internal {
            detail: "ClusterEvent plan must execute on the receiving Control Plane".into(),
        }),

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

/// Generic gather path.
///
/// A plan on one collection that is not cluster-partitioned lives wholly on
/// the core that owns the collection's vShard. It runs there alone, and its
/// payload returns verbatim: the shape a single core produces. That shape is
/// not always a msgpack array. A KV point read answers with the stored value
/// itself, and wrapping it as an array element hands the requesting node a
/// different value than a local read returns.
///
/// Every other plan fans across all local cores, and their row arrays merge
/// into one.
async fn generic_gather(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<NodeLevelResult> {
    use crate::control::server::exchange::gather::gather_all_cores;
    use crate::control::server::exchange::owning_core::dispatch_single_owning_core;

    // Forwarded `txn_id`, if any, is stamped on each core's request so a
    // transactional read honours its staged overlay. Inert when `None`.
    if !nodedb_physical::physical_plan::plan_contains_cluster_partitioned_leaf(&plan)
        && let Some(collection) = plan.collection()
    {
        let vshard_id =
            crate::types::VShardId::from_collection_in_database(database_id, collection);
        let resp = dispatch_single_owning_core(
            state,
            tenant_id,
            database_id,
            plan,
            vshard_id,
            trace_id,
            txn_id,
        )
        .await?;
        return Ok(NodeLevelResult {
            payload: resp.payload.to_vec(),
            watermark_lsn: resp.watermark_lsn,
            read_version_lsn: resp.read_version_lsn,
        });
    }
    let outcome = gather_all_cores(state, tenant_id, database_id, plan, trace_id, txn_id).await?;
    Ok(NodeLevelResult {
        payload: outcome.merged_array,
        watermark_lsn: outcome.watermark_lsn,
        read_version_lsn: outcome.read_version_lsn,
    })
}

/// Single-blob gather: fan `plan` across all local cores but return the lone
/// non-empty core's payload verbatim, with no row array-wrap.
///
/// The row gather would prepend a msgpack array header and corrupt a Meta op's
/// opaque blob. If more than one core returns non-empty, the first is kept.
async fn single_blob_gather(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<NodeLevelResult> {
    let responses = gather_graph_op_all_cores(
        state,
        tenant_id,
        database_id,
        plan,
        trace_id,
        txn_id,
        "single-blob",
    )
    .await?;

    let mut watermark_lsn = Lsn::ZERO;
    let mut read_version_lsn = Lsn::ZERO;
    let mut payload: Option<Vec<u8>> = None;
    for resp in responses {
        if resp.watermark_lsn > watermark_lsn {
            watermark_lsn = resp.watermark_lsn;
        }
        if resp.read_version_lsn > read_version_lsn {
            read_version_lsn = resp.read_version_lsn;
        }
        if payload.is_none() && !resp.payload.is_empty() {
            payload = Some(resp.payload.as_ref().to_vec());
        }
    }

    Ok(NodeLevelResult {
        payload: payload.unwrap_or_default(),
        watermark_lsn,
        read_version_lsn,
    })
}
