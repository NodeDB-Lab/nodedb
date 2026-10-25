// SPDX-License-Identifier: BUSL-1.1

//! Top-level plan-shaped routing for the all-local-cores fan primitive, plus
//! the two generic gather variants shared by every plan that doesn't need a
//! bespoke single-blob merge (see `snapshot.rs`, `bsp.rs`, `wcc.rs`).

use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId};
use nodedb_physical::physical_plan::{GraphOp, MetaOp, PhysicalPlan};

use super::bsp::fan_bsp_all_cores;
use super::fanout::gather_every_core;
use super::snapshot::fan_tenant_snapshot_all_cores;
use super::wcc::fan_wcc_all_cores;

/// The canonical node-level result of fanning a plan across all local cores
/// and merging into the SAME payload shape a single core produces.
pub struct NodeLevelResult {
    pub payload: Vec<u8>,
    pub watermark_lsn: Lsn,
    /// The read versions the fanned read observed, one per vShard. Empty for
    /// non-read results. Distinct from `watermark_lsn`: the comparand for
    /// cross-shard OCC read validation.
    pub read_versions: crate::types::ReadVersions,
    /// The plan's one owning core refused it with `NotFound`: a read that
    /// found no row. `read_versions` holds the versions it observed.
    pub not_found: bool,
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
            GraphOp::Match { scope, .. }
            | GraphOp::MatchContinuation { scope, .. }
            | GraphOp::MatchVarLenResume { scope, .. } => {
                let scope = *scope;
                use crate::control::server::graph_dispatch::match_broadcast::broadcast_match_to_all_cores;
                use crate::data::executor::handlers::graph_match::encode_match_envelope_raw;

                // Forwarded `txn_id` resolves the staged overlay once present on this node.
                // Not linearizable here: the callers confirm before they fan out.
                // A leg received from another node was confirmed by
                // `exec_receiver::read_leg` when it carried `read_groups`, and the
                // graph superstep path confirms in `cluster_resolve`.
                let outcome = broadcast_match_to_all_cores(
                    state,
                    tenant_id,
                    database_id,
                    plan,
                    trace_id,
                    crate::control::server::graph_dispatch::GraphRead {
                        txn_id,
                        linearizable: false,
                        scope,
                    },
                )
                .await?;

                // Carries truncation resume cursor(s) so a remote shard's truncation
                // reaches the coordinator instead of being silently dropped.
                let envelope = encode_match_envelope_raw(
                    outcome.rows_payload.as_ref(),
                    &outcome.frontier,
                    &outcome.resume,
                )?;

                // The served watermark rides back so the coordinator can put
                // the read on a transaction's read-set at its true version.
                Ok(NodeLevelResult {
                    payload: envelope,
                    watermark_lsn: outcome.watermark_lsn,
                    read_versions: outcome.read_versions,
                    not_found: false,
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
            | GraphOp::Stats { .. }
            | GraphOp::NodeEdgeGuard { .. }
            | GraphOp::NodePresenceGuard { .. }
            | GraphOp::TruncateEdges { .. }
            | GraphOp::NodePresenceRead { .. } => {
                generic_gather(state, tenant_id, database_id, plan, trace_id, txn_id).await
            }
        },

        // Most Meta ops return row arrays (generic gather); per-node snapshot ops
        // return one opaque per-node blob and must not be array-wrapped.
        PhysicalPlan::Meta(meta) => match meta {
            // Single `TenantDataSnapshot` map per core — the row gather will prepend a
            // fixarray header, breaking restore's decode.
            MetaOp::CreateTenantSnapshot { .. } => {
                fan_tenant_snapshot_all_cores(state, tenant_id, database_id, plan, trace_id).await
            }
            // One JSON result object from the core that restores the snapshot.
            MetaOp::RestoreTenantSnapshot { .. } => {
                let op = "RestoreTenantSnapshot";
                single_blob_gather(state, tenant_id, database_id, plan, trace_id, op).await
            }
            // One `RedoRecord` blob from the core that holds Calvin's staging state.
            MetaOp::CalvinResolve { .. } => {
                let op = "CalvinResolve";
                single_blob_gather(state, tenant_id, database_id, plan, trace_id, op).await
            }
            // A transaction meta-op runs on the one core that holds the
            // transaction's staging overlay (`execute_received_plan`). Fanned to
            // every core, it stages, resolves, or applies on each of them.
            MetaOp::StageWrite { .. } => Err(fanned_scoped_plan("StageWrite")),
            MetaOp::DropTxnOverlay { .. } => Err(fanned_scoped_plan("DropTxnOverlay")),
            MetaOp::ResolveTxn { .. } => Err(fanned_scoped_plan("ResolveTxn")),
            MetaOp::MarkSavepoint { .. } => Err(fanned_scoped_plan("MarkSavepoint")),
            MetaOp::RollbackToSavepoint { .. } => Err(fanned_scoped_plan("RollbackToSavepoint")),
            MetaOp::TransactionBatch { .. } => Err(fanned_scoped_plan("TransactionBatch")),
            MetaOp::ApplyTransactionRedo { .. } => Err(fanned_scoped_plan("ApplyTransactionRedo")),
            MetaOp::RestoreRedo(_) => Err(fanned_scoped_plan("RestoreRedo")),
            MetaOp::PurgeDepartedVShards { .. } => Err(crate::Error::Internal {
                detail: "PurgeDepartedVShards reached the all-core fan; the departure purge \
                         sends it to every core itself"
                    .into(),
            }),
            // One image per core, kept apart by core id. A merged payload
            // loses which core each image belongs to.
            // Each core answers the probes of the vShards it owns.
            MetaOp::HomeVersions { .. } => {
                super::home_versions::fan_home_versions(
                    state,
                    tenant_id,
                    database_id,
                    plan,
                    trace_id,
                )
                .await
            }
            MetaOp::CreateSnapshot => Err(crate::Error::Internal {
                detail: "CreateSnapshot reached the all-core fan; it runs through \
                         capture_base_on_local_cores"
                    .into(),
            }),
            // A refresh wait runs on the core that owns its root source's
            // vShard: only that core applies the cut it waits for.
            MetaOp::AwaitContinuousAggregateRefresh { collection, .. } => {
                let target = OwningCoreTarget {
                    tenant_id,
                    database_id,
                    vshard_id: nodedb_types::CollectionKey::from_qualified_str(
                        database_id,
                        collection.as_str(),
                    )?
                    .vshard(),
                    trace_id,
                    txn_id,
                };
                owning_core_result(state, target, plan).await
            }
            // Enumerated exhaustively (no `_ =>`) so a new single-blob MetaOp forces a decision.
            MetaOp::WalAppend { .. }
            | MetaOp::Cancel { .. }
            | MetaOp::Compact
            | MetaOp::Checkpoint
            | MetaOp::RegisterContinuousAggregate { .. }
            | MetaOp::UnregisterContinuousAggregate { .. }
            | MetaOp::ListContinuousAggregates
            | MetaOp::ContinuousAggregatesRegistered
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
            | MetaOp::VerifyHashChain { .. }
            | MetaOp::CalvinExecuteStatic { .. }
            | MetaOp::CalvinExecutePassive { .. }
            | MetaOp::CalvinExecuteActive { .. }
            | MetaOp::RebuildIndex { .. }
            | MetaOp::PutSynonymGroup { .. }
            | MetaOp::DeleteSynonymGroup { .. }
            | MetaOp::SetCustomCrsSet { .. }
            | MetaOp::CalvinDrop { .. } => {
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
/// Every other plan fans across all local cores and answers in the shape a
/// single core produces (`GatherOutcome::single_core_shape`): one payload as
/// is, several merged as row arrays. Two or more payloads that are not row
/// arrays fail the merge.
async fn generic_gather(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<NodeLevelResult> {
    use crate::control::server::exchange::gather::gather_all_cores;

    // Forwarded `txn_id`, if any, is stamped on each core's request so a
    // transactional read honours its staged overlay. Inert when `None`.
    if !nodedb_physical::physical_plan::plan_contains_cluster_partitioned_leaf(&plan)
        && let Some(collection) = plan.collection()
    {
        let target = OwningCoreTarget {
            tenant_id,
            database_id,
            vshard_id: nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?
                .vshard(),
            trace_id,
            txn_id,
        };
        return owning_core_result(state, target, plan).await;
    }
    let outcome = gather_all_cores(state, tenant_id, database_id, plan, trace_id, txn_id).await?;
    Ok(NodeLevelResult {
        payload: outcome.single_core_shape()?,
        watermark_lsn: outcome.watermark_lsn,
        read_versions: outcome.read_versions,
        not_found: false,
    })
}

/// The one core a plan runs on, and the request scope it runs under.
struct OwningCoreTarget {
    tenant_id: TenantId,
    database_id: DatabaseId,
    /// The vShard whose owning core runs the plan.
    vshard_id: crate::types::VShardId,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
}

/// Run `plan` on the core that owns `target.vshard_id`, and return its
/// payload in the shape that core produced.
async fn owning_core_result(
    state: &SharedState,
    target: OwningCoreTarget,
    plan: PhysicalPlan,
) -> crate::Result<NodeLevelResult> {
    use crate::control::server::exchange::owning_core::dispatch_single_owning_core;

    let OwningCoreTarget {
        tenant_id,
        database_id,
        vshard_id,
        trace_id,
        txn_id,
    } = target;
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
    Ok(NodeLevelResult {
        not_found: crate::control::local_dispatch::is_not_found(&resp),
        payload: resp.payload.to_vec(),
        watermark_lsn: resp.watermark_lsn,
        read_versions: resp.read_versions,
    })
}

/// The error for a vShard-scoped transaction meta-op that reached the
/// all-core fan instead of its one owning core.
fn fanned_scoped_plan(op: &'static str) -> crate::Error {
    crate::Error::Internal {
        detail: format!("{op} reached the all-core fan; it runs on its one owning core"),
    }
}

/// Single-blob gather: fan `plan` across all local cores and return the one
/// core's payload verbatim, with no row array-wrap.
///
/// The row gather will prepend a msgpack array header and corrupt a Meta op's
/// opaque blob. Every core must answer, and at most one answers with a
/// payload. `op` names the plan in the error when a second core does.
async fn single_blob_gather(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    op: &'static str,
) -> crate::Result<NodeLevelResult> {
    let responses =
        gather_every_core(state, tenant_id, database_id, plan, trace_id, "single-blob").await?;

    let mut watermark_lsn = Lsn::ZERO;
    let mut read_versions = crate::types::ReadVersions::new();
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(responses.len());
    for resp in responses {
        if resp.watermark_lsn > watermark_lsn {
            watermark_lsn = resp.watermark_lsn;
        }
        read_versions.merge(&resp.read_versions);
        payloads.push(resp.payload.as_ref().to_vec());
    }

    Ok(NodeLevelResult {
        payload: pick_single_blob(payloads, op)?,
        watermark_lsn,
        read_versions,
        not_found: false,
    })
}

/// Return the one non-empty payload, in core order, or an empty one when no
/// core answered with a payload. A second non-empty payload is an invariant
/// break named by `op`.
fn pick_single_blob(payloads: Vec<Vec<u8>>, op: &'static str) -> crate::Result<Vec<u8>> {
    let mut non_empty = payloads.into_iter().filter(|p| !p.is_empty());
    let first = non_empty.next().unwrap_or_default();
    let extra = non_empty.count();
    if extra == 0 {
        return Ok(first);
    }
    Err(crate::Error::Internal {
        detail: format!(
            "{op}: {} local cores answered with a payload, expected at most one",
            extra + 1
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lone_payload_is_returned() {
        let picked = pick_single_blob(
            vec![Vec::new(), b"redo".to_vec(), Vec::new()],
            "CalvinResolve",
        )
        .unwrap();
        assert_eq!(picked, b"redo".to_vec());
    }

    #[test]
    fn no_payload_is_empty() {
        let picked = pick_single_blob(vec![Vec::new(), Vec::new()], "CalvinResolve").unwrap();
        assert!(picked.is_empty());
    }

    #[test]
    fn a_second_payload_breaks_the_one_owner_invariant() {
        match pick_single_blob(
            vec![b"a".to_vec(), Vec::new(), b"b".to_vec()],
            "RestoreTenantSnapshot",
        ) {
            Err(crate::Error::Internal { detail }) => {
                assert!(detail.contains("RestoreTenantSnapshot"), "{detail}");
                assert!(detail.contains("2 local cores"), "{detail}");
            }
            other => panic!("expected an invariant error, got {other:?}"),
        }
    }

    #[test]
    fn a_fanned_scoped_plan_names_the_op() {
        match fanned_scoped_plan("StageWrite") {
            crate::Error::Internal { detail } => assert!(detail.contains("StageWrite")),
            other => panic!("expected an internal error, got {other:?}"),
        }
    }
}
