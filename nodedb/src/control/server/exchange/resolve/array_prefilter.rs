// SPDX-License-Identifier: BUSL-1.1

//! Resolve a vector search's `ARRAY_SLICE` prefilter across every shard.
//!
//! The planner lowers the prefilter to an `ArrayOp::SurrogateBitmapScan` in
//! the search's `inline_prefilter_plan`. The search runs on the core that
//! owns its collection, and that core holds only its own buckets of the
//! array, so the inline scan would admit only the cells that core holds.
//! This pass reads the slice's surrogates from every shard through the
//! array coordinator, intersects them into the search's `filter_bitmap`,
//! and clears the inline plan. A search with no prefilter, and a node with
//! no cluster transport, keeps its plan as it is.
//!
//! Every shard reads at the scan's system-time scope, the scope of the
//! search, so the candidates and the ranked rows read one cut. The reading
//! transaction's id rides along so a shard merges the cells the
//! transaction staged there.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use nodedb_physical::physical_plan::{ArrayOp, ExchangeOp, PhysicalPlan, QueryOp, VectorOp};

use crate::control::cluster::ClusterArrayExecutor;
use crate::control::state::SharedState;
use crate::types::TxnId;

/// Replace every array prefilter of a vector search in `plan` with the
/// cross-shard surrogate bitmap of its slice. Walks the coordinator-local
/// wrappers a search can sit under. `txn_id` is the reading transaction,
/// `None` for an autocommit read.
pub(super) fn resolve_array_prefilters<'a>(
    state: &'a SharedState,
    txn_id: Option<TxnId>,
    plan: &'a mut PhysicalPlan,
) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match plan {
            PhysicalPlan::Vector(op) => resolve_search(state, txn_id, op).await?,
            PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, .. }))
            | PhysicalPlan::Query(QueryOp::PostProcess { input: child, .. })
            | PhysicalPlan::Query(QueryOp::LateralTopK {
                outer_plan: child, ..
            })
            | PhysicalPlan::Query(QueryOp::LateralLoop {
                outer_plan: child, ..
            }) => resolve_array_prefilters(state, txn_id, child).await?,
            PhysicalPlan::Query(
                QueryOp::Aggregate { input, .. } | QueryOp::PartialAggregateState { input, .. },
            ) => {
                if let Some(child) = input.as_deref_mut() {
                    resolve_array_prefilters(state, txn_id, child).await?;
                }
            }
            PhysicalPlan::Query(QueryOp::HashJoin {
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
                ..
            }) => {
                for side in [left_input, right_input, left_bitmap, right_bitmap] {
                    if let Some(child) = side.as_deref_mut() {
                        resolve_array_prefilters(state, txn_id, child).await?;
                    }
                }
            }
            PhysicalPlan::Query(
                QueryOp::NestedLoopJoin {
                    left_input,
                    right_input,
                    ..
                }
                | QueryOp::SortMergeJoin {
                    left_input,
                    right_input,
                    ..
                },
            ) => {
                for side in [left_input, right_input] {
                    if let Some(child) = side.as_deref_mut() {
                        resolve_array_prefilters(state, txn_id, child).await?;
                    }
                }
            }
            PhysicalPlan::Query(QueryOp::SetOp { inputs, .. }) => {
                for child in inputs.iter_mut() {
                    resolve_array_prefilters(state, txn_id, child).await?;
                }
            }
            // No child plan and no vector search.
            PhysicalPlan::Query(
                QueryOp::ProviderScan { .. }
                | QueryOp::PartialAggregate { .. }
                | QueryOp::ShuffleJoinConsume { .. }
                | QueryOp::ShuffleAggregateConsume { .. }
                | QueryOp::FacetCounts { .. }
                | QueryOp::RecursiveScan { .. }
                | QueryOp::RecursiveValue { .. },
            )
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Document(_)
            | PhysicalPlan::Kv(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => {}
        }
        Ok(())
    })
}

/// Replace the array prefilter of a vector search with the cross-shard
/// bitmap. Every other vector op has no prefilter.
async fn resolve_search(
    state: &SharedState,
    txn_id: Option<TxnId>,
    op: &mut VectorOp,
) -> crate::Result<()> {
    let VectorOp::Search {
        inline_prefilter_plan,
        filter_bitmap,
        ..
    } = op
    else {
        return Ok(());
    };
    let Some(PhysicalPlan::Array(ArrayOp::SurrogateBitmapScan {
        array_id,
        slice_msgpack,
        system_time,
    })) = inline_prefilter_plan.as_deref()
    else {
        return Ok(());
    };
    let scan = PrefilterScan {
        array_id,
        slice_msgpack,
        system_time: *system_time,
        txn_id,
    };
    let Some(bitmap) = cluster_bitmap(state, scan).await? else {
        return Ok(());
    };
    *filter_bitmap = Some(match filter_bitmap.take() {
        Some(existing) => existing.intersect(&bitmap),
        None => bitmap,
    });
    *inline_prefilter_plan = None;
    Ok(())
}

/// One array prefilter scan to read across every shard.
struct PrefilterScan<'a> {
    array_id: &'a nodedb_array::types::ArrayId,
    slice_msgpack: &'a [u8],
    /// The cut every shard reads: the scope of the search.
    system_time: nodedb_types::SystemTimeScope,
    /// The reading transaction, `None` for an autocommit read.
    txn_id: Option<TxnId>,
}

/// The cross-shard surrogate bitmap of the scan's array inside its slice at
/// its cut, or `None` on a node with no cluster transport.
async fn cluster_bitmap(
    state: &SharedState,
    scan: PrefilterScan<'_>,
) -> crate::Result<Option<nodedb_types::SurrogateBitmap>> {
    let PrefilterScan {
        array_id,
        slice_msgpack,
        system_time,
        txn_id,
    } = scan;
    let (Some(transport), Some(routing)) = (
        state.cluster_transport.as_ref(),
        state.cluster_routing.as_ref(),
    ) else {
        return Ok(None);
    };
    let prefix_bits = {
        let catalog = state
            .array_catalog
            .read()
            .map_err(|_| crate::Error::Internal {
                detail: "array catalog lock poisoned".into(),
            })?;
        catalog
            .lookup_by_name_in_database(array_id.tenant_id, array_id.database_id, &array_id.name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("array prefilter: array '{}' not found", array_id.name),
            })?
            .prefix_bits
    };
    let executor = ClusterArrayExecutor::new(
        Arc::clone(transport),
        Arc::clone(routing),
        state.node_id,
        state.self_arc()?,
    );
    executor
        .surrogate_bitmap(array_id, slice_msgpack, prefix_bits, system_time, txn_id)
        .await
        .map(Some)
}
