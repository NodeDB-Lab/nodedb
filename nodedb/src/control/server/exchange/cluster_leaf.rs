// SPDX-License-Identifier: BUSL-1.1

//! A cross-node gather of a plan whose rows are spread by tile or node key.
//!
//! - A cluster array read (`ClusterArray` `Slice` / `Agg`) runs through the
//!   array executor, which already knows the shard that owns each tile. A
//!   leaf nested under a coordinator-local wrapper (aggregate, join input,
//!   post-process, set operation, lateral outer plan) is read first and
//!   inlined as a `ProviderScan`. The rest of the plan then gathers like any
//!   plan over ordinary collections. A slice read below its retention horizon
//!   raises the client notice its own shaping will report.
//! - Graph reads never reach a gather: the SQL planner emits no graph leaf,
//!   and every graph read runs through `graph_dispatch`. A local `Array` plan
//!   never reaches one in a cluster either, which plans arrays as
//!   `ClusterArray`. Either one is refused rather than read from this node's
//!   partitions alone.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use nodedb_physical::physical_plan::{ClusterArrayOp, ExchangeOp, PhysicalPlan, QueryOp};

use crate::control::cluster::ClusterArrayExecutor;
use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
use crate::control::state::SharedState;
use crate::data::executor::response_codec::{ArraySliceResponse, flatten_to_relational_rows};
use crate::types::{Lsn, TxnId};

use super::gather::GatherOutcome;
use super::read_scope::ReadScope;
use super::resolve::exchange::provider_scan_of_rows;

/// Gather `plan`, which holds a cluster-partitioned leaf, across the nodes
/// that own its rows.
pub(super) async fn gather_cluster_partitioned(
    state: &SharedState,
    plan: PhysicalPlan,
    scope: ReadScope,
) -> crate::Result<GatherOutcome> {
    if let PhysicalPlan::ClusterArray(op) = &plan {
        let rows = read_cluster_array(state, op, scope.txn_id).await?;
        return Ok(outcome_of(flatten_to_relational_rows(&rows)));
    }
    let mut local = plan;
    inline_cluster_arrays(state, &mut local, scope.txn_id).await?;
    if nodedb_physical::physical_plan::plan_contains_cluster_partitioned_leaf(&local) {
        return Err(crate::Error::Internal {
            detail: "a graph or local array plan reached a cross-node gather; graph reads \
                     run through the graph coordinators, and a cluster plans arrays as \
                     ClusterArray"
                .into(),
        });
    }
    // The rest of the plan reads ordinary collections, or nothing at all: it
    // routes like any other gathered plan. `Box::pin` breaks the recursion
    // back into `gather_all_vshards`, which cannot reach here again because
    // the plan no longer holds a cluster-partitioned leaf.
    Box::pin(super::gather::gather_all_vshards(state, local, scope)).await
}

/// Read one cluster array op through the array executor and return its rows
/// as one msgpack array.
async fn read_cluster_array(
    state: &SharedState,
    op: &ClusterArrayOp,
    txn_id: Option<TxnId>,
) -> crate::Result<Vec<u8>> {
    if matches!(
        op,
        ClusterArrayOp::Put { .. } | ClusterArrayOp::Delete { .. }
    ) {
        return Err(crate::Error::Internal {
            detail: "a cluster array write reached a cross-node gather".into(),
        });
    }
    let transport = state
        .cluster_transport
        .as_ref()
        .ok_or_else(|| crate::Error::Internal {
            detail: "cluster transport not available for a cluster array read".to_owned(),
        })?;
    let routing = state
        .cluster_routing
        .as_ref()
        .ok_or_else(|| crate::Error::Internal {
            detail: "cluster routing not available for a cluster array read".to_owned(),
        })?;
    let executor = ClusterArrayExecutor::new(
        Arc::clone(transport),
        Arc::clone(routing),
        state.node_id,
        state.self_arc()?,
    );
    let payload = executor.execute(op, txn_id).await?;
    cluster_array_rows(op, payload)
}

/// The rows of a cluster array read's payload, as one msgpack array.
///
/// A `Slice` answers an `ArraySliceResponse` envelope whose `rows_msgpack` is
/// the row array. An `Agg` answers the row array itself (`{group, result}` or
/// `{result}` maps). A slice whose `truncated_before_horizon` is set raises
/// the same client notice a slice shaped on its own reports
/// (`response_shape::compose::array_slice`), through the statement notice
/// slot (`session::statement_notice`).
fn cluster_array_rows(op: &ClusterArrayOp, payload: Vec<u8>) -> crate::Result<Vec<u8>> {
    match op {
        ClusterArrayOp::Slice { .. } => {
            let response: ArraySliceResponse =
                zerompk::from_msgpack(&payload).map_err(|e| crate::Error::Codec {
                    detail: format!("cluster array slice response decode: {e}"),
                })?;
            if response.truncated_before_horizon {
                crate::control::server::shared::session::statement_notice::raise(
                    crate::control::server::response_shape::compose::array_slice::TRUNCATED_BEFORE_HORIZON_NOTICE
                        .to_string(),
                );
            }
            Ok(response.rows_msgpack)
        }
        ClusterArrayOp::Agg { .. } | ClusterArrayOp::Put { .. } | ClusterArrayOp::Delete { .. } => {
            Ok(payload)
        }
    }
}

/// Replace every cluster array leaf under a coordinator-local wrapper with a
/// `ProviderScan` of its rows. The wrappers walked are the ones
/// `plan_contains_cluster_partitioned_leaf` looks through.
fn inline_cluster_arrays<'a>(
    state: &'a SharedState,
    plan: &'a mut PhysicalPlan,
    txn_id: Option<TxnId>,
) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match plan {
            PhysicalPlan::ClusterArray(op) => {
                let rows = read_cluster_array(state, op, txn_id).await?;
                *plan = provider_scan_of_rows(flatten_to_relational_rows(&rows));
            }
            PhysicalPlan::Query(QueryOp::Aggregate { input, .. }) => {
                if let Some(child) = input.as_deref_mut() {
                    inline_cluster_arrays(state, child, txn_id).await?;
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
                        inline_cluster_arrays(state, child, txn_id).await?;
                    }
                }
            }
            PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, .. }))
            | PhysicalPlan::Query(QueryOp::PostProcess { input: child, .. })
            | PhysicalPlan::Query(QueryOp::LateralTopK {
                outer_plan: child, ..
            })
            | PhysicalPlan::Query(QueryOp::LateralLoop {
                outer_plan: child, ..
            }) => {
                inline_cluster_arrays(state, child, txn_id).await?;
            }
            PhysicalPlan::Query(QueryOp::SetOp { inputs, .. }) => {
                for child in inputs.iter_mut() {
                    inline_cluster_arrays(state, child, txn_id).await?;
                }
            }
            _ => {}
        }
        Ok(())
    })
}

/// A gather outcome holding one payload.
fn outcome_of(payload: Vec<u8>) -> GatherOutcome {
    let merged_array = encode_msgpack_array(&extract_msgpack_elements(&payload));
    GatherOutcome {
        raw: payload,
        merged_array,
        watermark_lsn: Lsn::ZERO,
        read_versions: crate::types::ReadVersions::new(),
        shard_watermarks: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One flat row, the plain msgpack map `{"k": value}` a shard emits.
    /// `nodedb_types::Value` encodes as a tagged array, not a plain map, so
    /// the bytes are written directly: fixmap of 1, fixstr "k", positive
    /// fixint `value`.
    fn row(value: u8) -> Vec<u8> {
        assert!(value < 0x80, "a positive fixint holds 0..=127");
        vec![0x81, 0xa1, b'k', value]
    }

    fn array_id() -> nodedb_array::types::ArrayId {
        nodedb_array::types::ArrayId::new(nodedb_types::TenantId::new(1), "grid")
    }

    fn slice_op() -> ClusterArrayOp {
        ClusterArrayOp::Slice {
            array_id: array_id(),
            slice_msgpack: Vec::new(),
            attr_projection: Vec::new(),
            limit: 0,
            slice_hilbert_ranges: Vec::new(),
            prefix_bits: 0,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }
    }

    fn agg_op() -> ClusterArrayOp {
        ClusterArrayOp::Agg {
            array_id: array_id(),
            attr_idx: 0,
            reducer_msgpack: Vec::new(),
            group_by_dim: -1,
            slice_hilbert_ranges: Vec::new(),
            prefix_bits: 0,
            system_as_of: None,
            valid_at_ms: None,
        }
    }

    #[test]
    fn a_slice_envelope_flattens_to_its_rows() {
        let rows = vec![row(1), row(2)];
        let envelope = zerompk::to_msgpack_vec(&ArraySliceResponse {
            rows_msgpack: encode_msgpack_array(&rows),
            truncated_before_horizon: false,
        })
        .expect("encode slice envelope");
        let flat = flatten_to_relational_rows(
            &cluster_array_rows(&slice_op(), envelope).expect("slice rows"),
        );
        assert_eq!(extract_msgpack_elements(&flat), rows);
    }

    #[tokio::test]
    async fn a_truncated_slice_raises_the_horizon_notice() {
        use crate::control::server::shared::session::{conn_scope, statement_notice};
        conn_scope::scoped(async {
            let envelope = zerompk::to_msgpack_vec(&ArraySliceResponse {
                rows_msgpack: encode_msgpack_array(&[row(1)]),
                truncated_before_horizon: true,
            })
            .expect("encode slice envelope");
            cluster_array_rows(&slice_op(), envelope).expect("slice rows");
            assert_eq!(
                statement_notice::take(),
                vec![
                    crate::control::server::response_shape::compose::array_slice::TRUNCATED_BEFORE_HORIZON_NOTICE
                        .to_string()
                ]
            );
        })
        .await;
    }

    #[test]
    fn an_aggregate_payload_flattens_to_its_rows() {
        let rows = vec![row(10), row(20), row(30)];
        let flat = flatten_to_relational_rows(
            &cluster_array_rows(&agg_op(), encode_msgpack_array(&rows)).expect("agg rows"),
        );
        assert_eq!(extract_msgpack_elements(&flat), rows);
    }
}
