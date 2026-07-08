// SPDX-License-Identifier: BUSL-1.1

//! `RegistryShuffleProducer` — bridges the cluster `ShuffleProduce` trigger to
//! the local streaming executor + fan-out sink (E4a).
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the produce logic
//! lives here and is exposed to the transport via the
//! [`nodedb_cluster::ShuffleProducer`] hook. The `RaftLoop` is built
//! `with_shuffle_producer(Arc::new(RegistryShuffleProducer { .. }))`.
//!
//! # What it does
//!
//! On `on_shuffle_produce`, it:
//! 1. wraps the request's `plan_bytes` (+ tenant/db/deadline/trace/descriptors)
//!    in an [`ExecuteRequest`] — the exact body the local streaming-execution
//!    prologue already validates (deadline, descriptor versions, plan decode,
//!    leftover-`Exchange` rejection, SPSC dispatch);
//! 2. runs the scan through [`LocalPlanExecutor::execute_plan_streaming`] with a
//!    [`ShuffleFanoutSink`] (instead of the QUIC-response sink), which
//!    hash-partitions each scanned batch and fans the rows out to the part-owners
//!    (looping back into the local registry for self-owned parts);
//! 3. `finish`es the sink — flushing residuals and `End`ing every part — with the
//!    scan's terminal outcome so consumers either complete or fail fast.
//!
//! # Plane discipline
//!
//! This runs on the producer node's Control Plane (the Tokio transport reactor).
//! The scan itself is dispatched to the Data Plane by the streaming executor via
//! the SPSC bridge; this hook never touches storage or io_uring. The QUIC fan-out
//! is Control-Plane I/O, which is allowed here.

use std::sync::Arc;

use nodedb_cluster::rpc_codec::{DescriptorVersionEntry, ExecuteRequest};
use nodedb_cluster::{ShuffleProduceRequest, TypedClusterError};

use nodedb_cluster::PlanExecutor;

use super::fanout::{ShuffleFanoutSink, ShuffleFanoutSinkParams};
use crate::control::LocalPlanExecutor;
use crate::control::state::SharedState;

/// `nodedb`-side implementation of [`nodedb_cluster::ShuffleProducer`].
///
/// Holds the node's [`SharedState`] so it can reach the local transport (for
/// fan-out), the receiver registry (for loopback), this node's id, and construct
/// a [`LocalPlanExecutor`] for the scan.
pub struct RegistryShuffleProducer {
    state: Arc<SharedState>,
}

impl RegistryShuffleProducer {
    /// Build a producer over `state`.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }

    /// Run the produce, returning a terminal error (or `None` on clean produce).
    ///
    /// Factored out of the trait method so the trait impl stays a thin shim and
    /// every early-exit error path is `?`-propagated here, then mapped to the
    /// fan-out terminal once at the call boundary.
    async fn produce(&self, req: ShuffleProduceRequest) -> Option<TypedClusterError> {
        // The transport must have a transport handle (cluster mode) to fan out.
        let Some(transport) = self.state.cluster_transport.clone() else {
            return Some(TypedClusterError::Internal {
                code: 0,
                message: "shuffle produce requires a cluster transport (single-node mode)".into(),
            });
        };

        let part_node_map: Vec<(u32, u64)> = req
            .part_node_map
            .iter()
            .map(|e| (e.part, e.node_id))
            .collect();

        let mut sink = ShuffleFanoutSink::new(
            transport,
            Arc::clone(&self.state.shuffle_registry),
            ShuffleFanoutSinkParams {
                self_node_id: self.state.node_id,
                shuffle_id: req.shuffle_id,
                side: req.side,
                num_parts: req.num_parts,
                producer_count: req.producer_count,
                keys: req.keys.clone(),
                part_node_map: &part_node_map,
            },
        );

        // Mirror `ExecuteRequest`'s fields from the produce request so the local
        // streaming-execution prologue (deadline / descriptor / decode / Exchange
        // rejection / dispatch) is reused verbatim — no second validation path.
        let exec_req = ExecuteRequest {
            plan_bytes: req.plan_bytes,
            tenant_id: req.tenant_id,
            database_id: req.database_id,
            deadline_remaining_ms: req.deadline_remaining_ms,
            trace_id: req.trace_id,
            // Shuffle produce is not transactional.
            txn_id: None,
            descriptor_versions: req
                .descriptor_versions
                .iter()
                .map(|d| DescriptorVersionEntry {
                    collection: d.collection.clone(),
                    version: d.version,
                })
                .collect(),
        };

        let executor = LocalPlanExecutor::new(Arc::clone(&self.state));

        // Stream the scan into the fan-out sink. `&mut sink` keeps ownership so we
        // can `finish` it afterward. A `Some(err)` here is a terminal scan failure
        // (validation reject, decode, deadline, or data-plane stream error).
        let scan_outcome = executor.execute_plan_streaming(exec_req, &mut sink).await;

        // Finalize the fan-out: flush residuals (clean path) and `End` EVERY part
        // for this side — with the scan error if any — so each receiver's barrier
        // reaches `producer_count` and consumers fail fast on error.
        if let Err(e) = sink.finish(scan_outcome.clone()).await {
            // A fan-out finalize failure is itself terminal: some receiver may be
            // left without an `End`. Surface it (preferring the original scan
            // error if there was one) rather than reporting a clean produce.
            return Some(scan_outcome.unwrap_or(TypedClusterError::Internal {
                code: 0,
                message: format!("shuffle produce fan-out finalize failed: {e}"),
            }));
        }

        scan_outcome
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::ShuffleProducer for RegistryShuffleProducer {
    async fn on_shuffle_produce(&self, req: ShuffleProduceRequest) -> Option<TypedClusterError> {
        self.produce(req).await
    }
}
