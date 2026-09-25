// SPDX-License-Identifier: BUSL-1.1

//! Local execution of incoming `ExecuteRequest` / `ExecuteStreamRequest` RPCs.
//!
//! When this node leads the target vShard, [`LocalPlanExecutor`] validates
//! descriptor versions, decodes the `PhysicalPlan`, and fans it across all
//! local Data-Plane cores before returning the merged result.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use tracing::{Instrument, info_span};

use nodedb_cluster::forward::{ChunkSink, PlanExecutor};
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, TypedClusterError};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::exchange::execute_plan_all_local_cores;
use crate::control::state::SharedState;
use crate::control::trace_export::EmitSpanParams;
use crate::types::DatabaseId;

use super::plan_decode::decode_plan;
use super::request_validation::validate_request;
use super::support::{PLAN_DECODE_FAILED, SinkOutcome, execution_error_to_typed};

fn reject_unadmitted_crdt_apply(plan: &PhysicalPlan) -> Result<(), TypedClusterError> {
    if matches!(
        plan,
        PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply { .. }
                | nodedb_physical::physical_plan::CrdtOp::ApplyAuthenticated { .. }
                | nodedb_physical::physical_plan::CrdtOp::ImportSnapshot { .. }
        )
    ) {
        return Err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: crate::Error::CrdtApplyRequiresAdmission.to_string(),
        });
    }
    Ok(())
}

/// Executes pre-planned `PhysicalPlan` on the local Data Plane.
pub struct LocalPlanExecutor {
    state: Arc<SharedState>,
}

impl LocalPlanExecutor {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

impl PlanExecutor for LocalPlanExecutor {
    async fn execute_plan(&self, req: ExecuteRequest) -> ExecuteResponse {
        let trace_id = nodedb_types::TraceId(req.trace_id);
        let tenant_id = req.tenant_id;
        let exporter = Arc::clone(&self.state.trace_exporter);
        let start = SystemTime::now();
        let span = info_span!("executor.execute_plan", trace_id = %trace_id, tenant_id);
        let resp = self.execute_plan_inner(req).instrument(span).await;
        // Emit one OTLP executor span per leaseholder so the gateway's
        // upstream span joins the N leaseholder spans into a single
        // distributed trace via the shared `trace_id`.
        exporter.emit(EmitSpanParams {
            span_name: "executor.execute_plan",
            trace_id,
            start,
            end: SystemTime::now(),
            tenant_id,
            vshard_id: 0,
            status_ok: resp.success,
        });
        resp
    }

    async fn execute_plan_streaming(
        &self,
        req: ExecuteRequest,
        sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        let trace_id = nodedb_types::TraceId(req.trace_id);
        let tenant_id = req.tenant_id;
        let exporter = Arc::clone(&self.state.trace_exporter);
        let start = SystemTime::now();
        let span = info_span!("executor.execute_plan_streaming", trace_id = %trace_id, tenant_id);
        let outcome = self
            .execute_plan_streaming_inner(req, sink)
            .instrument(span)
            .await;
        exporter.emit(EmitSpanParams {
            span_name: "executor.execute_plan_streaming",
            trace_id,
            start,
            end: SystemTime::now(),
            tenant_id,
            vshard_id: 0,
            status_ok: outcome.is_none(),
        });
        outcome
    }
}

impl LocalPlanExecutor {
    /// Shared validation + decode prologue for both the one-shot and streaming
    /// paths: validate deadline + descriptor versions, decode the plan, reject
    /// unresolved Exchange nodes.  Returns `(plan, database_id, deadline)` on
    /// success or a typed cluster error to surface to the caller.
    fn validate_and_decode(
        &self,
        req: &ExecuteRequest,
    ) -> Result<
        (
            nodedb_physical::physical_plan::PhysicalPlan,
            DatabaseId,
            Duration,
        ),
        TypedClusterError,
    > {
        let (deadline, database_id) = validate_request(&self.state, req)?;
        let plan = decode_plan(&self.state, database_id, req.tenant_id, &req.plan_bytes)?;
        Ok((plan, database_id, deadline))
    }

    /// One-shot execution: validate + decode, fan across all local cores,
    /// merge, and return the merged payload.
    async fn execute_plan_inner(&self, req: ExecuteRequest) -> ExecuteResponse {
        let (plan, database_id, deadline) = match self.validate_and_decode(&req) {
            Ok(t) => t,
            Err(e) => return ExecuteResponse::err(e),
        };

        let tenant_id = crate::types::TenantId::new(req.tenant_id);
        let trace_id = nodedb_types::TraceId(req.trace_id);

        if let PhysicalPlan::ClusterEvent(
            nodedb_physical::physical_plan::ClusterEventOp::PublishTopic {
                database_id: topic_database_id,
                topic_name,
                payload,
            },
        ) = &plan
        {
            if *topic_database_id != database_id {
                return ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: "topic publish database does not match RPC database".into(),
                });
            }
            return match crate::event::topic::publish::publish_to_topic(
                &self.state,
                database_id,
                req.tenant_id,
                topic_name,
                payload,
            )
            .await
            {
                Ok(sequence) => match zerompk::to_msgpack_vec(&sequence) {
                    Ok(payload) => ExecuteResponse::ok(vec![payload], 0, 0),
                    Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
                        code: PLAN_DECODE_FAILED,
                        message: format!("topic response encoding failed: {error}"),
                    }),
                },
                Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: error.to_string(),
                }),
            };
        }

        if let PhysicalPlan::ClusterEvent(
            nodedb_physical::physical_plan::ClusterEventOp::ConsumeStream {
                database_id: stream_database_id,
                stream_name,
                group_name,
                partition,
                limit,
                committed_offsets,
            },
        ) = &plan
        {
            if let Err(error) = reject_consume_database_mismatch(*stream_database_id, database_id) {
                return ExecuteResponse::err(error);
            }
            let limit = match usize::try_from(*limit) {
                Ok(limit) => limit,
                Err(_) => {
                    return ExecuteResponse::err(TypedClusterError::Internal {
                        code: PLAN_DECODE_FAILED,
                        message: "CDC consume limit exceeds platform range".into(),
                    });
                }
            };
            let params = crate::event::cdc::consume::ConsumeParams {
                database_id: *stream_database_id,
                tenant_id: req.tenant_id,
                stream_name,
                group_name,
                partition: *partition,
                limit,
            };
            if let Err(error) =
                crate::event::cdc::consume::validate_consume_identity(&self.state, &params)
            {
                return ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: error.to_string(),
                });
            }
            let committed_offsets =
                match crate::event::cdc::consume::decode_remote_committed_offsets(committed_offsets)
                {
                    Ok(offsets) => offsets,
                    Err(error) => {
                        return ExecuteResponse::err(TypedClusterError::Internal {
                            code: PLAN_DECODE_FAILED,
                            message: error.to_string(),
                        });
                    }
                };
            // Events go to an authenticated peer node, not a subscriber; the
            // requesting node applies its caller's redaction at the delivery
            // surface (SELECT / HTTP poll / SSE), using the same replicated
            // catalog policies on both sides.
            return match crate::event::cdc::consume::consume_local_with_offsets(
                &self.state,
                &params,
                Some(&committed_offsets),
            ) {
                Ok(result) => {
                    let events = result
                        .events
                        .iter()
                        .map(|event| event.as_ref().clone())
                        .collect::<Vec<_>>();
                    match zerompk::to_msgpack_vec(&events) {
                        Ok(payload) => ExecuteResponse::ok(vec![payload], 0, 0),
                        Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
                            code: PLAN_DECODE_FAILED,
                            message: format!("CDC response encoding failed: {error}"),
                        }),
                    }
                }
                Err(error) => ExecuteResponse::err(TypedClusterError::Internal {
                    code: PLAN_DECODE_FAILED,
                    message: error.to_string(),
                }),
            };
        }

        // Replicable write: drive through Raft, not local cores. Fanning it
        // across local cores only would commit here without proposing to the
        // Raft group — silent write loss. Propose through the same proposer
        // the local pgwire write path uses. Reads / non-replicable plans fall
        // through to `execute_plan_all_local_cores` unchanged.
        //
        // The vshard is not carried on the wire; re-derive it as a pure
        // function of the plan's primary collection, matching the gateway
        // router's `CollectionHomed` arm (`vshard_for_collection`).
        let vshard_id = crate::types::VShardId::new(
            crate::control::gateway::version_set::touched_collections(&plan)
                .into_iter()
                .next()
                .map(|name| nodedb_cluster::routing::vshard_for_collection(database_id, &name))
                .unwrap_or(0),
        );
        if let Err(error) = reject_unadmitted_crdt_apply(&plan) {
            return ExecuteResponse::err(error);
        }

        if let Some(proposer) = self.state.async_raft_proposer() {
            let replicable =
                match crate::control::wal_replication::ReplicableWrite::decide_for_replication(
                    &plan,
                ) {
                    Ok(replicable) => replicable,
                    Err(e) => {
                        return ExecuteResponse::err(TypedClusterError::Internal {
                            code: PLAN_DECODE_FAILED,
                            message: e.to_string(),
                        });
                    }
                };
            match crate::control::wal_replication::to_replicated_entry(
                tenant_id,
                database_id,
                vshard_id,
                &replicable,
            ) {
                Err(e) => {
                    return ExecuteResponse::err(TypedClusterError::Internal {
                        code: PLAN_DECODE_FAILED,
                        message: e.to_string(),
                    });
                }
                Ok(Some(entry)) => {
                    return match crate::control::wal_replication::propose_replicated_entry(
                        &self.state,
                        proposer,
                        entry,
                    )
                    .await
                    {
                        // Replicated writes carry no read watermark → 0: it floors a
                        // session's later reads, and this RPC seam has no session.
                        Ok((payload, write_version)) => {
                            // Replicas apply with `ChangeFeedOwner::Unowned`. This
                            // node proposed the write once, so it publishes the
                            // change event.
                            crate::control::server::dispatch_utils::publish_change_set_with_lsn(
                                &self.state,
                                tenant_id,
                                database_id,
                                crate::control::server::dispatch_utils::extract_write_change_set(
                                    &plan, tenant_id,
                                ),
                                write_version,
                            );
                            ExecuteResponse::ok(vec![payload], 0, 0)
                        }
                        // A replicated write's apply verdict is a Data-Plane
                        // verdict: carry its code, never flatten to internal.
                        Err(e) => ExecuteResponse::err(execution_error_to_typed(e)),
                    };
                }
                Ok(None) => {}
            }
        }

        match tokio::time::timeout(
            deadline,
            execute_plan_all_local_cores(
                &self.state,
                tenant_id,
                database_id,
                plan,
                trace_id,
                req.txn_id,
            ),
        )
        .await
        {
            Ok(Ok(result)) => ExecuteResponse::ok(
                vec![result.payload],
                result.watermark_lsn.as_u64(),
                result.read_version_lsn.as_u64(),
            ),
            Ok(Err(e)) => ExecuteResponse::err(execution_error_to_typed(e)),
            Err(_) => ExecuteResponse::err(TypedClusterError::DeadlineExceeded {
                elapsed_ms: deadline.as_millis() as u64,
            }),
        }
    }

    /// Streaming execution: validate + decode, fan across all local cores via
    /// `gather_all_cores_stream`, push each frame to `sink` as it arrives.
    /// Returns `None` on clean end or when `send_chunk` fails (coordinator
    /// gone, no peer for a terminal frame), `Some(err)` on terminal failure.
    async fn execute_plan_streaming_inner(
        &self,
        req: ExecuteRequest,
        mut sink: impl ChunkSink,
    ) -> Option<TypedClusterError> {
        let (plan, database_id, deadline) = match self.validate_and_decode(&req) {
            Ok(t) => t,
            Err(e) => return Some(e),
        };

        if let Err(error) = reject_unadmitted_crdt_apply(&plan) {
            return Some(error);
        }
        if matches!(plan, PhysicalPlan::ClusterEvent(_)) {
            return Some(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "ClusterEvent operations do not support streaming RPC".into(),
            });
        }

        let tenant_id = crate::types::TenantId::new(req.tenant_id);
        let trace_id = nodedb_types::TraceId(req.trace_id);

        // Cluster RPC receiver (remote-node local execution): forward the
        // incoming request's transaction context so a transactional streaming
        // read honours its staged overlay. Inert when `None`.
        let mut stream = match crate::control::server::exchange::gather::gather_all_cores_stream(
            &self.state,
            tenant_id,
            database_id,
            plan,
            trace_id,
            req.txn_id,
        ) {
            Ok(s) => s,
            Err(e) => return Some(execution_error_to_typed(e)),
        };

        let stream_fut = async {
            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(b) => {
                        if let Err(_e) = sink
                            .send_chunk(
                                b.payload,
                                b.watermark_lsn.as_u64(),
                                b.read_version_lsn.as_u64(),
                            )
                            .await
                        {
                            // Coordinator gone — stop, no terminal frame.
                            return SinkOutcome::CoordinatorGone;
                        }
                    }
                    Err(e) => {
                        return SinkOutcome::StreamError(execution_error_to_typed(e));
                    }
                }
            }
            SinkOutcome::CleanEnd
        };

        match tokio::time::timeout(deadline, stream_fut).await {
            Ok(SinkOutcome::CleanEnd) => None,
            Ok(SinkOutcome::CoordinatorGone) => None,
            Ok(SinkOutcome::StreamError(e)) => Some(e),
            Err(_) => Some(TypedClusterError::DeadlineExceeded {
                elapsed_ms: deadline.as_millis() as u64,
            }),
        }
    }
}

fn reject_consume_database_mismatch(
    stream_database_id: crate::types::DatabaseId,
    envelope_database_id: crate::types::DatabaseId,
) -> Result<(), TypedClusterError> {
    if stream_database_id == envelope_database_id {
        Ok(())
    } else {
        Err(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: "CDC consume database does not match RPC database".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::CrdtOp;

    #[test]
    fn consume_stream_rejects_database_mismatch() {
        let op = nodedb_physical::physical_plan::ClusterEventOp::ConsumeStream {
            database_id: crate::types::DatabaseId::new(7),
            stream_name: "topic:orders".into(),
            group_name: "analytics".into(),
            partition: Some(0),
            limit: 1,
            committed_offsets: vec![(0, 0, 0)],
        };
        let nodedb_physical::physical_plan::ClusterEventOp::ConsumeStream { database_id, .. } = op
        else {
            panic!("expected typed consume operation");
        };
        assert!(matches!(
            reject_consume_database_mismatch(database_id, crate::types::DatabaseId::new(8)),
            Err(TypedClusterError::Internal { .. })
        ));
    }

    #[test]
    fn consume_stream_rejects_duplicate_caller_offsets() {
        assert!(matches!(
            crate::event::cdc::consume::decode_remote_committed_offsets(&[(3, 7, 1), (3, 8, 1)]),
            Err(crate::event::cdc::consume::ConsumeError::InvalidRemoteOffsets(_))
        ));
    }

    #[test]
    fn every_remote_execution_mode_rejects_unadmitted_crdt_apply() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "doc-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: nodedb_types::Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });

        assert!(matches!(
            reject_unadmitted_crdt_apply(&plan),
            Err(TypedClusterError::Internal { .. })
        ));
    }
}
