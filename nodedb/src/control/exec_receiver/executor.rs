// SPDX-License-Identifier: BUSL-1.1

//! Local execution of incoming `ExecuteRequest` / `ExecuteStreamRequest` RPCs.
//!
//! When this node leads the target vShard, [`LocalPlanExecutor`] validates
//! descriptor versions, decodes the `PhysicalPlan`, and runs it through
//! `execute_received_plan`: a vShard-scoped plan on its one owning core, every
//! other plan across all local cores.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::StreamExt;
use tracing::{Instrument, info_span};

use nodedb_cluster::forward::{ChunkSink, PlanExecutor};
use nodedb_cluster::rpc_codec::{ExecuteRequest, ExecuteResponse, TypedClusterError};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::exchange::execute_received_plan;
use crate::control::state::SharedState;
use crate::control::trace_export::EmitSpanParams;
use crate::types::DatabaseId;

use super::backup_cut::take_backup_cut;
use super::plan_decode::decode_plan;
use super::read_leg::confirm_read_leg;
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
    async fn validate_and_decode(
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
        // A backup's snapshot plan takes the backup's cut on this node first.
        let plan = take_backup_cut(&self.state, plan).await?;
        Ok((plan, database_id, deadline))
    }

    /// One-shot execution: validate + decode, fan across all local cores,
    /// merge, and return the merged payload.
    async fn execute_plan_inner(&self, req: ExecuteRequest) -> ExecuteResponse {
        let (plan, database_id, deadline) = match self.validate_and_decode(&req).await {
            Ok(t) => t,
            Err(e) => return ExecuteResponse::err(e),
        };

        let tenant_id = crate::types::TenantId::new(req.tenant_id);
        let trace_id = nodedb_types::TraceId(req.trace_id);

        if let Some(response) = super::backup_cut::answer_capture_plan(&self.state, &plan).await {
            return response;
        }
        if let Some(response) =
            super::tenant_marks::answer_marks_plan(&self.state, &plan, deadline).await
        {
            return response;
        }
        if let Some(response) = super::surrogate_binds::answer_binds_plan(&self.state, &plan) {
            return response;
        }
        if let Some(response) = super::surrogate_binds::answer_holders_plan(&self.state, &plan) {
            return response;
        }
        if let Some(response) =
            super::metadata_applied::answer_applied_plan(&self.state, &plan, deadline).await
        {
            return response;
        }

        if let Some(response) = super::stream_events::answer_stream_event_plan(
            &self.state,
            &plan,
            database_id,
            req.tenant_id,
        ) {
            return response;
        }

        // Replicable write: drive through Raft, not local cores. Fanning it
        // across local cores only commits here without proposing to the
        // Raft group — silent write loss. Propose through the same proposer
        // the local pgwire write path uses. Reads / non-replicable plans fall
        // through to `execute_received_plan` unchanged.
        //
        // Only a vShard-scoped plan carries its vShard on the wire. For a
        // replicable write, re-derive the vShard as a pure
        // function of the plan's primary collection, matching the gateway
        // router's `CollectionHomed` arm (`vshard_for_collection`). The plan
        // carries the database-qualified name, de-qualified into the
        // canonical key before hashing.
        let vshard_raw = match crate::control::gateway::version_set::touched_collections(&plan)
            .into_iter()
            .next()
        {
            Some(name) => match nodedb_types::CollectionKey::from_qualified_str(database_id, &name)
            {
                Ok(key) => nodedb_cluster::routing::vshard_for_collection(key),
                Err(error) => {
                    return ExecuteResponse::err(execution_error_to_typed(error.into()));
                }
            },
            None => 0,
        };
        let vshard_id = crate::types::VShardId::new(vshard_raw);
        if let Err(error) = reject_unadmitted_crdt_apply(&plan) {
            return ExecuteResponse::err(error);
        }

        {
            let proposer = match self.state.async_raft_proposer() {
                Ok(proposer) => proposer,
                Err(e) => return ExecuteResponse::err(execution_error_to_typed(e)),
            };
            // The entry carries resolved rows: a timeseries ingest resolves
            // here, on the proposer, before the entry exists.
            let resolved = match crate::control::write_resolve::resolve_for_log(
                &self.state,
                crate::control::write_resolve::WriteResolveContext {
                    tenant_id,
                    database_id,
                },
                vshard_id,
                &plan,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(e) => return ExecuteResponse::err(execution_error_to_typed(e)),
            };
            let replicable =
                match crate::control::wal_replication::ReplicableWrite::decide_for_replication(
                    resolved.as_ref().unwrap_or(&plan),
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
                        // The coordinator's remaining budget bounds this hop.
                        tokio::time::Instant::now() + deadline,
                    )
                    .await
                    {
                        // Replicated writes carry no read watermark → 0: it floors a
                        // session's later reads, and this RPC seam has no session.
                        // The write's versions go back to the coordinator.
                        Ok((payload, write_versions)) => {
                            ExecuteResponse::ok(vec![payload], 0, write_versions.to_wire())
                        }
                        // A replicated write's apply verdict is a Data-Plane
                        // verdict: carry its code, never flatten to internal.
                        Err(e) => ExecuteResponse::err(execution_error_to_typed(e)),
                    };
                }
                Ok(None) => {}
            }
        }

        if let Err(error) =
            confirm_read_leg(&self.state, database_id, &plan, &req.read_groups, deadline).await
        {
            return ExecuteResponse::err(error);
        }
        // A vShard-scoped plan runs on its one owning core. Every other plan
        // fans across all local cores.
        match tokio::time::timeout(
            deadline,
            execute_received_plan(
                &self.state,
                tenant_id,
                database_id,
                plan,
                trace_id,
                req.txn_id,
                req.vshard_id,
            ),
        )
        .await
        {
            // A read that found no row keeps its verdict and the versions it
            // observed, so the coordinator answers and validates it as a
            // local miss.
            Ok(Ok(result)) if result.not_found => ExecuteResponse::refused_with_versions(
                TypedClusterError::DataPlane {
                    code: crate::bridge::envelope::ErrorCode::NotFound.into(),
                },
                result.watermark_lsn.as_u64(),
                result.read_versions.to_wire(),
            ),
            Ok(Ok(result)) => ExecuteResponse::ok(
                vec![result.payload],
                result.watermark_lsn.as_u64(),
                result.read_versions.to_wire(),
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
        let (plan, database_id, deadline) = match self.validate_and_decode(&req).await {
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
        // A capture request is answered from parked captures, never from a
        // live read of the cores.
        if super::backup_cut::is_capture_plan(&plan) {
            return Some(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "a cut capture request does not support streaming RPC".into(),
            });
        }
        // The stream fans across every core, so it cannot serve a plan that
        // must run on one owning core.
        if req.vshard_id.is_some() || crate::control::gateway::router::is_task_vshard_scoped(&plan)
        {
            return Some(TypedClusterError::Internal {
                code: PLAN_DECODE_FAILED,
                message: "vShard-scoped plans do not support streaming RPC".into(),
            });
        }

        if let Err(error) =
            confirm_read_leg(&self.state, database_id, &plan, &req.read_groups, deadline).await
        {
            return Some(error);
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
                                b.read_versions.to_wire(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::CrdtOp;

    #[test]
    fn every_remote_execution_mode_rejects_unadmitted_crdt_apply() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "doc-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: nodedb_types::Surrogate::new(1),
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
