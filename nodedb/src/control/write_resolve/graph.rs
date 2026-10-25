// SPDX-License-Identifier: BUSL-1.1

//! Graph implementation of [`EngineWriteResolver`].
//!
//! Resolves a governed `GraphOp::EdgeDelete` by deciding the policy against
//! the edge's stored properties, then rebuilds the delete with the decided
//! check. A follower has no writing identity to evaluate `$auth.*`, so the
//! verdict travels on the wire instead of the predicate.

use crate::types::VShardId;
use async_trait::async_trait;
use nodedb_types::{DatabaseId, RlsWriteCheck};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local_on_vshard;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::GraphOp;

use super::resolved_rows::ResolvedRows;
use super::resolver::{EngineWriteResolver, WriteResolveContext};

/// A governed graph edge delete, extracted at interception.
pub struct GraphWriteResolver {
    /// Collection the edge belongs to. Names the write, but does NOT home it.
    collection: String,
    /// Source endpoint. An edge is key-homed, so this is the vshard key.
    src_id: String,
    /// The intercepted delete verbatim, live write predicate included.
    op: GraphOp,
}

/// The resolver for `op`, or `None` when it carries no live write predicate.
/// Exhaustive over `GraphOp` — a new op fails to compile here.
pub(super) fn resolver_for_graph_op(op: &GraphOp) -> Option<Box<dyn EngineWriteResolver>> {
    let (collection, src_id) = match op {
        GraphOp::EdgeDelete {
            collection,
            src_id,
            rls_write_check,
            ..
        } => {
            if !rls_write_check.has_predicate() {
                return None;
            }
            (collection, src_id)
        }
        // EdgePut is decided at injection; batch forms are refused outright
        // by RLS injection. Everything else is a read or unrelated write.
        GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::ResolveEdgeDelete(_)
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::Stats { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        | GraphOp::NodePresenceRead { .. } => return None,
    };
    Some(Box::new(GraphWriteResolver {
        collection: collection.as_str().to_owned(),
        src_id: src_id.clone(),
        op: op.clone(),
    }))
}

#[async_trait]
impl EngineWriteResolver for GraphWriteResolver {
    fn collection(&self) -> &str {
        &self.collection
    }

    /// Key-homed on the source endpoint, where the forward edge row lives.
    fn vshard(&self, _database_id: DatabaseId) -> crate::Result<VShardId> {
        Ok(VShardId::from_key(self.src_id.as_bytes()))
    }

    fn build_resolve_op(&self) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::ResolveEdgeDelete(Box::new(self.op.clone())))
    }

    /// A refused edge surfaces as `DataPlane(RejectedAuthz)`, same as the
    /// direct delete — the resolve handler runs the same gate.
    async fn resolve(
        &self,
        state: &SharedState,
        ctx: WriteResolveContext,
        op: PhysicalPlan,
    ) -> crate::Result<ResolvedRows> {
        let collection = &self.collection;
        let resp = dispatch_local_on_vshard(
            state,
            ctx.tenant_id,
            ctx.database_id,
            self.vshard(ctx.database_id)?,
            op,
            None,
        )
        .await?;
        if resp.status != Status::Ok {
            return Err(match resp.error_code {
                Some(code) => crate::Error::DataPlane(*code),
                None => crate::Error::Dispatch {
                    detail: format!(
                        "graph governed edge delete: resolve on '{collection}' returned status \
                         {:?} with no error code",
                        resp.status
                    ),
                },
            });
        }
        Ok(ResolvedRows::GraphEdgeDeleteAdmitted)
    }

    fn apply(&self, resolved: ResolvedRows) -> crate::Result<PhysicalPlan> {
        let ResolvedRows::GraphEdgeDeleteAdmitted = resolved else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "graph write resolver for '{}' was handed another engine's resolution; \
                     resolver_for_plan dispatched the wrong engine",
                    self.collection
                ),
            });
        };
        let GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            rls_write_check: _,
        } = self.op.clone()
        else {
            return Err(crate::Error::Internal {
                detail: format!(
                    "graph write resolver for '{}' holds a plan that is not an edge delete",
                    self.collection
                ),
            });
        };
        Ok(PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
        }))
    }
}
