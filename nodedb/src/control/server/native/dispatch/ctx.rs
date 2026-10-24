// SPDX-License-Identifier: BUSL-1.1

//! Dispatch context: holds references needed by all per-opcode handlers.

use std::sync::Arc;

use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::session::SessionStore;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{GraphOp, PhysicalPlan};
use nodedb_types::CollectionKey;

use crate::types::{TenantId, VShardId};

/// Dispatch context: holds references needed by all handlers.
///
/// `scope` is the single, request-scoped auth contract: it is built once per
/// request (in `session::request::handle_request`) and carries both the
/// resolved `database_id` and the scope-enriched `AuthContext` in lockstep.
/// There is deliberately no separate `auth_context` or `database_id` field
/// here — every handler reads both through `scope` so the two values can
/// never drift apart across native call sites (SQL, direct ops, MATCH,
/// SQL-admin) the way they did before `RequestAuthScope` existed.
pub(crate) struct DispatchCtx<'a> {
    pub state: &'a Arc<SharedState>,
    pub identity: &'a AuthenticatedIdentity,
    pub scope: RequestAuthScope<'a>,
    pub query_ctx: &'a QueryContext,
    pub sessions: &'a SessionStore,
    pub peer_addr: &'a std::net::SocketAddr,
}

impl DispatchCtx<'_> {
    pub(super) fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id
    }

    /// Database scope for this request, as resolved once by `scope` at
    /// request setup. Delegating here (rather than re-querying
    /// `self.sessions` independently) is what keeps this value in lockstep
    /// with `scope.auth().database_id` — see the struct docs.
    pub(super) fn database_id(&self) -> crate::types::DatabaseId {
        self.scope.database_id()
    }

    /// The resolved, scope-enriched `AuthContext` for `$auth.*` RLS
    /// substitution. Every native call site must read this instead of
    /// `ctx.auth_context`, so RLS enforcement (including
    /// `$auth.scope_status(...)`) is identical regardless of which opcode
    /// dispatched the request.
    pub(crate) fn auth_context(&self) -> &crate::control::security::auth_context::AuthContext {
        self.scope.auth()
    }

    /// The vShard a direct-op task carries.
    ///
    /// A graph plan is homed by node key: an edge write by its source node,
    /// any other graph op by `document_id`, else the collection name. Every
    /// other plan is homed by its collection's canonical key, the bare name
    /// in this request's database. That is the vShard the planner, the
    /// gateway and the staging gate use for the same collection.
    pub(super) fn task_vshard(
        &self,
        plan: &PhysicalPlan,
        document_id: Option<&str>,
        collection: &str,
    ) -> VShardId {
        if let PhysicalPlan::Graph(op) = plan {
            // A presence guard, its read and a TRUNCATE's edge share name
            // their vShard.
            if let GraphOp::NodePresenceGuard { vshard, .. }
            | GraphOp::NodePresenceRead { vshard, .. }
            | GraphOp::TruncateEdges { vshard, .. } = op
            {
                return VShardId::new(*vshard);
            }
            let node_key = match op {
                GraphOp::EdgePut { src_id, .. } | GraphOp::EdgeDelete { src_id, .. } => {
                    src_id.as_str()
                }
                GraphOp::NodeEdgeGuard { node_id, .. } => node_id.as_str(),
                GraphOp::EdgePutBatch { .. }
                | GraphOp::ResolveEdgeDelete(_)
                | GraphOp::EdgeDeleteBatch { .. }
                | GraphOp::Hop { .. }
                | GraphOp::Neighbors { .. }
                | GraphOp::NeighborsMulti { .. }
                | GraphOp::Path { .. }
                | GraphOp::Subgraph { .. }
                | GraphOp::RagFusion { .. }
                | GraphOp::Algo { .. }
                | GraphOp::Match { .. }
                | GraphOp::MatchContinuation { .. }
                | GraphOp::MatchVarLenResume { .. }
                | GraphOp::BspSuperstep(_)
                | GraphOp::WccSuperstep(_)
                | GraphOp::SetNodeLabels { .. }
                | GraphOp::RemoveNodeLabels { .. }
                | GraphOp::TemporalNeighbors { .. }
                | GraphOp::Stats { .. }
                | GraphOp::NodePresenceGuard { .. }
                | GraphOp::NodePresenceRead { .. }
                | GraphOp::TruncateEdges { .. } => document_id.unwrap_or(collection),
            };
            return VShardId::from_key(node_key.as_bytes());
        }
        CollectionKey::from_bare(self.database_id(), collection).vshard()
    }
}
