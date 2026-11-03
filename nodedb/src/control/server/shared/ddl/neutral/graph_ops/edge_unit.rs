// SPDX-License-Identifier: BUSL-1.1

//! The steps every edge-list write shares: `GRAPH INSERT EDGE`,
//! `GRAPH DELETE EDGE`, and their native batch opcodes.
//!
//! A write is admitted whole (edge cap, write grant), binds its distinct
//! endpoints in one surrogate exchange, and is written as one unit. A
//! single edge is one `EdgePut` / `EdgeDelete`. Several edges are one
//! `EdgePutBatch` / `EdgeDeleteBatch`. When the edges' homes span more than
//! one vShard, that unit is one Calvin transaction, and the sequencer slices
//! the batch by home pair. Inside a transaction block every edge stages on
//! its own homes instead.

use std::collections::{BTreeSet, HashMap, HashSet};

use nodedb_physical::physical_plan::{BatchEdge, GraphOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_types::protocol::edge_batch_over_cap;
use nodedb_types::{CollectionKey, Surrogate};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{build_static_tx_class, submit_calvin_routed_write};
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::ddl::neutral::read_gate::CollectionReadGate;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::surrogate_exchange::assign_surrogates_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, RecordHomes, TenantId, TraceId, VShardId};

use super::super::super::result::DdlError;
use super::edge::{EdgeHomes, response_affected};
use super::support::{data_plane_verdict, ddl_err};

/// SQLSTATE `program_limit_exceeded`.
const PROGRAM_LIMIT_EXCEEDED: &str = "54000";

/// An edge ready to write: its `EdgePut` / `EdgeDelete`, after write
/// policy, and its homes.
pub(super) struct PreparedEdge {
    pub op: GraphOp,
    pub homes: RecordHomes,
}

/// Whether a unit inserts or deletes its edges.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum EdgeWriteKind {
    Insert,
    Delete,
}

impl EdgeWriteKind {
    /// The statement name an error message uses.
    pub(super) fn statement(self) -> &'static str {
        match self {
            Self::Insert => "GRAPH INSERT EDGE",
            Self::Delete => "GRAPH DELETE EDGE",
        }
    }
}

/// Refuse a write of `count` edges over the per-statement cap, then
/// require the caller's write grant on `collection`. Runs before any
/// endpoint is bound or any edge is resolved, so a refused write touches
/// nothing.
pub(super) fn admit_edge_write(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    kind: EdgeWriteKind,
    collection: &str,
    count: usize,
) -> Result<(), DdlError> {
    let statement = kind.statement();
    if collection.is_empty() {
        return Err(ddl_err(
            "42601",
            format!("{statement} requires IN <collection>"),
        ));
    }
    if count == 0 {
        return Err(ddl_err(
            "42601",
            format!("{statement} requires FROM, TO and TYPE"),
        ));
    }
    if let Some(refusal) = edge_batch_over_cap(statement, count) {
        return Err(ddl_err(PROGRAM_LIMIT_EXCEEDED, refusal));
    }
    CollectionReadGate::for_request(state, identity, database_id)?
        .authorize_permission(collection, Permission::Write)
}

/// Bind every distinct endpoint of `endpoints` in one surrogate exchange.
pub(super) async fn endpoint_surrogates(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    endpoints: &[&str],
) -> Result<HashMap<String, Surrogate>, DdlError> {
    let mut ids: Vec<&str> = Vec::with_capacity(endpoints.len());
    let mut seen: HashSet<&str> = HashSet::with_capacity(endpoints.len());
    for &id in endpoints {
        if seen.insert(id) {
            ids.push(id);
        }
    }
    let keys: Vec<&[u8]> = ids.iter().map(|id| id.as_bytes()).collect();
    let bound = assign_surrogates_routed(
        state,
        CollectionKey::from_bare(database_id, collection),
        tenant_id,
        &keys,
        TraceId::ZERO,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;
    if bound.len() != ids.len() {
        return Err(DdlError::internal(format!(
            "surrogate exchange bound {} of {} edge endpoints in '{collection}'",
            bound.len(),
            ids.len()
        )));
    }
    Ok(ids.into_iter().map(str::to_owned).zip(bound).collect())
}

/// The surrogate [`endpoint_surrogates`] bound for `node`.
pub(super) fn bound_surrogate(
    surrogates: &HashMap<String, Surrogate>,
    node: &str,
) -> Result<Surrogate, DdlError> {
    surrogates
        .get(node)
        .copied()
        .ok_or_else(|| DdlError::internal(format!("edge endpoint '{node}' has no bound surrogate")))
}

/// Whether the session runs a transaction block, where edges stage instead
/// of applying.
pub(super) fn in_transaction_block(txn_ctx: &DmlTxnCtx<'_>) -> bool {
    txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock
}

/// Stage every edge of `prepared` into the session transaction's overlay on
/// each of its homes. Returns the staged count.
pub(super) async fn stage_edges(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    prepared: Vec<PreparedEdge>,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<u64, DdlError> {
    let mut staged = 0u64;
    for PreparedEdge { op, homes } in prepared {
        staged += super::edge_stage::stage_edge_dual_home(
            state,
            tenant_id,
            database_id,
            EdgeHomes {
                vsrc: homes.owner(),
                vdst: homes.second(),
                single_home: homes.is_single(),
            },
            op,
            txn_ctx,
        )
        .await?;
    }
    Ok(staged)
}

/// Write `prepared` as one unit: a direct write when every edge homes on one
/// vShard, otherwise one Calvin transaction over the union of the homes.
/// Returns the Data Plane's affected count.
pub(super) async fn write_edge_unit(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    kind: EdgeWriteKind,
    mut prepared: Vec<PreparedEdge>,
) -> Result<u64, DdlError> {
    let mut vshards: Vec<VShardId> = Vec::new();
    let mut seen: BTreeSet<u32> = BTreeSet::new();
    for edge in &prepared {
        for vshard in edge.homes.iter() {
            if seen.insert(vshard.as_u32()) {
                vshards.push(vshard);
            }
        }
    }
    let Some(&first_home) = vshards.first() else {
        return Err(DdlError::internal("an edge write has no home vShard"));
    };

    let op = if prepared.len() == 1 {
        let Some(edge) = prepared.pop() else {
            return Err(DdlError::internal("an edge write lost its only edge"));
        };
        edge.op
    } else {
        let edges = prepared
            .into_iter()
            .map(|edge| batch_edge(edge.op))
            .collect::<Result<Vec<_>, _>>()?;
        match kind {
            EdgeWriteKind::Insert => GraphOp::EdgePutBatch { edges },
            EdgeWriteKind::Delete => GraphOp::EdgeDeleteBatch { edges },
        }
    };
    let plan = PhysicalPlan::Graph(op);

    let response = if let [only_home] = vshards.as_slice() {
        // Every endpoint shares one home, so one write there applies the unit.
        crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
            state,
            tenant_id,
            database_id,
            *only_home,
            plan,
            crate::event::EventSource::User,
        )
        .await
        .map_err(|e| DdlError::from_error(&e))?
    } else {
        // Cross-shard: every home commits atomically through Calvin, each
        // running the edges it holds with identical surrogates.
        let task = PhysicalTask {
            tenant_id,
            vshard_id: first_home,
            database_id,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let tx_class =
            build_static_tx_class(&[task], tenant_id, &[]).map_err(|e| DdlError::from_error(&e))?;
        // Every participant of this graph-only transaction reports its
        // answer in its completion ack, so the count arrives on any node.
        submit_calvin_routed_write(state, tx_class)
            .await
            .map_err(|e| DdlError::from_error(&e))?
    };
    data_plane_verdict(&response)?;
    response_affected(&response)
}

/// The batch form of a resolved `EdgePut` or `EdgeDelete`. A batch delete
/// carries no write check: the caller decides the policy for every edge
/// before the unit is built.
fn batch_edge(op: GraphOp) -> Result<BatchEdge, DdlError> {
    match op {
        GraphOp::EdgePut {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            src_surrogate,
            dst_surrogate,
            valid_from_ms,
            valid_until_ms,
        } => Ok(BatchEdge {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            valid_from_ms,
            valid_until_ms,
            properties,
        }),
        GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            src_surrogate,
            dst_surrogate,
            rls_write_check,
        } => {
            if rls_write_check.has_predicate() || rls_write_check.is_pending_injection() {
                return Err(DdlError::internal(format!(
                    "edge delete {src_id} -[{label}]-> {dst_id} reached a batch with an \
                     undecided write policy"
                )));
            }
            Ok(BatchEdge {
                collection,
                src_id,
                label,
                dst_id,
                src_surrogate,
                dst_surrogate,
                valid_from_ms: None,
                valid_until_ms: None,
                properties: Vec::new(),
            })
        }
        other => Err(DdlError::internal(format!(
            "an edge write resolved to a non-edge plan: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate};

    use super::*;

    fn delete(check: RlsWriteCheck) -> GraphOp {
        GraphOp::EdgeDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "g"),
            src_id: "a".into(),
            label: "l".into(),
            dst_id: "b".into(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            rls_write_check: check,
        }
    }

    /// A batch delete has no check slot, so only an edge whose policy is
    /// already decided may join one.
    #[test]
    fn an_undecided_delete_never_joins_a_batch() {
        for check in [
            RlsWriteCheck::Predicate(vec![1, 2, 3]),
            RlsWriteCheck::pending_injection(),
        ] {
            assert!(batch_edge(delete(check)).is_err());
        }
        for check in [
            RlsWriteCheck::NoPolicyApplies,
            RlsWriteCheck::decided_earlier_in_request(),
        ] {
            let edge = batch_edge(delete(check)).expect("a decided delete batches");
            assert_eq!((edge.src_id.as_str(), edge.dst_id.as_str()), ("a", "b"));
            assert!(edge.properties.is_empty());
        }
    }

    #[test]
    fn a_non_edge_plan_never_joins_a_batch() {
        let op = GraphOp::SetNodeLabels {
            node_id: "a".into(),
            labels: vec!["P".into()],
        };
        assert!(batch_edge(op).is_err());
    }
}
