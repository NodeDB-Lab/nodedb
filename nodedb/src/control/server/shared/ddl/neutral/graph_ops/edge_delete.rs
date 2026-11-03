// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH DELETE EDGE` and the native `EdgeDeleteBatch`: one or more edges
//! out of one collection, deleted as one unit (see `edge_unit`).
//!
//! A delete carries no image of its own. The collection's write policy is
//! decided per edge against that edge's stored properties:
//!
//! - Inside a transaction block each edge stages with its compiled
//!   predicate, and the Data Plane decides it against the stored edge.
//! - Outside a block, every governed edge is decided first by a read-only
//!   resolve on its source home. One refused edge refuses the whole
//!   statement before any edge is written. The unit then applies with the
//!   policy decided, as a governed single-edge delete does.

use nodedb_physical::physical_plan::GraphOp;
use nodedb_sql::ddl_ast::GraphEdgeKey;
use nodedb_types::protocol::EdgeKey;
use nodedb_types::{QualifiedCollection, RlsWriteCheck};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::control::write_resolve::{WriteResolveContext, resolver_for_plan};
use crate::types::{DatabaseId, RecordHomes};

use super::super::super::result::{DdlError, DdlResult};
use super::edge_parse::validate_edge_label;
use super::edge_unit::{
    EdgeWriteKind, PreparedEdge, admit_edge_write, bound_surrogate, endpoint_surrogates,
    in_transaction_block, stage_edges, write_edge_unit,
};
use super::support::ddl_err;

/// One edge to delete: its endpoints and label.
pub struct EdgeRemoval {
    pub src: String,
    pub dst: String,
    pub label: String,
}

impl From<GraphEdgeKey> for EdgeRemoval {
    fn from(key: GraphEdgeKey) -> Self {
        Self {
            src: key.src,
            dst: key.dst,
            label: key.label,
        }
    }
}

impl From<EdgeKey> for EdgeRemoval {
    fn from(key: EdgeKey) -> Self {
        Self {
            src: key.src.as_str().to_owned(),
            dst: key.dst.as_str().to_owned(),
            label: key.label,
        }
    }
}

/// Delete `edges` from `collection` as one unit. The command tag counts the
/// edges that were live and are now deleted.
pub async fn delete_edges(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: String,
    edges: Vec<EdgeRemoval>,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    admit_edge_write(
        state,
        identity,
        database_id,
        EdgeWriteKind::Delete,
        &collection,
        edges.len(),
    )?;
    for edge in &edges {
        if edge.src.is_empty() || edge.dst.is_empty() {
            return Err(ddl_err("42601", "GRAPH DELETE EDGE requires FROM and TO"));
        }
        validate_edge_label(&edge.label)?;
    }
    let tenant_id = identity.tenant_id;

    let surrogates = endpoint_surrogates(
        state,
        database_id,
        tenant_id,
        &collection,
        &edges
            .iter()
            .flat_map(|edge| [edge.src.as_str(), edge.dst.as_str()])
            .collect::<Vec<_>>(),
    )
    .await?;
    let mut prepared = Vec::with_capacity(edges.len());
    for EdgeRemoval { src, dst, label } in edges {
        // Dual-home: stored forward on `from_key(src)` and reverse on
        // `from_key(dst)`, so a delete tombstones both homes.
        let homes = RecordHomes::edge(&src, &dst);
        let src_surrogate = bound_surrogate(&surrogates, &src)?;
        let dst_surrogate = bound_surrogate(&surrogates, &dst)?;
        // The policy compiles into the plan's write-gate slot, to be decided
        // against the edge's stored properties.
        let op = super::edge_rls::resolve_edge_write_rls(
            state,
            identity,
            database_id,
            GraphOp::EdgeDelete {
                collection: QualifiedCollection::new(database_id, &collection),
                src_id: src,
                label,
                dst_id: dst,
                src_surrogate,
                dst_surrogate,
                rls_write_check: RlsWriteCheck::pending_injection(),
            },
        )?;
        prepared.push(PreparedEdge { op, homes });
    }

    // Inside a transaction, each edge delete stages into `GraphTxnOverlay`
    // with its own predicate, so RYOW sees it removed; COMMIT replays it,
    // ROLLBACK discards it.
    let affected = if in_transaction_block(txn_ctx) {
        stage_edges(state, tenant_id, database_id, prepared, txn_ctx).await?
    } else {
        let decided = decide_edge_policies(state, identity, database_id, prepared).await?;
        write_edge_unit(
            state,
            tenant_id,
            database_id,
            EdgeWriteKind::Delete,
            decided,
        )
        .await?
    };

    Ok(vec![DdlResult::Status {
        command: "DELETE EDGE".to_string(),
        rows_affected: Some(affected),
    }])
}

/// Decide the write policy of every governed edge in `prepared` against its
/// stored properties, before any edge is written.
///
/// A governed delete cannot be proposed with its predicate: a follower has
/// no writing identity to decide it. Each governed edge runs the read-only
/// resolve on its source home, which refuses an edge the policy excludes.
/// An admitted edge carries the decided check from then on. An ungoverned
/// edge passes through.
async fn decide_edge_policies(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    prepared: Vec<PreparedEdge>,
) -> Result<Vec<PreparedEdge>, DdlError> {
    let ctx = WriteResolveContext {
        tenant_id: identity.tenant_id,
        database_id,
    };
    let mut decided = Vec::with_capacity(prepared.len());
    for PreparedEdge { op, homes } in prepared {
        let plan = PhysicalPlan::Graph(op);
        let Some(resolver) = resolver_for_plan(&plan) else {
            let PhysicalPlan::Graph(op) = plan else {
                return Err(DdlError::internal(
                    "an edge delete changed shape during policy resolution",
                ));
            };
            decided.push(PreparedEdge { op, homes });
            continue;
        };
        let resolved = resolver
            .resolve(state, ctx, resolver.build_resolve_op())
            .await
            .map_err(|e| DdlError::from_error(&e))?;
        match resolver
            .apply(resolved)
            .map_err(|e| DdlError::from_error(&e))?
        {
            PhysicalPlan::Graph(op) => decided.push(PreparedEdge { op, homes }),
            other => {
                return Err(DdlError::internal(format!(
                    "a governed edge delete resolved to a non-graph plan: {other:?}"
                )));
            }
        }
    }
    Ok(decided)
}
