// SPDX-License-Identifier: BUSL-1.1

//! Edge mutation handlers: GRAPH INSERT EDGE, GRAPH DELETE EDGE,
//! GRAPH LABEL / GRAPH UNLABEL.
//!
//! Each function receives already-parsed typed fields; handlers never touch
//! `&str` parse paths.

use nodedb_sql::ddl_ast::GraphProperties;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{build_static_tx_class, submit_calvin_routed_write};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::shared::sql::staging_predicates::require_affected_count;
use crate::control::server::surrogate_exchange::assign_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, RecordHomes, TraceId, VShardId};
use nodedb_physical::physical_plan::GraphOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::result::{DdlError, DdlResult};
use super::edge_parse::{properties_to_msgpack, validate_edge_label};
use super::support::{data_plane_verdict, ddl_err};

/// Read the affected count off a Data-Plane response. A missing count is an
/// error, never a default.
fn response_affected(response: &crate::bridge::envelope::Response) -> Result<u64, DdlError> {
    require_affected_count(response.payload.as_bytes()).map_err(|e| {
        DdlError::from_error_in_context("edge write response is missing its affected count", &e)
    })
}

/// `GRAPH INSERT EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>'`
/// `[PROPERTIES '<json object>' | { ... }]`
///
/// Edge identity is bundled in [`EdgeRef`] to stay within the argument budget.
pub async fn insert_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    edge: EdgeRef,
    properties: GraphProperties,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let EdgeRef {
        collection,
        src,
        dst,
        label,
    } = edge;
    if collection.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH INSERT EDGE requires IN <collection>",
        ));
    }
    if src.is_empty() || dst.is_empty() {
        return Err(ddl_err("42601", "GRAPH INSERT EDGE requires FROM and TO"));
    }
    validate_edge_label(&label)?;
    let properties = properties_to_msgpack(properties)?;
    let tenant_id = identity.tenant_id;

    // Flags the collection edge-bearing so a later predicate DELETE routes through
    // OLLP instead of the fast path. Idempotent.
    crate::control::planner::implicit_edges::mark_collection_edge_bearing(
        state,
        database_id,
        tenant_id,
        &collection,
    )
    .await
    .map_err(|e| DdlError::from_error(&e))?;

    // Dual-home: a cross-shard edge must be written on the home vShard of both src
    // and dst, or reverse/IN traversal never finds it.
    let homes = RecordHomes::edge(&src, &dst);
    let (vsrc, vdst) = (homes.owner(), homes.second());

    let key = nodedb_types::CollectionKey::from_bare(database_id, &collection);
    let src_surrogate =
        assign_surrogate_routed(state, key, tenant_id, src.as_bytes(), TraceId::ZERO)
            .await
            .map_err(|e| DdlError::from_error(&e))?;
    let dst_surrogate =
        assign_surrogate_routed(state, key, tenant_id, dst.as_bytes(), TraceId::ZERO)
            .await
            .map_err(|e| DdlError::from_error(&e))?;

    // Write policy decides the `PROPERTIES` image before staging: this handler
    // dispatches as trusted internal work, so nothing downstream resolves a policy.
    let edge_put = super::edge_rls::resolve_edge_write_rls(
        state,
        identity,
        database_id,
        GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            src_id: src,
            label,
            dst_id: dst,
            properties,
            src_surrogate,
            dst_surrogate,
        },
    )?;

    // A cross-shard edge commits on both homes atomically through Calvin.
    let single_home = homes.is_single();

    // In a transaction, an insert stages into `GraphTxnOverlay` instead of applying now
    // (COMMIT replays, ROLLBACK discards); a cross-shard edge stages into both endpoints.
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock {
        let affected = super::edge_stage::stage_edge_dual_home(
            state,
            tenant_id,
            database_id,
            EdgeHomes {
                vsrc,
                vdst,
                single_home,
            },
            edge_put,
            txn_ctx,
        )
        .await?;
        return Ok(vec![DdlResult::Status {
            command: "INSERT EDGE".to_string(),
            rows_affected: Some(affected),
        }]);
    }

    let affected = if single_home {
        // Both endpoints share one home, so one write to `vsrc` stores the edge.
        let plan = PhysicalPlan::Graph(edge_put);
        let response =
            crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                state,
                tenant_id,
                database_id,
                vsrc,
                plan,
                crate::event::EventSource::User,
            )
            .await
            .map_err(|e| DdlError::from_error(&e))?;
        data_plane_verdict(&response)?;
        response_affected(&response)?
    } else {
        // Cross-shard: dual-home atomically via Calvin. `build_static_tx_class` enumerates
        // {vsrc, vdst}, each running the same EdgePut with identical surrogates.
        let task = PhysicalTask {
            tenant_id,
            vshard_id: vsrc,
            database_id,
            plan: PhysicalPlan::Graph(edge_put),
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let tx_class =
            build_static_tx_class(&[task], tenant_id, &[]).map_err(|e| DdlError::from_error(&e))?;
        // Every participant of this graph-only transaction reports its
        // answer in its completion ack, so the count arrives on any node.
        let response = submit_calvin_routed_write(state, tx_class)
            .await
            .map_err(|e| DdlError::from_error(&e))?;
        data_plane_verdict(&response)?;
        response_affected(&response)?
    };

    Ok(vec![DdlResult::Status {
        command: "INSERT EDGE".to_string(),
        rows_affected: Some(affected),
    }])
}

/// A parsed edge identity: collection, endpoints, and label. Bundled so
/// [`insert_edge`] and [`delete_edge`] stay within the argument budget.
pub struct EdgeRef {
    pub collection: String,
    pub src: String,
    pub dst: String,
    pub label: String,
}

/// The home vShard(s) an edge resolves to: `vsrc` holds the forward row, `vdst`
/// the reverse row. `single_home` is true when both share one vShard.
/// Bundled for [`stage_edge_dual_home`](super::edge_stage::stage_edge_dual_home).
pub struct EdgeHomes {
    pub vsrc: VShardId,
    pub vdst: VShardId,
    pub single_home: bool,
}

/// `GRAPH DELETE EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>'`
pub async fn delete_edge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    edge: EdgeRef,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let EdgeRef {
        collection,
        src,
        dst,
        label,
    } = edge;
    if collection.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH DELETE EDGE requires IN <collection>",
        ));
    }
    if src.is_empty() || dst.is_empty() {
        return Err(ddl_err("42601", "GRAPH DELETE EDGE requires FROM and TO"));
    }
    validate_edge_label(&label)?;
    let tenant_id = identity.tenant_id;

    // Dual-home: stored forward on `from_key(src)` and reverse on `from_key(dst)`,
    // so delete must tombstone both homes.
    let homes = RecordHomes::edge(&src, &dst);
    let (vsrc, vdst) = (homes.owner(), homes.second());

    let key = nodedb_types::CollectionKey::from_bare(database_id, &collection);
    let src_surrogate =
        assign_surrogate_routed(state, key, tenant_id, src.as_bytes(), TraceId::ZERO)
            .await
            .map_err(|e| DdlError::from_error(&e))?;
    let dst_surrogate =
        assign_surrogate_routed(state, key, tenant_id, dst.as_bytes(), TraceId::ZERO)
            .await
            .map_err(|e| DdlError::from_error(&e))?;

    // A delete carries no image, so the policy compiles into the plan's write-gate
    // slot and is decided in the Data Plane against the edge's stored properties.
    let edge_delete = super::edge_rls::resolve_edge_write_rls(
        state,
        identity,
        database_id,
        GraphOp::EdgeDelete {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            src_id: src,
            label,
            dst_id: dst,
            src_surrogate,
            dst_surrogate,
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        },
    )?;

    // A cross-shard edge delete commits on both homes atomically through Calvin.
    let single_home = homes.is_single();

    // Inside a transaction, an edge delete stages into `GraphTxnOverlay` instead of
    // applying now, so RYOW sees it removed; COMMIT replays it, ROLLBACK discards it.
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock {
        let affected = super::edge_stage::stage_edge_dual_home(
            state,
            tenant_id,
            database_id,
            EdgeHomes {
                vsrc,
                vdst,
                single_home,
            },
            edge_delete,
            txn_ctx,
        )
        .await?;
        return Ok(vec![DdlResult::Status {
            command: "DELETE EDGE".to_string(),
            rows_affected: Some(affected),
        }]);
    }

    // A governed delete can't be proposed with its predicate — a follower has no
    // writing identity to decide it. Resolve against stored properties while it's live.
    if let Some(resolver) =
        crate::control::write_resolve::resolver_for_plan(&PhysicalPlan::Graph(edge_delete.clone()))
    {
        let ctx = crate::control::write_resolve::WriteResolveContext {
            tenant_id,
            database_id,
        };
        let response = crate::control::write_resolve::run_write_resolve(state, ctx, &*resolver)
            .await
            .map_err(|e| DdlError::from_error(&e))?;
        return Ok(vec![DdlResult::Status {
            command: "DELETE EDGE".to_string(),
            rows_affected: Some(response_affected(&response)?),
        }]);
    }

    let affected = if single_home {
        // Both endpoints share one home, so one delete on `vsrc` removes the edge.
        let plan = PhysicalPlan::Graph(edge_delete);
        let response =
            crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                state,
                tenant_id,
                database_id,
                vsrc,
                plan,
                crate::event::EventSource::User,
            )
            .await
            .map_err(|e| DdlError::from_error(&e))?;
        data_plane_verdict(&response)?;
        response_affected(&response)?
    } else {
        // Cross-shard edge: dual-home the delete atomically via Calvin, mirroring
        // the insert path — {vsrc, vdst} each run the same EdgeDelete.
        let task = PhysicalTask {
            tenant_id,
            vshard_id: vsrc,
            database_id,
            plan: PhysicalPlan::Graph(edge_delete),
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let tx_class =
            build_static_tx_class(&[task], tenant_id, &[]).map_err(|e| DdlError::from_error(&e))?;
        // Every participant of this graph-only transaction reports its
        // answer in its completion ack, so the count arrives on any node.
        let response = submit_calvin_routed_write(state, tx_class)
            .await
            .map_err(|e| DdlError::from_error(&e))?;
        data_plane_verdict(&response)?;
        response_affected(&response)?
    };

    Ok(vec![DdlResult::Status {
        command: "DELETE EDGE".to_string(),
        rows_affected: Some(affected),
    }])
}

/// `GRAPH LABEL '<node_id>' AS '<label>' [, '<label2>']`
/// `GRAPH UNLABEL '<node_id>' AS '<label>'`
pub async fn set_node_labels(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    node_id: String,
    labels: Vec<String>,
    remove: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    if node_id.is_empty() {
        return Err(ddl_err(
            "42601",
            "GRAPH LABEL/UNLABEL requires a quoted node id",
        ));
    }
    if labels.is_empty() {
        return Err(ddl_err("42601", "missing AS '<label>' [, '<label2>']"));
    }

    let tenant_id = identity.tenant_id;
    let vshard_id = VShardId::from_key(node_id.as_bytes());

    let plan = if remove {
        PhysicalPlan::Graph(GraphOp::RemoveNodeLabels { node_id, labels })
    } else {
        PhysicalPlan::Graph(GraphOp::SetNodeLabels { node_id, labels })
    };

    // Single-keyed on `node_id`, so single-home: route to `from_key(node_id)`.
    // No redb durability: the WAL record the Raft entry's apply appends on
    // every replica is the bitset's only backing.
    let response =
        crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
            state,
            tenant_id,
            DatabaseId::DEFAULT,
            vshard_id,
            plan,
            crate::event::EventSource::User,
        )
        .await
        .map_err(|e| DdlError::from_error(&e))?;
    data_plane_verdict(&response)?;

    let tag = if remove { "UNLABEL" } else { "LABEL" };
    Ok(vec![DdlResult::Status {
        command: tag.to_string(),
        rows_affected: Some(response_affected(&response)?),
    }])
}
