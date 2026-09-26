// SPDX-License-Identifier: BUSL-1.1

//! Edge mutation handlers: GRAPH INSERT EDGE, GRAPH DELETE EDGE,
//! GRAPH LABEL / GRAPH UNLABEL.
//!
//! Each function receives already-parsed typed fields; handlers never touch
//! `&str` parse paths.

use nodedb_sql::ddl_ast::GraphProperties;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::calvin::{build_static_tx_class, submit_calvin_routed};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::server::shared::sql::staging_predicates::require_affected_count;
use crate::control::server::surrogate_exchange::assign_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TraceId, VShardId};
use nodedb_physical::physical_plan::GraphOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::result::{DdlError, DdlResult};
use super::edge_parse::{properties_to_json, validate_edge_label};
use super::support::{data_plane_verdict, ddl_err};

/// Read the affected count off a Data-Plane response, mapping a missing count
/// to a [`DdlError`] via `ddl_err` — never a default.
fn response_affected(response: &crate::bridge::envelope::Response) -> Result<u64, DdlError> {
    require_affected_count(response.payload.as_bytes()).map_err(|e| {
        ddl_err(
            "XX000",
            format!("edge write response is missing its affected count: {e}"),
        )
    })
}

/// `GRAPH INSERT EDGE IN '<collection>' FROM '<src>' TO '<dst>' TYPE '<label>' [PROPERTIES '<json>' | { ... }]`
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
    let properties_json = properties_to_json(properties)?;
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
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

    // Dual-home: a cross-shard edge must be written on the home vShard of both src
    // and dst, or reverse/IN traversal never finds it.
    let vsrc = VShardId::from_key(src.as_bytes());
    let vdst = VShardId::from_key(dst.as_bytes());

    let src_surrogate = assign_surrogate_routed(
        state,
        vsrc,
        database_id,
        tenant_id,
        &collection,
        src.as_bytes(),
        TraceId::ZERO,
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))?;
    let dst_surrogate = assign_surrogate_routed(
        state,
        vdst,
        database_id,
        tenant_id,
        &collection,
        dst.as_bytes(),
        TraceId::ZERO,
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

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
            properties: properties_json.into_bytes(),
            src_surrogate,
            dst_surrogate,
        },
    )?;

    // Calvin cross-shard atomicity needs cluster mode with a wired sequencer. In
    // single-node, one write already lands both EDGES and REVERSE_EDGES locally.
    let calvin_available =
        state.cluster_transport.is_some() && state.sequencer_inbox.get().is_some();
    let single_home = vsrc == vdst || !calvin_available;

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
        // F1a fast path: single-home write to `vsrc` covers both forward and reverse.
        let plan = PhysicalPlan::Graph(edge_put);
        let response =
            crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                state,
                tenant_id,
                database_id,
                vsrc,
                plan,
                TraceId::ZERO,
                crate::event::EventSource::User,
            )
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
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
        let tx_class = build_static_tx_class(&[task], tenant_id, &[])
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        let response = submit_calvin_routed(state, tx_class)
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        match response {
            Some(response) => {
                data_plane_verdict(&response)?;
                response_affected(&response)?
            }
            // `plans_have_primary_write` treats a Graph-only tx_class (no
            // accompanying primary DML, exactly this DSL's own tx_class) as
            // primary, so `commit_apply_tail` always deposits this
            // participant's applied response. A missing deposit here is a
            // scheduler invariant violation, never a value to guess.
            None => {
                return Err(ddl_err(
                    "XX000",
                    "cross-shard edge insert completed with no applied response to read \
                     its affected count from",
                ));
            }
        }
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
/// the reverse row. `single_home` is true when both share one vShard or Calvin
/// is unavailable. Bundled for [`stage_edge_dual_home`](super::edge_stage::stage_edge_dual_home).
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
    let vsrc = VShardId::from_key(src.as_bytes());
    let vdst = VShardId::from_key(dst.as_bytes());

    let src_surrogate = assign_surrogate_routed(
        state,
        vsrc,
        database_id,
        tenant_id,
        &collection,
        src.as_bytes(),
        TraceId::ZERO,
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))?;
    let dst_surrogate = assign_surrogate_routed(
        state,
        vdst,
        database_id,
        tenant_id,
        &collection,
        dst.as_bytes(),
        TraceId::ZERO,
    )
    .await
    .map_err(|e| ddl_err("XX000", e.to_string()))?;

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

    // Calvin needs cluster mode with a wired sequencer; single-node already
    // tombstones both EDGES and REVERSE_EDGES in one write.
    let calvin_available =
        state.cluster_transport.is_some() && state.sequencer_inbox.get().is_some();
    let single_home = vsrc == vdst || !calvin_available;

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
        && state.async_raft_proposer().is_some()
    {
        let ctx = crate::control::write_resolve::WriteResolveContext {
            tenant_id,
            database_id,
        };
        let response = crate::control::write_resolve::run_write_resolve(state, ctx, &*resolver)
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        return Ok(vec![DdlResult::Status {
            command: "DELETE EDGE".to_string(),
            rows_affected: Some(response_affected(&response)?),
        }]);
    }

    let affected = if single_home {
        // F1a fast path: single-home write to `vsrc` tombstones both rows together.
        let plan = PhysicalPlan::Graph(edge_delete);
        let response =
            crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_sync_response(
                state,
                tenant_id,
                database_id,
                vsrc,
                plan,
                TraceId::ZERO,
                crate::event::EventSource::User,
            )
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
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
        let tx_class = build_static_tx_class(&[task], tenant_id, &[])
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        let response = submit_calvin_routed(state, tx_class)
            .await
            .map_err(|e| ddl_err("XX000", e.to_string()))?;
        match response {
            Some(response) => {
                data_plane_verdict(&response)?;
                response_affected(&response)?
            }
            // `plans_have_primary_write` treats a Graph-only tx_class (no
            // accompanying primary DML, exactly this DSL's own tx_class) as
            // primary, so `commit_apply_tail` always deposits this
            // participant's applied response. A delete's count is not
            // deterministic from the plan alone (the edge may already be
            // absent), so a missing deposit here is a scheduler invariant
            // violation, never a value to guess.
            None => {
                return Err(ddl_err(
                    "XX000",
                    "cross-shard edge delete completed with no applied response to read \
                     its affected count from",
                ));
            }
        }
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
    // No redb durability — a WAL record is the bitset's only backing. The
    // record's outcome-floor window opens before the append and closes from
    // the dispatch's outcome.
    let owner = crate::control::server::dispatch_utils::RecordOwner {
        tenant_id,
        database_id: DatabaseId::DEFAULT,
        vshard_id,
    };
    let minted = crate::control::server::dispatch_utils::MintedRecords::open(&state.outcome_floor);
    if let Err(e) = minted.append_plan(
        &state.wal,
        owner,
        &plan,
        // The same source the edge write is dispatched with below.
        crate::event::EventSource::User,
    ) {
        // Any record appended before the error never reaches a core.
        minted
            .cancel(&state.wal, owner, 0)
            .await
            .map_err(|c| ddl_err("XX000", c.to_string()))?;
        return Err(ddl_err("XX000", e.to_string()));
    }

    let response =
        crate::control::server::sync::raft_dispatch::dispatch_trusted_internal_minted_sync_response(
            state,
            owner,
            plan,
            crate::event::EventSource::User,
            minted,
        )
        .await
        .map_err(|e| ddl_err("XX000", e.to_string()))?;
    data_plane_verdict(&response)?;

    let tag = if remove { "UNLABEL" } else { "LABEL" };
    Ok(vec![DdlResult::Status {
        command: tag.to_string(),
        rows_affected: Some(response_affected(&response)?),
    }])
}
