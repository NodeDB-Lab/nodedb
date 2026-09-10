// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral forwarding of per-session planning overrides into the
//! [`QueryContext`] before a plan call.
//!
//! Every server entrypoint (pgwire, native) resolves the same set of session
//! GUCs into the shared query context immediately before planning: the tenant's
//! vector-dimension quota, the force-shuffle-join / force-shuffle-aggregate
//! overrides and their partition counts, and the broadcast / shuffle-aggregate
//! cost thresholds. Housing that resolution here keeps the two transports from
//! diverging — a GUC honored on pgwire but silently dropped on native (the
//! canonical transport) is a real parity defect, not a cosmetic one.

use crate::control::planner::context::QueryContext;
use crate::control::planner::context::query::DEFAULT_SHUFFLE_AGG_THRESHOLD;
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::session::{SessionId, SessionStore};

/// Parse a PostgreSQL-style boolean session value. Returns `None` for any value
/// that is not a recognized boolean spelling, so a SET handler can reject it
/// with `22023` rather than silently storing garbage.
pub fn parse_bool_session_value(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "t" | "yes" | "y" | "1" => Some(true),
        "off" | "false" | "f" | "no" | "n" | "0" => Some(false),
        _ => None,
    }
}

/// The subset of resolved override state a caller needs after forwarding to
/// decide whether the plan cache must be bypassed. The cache key encodes none of
/// these strategy knobs, so a plan built (or served) under any of them would
/// otherwise leak a strategy-specific plan into a later default query.
#[derive(Debug, Clone, Copy)]
pub struct PlanningOverrideFlags {
    /// `nodedb.force_shuffle_join` was engaged for this plan call.
    pub force_shuffle_join: bool,
    /// `nodedb.force_shuffle_agg` was engaged for this plan call.
    pub force_shuffle_agg: bool,
    /// `nodedb.broadcast_threshold_bytes` was set to a value differing from the
    /// node's tuning default.
    pub threshold_overridden: bool,
    /// `nodedb.shuffle_agg_threshold` was set to a value differing from
    /// [`DEFAULT_SHUFFLE_AGG_THRESHOLD`].
    pub agg_threshold_overridden: bool,
}

impl PlanningOverrideFlags {
    /// Whether any engaged override makes a cached (or to-be-cached) plan's
    /// strategy assumption unsafe to share across this session's queries.
    pub fn bypass_plan_cache(&self) -> bool {
        self.force_shuffle_join
            || self.force_shuffle_agg
            || self.threshold_overridden
            || self.agg_threshold_overridden
    }
}

/// Forward every per-session planning GUC from the session parameter bag into
/// `query_ctx` for the next plan call, returning the flags a caller needs for
/// plan-cache bypass. Protocol-neutral: pgwire and native call this identically
/// so both honor the overrides the same way. Values were validated at SET time,
/// so a parse miss here defaults to the documented off / zero / node-default.
pub fn apply_planning_session_overrides(
    query_ctx: &QueryContext,
    sessions: &SessionStore,
    state: &SharedState,
    session_id: impl Into<SessionId>,
    tenant_id: TenantId,
) -> PlanningOverrideFlags {
    let session_id = session_id.into();
    // Propagate the tenant's vector-dimension quota so ConvertContext can reject
    // oversized vectors without an extra lock inside the planner.
    {
        let tenants = match state.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };
        query_ctx.set_max_vector_dim(tenants.quota(tenant_id).max_vector_dim);
    }

    // Sequence accessors resolve at plan time, so the planner needs this
    // connection's `currval` map before it plans.
    query_ctx.set_session_sequences(sessions.sequence_values(session_id));

    // Distributed shuffle-join override (`SET nodedb.force_shuffle_join = on`
    // and, optionally, `SET nodedb.shuffle_num_parts = N`).
    let force_shuffle_join = sessions
        .get_parameter(session_id, "nodedb.force_shuffle_join")
        .as_deref()
        .and_then(parse_bool_session_value)
        .unwrap_or(false);
    let shuffle_num_parts = sessions
        .get_parameter(session_id, "nodedb.shuffle_num_parts")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    query_ctx.set_force_shuffle_join(force_shuffle_join, shuffle_num_parts);

    // Distributed shuffle-aggregate override (`SET nodedb.force_shuffle_agg = on`
    // and, optionally, `SET nodedb.shuffle_agg_num_parts = N`).
    let force_shuffle_agg = sessions
        .get_parameter(session_id, "nodedb.force_shuffle_agg")
        .as_deref()
        .and_then(parse_bool_session_value)
        .unwrap_or(false);
    let shuffle_agg_num_parts = sessions
        .get_parameter(session_id, "nodedb.shuffle_agg_num_parts")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    query_ctx.set_force_shuffle_agg(force_shuffle_agg, shuffle_agg_num_parts);

    // Auto-shuffle cost threshold: the session override
    // `nodedb.broadcast_threshold_bytes` when set, otherwise the node's
    // configured `[tuning.cluster_transport] broadcast_threshold_bytes`. Passing
    // the resolved value (not just the override) makes a SET then RESET correctly
    // revert to the tuning default for this session.
    let tuning_threshold = state.tuning.cluster_transport.broadcast_threshold_bytes;
    let session_threshold = sessions
        .get_parameter(session_id, "nodedb.broadcast_threshold_bytes")
        .and_then(|v| v.parse::<usize>().ok());
    let broadcast_threshold_bytes = session_threshold.unwrap_or(tuning_threshold);
    query_ctx.set_broadcast_threshold_bytes(broadcast_threshold_bytes);

    // Auto-shuffle-aggregate cost threshold (distinct-group units): the session
    // override `nodedb.shuffle_agg_threshold` when set, otherwise the planner
    // default. Passing the resolved value keeps a SET then RESET reverting to the
    // default. Mirrors `broadcast_threshold_bytes`.
    let session_agg_threshold = sessions
        .get_parameter(session_id, "nodedb.shuffle_agg_threshold")
        .and_then(|v| v.parse::<usize>().ok());
    let shuffle_agg_threshold = session_agg_threshold.unwrap_or(DEFAULT_SHUFFLE_AGG_THRESHOLD);
    query_ctx.set_shuffle_agg_threshold(shuffle_agg_threshold);

    PlanningOverrideFlags {
        force_shuffle_join,
        force_shuffle_agg,
        threshold_overridden: session_threshold.is_some_and(|t| t != tuning_threshold),
        agg_threshold_overridden: session_agg_threshold
            .is_some_and(|t| t != DEFAULT_SHUFFLE_AGG_THRESHOLD),
    }
}
