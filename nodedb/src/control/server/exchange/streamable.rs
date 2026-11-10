// SPDX-License-Identifier: BUSL-1.1

//! Shared streaming-eligibility predicate for lazy query sinks.
//!
//! The pgwire fast path ([`maybe_stream_select`]), the native protocol, and
//! the HTTP-NDJSON route all stream an autocommit, multi-row, unordered SELECT
//! straight to the client instead of materializing it first. This module
//! centralizes the *plan-shape* half of the eligibility check so every sink
//! agrees on what is streamable:
//!
//! - the plan is exactly `Query(Exchange(ExchangeOp{ child, Gather{
//!   as_aggregate: false }}))`, and
//! - `child.is_streamable_unordered_scan()` holds.
//!
//! The caller is responsible for the remaining, sink-specific gates:
//!
//! - **single task** — `tasks.len() == 1` (a multi-task statement, e.g. a
//!   multi-statement simple query, is not streamed),
//! - **no post-set-op** — `post_set_op == PostSetOp::None` (UNION / INTERSECT /
//!   EXCEPT need the full sets), and
//! - **autocommit** — for stateful protocols (pgwire, native), the connection
//!   must not be inside a `BEGIN..COMMIT` block. HTTP is stateless and has no
//!   transaction to check.
//!
//! [`open_streamable_scan`] adds the routing half every sink shares: a scan
//! that reads through an unmaterialized clone never streams. It takes the
//! materialized path, whose clone read merges target and source rows.
//!
//! [`maybe_stream_select`]: crate::control::server::pgwire
//!   ::handler::routing::streaming

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::clone::resolver::reads_live_clone;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, CloneCheckedTask, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use crate::control::state::SharedState;

/// A streamable scan cleared to dispatch as a lazy row stream.
pub(crate) struct StreamableScan {
    /// The gather child, clone-checked and authorized.
    pub checked: CloneCheckedTask,
    /// The gather child plan the stream reads.
    pub child_plan: PhysicalPlan,
    /// Global take-N across the whole stream.
    pub limit: usize,
}

/// Clear `task` to stream, or return `None` so the caller takes the
/// materialized path.
///
/// Applies the plan-shape check of [`streamable_gather_child`], then
/// declines a scan that reads through an unmaterialized clone, then runs the
/// dispatch gate on the gather child. A gate that answers the child itself
/// also declines the stream. The caller still applies the
/// single-task, no-set-op, and autocommit gates first.
pub(crate) async fn open_streamable_scan(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    task: &PhysicalTask,
) -> crate::Result<Option<StreamableScan>> {
    let Some((child_plan, limit)) = streamable_gather_child(&task.plan) else {
        return Ok(None);
    };
    let mut child_task = task.clone();
    child_task.plan = child_plan.clone();
    if reads_live_clone(state, &child_task)? {
        return Ok(None);
    }
    let tenant_id = child_task.tenant_id;
    let emitter = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    let outcome = intercept_and_authorize(InterceptAndAuthorizeParams {
        state,
        task: child_task,
        identity,
        tenant_id,
        permissions: &state.permissions,
        roles: &state.roles,
        emitter: &emitter,
    })
    .await?;
    match outcome {
        // A clone read is read-only, so the materialized path can run it again.
        CloneCheckedOutcome::Handled(_) => Ok(None),
        CloneCheckedOutcome::Proceed(checked) => Ok(Some(StreamableScan {
            checked,
            child_plan,
            limit,
        })),
    }
}

/// If `plan` is a streamable `Exchange{Gather{as_aggregate:false}}` over an
/// unordered scan, return a clone of the gather *child* plan together with the
/// global take-N to apply while streaming. Otherwise return `None`.
///
/// This is the plan-shape half of the eligibility predicate shared by every
/// lazy query sink — the caller must still apply the single-task, no-set-op,
/// and (for stateful protocols) autocommit gates before streaming.
pub(crate) fn streamable_gather_child(plan: &PhysicalPlan) -> Option<(PhysicalPlan, usize)> {
    let PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
        child,
        mode: ExchangeMode::Gather {
            as_aggregate: false,
        },
    })) = plan
    else {
        return None;
    };

    if !child.is_streamable_unordered_scan() {
        return None;
    }

    let limit = child.streamable_scan_limit();
    Some(((**child).clone(), limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, ExchangeMode, ExchangeOp, QueryOp};

    fn unordered_scan() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            filters: Vec::new(),
            limit: 1234,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        })
    }

    fn gather(child: PhysicalPlan, as_aggregate: bool) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(child),
            mode: ExchangeMode::Gather { as_aggregate },
        }))
    }

    #[test]
    fn streamable_gather_over_scan_yields_child_and_limit() {
        let plan = gather(unordered_scan(), false);
        let (child, limit) = streamable_gather_child(&plan).expect("eligible");
        assert!(child.is_streamable_unordered_scan());
        assert_eq!(limit, 1234);
    }

    #[test]
    fn aggregate_gather_is_not_streamable() {
        let plan = gather(unordered_scan(), true);
        assert!(streamable_gather_child(&plan).is_none());
    }

    #[test]
    fn bare_scan_without_exchange_is_not_streamable() {
        assert!(streamable_gather_child(&unordered_scan()).is_none());
    }
}
