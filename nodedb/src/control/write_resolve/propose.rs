// SPDX-License-Identifier: BUSL-1.1

//! Propose a resolved write through Raft, exactly as an ordinary replicated
//! write is proposed.

use std::sync::atomic::Ordering;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::state::SharedState;
use crate::control::wal_replication::{
    ReplicableWrite, propose_replicated_entry, to_replicated_entry,
};
use crate::types::{RequestId, VShardId};

use super::resolver::WriteResolveContext;

/// One propose attempt's outcome.
pub(super) enum ProposeOutcome {
    /// Committed and applied; carries the response the statement returns.
    Applied(Response),
    /// The shipped row set no longer matches current state (concurrent
    /// drift). Nothing was applied — re-resolve and retry.
    RetryRequired,
}

/// Propose `plan` — a resolved write — through the live Raft proposer and
/// await commit + apply.
pub(super) async fn propose_resolved(
    state: &SharedState,
    ctx: WriteResolveContext,
    collection: &str,
    vshard_id: VShardId,
    plan: PhysicalPlan,
) -> crate::Result<ProposeOutcome> {
    let proposer = state.async_raft_proposer()?;
    // The resolved op stamps `DecidedEarlierInRequest`, so this never refuses.
    let replicable = ReplicableWrite::decide_for_replication(&plan)?;
    let entry = to_replicated_entry(ctx.tenant_id, ctx.database_id, vshard_id, &replicable)?
        .ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "write-resolve: resolved plan for '{collection}' did not map to a replicated write"
            ),
        })?;

    let deadline = crate::control::wal_replication::statement_propose_deadline(state);
    match propose_replicated_entry(state, proposer, entry, deadline).await {
        Ok((payload, write_versions)) => {
            let request_id =
                RequestId::new(state.request_id_counter.fetch_add(1, Ordering::Relaxed));
            let response = Response {
                request_id,
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload: payload.into(),
                // A write carries no read watermark.
                watermark_lsn: crate::types::Lsn::ZERO,
                error_code: None,
                stage_vote: None,
                read_versions: write_versions,
                write_set: Vec::new(),
            };
            Ok(ProposeOutcome::Applied(response))
        }
        Err(crate::Error::DataPlane(ErrorCode::OllpRetryRequired)) => {
            Ok(ProposeOutcome::RetryRequired)
        }
        Err(e) => Err(e),
    }
}
