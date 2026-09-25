// SPDX-License-Identifier: BUSL-1.1

//! The holder's renewal loop.
//!
//! Every renewal interval the node computes its confirmed coverage (see
//! [`super::coverage`]) and sends it to the metadata leader. A grant extends
//! the lease from the moment the request left. A withheld or failed renewal
//! extends nothing, so the lease lapses unless a later renewal succeeds.

use std::sync::Arc;
use std::time::Instant;

use nodedb_cluster::{
    AuthLeaseRenewOutcome, AuthLeaseRenewRequest, AuthLeaseRenewResponse, GroupCoverage, RaftRpc,
};

use crate::control::shutdown::ShutdownReceiver;
use crate::control::state::SharedState;

use super::coverage::confirmed_coverage;
use super::leadership::{metadata_leader, send_to_leader};
use super::timing::LeaseTiming;

/// Renew this node's lease until shutdown.
pub async fn run_renew_loop(
    state: Arc<SharedState>,
    timing: LeaseTiming,
    mut shutdown: ShutdownReceiver,
) {
    let mut confirmed: Vec<GroupCoverage> = Vec::new();
    loop {
        match confirmed_coverage(&state, &confirmed, timing.renew_every).await {
            Ok(coverage) => {
                confirmed = coverage;
                renew_once(&state, timing, &confirmed).await;
            }
            Err(error) => {
                tracing::warn!(%error, "authorization lease: coverage could not be computed");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(timing.renew_every) => {}
            _ = shutdown.wait_cancelled() => return,
        }
    }
}

/// Send one renewal and install a granted lease.
async fn renew_once(state: &SharedState, timing: LeaseTiming, coverage: &[GroupCoverage]) {
    let Some((leader_id, _)) = metadata_leader(state).filter(|(leader, _)| *leader != 0) else {
        return;
    };
    let request = AuthLeaseRenewRequest {
        node_id: state.node_id,
        coverage: coverage.to_vec(),
    };
    let sent_at = Instant::now();
    let response = if leader_id == state.node_id {
        match state.authorization_fence.leader() {
            Some(service) => service.renew_lease(request).await,
            None => return,
        }
    } else {
        match send_to_leader(
            state,
            leader_id,
            RaftRpc::AuthLeaseRenewRequest(request),
            timing.lease,
        )
        .await
        {
            Ok(RaftRpc::AuthLeaseRenewResponse(response)) => response,
            Ok(other) => {
                tracing::warn!(
                    leader_id,
                    "authorization lease: unexpected renewal reply {other:?}"
                );
                return;
            }
            Err(error) => {
                tracing::debug!(%error, "authorization lease: renewal not delivered");
                return;
            }
        }
    };
    install(state, timing, sent_at, response);
}

fn install(
    state: &SharedState,
    timing: LeaseTiming,
    sent_at: Instant,
    response: AuthLeaseRenewResponse,
) {
    match response.outcome {
        AuthLeaseRenewOutcome::Granted { lease_ms } => {
            let granted = std::time::Duration::from_millis(lease_ms);
            state
                .authorization_fence
                .holder()
                .install(timing.holder_expiry(sent_at, granted));
        }
        AuthLeaseRenewOutcome::Withheld => {
            tracing::debug!("authorization lease: renewal withheld until coverage catches up");
        }
        AuthLeaseRenewOutcome::NotLeader { leader_hint } => {
            tracing::debug!(
                ?leader_hint,
                "authorization lease: renewal reached a non-leader"
            );
        }
    }
}
