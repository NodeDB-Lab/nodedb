// SPDX-License-Identifier: BUSL-1.1

//! The metadata leader's reclaim of a DDL preparation lease whose owner died,
//! left, or held it past its lease.
//!
//! One loop runs on every node and acts only while the node leads the
//! metadata group. A proposer waiting for the lease never reclaims it itself:
//! a waiter on a follower cannot judge the owner dead, and the leader can have
//! no DDL of its own. The loop covers both.

use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;
use crate::error::Error;

use super::ddl_owner::{current_owner, reclaim_cause};
use super::ddl_prepare::abandon_token;
use super::handle::MetadataRaftHandle;

/// How often the leader checks the lease owner.
const RECLAIM_POLL: Duration = Duration::from_millis(250);

/// Spawn the reclaim loop. `start_raft` calls it once the metadata raft
/// handle is installed.
pub(crate) fn spawn_ddl_lease_reclaimer(shared: &Arc<SharedState>) {
    let weak = Arc::downgrade(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "ddl_lease_reclaimer",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(RECLAIM_POLL);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {
                        if !reclaim_if_due(&weak).await {
                            break;
                        }
                    }
                }
            }
        },
    );
}

/// Reclaim the lease when its owner qualifies. Returns `false` once the
/// state is gone.
async fn reclaim_if_due(shared: &Weak<SharedState>) -> bool {
    let Some(state) = shared.upgrade() else {
        return false;
    };
    let Some(owner) = current_owner(&state) else {
        return true;
    };
    let Some(cause) = reclaim_cause(&state, &owner) else {
        return true;
    };
    let handle = match state.metadata_raft_handle() {
        Ok(handle) => Arc::clone(handle),
        Err(error) => {
            tracing::warn!(%error, "DDL lease reclaim: no metadata raft handle");
            return true;
        }
    };
    match reclaim_ddl_prepare_lease(&state, handle.as_ref(), owner.token).await {
        Ok(()) => tracing::info!(
            token = owner.token,
            owner_node = owner.node_id,
            ?cause,
            "reclaimed the DDL preparation lease"
        ),
        Err(error) => tracing::warn!(
            token = owner.token,
            owner_node = owner.node_id,
            ?cause,
            %error,
            "DDL lease reclaim did not apply; the next check retries"
        ),
    }
    true
}

/// Cancel `token`'s pending DDL record, then release its lease, and await
/// both applies here.
///
/// The cancel goes first, so a pending record never outlives its lease. It is
/// proposed even when this node holds no record: the stuck owner's own
/// `DdlPendingPropose` can still be in flight. Both entries are idempotent, and
/// the release applies only while `token` still owns the lease, so a reclaim
/// that races the owner's own release is a no-op.
pub(crate) async fn reclaim_ddl_prepare_lease(
    shared: &SharedState,
    handle: &dyn MetadataRaftHandle,
    token: u64,
) -> Result<(), Error> {
    abandon_token(shared, handle, token).await
}
