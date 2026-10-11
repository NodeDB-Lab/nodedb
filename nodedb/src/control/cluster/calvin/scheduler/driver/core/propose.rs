// SPDX-License-Identifier: BUSL-1.1

//! Propose a passive participant's read values to an active vShard's data
//! group as a `ReplicatedWrite::CalvinReadResult` entry.
//!
//! The passive vShard's data-group leader proposes to the data group of each
//! active vShard. That group's leader is often another node, so the entry
//! travels through the node's async propose phase, which forwards it to the
//! group leader. The phase takes no vShard admission slot: the entry writes
//! no row, and the target leader's write gate exempts it.
//!
//! A refused proposal is retried until the deadline. A second copy of an
//! entry that already landed changes nothing: the first result of a passive
//! vShard is the one a barrier counts. A proposal that never lands leaves
//! the active barrier waiting, and its leader's timeout entry ends it on
//! every replica alike.

use std::sync::Weak;
use std::time::Duration;

use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite};
use crate::types::{DatabaseId, TenantId};

/// Pause between two attempts of a refused proposal.
const RETRY_PAUSE: Duration = Duration::from_millis(20);

/// One passive vShard's read values, for one active vShard's data group.
pub struct CalvinReadResultProposal {
    /// The active vShard whose data group the entry goes to.
    pub target_vshard: u32,
    pub epoch: u64,
    pub position: u32,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The vShard that read the values.
    pub passive_vshard: u32,
    /// The zerompk-encoded `Vec<(PassiveReadKeyId, Value)>` the passive read
    /// returned.
    pub values: Vec<u8>,
}

/// Propose `proposal` to the data group of `proposal.target_vshard`, and
/// wait for this node's apply of it when this node replicates that group.
///
/// Retries a refused proposal until `deadline`. Returns the last refusal
/// once the deadline passed. Returns `Ok` once an attempt landed in the
/// target leader's log, whatever this node's apply wait reports: a node that
/// replicates no copy of the group never applies it.
pub async fn propose_calvin_read_result(
    state: Weak<SharedState>,
    proposal: CalvinReadResultProposal,
    deadline: tokio::time::Instant,
) -> crate::Result<()> {
    let CalvinReadResultProposal {
        target_vshard,
        epoch,
        position,
        tenant_id,
        database_id,
        passive_vshard,
        values,
    } = proposal;
    let entry = ReplicatedEntry::new(
        tenant_id.as_u64(),
        database_id.as_u64(),
        target_vshard,
        ReplicatedWrite::CalvinReadResult {
            epoch,
            position,
            passive_vshard,
            tenant_id: tenant_id.as_u64(),
            values,
        },
    );
    let bytes = entry.encode()?;
    loop {
        let submit = {
            let Some(state) = state.upgrade() else {
                return Err(crate::Error::Internal {
                    detail: "calvin read result not proposed: the node is shutting down".into(),
                });
            };
            std::sync::Arc::clone(state.async_raft_submit()?)
        };
        match submit(
            target_vshard,
            entry.idempotency_key,
            bytes.clone(),
            deadline,
        )
        .await
        {
            Ok(proposed) => {
                if let Err(error) = proposed.applied.await {
                    tracing::debug!(
                        target_vshard,
                        epoch,
                        position,
                        %error,
                        "calvin: read result proposed; this node's apply wait ended without it"
                    );
                }
                return Ok(());
            }
            Err(error) => {
                // no-determinism: the retry budget is liveness only; the log order decides the result.
                if tokio::time::Instant::now() >= deadline {
                    return Err(error);
                }
                tracing::debug!(
                    target_vshard,
                    epoch,
                    position,
                    %error,
                    "calvin: read result proposal refused; proposing it again"
                );
                tokio::time::sleep(
                    RETRY_PAUSE
                        // no-determinism: a retry pause only.
                        .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
                )
                .await;
            }
        }
    }
}
