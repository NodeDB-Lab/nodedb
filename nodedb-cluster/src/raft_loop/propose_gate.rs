// SPDX-License-Identifier: BUSL-1.1

//! The data-group leader's write gate.
//!
//! The leader of a vShard's data group admits every proposal to that group
//! through the host's write gate before the entry enters its log. The gate
//! takes the write's lock keys on the vShard's Calvin lock table. A Calvin
//! transaction that needs one of those keys then waits until this leader
//! started the entry's apply, so its own effects order after the write.
//!
//! A local proposal and a forwarded one pass the same gate: both reach the
//! leader through [`RaftLoop::propose_admitted`]. `nodedb-cluster` cannot
//! depend on `nodedb`, so the gate lives in the host crate behind
//! [`DataProposeGate`].

use crate::error::Result;
use crate::forward::PlanExecutor;

use super::loop_core::{CommitApplier, RaftLoop};

/// The lock keys one admitted proposal holds. Dropping it releases them.
pub trait ProposeHold: Send {
    /// The proposal landed at `log_index` of `group_id`. The host keeps the
    /// keys until this node starts the apply of that entry.
    fn landed(self: Box<Self>, group_id: u64, log_index: u64);
}

/// The host's write gate for data-group proposals on their leader.
#[async_trait::async_trait]
pub trait DataProposeGate: Send + Sync + 'static {
    /// Admit the encoded entry `entry`, proposed to the group of
    /// `vshard_id`, on this leader. A contended entry waits for its keys
    /// until `deadline`, the proposer's own deadline.
    ///
    /// - `Ok(None)`: the entry takes no lock key.
    /// - `Ok(Some(hold))`: the entry holds its keys until `hold` releases
    ///   them.
    /// - `Err(ClusterError::Calvin(CalvinError::RouteToSequencer))`: a held
    ///   key blocks the write, and the proposer submits it to the Calvin
    ///   sequencer.
    /// - `Err(ClusterError::Calvin(CalvinError::AdmissionTimedOut))`: the
    ///   keys stayed held until `deadline`. Nothing is proposed.
    async fn admit(
        &self,
        vshard_id: u32,
        entry: &[u8],
        deadline: tokio::time::Instant,
    ) -> Result<Option<Box<dyn ProposeHold>>>;
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Propose `data` to the group of `vshard_id` on this node. When this
    /// node leads the group, the write gate admits the entry first, waiting
    /// no later than `deadline`, the proposer's deadline.
    ///
    /// A node that does not lead the group proposes with no gate. The
    /// propose refuses with `NotLeader`, and the caller forwards the entry to
    /// the leader, which gates it there. A propose that fails after the gate
    /// admitted the entry drops its hold, which releases the keys.
    pub(super) async fn propose_admitted(
        &self,
        vshard_id: u32,
        data: &[u8],
        deadline: tokio::time::Instant,
    ) -> Result<(u64, u64)> {
        let hold = match &self.data_propose_gate {
            Some(gate) if self.leads_group_of(vshard_id) => {
                gate.admit(vshard_id, data, deadline).await?
            }
            Some(_) | None => None,
        };
        let (group_id, log_index) = self.propose(vshard_id, data.to_vec())?;
        if let Some(hold) = hold {
            hold.landed(group_id, log_index);
        }
        Ok((group_id, log_index))
    }

    /// Whether this node leads the data group that owns `vshard_id`.
    fn leads_group_of(&self, vshard_id: u32) -> bool {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let group_id = mr
            .routing()
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .group_for_vshard(vshard_id);
        group_id.is_ok_and(|group_id| mr.is_group_leader(group_id))
    }
}
