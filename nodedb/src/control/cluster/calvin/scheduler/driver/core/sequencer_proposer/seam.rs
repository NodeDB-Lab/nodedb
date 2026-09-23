// SPDX-License-Identifier: BUSL-1.1

//! The [`SequencerProposer`] seam and its result types.

use nodedb_cluster::error::ClusterError;

/// Where a sequencer proposal went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeDispatch {
    /// This node leads the sequencer group and appended the entry.
    Local,
    /// A forward task carries the entry to the sequencer leader.
    Forwarded { leader: u64 },
}

/// Why a sequencer proposal did not leave this node.
#[derive(Debug, thiserror::Error)]
pub enum SequencerProposeError {
    /// This node sees no sequencer leader, or sees itself as leader after it
    /// stepped down.
    #[error("no sequencer leader is known")]
    NoLeader,
    /// This node does not lead the sequencer group and has no cluster
    /// transport to reach the leader.
    #[error("sequencer leader is node {leader}, and this node has no cluster transport")]
    NoTransport { leader: u64 },
    /// The forward limit is reached. The owed-entry sweep proposes the entry
    /// again on a later tick.
    #[error("{limit} sequencer forwards are in flight; the entry for node {leader} waits")]
    ForwardBusy { leader: u64, limit: usize },
    /// The local sequencer group refused the proposal.
    #[error("sequencer propose: {0}")]
    Cluster(#[from] ClusterError),
}

impl From<SequencerProposeError> for crate::Error {
    fn from(e: SequencerProposeError) -> Self {
        crate::Error::Dispatch {
            detail: e.to_string(),
        }
    }
}

/// Hands encoded sequencer entries to the sequencer Raft group.
///
/// `Ok` means the entry left this node. It does not mean the entry is
/// applied: a leader change can drop it. The scheduler learns that an entry
/// is applied from the completion registry, and proposes it again until then.
pub trait SequencerProposer: Send + Sync {
    /// Propose one msgpack-encoded `SequencerEntry`.
    fn propose(&self, bytes: Vec<u8>) -> Result<ProposeDispatch, SequencerProposeError>;
}
