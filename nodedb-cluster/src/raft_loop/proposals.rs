// SPDX-License-Identifier: BUSL-1.1

//! Raft proposal API — local and leader-forwarded proposals for both
//! the metadata group (group 0) and data groups.

use crate::conf_change::ConfChange;
use crate::error::Result;

use super::loop_core::{CommitApplier, RaftLoop};
use crate::forward::PlanExecutor;

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// Propose a command to the Raft group owning the given vShard.
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose(&self, vshard_id: u32, data: Vec<u8>) -> Result<(u64, u64)> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose(vshard_id, data)
    }

    /// Durably record `applied_index` as the group's applied floor.
    ///
    /// `applied_index` MUST name an entry whose state-machine effects are
    /// already durable — callers invoke this from the data-plane
    /// apply-completion path, after the write funnel's fsync barrier has
    /// returned for that entry. The next boot resumes Raft delivery at
    /// `applied_index + 1`, which is what stops WAL replay and Raft replay
    /// both applying it.
    ///
    /// Monotonic per group: an index at or below the current floor is a no-op.
    pub fn save_applied_index(&self, group_id: u64, applied_index: u64) -> Result<()> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.save_applied_index(group_id, applied_index)
    }

    /// Auto-compact a group's Raft log if its configured threshold has
    /// been reached, given the DATA-PLANE applied watermark
    /// `applied_index`.
    ///
    /// `applied_index` MUST be the index the data-plane state machine has
    /// durably applied to (NOT raft's commit index). Callers invoke this
    /// from the data-plane apply-completion path so the
    /// `SnapshotBuilder` can never be asked to serialize state past what
    /// the engines have actually applied.
    ///
    /// Returns `Ok(true)` when a compaction was performed. No-op
    /// (`Ok(false)`) when the group is absent, the threshold is `None`,
    /// or the retained-entry count is below the threshold.
    pub fn maybe_compact_group(&self, group_id: u64, applied_index: u64) -> Result<bool> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.maybe_compact_group(group_id, applied_index)
    }

    /// Propose a command directly to the metadata Raft group (group 0).
    ///
    /// Used by the host crate's metadata proposer and by integration
    /// tests that exercise the replicated-catalog path without a
    /// pgwire client. Fails with `ClusterError::GroupNotFound` if
    /// group 0 does not exist on this node, and with
    /// `ClusterError::Raft(NotLeader)` if this node is not the
    /// current leader of group 0.
    pub fn propose_to_metadata_group(&self, data: Vec<u8>) -> Result<u64> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose_to_group(crate::metadata_group::METADATA_GROUP_ID, data)
    }

    /// Propose to the metadata Raft group, transparently forwarding
    /// to the current leader if this node is not it.
    ///
    /// Tries a local propose first. On
    /// `ClusterError::Raft(NotLeader { leader_hint })`, looks up the
    /// hinted leader's address in cluster topology and sends a
    /// [`crate::rpc_codec::MetadataProposeRequest`] over QUIC. The
    /// receiving leader applies the proposal locally and returns
    /// the log index.
    ///
    /// On `NotLeader { leader_hint: None }` (election in progress,
    /// no observed leader yet) the call returns the original
    /// `NotLeader` error so the caller can decide whether to retry.
    /// We deliberately do not implement a wait-and-retry loop here
    /// because the caller (the host-side proposer) may have a
    /// shorter deadline than any reasonable retry budget.
    ///
    /// The leader-side path through this function is identical to
    /// the bare `propose_to_metadata_group` — the only extra cost is
    /// an `is_leader_locally` check before the local propose.
    pub async fn propose_to_metadata_group_via_leader(&self, data: Vec<u8>) -> Result<u64> {
        // First, try a local propose.
        match self.propose_to_metadata_group(data.clone()) {
            Ok(idx) => Ok(idx),
            Err(crate::error::ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint,
            })) => {
                let Some(leader_id) = leader_hint else {
                    return Err(crate::error::ClusterError::Raft(
                        nodedb_raft::RaftError::NotLeader { leader_hint: None },
                    ));
                };
                if leader_id == self.node_id {
                    // Should not happen — local propose said we
                    // weren't leader but the hint points at us. Fall
                    // through to the original error so the caller
                    // sees the contradiction.
                    return Err(crate::error::ClusterError::Raft(
                        nodedb_raft::RaftError::NotLeader {
                            leader_hint: Some(leader_id),
                        },
                    ));
                }
                // Otherwise forward to the hinted leader.
                self.forward_metadata_propose(leader_id, data).await
            }
            Err(other) => Err(other),
        }
    }

    /// Send a `MetadataProposeRequest` to `leader_id`. Looks up the
    /// leader's listen address via the local topology snapshot and
    /// dispatches through the existing peer transport.
    async fn forward_metadata_propose(&self, leader_id: u64, data: Vec<u8>) -> Result<u64> {
        // Resolve and register the leader's address with the
        // transport so `send_rpc` has a destination. Topology is
        // updated by the membership / health subsystem; if the
        // leader isn't in our local topology yet we fail loudly so
        // the caller can fall back to its own retry policy rather
        // than silently dropping the proposal.
        {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            let Some(node) = topo.get_node(leader_id) else {
                return Err(crate::error::ClusterError::Transport {
                    detail: format!(
                        "metadata propose forward: leader {leader_id} not in local topology"
                    ),
                });
            };
            let Some(addr) = node.socket_addr() else {
                return Err(crate::error::ClusterError::Transport {
                    detail: format!(
                        "metadata propose forward: leader {leader_id} has unparseable addr {:?}",
                        node.addr
                    ),
                });
            };
            // Idempotent: register_peer overwrites any prior mapping.
            self.transport.register_peer(leader_id, addr);
        }

        let req = crate::rpc_codec::RaftRpc::MetadataProposeRequest(
            crate::rpc_codec::MetadataProposeRequest { bytes: data },
        );
        let resp = self.transport.send_rpc(leader_id, req).await?;
        match resp {
            crate::rpc_codec::RaftRpc::MetadataProposeResponse(r) => {
                if r.success {
                    Ok(r.log_index)
                } else if let Some(hint) = r.leader_hint {
                    // The receiving node was also not the leader
                    // (rare: leader changed between our local check
                    // and the forwarded RPC). Surface as NotLeader
                    // so the caller's normal retry path runs.
                    Err(crate::error::ClusterError::Raft(
                        nodedb_raft::RaftError::NotLeader {
                            leader_hint: Some(hint),
                        },
                    ))
                } else {
                    Err(crate::error::ClusterError::Transport {
                        detail: format!("metadata propose forward failed: {}", r.error_message),
                    })
                }
            }
            other => Err(crate::error::ClusterError::Transport {
                detail: format!("metadata propose forward: unexpected response variant {other:?}"),
            }),
        }
    }

    /// Propose a command to the data Raft group owning the given vShard,
    /// transparently forwarding to the group leader if this node is not it.
    ///
    /// Tries a local propose first. On `NotLeader { leader_hint: Some(id) }`,
    /// looks up the hinted leader's address in the cluster topology and sends
    /// a `DataProposeRequest` over QUIC. The receiving leader applies the
    /// proposal locally and returns `(group_id, log_index)`.
    ///
    /// On `NotLeader { leader_hint: None }` (election in progress) the call
    /// returns the original `NotLeader` error so the caller can retry.
    pub async fn propose_via_data_leader(
        &self,
        vshard_id: u32,
        data: Vec<u8>,
    ) -> Result<(u64, u64)> {
        // First, try a local propose.
        match self.propose(vshard_id, data.clone()) {
            Ok(pair) => Ok(pair),
            Err(crate::error::ClusterError::Raft(nodedb_raft::RaftError::NotLeader {
                leader_hint,
            })) => {
                let Some(leader_id) = leader_hint else {
                    return Err(crate::error::ClusterError::Raft(
                        nodedb_raft::RaftError::NotLeader { leader_hint: None },
                    ));
                };
                if leader_id == self.node_id {
                    return Err(crate::error::ClusterError::Raft(
                        nodedb_raft::RaftError::NotLeader {
                            leader_hint: Some(leader_id),
                        },
                    ));
                }
                // Otherwise forward to the hinted leader.
                self.forward_data_propose(leader_id, vshard_id, data).await
            }
            Err(other) => Err(other),
        }
    }

    /// Send a `DataProposeRequest` to `leader_id`.
    async fn forward_data_propose(
        &self,
        leader_id: u64,
        vshard_id: u32,
        data: Vec<u8>,
    ) -> Result<(u64, u64)> {
        {
            let topo = self.topology.read().unwrap_or_else(|p| p.into_inner());
            let Some(node) = topo.get_node(leader_id) else {
                return Err(crate::error::ClusterError::Transport {
                    detail: format!(
                        "data propose forward: leader {leader_id} not in local topology"
                    ),
                });
            };
            let Some(addr) = node.socket_addr() else {
                return Err(crate::error::ClusterError::Transport {
                    detail: format!(
                        "data propose forward: leader {leader_id} has unparseable addr {:?}",
                        node.addr
                    ),
                });
            };
            self.transport.register_peer(leader_id, addr);
        }

        let req =
            crate::rpc_codec::RaftRpc::DataProposeRequest(crate::rpc_codec::DataProposeRequest {
                target: crate::rpc_codec::ProposeTarget::VShard(vshard_id),
                bytes: data,
            });
        let resp = self.transport.send_rpc(leader_id, req).await?;
        match resp {
            crate::rpc_codec::RaftRpc::DataProposeResponse(r) => {
                if r.success {
                    Ok((r.group_id, r.log_index))
                } else if let Some(hint) = r.leader_hint {
                    Err(crate::error::ClusterError::Raft(
                        nodedb_raft::RaftError::NotLeader {
                            leader_hint: Some(hint),
                        },
                    ))
                } else {
                    Err(crate::error::ClusterError::Transport {
                        detail: format!("data propose forward failed: {}", r.error_message),
                    })
                }
            }
            other => Err(crate::error::ClusterError::Transport {
                detail: format!("data propose forward: unexpected response variant {other:?}"),
            }),
        }
    }

    /// Propose a configuration change to a Raft group.
    ///
    /// Returns `(group_id, log_index)` on success.
    pub fn propose_conf_change(&self, group_id: u64, change: &ConfChange) -> Result<(u64, u64)> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose_conf_change(group_id, change)
    }
}
