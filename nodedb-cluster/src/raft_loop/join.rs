//! Server-side `JoinRequest` orchestration.
//!
//! This is the async flow invoked by the `RaftRpc::JoinRequest` arm in
//! [`super::handle_rpc`]. It turns a remote node's desire to join the
//! cluster into a series of durable Raft conf-changes and returns a
//! `JoinResponse` containing everything the joining node needs to
//! reconstruct its local `MultiRaft` in the `Learner` role.
//!
//! ## Flow
//!
//! 1. **Leader check.** Snapshot the group-0 leader id and clone the
//!    routing table under a single `MultiRaft` lock. If another node is
//!    the leader, return a redirect response with that node's address.
//! 2. **Validate address.** Parse `req.listen_addr`. On failure, return
//!    an error response.
//! 3. **Idempotency / collision check.** If the node id is already in
//!    topology with the same address and is Active, rebuild and return
//!    the current response without any further Raft activity. If the
//!    node id exists with a different address, reject.
//! 4. **Register transport peer.** Add the new peer address to the
//!    local transport so the leader can immediately send AppendEntries
//!    to the learner-to-be.
//! 5. **Admit into topology.** Under a short `topology.write()` guard,
//!    call `bootstrap::handle_join_request` — the only side effect is
//!    inserting the new `NodeInfo`. The routing-table clone we took in
//!    step 1 is intentionally *not* reused for the final response; a
//!    fresh clone is taken after step 6 so the response reflects the
//!    post-AddLearner routing state.
//! 6. **Propose AddLearner on every group.** For each Raft group, take
//!    the `MultiRaft` lock, propose
//!    `ConfChange::AddLearner(new_node_id)`, and record the resulting
//!    log index. Drop the lock between groups. If this node is not the
//!    leader of a particular group the propose will fail with
//!    `NotLeader` — we surface that as a failure response. (For the
//!    3-node bootstrap case in the integration test the bootstrap seed
//!    leads every group, so this path is exercised end-to-end.)
//! 7. **Wait for each conf-change to commit.** Poll `commit_index_for`
//!    on each group every 20 ms with a 5-second deadline. A
//!    single-voter group (the bootstrap seed before any voters have
//!    been added) commits instantly. Multi-voter groups wait for
//!    quorum. On timeout, return an error response — the joining node
//!    will retry the whole flow.
//! 8. **Persist topology + routing to catalog** (when a catalog is
//!    attached). Order matters: Raft log → catalog → response.
//! 9. **Broadcast TopologyUpdate** to every currently-active peer so
//!    followers learn the new node's address. Fire-and-forget.
//! 10. **Build and return JoinResponse** with the updated routing
//!     (which now includes the new node as a learner on every group).
//!
//! The Raft-level promotion from learner to voter happens asynchronously
//! in the tick loop (`super::tick::promote_ready_learners`) once the
//! learner's `match_index` catches up. That avoids blocking the join
//! handler on replication progress while still completing the
//! two-phase single-server add.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use crate::bootstrap::handle_join_request;
use crate::conf_change::{ConfChange, ConfChangeType};
use crate::error::{ClusterError, Result};
use crate::forward::RequestForwarder;
use crate::health;
use crate::multi_raft::GroupStatus;
use crate::routing::RoutingTable;
use crate::rpc_codec::{JoinRequest, JoinResponse, LEADER_REDIRECT_PREFIX};

use super::handle_rpc::{JoinDecision, TOPOLOGY_GROUP_ID, decide_join};
use super::loop_core::{CommitApplier, RaftLoop};

/// Maximum time we wait for any one `AddLearner` conf-change to commit
/// before giving up and returning a failure response to the joining
/// node.
const CONF_CHANGE_COMMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Polling interval for the commit-wait loop.
const CONF_CHANGE_POLL_INTERVAL: Duration = Duration::from_millis(20);

impl<A: CommitApplier, F: RequestForwarder> RaftLoop<A, F> {
    /// Full server-side `JoinRequest` handler. See module docs for the
    /// phase-by-phase description.
    pub(super) async fn join_flow(&self, req: JoinRequest) -> JoinResponse {
        // 1. Snapshot group-0 leader + clone routing under one lock.
        let (group0_leader, routing): (u64, RoutingTable) = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let routing = mr.routing().clone();
            let leader_id = mr
                .group_statuses()
                .into_iter()
                .find(|s: &GroupStatus| s.group_id == TOPOLOGY_GROUP_ID)
                .map(|s| s.leader_id)
                .unwrap_or(0);
            (leader_id, routing)
        };

        // Leader check.
        let leader_addr_hint = if group0_leader != 0 && group0_leader != self.node_id {
            self.topology
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .get_node(group0_leader)
                .map(|n| n.addr.clone())
        } else {
            None
        };
        if let JoinDecision::Redirect { leader_addr } =
            decide_join(group0_leader, self.node_id, leader_addr_hint)
        {
            warn!(
                joining_node = req.node_id,
                leader_id = group0_leader,
                leader_addr = %leader_addr,
                "JoinRequest received on non-leader; redirecting"
            );
            return reject(format!("{LEADER_REDIRECT_PREFIX}{leader_addr}"));
        }

        // 2. Validate the address.
        let new_addr: SocketAddr = match req.listen_addr.parse() {
            Ok(a) => a,
            Err(e) => {
                return reject(format!("invalid listen_addr '{}': {e}", req.listen_addr));
            }
        };

        // 3. Idempotency / collision check against topology.
        //    `handle_join_request` in step 5 handles the fine-grained
        //    semantics, but we check here first so idempotent re-joins
        //    short-circuit *before* we propose any Raft conf changes.
        let existing = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get_node(req.node_id)
            .cloned();
        if let Some(existing) = existing {
            if existing.addr != req.listen_addr {
                return reject(format!(
                    "node_id {} already registered with different address {} (request: {})",
                    req.node_id, existing.addr, req.listen_addr
                ));
            }
            // Same id + same addr → idempotent replay. Just rebuild the
            // current response from the latest routing state without
            // proposing any conf changes.
            debug!(
                joining_node = req.node_id,
                "idempotent re-join; returning current cluster state"
            );
            return self.build_current_response(&req);
        }

        // 4. Register transport peer so the leader can reach it.
        self.transport.register_peer(req.node_id, new_addr);

        // Read the local cluster id from the catalog and echo it
        // on every successful `JoinResponse`. The joining node
        // persists this value so its next boot takes the
        // `restart()` path instead of re-bootstrapping.
        //
        // Strict contract:
        //
        // - If a catalog is attached and is missing a cluster_id,
        //   the server is lying about being bootstrapped — this
        //   is an invariant violation, so we reject the join
        //   loudly instead of papering over it with a sentinel
        //   zero that would silently collapse two different
        //   clusters into one "cluster 0".
        // - If a catalog is not attached (unit-test path), we
        //   fall back to `self.node_id`. This is a test-only
        //   affordance: it keeps the response well-formed without
        //   inventing a cross-cluster identity, because in tests
        //   every node id is locally unique by construction.
        let cluster_id = match self.catalog.as_ref() {
            Some(catalog) => match catalog.load_cluster_id() {
                Ok(Some(id)) => id,
                Ok(None) => {
                    return reject(
                        "server catalog is attached but has no cluster_id — refusing to \
                         issue a JoinResponse without a real cluster identity"
                            .to_string(),
                    );
                }
                Err(e) => {
                    return reject(format!("failed to read cluster_id from catalog: {e}"));
                }
            },
            None => self.node_id,
        };

        // 5. Admit into topology.
        {
            let mut topo = self.topology.write().unwrap_or_else(|p| p.into_inner());
            let initial_resp = handle_join_request(&req, &mut topo, &routing, cluster_id);
            if !initial_resp.success {
                // Reject bubbled up from the shared function (e.g., the
                // collision check we just did, repeated under the write
                // guard in case something raced).
                return initial_resp;
            }
        }

        // 6. Propose AddLearner on every group.
        let group_ids: Vec<u64> = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.routing().group_ids()
        };

        let mut pending: Vec<(u64, u64)> = Vec::with_capacity(group_ids.len()); // (group_id, log_index)
        for gid in &group_ids {
            let change = ConfChange {
                change_type: ConfChangeType::AddLearner,
                node_id: req.node_id,
            };
            let propose_result = {
                let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                mr.propose_conf_change(*gid, &change)
            };
            match propose_result {
                Ok((_, log_index)) => pending.push((*gid, log_index)),
                Err(ClusterError::Transport { detail }) => {
                    return reject(format!(
                        "failed to propose AddLearner on group {gid}: {detail}"
                    ));
                }
                Err(e) => {
                    return reject(format!("failed to propose AddLearner on group {gid}: {e}"));
                }
            }
        }

        // 7. Wait for every conf change to actually *apply* to
        //    routing. Earlier versions of this flow polled
        //    `commit_index_for` and relied on an unconditional
        //    inline apply inside `propose_conf_change` — which
        //    was racy for multi-voter groups where the commit
        //    can be deferred until quorum replicates the log
        //    entry. The correct semantic signal is "the new node
        //    appears in `routing.group_info(gid).learners`",
        //    because that's what `apply_conf_change` writes after
        //    the commit lands. Polling this also works cleanly
        //    for single-voter groups (the inline apply makes the
        //    condition true on the first poll) and multi-voter
        //    groups (the tick loop runs concurrently with this
        //    `await`, drains `committed_entries`, and calls
        //    `apply_conf_change` → routing update → condition
        //    flips).
        let deadline = Instant::now() + CONF_CHANGE_COMMIT_TIMEOUT;
        for (gid, log_index) in &pending {
            if let Err(err) = self
                .wait_for_learner_applied(*gid, req.node_id, *log_index, deadline)
                .await
            {
                return reject(err.to_string());
            }
        }

        // 8. Persist catalog (topology + post-AddLearner routing).
        if let Some(catalog) = self.catalog.as_ref() {
            let topo_snapshot = self
                .topology
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let routing_snapshot = {
                let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                mr.routing().clone()
            };
            if let Err(e) = catalog.save_topology(&topo_snapshot) {
                warn!(error = %e, "failed to persist topology after join");
                return reject(format!("catalog save_topology failed: {e}"));
            }
            if let Err(e) = catalog.save_routing(&routing_snapshot) {
                warn!(error = %e, "failed to persist routing after join");
                return reject(format!("catalog save_routing failed: {e}"));
            }
        }

        // 9. Broadcast topology to everyone so peers learn the new addr.
        health::broadcast_topology(self.node_id, &self.topology, &self.transport);

        // 10. Build the final response from the post-AddLearner state.
        info!(
            joining_node = req.node_id,
            groups = pending.len(),
            "join accepted; learner AddLearner commits complete"
        );
        self.build_current_response(&req)
    }

    /// Wait for the semantic goal of "learner is now tracked in
    /// `routing.group_info(group_id).learners`", polling every
    /// [`CONF_CHANGE_POLL_INTERVAL`] up to `deadline`.
    ///
    /// This is the post-apply condition that `apply_conf_change`
    /// writes once a committed `AddLearner` entry has been
    /// applied to the local state. Polling this rather than the
    /// raw `commit_index` is what lets the join flow stay
    /// correct on multi-voter groups where the commit is
    /// deferred until quorum replicates.
    ///
    /// `log_index` is carried into the error enum for debugging
    /// only; the condition is not gated on it.
    ///
    /// Surfaces failure through [`ClusterError::JoinCommitTimeout`]
    /// and [`ClusterError::JoinGroupDisappeared`] so the join
    /// flow can match the cause and so the crate's central
    /// error enum owns the human-readable rendering.
    async fn wait_for_learner_applied(
        &self,
        group_id: u64,
        learner_id: u64,
        log_index: u64,
        deadline: Instant,
    ) -> Result<()> {
        loop {
            let applied = {
                let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                mr.routing()
                    .group_info(group_id)
                    .map(|info| info.learners.contains(&learner_id))
            };
            match applied {
                Some(true) => return Ok(()),
                Some(false) => {}
                None => return Err(ClusterError::JoinGroupDisappeared { group_id }),
            }
            if Instant::now() >= deadline {
                return Err(ClusterError::JoinCommitTimeout {
                    group_id,
                    log_index,
                });
            }
            tokio::time::sleep(CONF_CHANGE_POLL_INTERVAL).await;
        }
    }

    /// Build a `JoinResponse` snapshotting the current topology
    /// and routing. Used both by the happy-path return and by the
    /// idempotent re-join short-circuit. The strict cluster_id
    /// check is the same as the one at the top of `join_flow` —
    /// a catalog-attached server with no stamped cluster_id is an
    /// invariant violation and we reject the join rather than
    /// synthesise a sentinel identity.
    fn build_current_response(&self, req: &JoinRequest) -> JoinResponse {
        let cluster_id = match self.catalog.as_ref() {
            Some(catalog) => match catalog.load_cluster_id() {
                Ok(Some(id)) => id,
                Ok(None) => {
                    return reject(
                        "server catalog is attached but has no cluster_id — refusing to \
                         issue a JoinResponse without a real cluster identity"
                            .to_string(),
                    );
                }
                Err(e) => {
                    return reject(format!("failed to read cluster_id from catalog: {e}"));
                }
            },
            None => self.node_id,
        };

        let topology_clone = self
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let routing_clone = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.routing().clone()
        };
        // Re-use the pure builder from `bootstrap/handle_join.rs`.
        // `handle_join_request` is idempotent against the same
        // (id, addr) — at this point the topology already
        // contains the new node, so this call only rebuilds the
        // wire response.
        let mut topo = topology_clone;
        handle_join_request(req, &mut topo, &routing_clone, cluster_id)
    }
}

/// Build a failure `JoinResponse` with the given error message.
fn reject(error: String) -> JoinResponse {
    JoinResponse {
        success: false,
        error,
        cluster_id: 0,
        nodes: vec![],
        vshard_to_group: vec![],
        groups: vec![],
    }
}
