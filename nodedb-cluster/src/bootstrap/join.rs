//! Join path: contact seeds, receive full cluster state, apply locally.
//!
//! The join loop is deliberately robust against two realistic cluster
//! startup failure modes:
//!
//! 1. **Slow start**: the designated bootstrapper has not yet
//!    completed its first Raft election when this node first calls
//!    `join()`. Every seed may return "unreachable" or "not leader"
//!    for a brief window. We retry the whole loop with exponential
//!    backoff so the join eventually succeeds without operator
//!    intervention.
//!
//! 2. **Leader redirect**: the seed we contacted is alive but isn't
//!    the group-0 leader. It returns
//!    `JoinResponse { success: false, error: "not leader; retry at <addr>" }`
//!    and we follow the hint up to a small number of hops before
//!    falling through to the next seed. The string format is the
//!    contract set by `raft_loop::join::join_flow` — keep this parser
//!    in lock-step with that producer.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::catalog::ClusterCatalog;
use crate::error::{ClusterError, Result};
use crate::multi_raft::MultiRaft;
use crate::routing::{GroupInfo, RoutingTable};
use crate::rpc_codec::{JoinRequest, JoinResponse, RaftRpc};
use crate::topology::{ClusterTopology, NodeInfo, NodeState};
use crate::transport::NexarTransport;

use super::config::{ClusterConfig, ClusterState};

/// Maximum number of outer retry attempts before `join()` gives up and
/// returns the last concrete error to its caller. With the backoff
/// schedule below this gives a total window of roughly 32 seconds.
const MAX_JOIN_ATTEMPTS: u32 = 8;

/// Maximum number of leader-redirect hops inside a single join
/// attempt. The redirect chain starts at whichever seed we first
/// contact; each hop costs a round-trip, so keep this small.
const MAX_REDIRECTS_PER_ATTEMPT: u32 = 3;

/// Exponential-backoff delay between join attempts, capped at 16 s.
///
/// Attempt 0 is immediate. Subsequent attempts sleep 500 ms, 1 s, 2 s,
/// 4 s, 8 s, then 16 s for every further attempt. Total window for
/// the default `MAX_JOIN_ATTEMPTS = 8` is roughly 32 s.
///
/// Pure so it can be unit-tested in isolation — no time-source
/// dependency.
pub(crate) fn next_backoff(attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    let secs_millis: u64 = match attempt {
        1 => 500,
        2 => 1_000,
        3 => 2_000,
        4 => 4_000,
        5 => 8_000,
        _ => 16_000,
    };
    Duration::from_millis(secs_millis)
}

/// Parse a `JoinResponse::error` string as a leader redirect hint.
///
/// The producer — `raft_loop::join::join_flow` — formats non-leader
/// rejections as exactly `"not leader; retry at <addr>"` where
/// `<addr>` is an `std::net::SocketAddr` text form. This parser is the
/// mirror and is the **only** place that interprets that string as a
/// redirect. Any other kind of rejection (collision, parse error,
/// catalog persist failure, commit timeout, etc.) is treated as a
/// hard failure that bubbles through the normal error path.
///
/// Returns `None` for any string that doesn't start with the expected
/// prefix, or where the address portion does not parse as a valid
/// `SocketAddr`.
pub(crate) fn parse_leader_hint(error: &str) -> Option<SocketAddr> {
    error
        .strip_prefix("not leader; retry at ")
        .and_then(|s| s.trim().parse().ok())
}

/// Join an existing cluster by contacting seed nodes.
///
/// The loop has two layers:
///
/// - **Outer**: up to `MAX_JOIN_ATTEMPTS` retry passes with
///   exponential backoff. Handles the "bootstrapper not up yet"
///   startup race.
/// - **Inner**: walk the seed list plus any leader-redirect hops for
///   this attempt. A successful `JoinResponse` short-circuits the
///   whole function; failures on one candidate fall through to the
///   next.
pub(super) async fn join(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
) -> Result<ClusterState> {
    info!(
        node_id = config.node_id,
        seeds = ?config.seed_nodes,
        "joining existing cluster"
    );

    if config.seed_nodes.is_empty() {
        return Err(ClusterError::Transport {
            detail: "no seed nodes configured".into(),
        });
    }

    let req_template = JoinRequest {
        node_id: config.node_id,
        listen_addr: config.listen_addr.to_string(),
    };

    let mut last_err: Option<ClusterError> = None;

    for attempt in 0..MAX_JOIN_ATTEMPTS {
        let delay = next_backoff(attempt);
        if !delay.is_zero() {
            debug!(
                node_id = config.node_id,
                attempt,
                delay_ms = delay.as_millis() as u64,
                "backing off before next join attempt"
            );
            tokio::time::sleep(delay).await;
        }

        match try_join_once(config, catalog, transport, &req_template).await {
            Ok(state) => return Ok(state),
            Err(e) => {
                warn!(
                    node_id = config.node_id,
                    attempt,
                    error = %e,
                    "join attempt failed; will retry"
                );
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| ClusterError::Transport {
        detail: format!("join exhausted {MAX_JOIN_ATTEMPTS} attempts with no concrete error"),
    }))
}

/// One pass over the seed list plus up to `MAX_REDIRECTS_PER_ATTEMPT`
/// leader-redirect hops. Returns `Ok(state)` on the first successful
/// `JoinResponse` or an error describing the last failure in this
/// attempt.
async fn try_join_once(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
    req_template: &JoinRequest,
) -> Result<ClusterState> {
    // Work list: start with the configured seeds, prepend leader hints
    // as they arrive. `HashSet` deduplicates so a redirect loop can't
    // consume all attempts against the same address.
    let mut work: Vec<SocketAddr> = config.seed_nodes.clone();
    let mut visited: HashSet<SocketAddr> = HashSet::new();
    let mut redirects: u32 = 0;
    let mut last_err: Option<ClusterError> = None;

    while let Some(addr) = work.pop() {
        if !visited.insert(addr) {
            continue;
        }

        let rpc = RaftRpc::JoinRequest(req_template.clone());
        match transport.send_rpc_to_addr(addr, rpc).await {
            Ok(RaftRpc::JoinResponse(resp)) => {
                if resp.success {
                    return apply_join_response(config, catalog, transport, &resp);
                }
                // Rejected — is it a leader redirect we can follow?
                if let Some(leader) = parse_leader_hint(&resp.error) {
                    if redirects < MAX_REDIRECTS_PER_ATTEMPT && !visited.contains(&leader) {
                        info!(
                            node_id = config.node_id,
                            from = %addr,
                            to = %leader,
                            "following leader redirect"
                        );
                        redirects += 1;
                        work.push(leader);
                        continue;
                    }
                    debug!(
                        node_id = config.node_id,
                        from = %addr,
                        leader = %leader,
                        redirects,
                        "redirect cap reached or loop detected; falling through"
                    );
                }
                last_err = Some(ClusterError::Transport {
                    detail: format!("join rejected by {addr}: {}", resp.error),
                });
            }
            Ok(other) => {
                last_err = Some(ClusterError::Transport {
                    detail: format!("unexpected response from {addr}: {other:?}"),
                });
            }
            Err(e) => {
                debug!(%addr, error = %e, "seed unreachable");
                last_err = Some(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| ClusterError::Transport {
        detail: "no seed nodes produced a response".into(),
    }))
}

/// Apply a JoinResponse: reconstruct topology, routing, and MultiRaft
/// from wire data.
///
/// Order of operations is load-bearing for crash safety:
///
/// 1. Reconstruct the `ClusterTopology` and `RoutingTable` in memory.
/// 2. Persist topology + routing to the catalog **first**, before any
///    on-disk side effects. If we crash after this step, the next
///    boot sees `catalog.is_bootstrapped() == true` and takes the
///    `restart()` path, which reconstructs cleanly from the catalog.
/// 3. Create the `MultiRaft` and add groups. `add_group` opens redb
///    files on disk per group; these are idempotent per group id, so
///    a crash mid-way leaves a recoverable state.
/// 4. Register every peer address in the transport before returning
///    so the first outgoing AppendEntries has a known destination.
fn apply_join_response(
    config: &ClusterConfig,
    catalog: &ClusterCatalog,
    transport: &NexarTransport,
    resp: &JoinResponse,
) -> Result<ClusterState> {
    // 1. Reconstruct topology.
    let mut topology = ClusterTopology::new();
    for node in &resp.nodes {
        let state = NodeState::from_u8(node.state).unwrap_or(NodeState::Active);
        let mut info = NodeInfo {
            node_id: node.node_id,
            addr: node.addr.clone(),
            state,
            raft_groups: node.raft_groups.clone(),
        };
        if node.node_id == config.node_id {
            info.state = NodeState::Active;
        }
        topology.add_node(info);
    }

    // 1. Reconstruct routing table.
    let mut group_members = std::collections::HashMap::new();
    for g in &resp.groups {
        group_members.insert(
            g.group_id,
            GroupInfo {
                leader: g.leader,
                members: g.members.clone(),
                learners: g.learners.clone(),
            },
        );
    }
    let routing = RoutingTable::from_parts(resp.vshard_to_group.clone(), group_members);

    // 2. Persist to catalog before any on-disk Raft side effects.
    catalog.save_topology(&topology)?;
    catalog.save_routing(&routing)?;

    // 3. Create MultiRaft — join any group that includes this node,
    //    either as a voter (group members) or as a learner (group
    //    learners). A learner-started group boots in the `Learner`
    //    role and will not run an election until a subsequent
    //    `PromoteLearner` conf change is applied.
    let mut multi_raft = MultiRaft::new(config.node_id, routing.clone(), config.data_dir.clone());
    for g in &resp.groups {
        let is_voter = g.members.contains(&config.node_id);
        let is_learner = g.learners.contains(&config.node_id);

        if is_voter {
            let peers: Vec<u64> = g
                .members
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group(g.group_id, peers)?;
        } else if is_learner {
            let voters = g.members.clone();
            let other_learners: Vec<u64> = g
                .learners
                .iter()
                .copied()
                .filter(|&id| id != config.node_id)
                .collect();
            multi_raft.add_group_as_learner(g.group_id, voters, other_learners)?;
        }
    }

    // 4. Register peer addresses in the transport.
    for node in &resp.nodes {
        if node.node_id != config.node_id
            && let Ok(addr) = node.addr.parse::<SocketAddr>()
        {
            transport.register_peer(node.node_id, addr);
        }
    }

    info!(
        node_id = config.node_id,
        nodes = topology.node_count(),
        groups = routing.num_groups(),
        "joined cluster"
    );

    Ok(ClusterState {
        topology,
        routing,
        multi_raft,
    })
}

#[cfg(test)]
mod tests {
    use super::super::bootstrap_fn::bootstrap;
    use super::super::handle_join::handle_join_request;
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    // ── Pure-function tests ───────────────────────────────────────

    #[test]
    fn parse_leader_hint_extracts_valid_addr() {
        assert_eq!(
            parse_leader_hint("not leader; retry at 10.0.0.1:9400"),
            Some("10.0.0.1:9400".parse().unwrap())
        );
        assert_eq!(
            parse_leader_hint("not leader; retry at 127.0.0.1:65535"),
            Some("127.0.0.1:65535".parse().unwrap())
        );
    }

    #[test]
    fn parse_leader_hint_rejects_unrelated_error() {
        assert_eq!(
            parse_leader_hint("node_id 2 already registered with different address 10.0.0.2:9400"),
            None
        );
        assert_eq!(parse_leader_hint(""), None);
        assert_eq!(
            parse_leader_hint("conf change commit timeout on group 0"),
            None
        );
    }

    #[test]
    fn parse_leader_hint_rejects_malformed_addr() {
        assert_eq!(parse_leader_hint("not leader; retry at notanaddress"), None);
        assert_eq!(parse_leader_hint("not leader; retry at "), None);
        assert_eq!(parse_leader_hint("not leader; retry at 10.0.0.1"), None);
    }

    #[test]
    fn next_backoff_schedule() {
        assert_eq!(next_backoff(0), Duration::ZERO);
        assert_eq!(next_backoff(1), Duration::from_millis(500));
        assert_eq!(next_backoff(2), Duration::from_secs(1));
        assert_eq!(next_backoff(3), Duration::from_secs(2));
        assert_eq!(next_backoff(4), Duration::from_secs(4));
        assert_eq!(next_backoff(5), Duration::from_secs(8));
        assert_eq!(next_backoff(6), Duration::from_secs(16));
        assert_eq!(next_backoff(7), Duration::from_secs(16));
        assert_eq!(next_backoff(100), Duration::from_secs(16));
    }

    // ── End-to-end bootstrap + join flow over QUIC ────────────────

    #[tokio::test]
    async fn full_bootstrap_join_flow() {
        // Node 1 bootstraps, Node 2 joins via QUIC.
        let t1 = Arc::new(NexarTransport::new(1, "127.0.0.1:0".parse().unwrap()).unwrap());
        let t2 = Arc::new(NexarTransport::new(2, "127.0.0.1:0".parse().unwrap()).unwrap());

        let (_dir1, catalog1) = temp_catalog();
        let (_dir2, catalog2) = temp_catalog();

        let addr1 = t1.local_addr();
        let addr2 = t2.local_addr();

        let config1 = ClusterConfig {
            node_id: 1,
            listen_addr: addr1,
            seed_nodes: vec![addr1],
            num_groups: 2,
            replication_factor: 1,
            data_dir: _dir1.path().to_path_buf(),
            force_bootstrap: false,
        };
        let state1 = bootstrap(&config1, &catalog1).unwrap();

        let topology1 = Arc::new(Mutex::new(state1.topology));
        let routing1 = Arc::new(state1.routing);

        struct JoinHandler {
            topology: Arc<Mutex<ClusterTopology>>,
            routing: Arc<RoutingTable>,
        }

        impl crate::transport::RaftRpcHandler for JoinHandler {
            async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
                match rpc {
                    RaftRpc::JoinRequest(req) => {
                        let mut topo = self.topology.lock().unwrap();
                        let resp = handle_join_request(&req, &mut topo, &self.routing);
                        Ok(RaftRpc::JoinResponse(resp))
                    }
                    other => Err(ClusterError::Transport {
                        detail: format!("unexpected: {other:?}"),
                    }),
                }
            }
        }

        let handler = Arc::new(JoinHandler {
            topology: topology1.clone(),
            routing: routing1.clone(),
        });

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let t1c = t1.clone();
        tokio::spawn(async move {
            t1c.serve(handler, shutdown_rx).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(30)).await;

        let config2 = ClusterConfig {
            node_id: 2,
            listen_addr: addr2,
            seed_nodes: vec![addr1],
            num_groups: 2,
            replication_factor: 1,
            data_dir: _dir2.path().to_path_buf(),
            force_bootstrap: false,
        };

        let state2 = join(&config2, &catalog2, &t2).await.unwrap();

        assert_eq!(state2.topology.node_count(), 2);
        assert_eq!(state2.routing.num_groups(), 2);

        // Verify node 2's state was persisted (reorder check: catalog
        // is saved before MultiRaft files are touched).
        assert!(catalog2.load_topology().unwrap().is_some());
        assert!(catalog2.load_routing().unwrap().is_some());

        // Verify node 1's topology was updated.
        let topo1 = topology1.lock().unwrap();
        assert_eq!(topo1.node_count(), 2);
        assert!(topo1.contains(2));

        shutdown_tx.send(true).unwrap();
    }
}
