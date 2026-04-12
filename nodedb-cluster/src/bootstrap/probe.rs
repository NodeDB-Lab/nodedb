//! Bootstrap probe: deterministic "who should bootstrap" decision and
//! the live Ping probe used to confirm the elected bootstrapper is up.
//!
//! # The rule
//!
//! To eliminate the `should_bootstrap` race where multiple seeds each
//! saw "no other seed is up" and all bootstrapped disjoint clusters,
//! we use a **deterministic elected-bootstrapper** rule:
//!
//! > The seed whose `SocketAddr` is lexicographically smallest is the
//! > designated bootstrapper. Every other seed calls `join()`.
//!
//! `SocketAddr` has a total ordering (IPv4 octets compare before IPv6,
//! and ports tie-break), so every node given the same seed list agrees
//! on the same bootstrapper without any network round-trips — no race
//! is possible.
//!
//! # The Ping probe
//!
//! When this node is **not** the designated bootstrapper, we still
//! want to give the elected seed a short window to come up before
//! entering the retry-backoff loop in `join()`. `ping_probe` sends a
//! cheap, side-effect-free `RaftRpc::Ping` to the elected seed up to
//! `MAX_PROBE_ATTEMPTS` times at `PROBE_INTERVAL` spacing. Any
//! successful `Pong` response means the bootstrapper is alive — we
//! immediately return `false` so the caller falls through to `join()`.
//! If every attempt fails we still return `false` (the caller's join
//! loop has its own retry schedule and will handle the slow-start
//! case).
//!
//! # The force flag
//!
//! `ClusterConfig.force_bootstrap` is an operator escape hatch for
//! disaster recovery — the designated bootstrapper has been lost
//! permanently and the operator wants a different seed to take over.
//! When set, `should_bootstrap` returns `true` without probing.

use std::net::SocketAddr;
use std::time::Duration;

use tracing::{debug, info};

use crate::rpc_codec::{PingRequest, RaftRpc};
use crate::transport::NexarTransport;

use super::config::ClusterConfig;

/// How many Ping attempts to make against the designated bootstrapper
/// before giving up and handing off to the caller's join loop.
const MAX_PROBE_ATTEMPTS: u32 = 10;

/// Delay between consecutive Ping attempts when the previous one
/// failed. Tunes the probe cadence for the common case of "designated
/// bootstrapper has not finished its first election yet".
const PROBE_INTERVAL: Duration = Duration::from_millis(300);

/// Hard per-attempt timeout. The underlying QUIC transport can block
/// for seconds on an unreachable endpoint while it retries the
/// handshake; bounding each attempt explicitly keeps the total
/// `should_bootstrap` window predictable regardless of the transport's
/// internal schedule.
const PROBE_TIMEOUT: Duration = Duration::from_millis(200);

/// Decide whether this node should bootstrap a new cluster.
///
/// Returns `true` iff any of the following hold:
///
/// 1. `config.force_bootstrap` is set (operator escape hatch).
/// 2. This node's `listen_addr` is the lexicographically smallest
///    entry in `config.seed_nodes` — the deterministic elected
///    bootstrapper.
///
/// Returns `false` in every other case, including when the designated
/// bootstrapper is currently unreachable: the caller's `join()` loop
/// owns its own retry schedule and is the correct place to wait for
/// the bootstrapper to come up.
///
/// This function performs a live Ping probe **only** when it is about
/// to return `false` anyway — the probe is best-effort observability
/// and logging, not a decision input. With a deterministic rule there
/// is nothing to race on.
pub(super) async fn should_bootstrap(config: &ClusterConfig, transport: &NexarTransport) -> bool {
    if config.force_bootstrap {
        info!(
            node_id = config.node_id,
            listen_addr = %config.listen_addr,
            "force_bootstrap flag set — bootstrapping unconditionally"
        );
        return true;
    }

    let designated = match designated_bootstrapper(&config.seed_nodes) {
        Some(addr) => addr,
        None => {
            // Empty seed list — caller already treats this as an
            // implicit single-node bootstrap (`seed_nodes = [self]`
            // fallback in `TestNode::spawn` / the main binary's
            // config layer). Bootstrap is the only reasonable choice.
            return true;
        }
    };

    if designated == config.listen_addr {
        info!(
            node_id = config.node_id,
            listen_addr = %config.listen_addr,
            "this node is the designated bootstrapper"
        );
        return true;
    }

    info!(
        node_id = config.node_id,
        listen_addr = %config.listen_addr,
        %designated,
        "deferring to designated bootstrapper; probing for liveness"
    );

    // Non-blocking best-effort probe — each attempt is bounded by
    // PROBE_TIMEOUT, so the total window is at most
    // MAX_PROBE_ATTEMPTS * (PROBE_TIMEOUT + PROBE_INTERVAL). Exits
    // early as soon as the bootstrapper answers, so the common case is
    // a single sub-second round trip.
    for attempt in 0..MAX_PROBE_ATTEMPTS {
        let probe_result = tokio::time::timeout(
            PROBE_TIMEOUT,
            ping_probe(designated, transport, config.node_id),
        )
        .await;

        match probe_result {
            Ok(Ok(())) => {
                info!(
                    node_id = config.node_id,
                    %designated,
                    attempt,
                    "designated bootstrapper is up"
                );
                return false;
            }
            Ok(Err(e)) => {
                debug!(
                    node_id = config.node_id,
                    %designated,
                    attempt,
                    error = %e,
                    "ping probe failed"
                );
            }
            Err(_elapsed) => {
                debug!(
                    node_id = config.node_id,
                    %designated,
                    attempt,
                    timeout_ms = PROBE_TIMEOUT.as_millis() as u64,
                    "ping probe timed out"
                );
            }
        }
        if attempt + 1 < MAX_PROBE_ATTEMPTS {
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
    }

    info!(
        node_id = config.node_id,
        %designated,
        "designated bootstrapper did not respond; proceeding to join loop"
    );
    false
}

/// Look up the lexicographically smallest seed address, which is the
/// designated bootstrapper under the single-elected-bootstrapper rule.
///
/// Pure function — exported for unit testing.
pub(super) fn designated_bootstrapper(seed_nodes: &[SocketAddr]) -> Option<SocketAddr> {
    seed_nodes.iter().min().copied()
}

/// Send a single Ping RPC to `addr` and wait for the response.
///
/// Returns `Ok(())` on any successful `Pong` reply, or an error
/// describing the failure. Unlike the old `JoinRequest`-as-probe, Ping
/// is idempotent and has no state-mutation intent.
async fn ping_probe(
    addr: SocketAddr,
    transport: &NexarTransport,
    self_node_id: u64,
) -> crate::error::Result<()> {
    let rpc = RaftRpc::Ping(PingRequest {
        sender_id: self_node_id,
        topology_version: 0,
    });
    match transport.send_rpc_to_addr(addr, rpc).await {
        Ok(RaftRpc::Pong(_)) => Ok(()),
        Ok(other) => Err(crate::error::ClusterError::Transport {
            detail: format!("unexpected response to Ping from {addr}: {other:?}"),
        }),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn cfg_with_seeds(node_id: u64, listen: &str, seeds: &[&str]) -> ClusterConfig {
        // `data_dir` is not touched by `should_bootstrap` — the probe
        // phase is network-only. A placeholder path is fine.
        ClusterConfig {
            node_id,
            listen_addr: addr(listen),
            seed_nodes: seeds.iter().map(|s| addr(s)).collect(),
            num_groups: 2,
            replication_factor: 1,
            data_dir: std::env::temp_dir(),
            force_bootstrap: false,
        }
    }

    #[test]
    fn designated_bootstrapper_picks_smallest() {
        let seeds = vec![
            addr("10.0.0.3:9400"),
            addr("10.0.0.1:9400"),
            addr("10.0.0.2:9400"),
        ];
        assert_eq!(designated_bootstrapper(&seeds), Some(addr("10.0.0.1:9400")));
    }

    #[test]
    fn designated_bootstrapper_empty_is_none() {
        assert!(designated_bootstrapper(&[]).is_none());
    }

    #[test]
    fn designated_bootstrapper_tie_break_by_port() {
        // Same IP, different ports — smaller port wins.
        let seeds = vec![addr("10.0.0.1:9401"), addr("10.0.0.1:9400")];
        assert_eq!(designated_bootstrapper(&seeds), Some(addr("10.0.0.1:9400")));
    }

    #[tokio::test]
    async fn should_bootstrap_when_self_is_lowest_seed() {
        let cfg = cfg_with_seeds(
            1,
            "10.0.0.1:9400",
            &["10.0.0.1:9400", "10.0.0.2:9400", "10.0.0.3:9400"],
        );
        let transport = Arc::new(NexarTransport::new(1, "127.0.0.1:0".parse().unwrap()).unwrap());
        assert!(should_bootstrap(&cfg, &transport).await);
    }

    #[tokio::test]
    async fn force_bootstrap_overrides_rule() {
        // Not the lowest seed, but force flag is set.
        let mut cfg = cfg_with_seeds(
            3,
            "10.0.0.3:9400",
            &["10.0.0.1:9400", "10.0.0.2:9400", "10.0.0.3:9400"],
        );
        cfg.force_bootstrap = true;
        let transport = Arc::new(NexarTransport::new(3, "127.0.0.1:0".parse().unwrap()).unwrap());
        assert!(should_bootstrap(&cfg, &transport).await);
    }

    #[tokio::test]
    async fn should_bootstrap_false_when_designated_unreachable() {
        // Not the lowest seed, force flag unset, designated bootstrapper
        // (10.0.0.1:9400) is unreachable — probe should eventually fail
        // and return `false` so the caller proceeds to the join loop.
        //
        // Use `127.0.0.1` addresses to keep the probe on loopback but
        // dial a port we know nobody is bound on to force an error.
        //
        // Override MAX_PROBE_ATTEMPTS indirectly by pointing at a seed
        // address this test owns: we construct the config with a
        // non-routable designated seed (`127.0.0.1:1` which is below
        // the privileged-port range and nothing can bind there without
        // root). If the probe quietly succeeds that's a bug.
        let cfg = cfg_with_seeds(2, "127.0.0.1:9400", &["127.0.0.1:1", "127.0.0.1:9400"]);
        let transport = Arc::new(NexarTransport::new(2, "127.0.0.1:0".parse().unwrap()).unwrap());
        // This test is bounded by MAX_PROBE_ATTEMPTS * PROBE_INTERVAL
        // ~= 5 s. Wrap in a timeout so a regression hangs instead of
        // stalling the whole test suite.
        let result =
            tokio::time::timeout(Duration::from_secs(8), should_bootstrap(&cfg, &transport))
                .await
                .expect("should_bootstrap should not hang");
        assert!(!result);
    }
}
