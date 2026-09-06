// SPDX-License-Identifier: BUSL-1.1

//! Startup rejection of malformed and inapplicable cluster overrides.
//!
//! Orchestrated deployments set cluster identity and membership from the
//! environment. A node that boots with a fallback identity joins the wrong
//! group, or fails to join. The operator learns it from the cluster.

mod support;

use nodedb::ServerConfig;
use nodedb::config::server::{ClusterSettings, apply_env_overrides};
use support::env_guard::{EnvGuard, assert_rejected};

fn cluster_config(node_id: u64) -> ServerConfig {
    ServerConfig {
        cluster: Some(ClusterSettings {
            node_id,
            listen: "0.0.0.0:9400".parse().expect("listen address"),
            seed_nodes: vec!["127.0.0.1:9400".parse().expect("seed address")],
            num_groups: 4,
            replication_factor: 3,
            force_bootstrap: false,
            tls: None,
            max_active_sessions: 0,
            login_attempts_per_ip_per_min: 30,
            login_attempts_per_user_per_min: 10,
            insecure_transport: false,
            log_compaction_threshold: None,
            join_retry_max_attempts: 8,
            join_retry_max_backoff_secs: 32,
        }),
        ..Default::default()
    }
}

#[test]
fn malformed_node_id_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_NODE_ID", "not_a_number");
    let mut cfg = cluster_config(7);
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_NODE_ID",
        "not_a_number",
    );
}

/// A node id set with no `[cluster]` section is well-formed and still lost.
/// The operator asked for a cluster member. A standalone node that answers
/// queries hides the mistake better than a refused boot.
#[test]
fn node_id_without_cluster_section_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_NODE_ID", "42");
    let mut cfg = ServerConfig::default();
    assert_rejected(apply_env_overrides(&mut cfg), "NODEDB_NODE_ID", "42");
}

#[test]
fn malformed_seed_node_entry_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_SEED_NODES", "10.0.0.1:9400,garbage");
    let mut cfg = cluster_config(1);
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_SEED_NODES",
        "garbage",
    );
}

/// One bad entry must not drop the whole list back to the config value. The
/// surviving seeds are the ones the operator did not choose.
#[test]
fn seed_node_missing_port_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_SEED_NODES", "10.0.0.1,10.0.0.2:9400");
    let mut cfg = cluster_config(1);
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_SEED_NODES",
        "10.0.0.1",
    );
}

#[test]
fn seed_nodes_without_cluster_section_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_SEED_NODES", "10.0.0.1:9400");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_SEED_NODES",
        "10.0.0.1:9400",
    );
}

/// Cluster bring-up reads the join retry policy far from config load.
/// Validating it there is too late to refuse the boot. The gate checks it in
/// the same pass as every other override.
#[test]
fn malformed_join_retry_max_attempts_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_JOIN_RETRY_MAX_ATTEMPTS", "lots");
    let mut cfg = cluster_config(1);
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_JOIN_RETRY_MAX_ATTEMPTS",
        "lots",
    );
}

#[test]
fn zero_join_retry_max_attempts_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_JOIN_RETRY_MAX_ATTEMPTS", "0");
    let mut cfg = cluster_config(1);
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_JOIN_RETRY_MAX_ATTEMPTS",
        "0",
    );
}

#[test]
fn malformed_join_retry_max_backoff_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_JOIN_RETRY_MAX_BACKOFF_SECS", "30s");
    let mut cfg = cluster_config(1);
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_JOIN_RETRY_MAX_BACKOFF_SECS",
        "30s",
    );
}
