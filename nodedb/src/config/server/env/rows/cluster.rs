// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_NODE_ID` / `NODEDB_SEED_NODES` / `NODEDB_JOIN_RETRY_MAX_ATTEMPTS`
//! / `NODEDB_JOIN_RETRY_MAX_BACKOFF_SECS` overrides.
//!
//! Every row here needs a `[cluster]` section already in the loaded config.
//! The process cannot invent a cluster identity for itself.

use crate::config::server::ServerConfig;

use super::super::parse::{parse_u32_positive, parse_u64_positive};
use super::super::seed_nodes::parse_seed_nodes;
use super::super::table::EnvRow;

const NO_CLUSTER: &str = "a [cluster] section in the config file";

fn apply_node_id(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    let node_id = raw.trim().parse::<u64>().map_err(|_| "a u64 node id")?;
    let cluster = config.cluster.as_mut().ok_or(NO_CLUSTER)?;
    cluster.node_id = node_id;
    Ok(())
}

fn apply_seed_nodes(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    let addrs = parse_seed_nodes(raw)?;
    let cluster = config.cluster.as_mut().ok_or(NO_CLUSTER)?;
    cluster.seed_nodes = addrs;
    Ok(())
}

fn apply_join_retry_max_attempts(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    let attempts = parse_u32_positive(raw)?;
    let cluster = config.cluster.as_mut().ok_or(NO_CLUSTER)?;
    cluster.join_retry_max_attempts = attempts;
    Ok(())
}

fn apply_join_retry_max_backoff_secs(
    config: &mut ServerConfig,
    raw: &str,
) -> Result<(), &'static str> {
    let secs = parse_u64_positive(raw)?;
    let cluster = config.cluster.as_mut().ok_or(NO_CLUSTER)?;
    cluster.join_retry_max_backoff_secs = secs;
    Ok(())
}

pub(in super::super) const ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_NODE_ID",
        apply: apply_node_id,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_SEED_NODES",
        apply: apply_seed_nodes,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_JOIN_RETRY_MAX_ATTEMPTS",
        apply: apply_join_retry_max_attempts,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_JOIN_RETRY_MAX_BACKOFF_SECS",
        apply: apply_join_retry_max_backoff_secs,
        redact: false,
    },
];

#[cfg(test)]
mod tests {
    use super::super::super::apply_env_overrides;
    use super::*;
    use crate::config::server::ClusterSettings;

    fn make_cluster(node_id: u64) -> ClusterSettings {
        ClusterSettings {
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
        }
    }

    #[test]
    fn env_cluster_overrides() {
        unsafe {
            std::env::set_var("NODEDB_NODE_ID", "42");
            std::env::set_var("NODEDB_SEED_NODES", "10.0.0.1:9400,10.0.0.2:9400");
        }
        let mut cfg = ServerConfig {
            cluster: Some(make_cluster(1)),
            ..Default::default()
        };
        apply_env_overrides(&mut cfg).expect("valid cluster overrides must apply");
        let cluster = cfg.cluster.as_ref().expect("cluster section");
        assert_eq!(
            cluster.node_id, 42,
            "NODEDB_NODE_ID=42 must override node_id"
        );
        assert_eq!(
            cluster.seed_nodes.len(),
            2,
            "both seed addresses must apply"
        );
        assert_eq!(cluster.seed_nodes[0].to_string(), "10.0.0.1:9400");
        assert_eq!(cluster.seed_nodes[1].to_string(), "10.0.0.2:9400");
        unsafe {
            std::env::remove_var("NODEDB_NODE_ID");
            std::env::remove_var("NODEDB_SEED_NODES");
        }
    }
}
