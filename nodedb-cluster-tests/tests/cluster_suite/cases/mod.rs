// SPDX-License-Identifier: BUSL-1.1

// Shared cluster-bringup helper, declared once here: a file loaded as a
// module from two places is compiled twice under two paths.
#[path = "../../cluster_common/mod.rs"]
mod cluster_common;

mod calvin_3node_normal;
mod calvin_3node_shard_failover;
mod calvin_e2e_ollp;
mod calvin_e2e_pgwire;
mod calvin_sequencer_cut_floor_failover;
mod calvin_sequencer_failover;
mod cluster_join;
mod cluster_join_build_id_mismatch;
mod cluster_join_idempotent;
mod cluster_join_leader_crash;
mod cluster_join_race;
mod cluster_join_redirect;
mod elastic_scaling;
mod elastic_scaling_churn;
mod metadata_replication;
mod migration_crash_recovery;
