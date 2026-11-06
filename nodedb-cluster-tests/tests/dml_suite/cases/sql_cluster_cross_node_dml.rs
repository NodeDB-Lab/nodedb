// SPDX-License-Identifier: BUSL-1.1
//! End-to-end cluster test: CREATE / INSERT / SELECT across 3 pgwire
//! clients, one per node.
//!
//! Acceptance gate for the replicated catalog path. Replays the
//! production failure mode that motivated it:
//!
//! > CREATE COLLECTION on node 1, SELECT on node 2 → "unknown table"
//!
//! Tests are split by concern in `sql_cluster_cross_node_dml_tests/`.

#[path = "../../sql_cluster_cross_node_dml_tests/auth_objects.rs"]
mod auth_objects;
#[path = "../../sql_cluster_cross_node_dml_tests/calvin_read_failover_cross_node.rs"]
mod calvin_read_failover_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/cluster_boot.rs"]
mod cluster_boot;
#[path = "../../sql_cluster_cross_node_dml_tests/ddl_objects.rs"]
mod ddl_objects;
#[path = "../../sql_cluster_cross_node_dml_tests/gather_cross_node.rs"]
mod gather_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_algo_pagerank_as_of_cross_node.rs"]
mod graph_algo_pagerank_as_of_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_algo_pagerank_cross_node.rs"]
mod graph_algo_pagerank_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_algo_pagerank_dangling_cross_node.rs"]
mod graph_algo_pagerank_dangling_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_algo_pagerank_personalized_cross_node.rs"]
mod graph_algo_pagerank_personalized_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_algo_wcc_cross_node.rs"]
mod graph_algo_wcc_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_delete_reverse_cross_node.rs"]
mod graph_delete_reverse_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_edge_predicate_cross_node.rs"]
mod graph_edge_predicate_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_homed_read_failover.rs"]
mod graph_homed_read_failover;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_homed_read_per_core.rs"]
mod graph_homed_read_per_core;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_implicit_reverse_cross_node.rs"]
mod graph_implicit_reverse_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_match_cross_node.rs"]
mod graph_match_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_match_label_as_of_cross_node.rs"]
mod graph_match_label_as_of_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_match_read_set_cross_node.rs"]
mod graph_match_read_set_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_match_ryow_cross_node.rs"]
mod graph_match_ryow_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_match_varlen_truncation_recovery_cross_node.rs"]
mod graph_match_varlen_truncation_recovery_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_multicore_cross_node.rs"]
mod graph_multicore_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_native_capped_walks_cross_node.rs"]
mod graph_native_capped_walks_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_native_walks_cross_node.rs"]
mod graph_native_walks_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_native_walks_support.rs"]
mod graph_native_walks_support;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_owner_reads_cross_node.rs"]
mod graph_owner_reads_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_rag_fusion_cross_node.rs"]
mod graph_rag_fusion_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_rag_fusion_naming_cross_node.rs"]
mod graph_rag_fusion_naming_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_traverse_as_of_cross_node.rs"]
mod graph_traverse_as_of_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_traverse_cross_node.rs"]
mod graph_traverse_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_traverse_reverse_cross_node.rs"]
mod graph_traverse_reverse_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/join_cross_node.rs"]
mod join_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_apply_two_clusters.rs"]
mod mirror_apply_two_clusters;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_compaction.rs"]
mod mirror_bootstrap_compaction;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_engines.rs"]
mod mirror_bootstrap_engines;
#[cfg(feature = "failpoints")]
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_final_cut.rs"]
mod mirror_bootstrap_final_cut;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_fixture.rs"]
mod mirror_bootstrap_fixture;
#[cfg(feature = "failpoints")]
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_kill.rs"]
mod mirror_bootstrap_kill;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_promote.rs"]
mod mirror_bootstrap_promote;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_purge_convert.rs"]
mod mirror_bootstrap_purge_convert;
#[cfg(feature = "failpoints")]
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_recover.rs"]
mod mirror_bootstrap_recover;
#[cfg(feature = "failpoints")]
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_recover_fixture.rs"]
mod mirror_bootstrap_recover_fixture;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_retention.rs"]
mod mirror_bootstrap_retention;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_retention_fixture.rs"]
mod mirror_bootstrap_retention_fixture;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_retention_purge.rs"]
mod mirror_bootstrap_retention_purge;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_sync.rs"]
mod mirror_bootstrap_sync;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_bootstrap_two_clusters.rs"]
mod mirror_bootstrap_two_clusters;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_read_only_cross_node.rs"]
mod mirror_read_only_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/mirror_source_cross_node.rs"]
mod mirror_source_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/native_implicit_edge_delete_cross_node.rs"]
mod native_implicit_edge_delete_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/native_remote_home_surrogate_cross_node.rs"]
mod native_remote_home_surrogate_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/permission_tree_cross_node.rs"]
mod permission_tree_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/permission_tree_lease_partition.rs"]
mod permission_tree_lease_partition;
#[path = "../../sql_cluster_cross_node_dml_tests/schema_objects.rs"]
mod schema_objects;
#[path = "../../sql_cluster_cross_node_dml_tests/select_remote_stream_cross_node.rs"]
mod select_remote_stream_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/select_streaming_cross_node.rs"]
mod select_streaming_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/surrogate_identity_cross_node.rs"]
mod surrogate_identity_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/txn_resolve_on_follower_cross_node.rs"]
mod txn_resolve_on_follower_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/txn_stage_forward_cross_node.rs"]
mod txn_stage_forward_cross_node;
