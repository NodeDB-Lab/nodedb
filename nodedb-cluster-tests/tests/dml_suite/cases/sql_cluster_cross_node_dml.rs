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
#[path = "../../sql_cluster_cross_node_dml_tests/cluster_boot.rs"]
mod cluster_boot;
#[path = "../../sql_cluster_cross_node_dml_tests/ddl_objects.rs"]
mod ddl_objects;
#[path = "../../sql_cluster_cross_node_dml_tests/gather_cross_node.rs"]
mod gather_cross_node;
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
#[path = "../../sql_cluster_cross_node_dml_tests/graph_implicit_reverse_cross_node.rs"]
mod graph_implicit_reverse_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_match_cross_node.rs"]
mod graph_match_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_match_ryow_cross_node.rs"]
mod graph_match_ryow_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_match_varlen_truncation_recovery_cross_node.rs"]
mod graph_match_varlen_truncation_recovery_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_multicore_cross_node.rs"]
mod graph_multicore_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_traverse_cross_node.rs"]
mod graph_traverse_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/graph_traverse_reverse_cross_node.rs"]
mod graph_traverse_reverse_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/join_cross_node.rs"]
mod join_cross_node;
#[path = "../../sql_cluster_cross_node_dml_tests/native_implicit_edge_delete_cross_node.rs"]
mod native_implicit_edge_delete_cross_node;
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
