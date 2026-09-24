// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for CoreLoop execution across all engines.

#[path = "executor_tests/helpers.rs"]
pub(in crate::cases) mod helpers;
#[path = "executor_tests/test_aggregate_aliases.rs"]
mod test_aggregate_aliases;
#[path = "executor_tests/test_aggregate_chunk_limit.rs"]
mod test_aggregate_chunk_limit;
#[path = "executor_tests/test_array_ops.rs"]
mod test_array_ops;
#[path = "executor_tests/test_columnar_aggregate.rs"]
mod test_columnar_aggregate;
#[path = "executor_tests/test_conditional_update.rs"]
mod test_conditional_update;
#[path = "executor_tests/test_conflict_policy_register.rs"]
mod test_conflict_policy_register;
#[path = "executor_tests/test_cross_engine_validation.rs"]
mod test_cross_engine_validation;
#[path = "executor_tests/test_cross_type_join/mod.rs"]
mod test_cross_type_join;
#[path = "executor_tests/test_document.rs"]
mod test_document;
#[path = "executor_tests/test_facet.rs"]
mod test_facet;
#[path = "executor_tests/test_generated_columns.rs"]
mod test_generated_columns;
#[path = "executor_tests/test_graph.rs"]
mod test_graph;
#[path = "executor_tests/test_graph_bounds.rs"]
mod test_graph_bounds;
#[path = "executor_tests/test_graph_savepoint_overlay.rs"]
mod test_graph_savepoint_overlay;
#[path = "executor_tests/test_group_by_alias.rs"]
mod test_group_by_alias;
#[path = "executor_tests/test_kv.rs"]
mod test_kv;
#[path = "executor_tests/test_kv_advanced.rs"]
mod test_kv_advanced;
#[path = "executor_tests/test_kv_scan_budget.rs"]
mod test_kv_scan_budget;
#[path = "executor_tests/test_kv_ttl_overlay.rs"]
mod test_kv_ttl_overlay;
#[path = "executor_tests/test_ollp_verification.rs"]
mod test_ollp_verification;
#[path = "executor_tests/test_range_scan_bitemporal.rs"]
mod test_range_scan_bitemporal;
#[path = "executor_tests/test_security_and_isolation.rs"]
mod test_security_and_isolation;
#[path = "executor_tests/test_tenant_cache_isolation.rs"]
mod test_tenant_cache_isolation;
#[path = "executor_tests/test_tenant_isolation_cdc.rs"]
mod test_tenant_isolation_cdc;
#[path = "executor_tests/test_tenant_isolation_cdc_negative.rs"]
mod test_tenant_isolation_cdc_negative;
#[path = "executor_tests/test_tenant_isolation_fulltext.rs"]
mod test_tenant_isolation_fulltext;
#[path = "executor_tests/test_tenant_isolation_fulltext_negative.rs"]
mod test_tenant_isolation_fulltext_negative;
#[path = "executor_tests/test_tenant_isolation_graph.rs"]
mod test_tenant_isolation_graph;
#[path = "executor_tests/test_tenant_isolation_graph_negative.rs"]
mod test_tenant_isolation_graph_negative;
#[path = "executor_tests/test_tenant_isolation_kv.rs"]
mod test_tenant_isolation_kv;
#[path = "executor_tests/test_tenant_isolation_kv_negative.rs"]
mod test_tenant_isolation_kv_negative;
#[path = "executor_tests/test_tenant_isolation_rls.rs"]
mod test_tenant_isolation_rls;
#[path = "executor_tests/test_tenant_isolation_rls_negative.rs"]
mod test_tenant_isolation_rls_negative;
#[path = "executor_tests/test_tenant_isolation_sparse.rs"]
mod test_tenant_isolation_sparse;
#[path = "executor_tests/test_tenant_isolation_sparse_negative.rs"]
mod test_tenant_isolation_sparse_negative;
#[path = "executor_tests/test_tenant_isolation_timeseries.rs"]
mod test_tenant_isolation_timeseries;
#[path = "executor_tests/test_tenant_isolation_timeseries_negative.rs"]
mod test_tenant_isolation_timeseries_negative;
#[path = "executor_tests/test_tenant_isolation_vector.rs"]
mod test_tenant_isolation_vector;
#[path = "executor_tests/test_tenant_isolation_vector_negative.rs"]
mod test_tenant_isolation_vector_negative;
#[path = "executor_tests/test_tenant_purge.rs"]
mod test_tenant_purge;
#[path = "executor_tests/test_tenant_quota.rs"]
mod test_tenant_quota;
#[path = "executor_tests/test_timeseries.rs"]
mod test_timeseries;
#[path = "executor_tests/test_timeseries_budget.rs"]
mod test_timeseries_budget;
#[path = "executor_tests/test_timeseries_scan_budget.rs"]
mod test_timeseries_scan_budget;
#[path = "executor_tests/test_transaction.rs"]
mod test_transaction;
#[path = "executor_tests/test_transaction_cross_engine.rs"]
mod test_transaction_cross_engine;
#[path = "executor_tests/test_transaction_matrix.rs"]
mod test_transaction_matrix;
#[path = "executor_tests/test_transaction_matrix_helpers.rs"]
mod test_transaction_matrix_helpers;
#[path = "executor_tests/test_transaction_matrix_kv.rs"]
mod test_transaction_matrix_kv;
#[path = "executor_tests/test_vector.rs"]
mod test_vector;
