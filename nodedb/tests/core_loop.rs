//! Integration tests for CoreLoop execution across all engines.

#[path = "executor_tests/helpers.rs"]
mod helpers;
#[path = "executor_tests/test_cross_engine_validation.rs"]
mod test_cross_engine_validation;
#[path = "executor_tests/test_document.rs"]
mod test_document;
#[path = "executor_tests/test_graph.rs"]
mod test_graph;
#[path = "executor_tests/test_graph_bounds.rs"]
mod test_graph_bounds;
#[path = "executor_tests/test_transaction.rs"]
mod test_transaction;
#[path = "executor_tests/test_vector.rs"]
mod test_vector;
