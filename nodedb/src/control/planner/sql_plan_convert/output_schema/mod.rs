// SPDX-License-Identifier: BUSL-1.1

pub mod build;
pub mod columns;
pub mod cte_types;
pub mod join_types;
pub mod returning;
pub mod set_op_columns;

pub use build::build_output_schema;
pub use returning::build_returning_schema;
pub use set_op_columns::{NoCollectionCatalog, check_set_op_arity, set_op_branch_columns};
