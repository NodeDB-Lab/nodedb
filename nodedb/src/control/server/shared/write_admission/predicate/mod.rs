// SPDX-License-Identifier: BUSL-1.1

//! Write-classification predicates for `PhysicalPlan`.

pub mod plan_is_write;
pub mod txn_buffering;
pub mod user_data_write;

pub use plan_is_write::plan_is_write;
pub use txn_buffering::{all_writes_bufferable, plan_requires_txn_buffering};
pub use user_data_write::plan_writes_user_data;
