// SPDX-License-Identifier: Apache-2.0

//! INSERT, UPSERT, UPDATE, DELETE, and TRUNCATE planning.

mod insert;
mod target;
mod update_delete;
mod upsert;

pub use insert::plan_insert;
pub use update_delete::{plan_delete, plan_truncate_stmt, plan_update};
pub use upsert::plan_upsert;
