// SPDX-License-Identifier: BUSL-1.1

mod copy_returning;
pub(crate) mod copy_rows;
pub(crate) mod expand_staged;
pub mod orchestrator;

pub(crate) use expand_staged::resolve_and_emit_insert_select_ops;
pub use orchestrator::run_authorized_insert_select;
