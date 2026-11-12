//! Protocol-neutral machinery shared by every server entrypoint (pgwire, native, http).
pub mod authorization;
pub(crate) mod backup_metering;
pub(crate) mod calvin_metered_rows;
pub mod check_constraint;
pub mod clone_read;
pub mod clone_write;
pub(crate) mod cluster_array_dispatch;
pub mod ddl;
pub mod metering;
pub mod panic_isolation;
pub mod plan_admission;
mod plan_admission_request;
#[cfg(test)]
mod plan_admission_tests;
pub mod plan_util;
pub mod planning_overrides;
pub mod quota_admission;
pub(crate) mod request_collection;
pub mod request_route;
pub mod response_payload;
pub mod retry;
pub mod returning;
pub mod session;
pub mod sql;
pub(crate) mod sql_split;
#[cfg(test)]
mod sql_split_syntax_tests;
pub mod statement_exec;
pub mod txn_control;
pub mod txn_route;
pub mod write_admission;

pub use panic_isolation::{ConnectionFutureOutcome, isolate_connection_future};
