//! Protocol-neutral machinery shared by every server entrypoint (pgwire, native, http).
pub mod authorization;
pub mod check_constraint;
pub mod ddl;
pub mod plan_util;
pub mod session;
pub mod sql;
pub mod write_admission;
