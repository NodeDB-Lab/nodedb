//! Protocol-neutral machinery shared by every server entrypoint (pgwire, native, http).
pub mod authorization;
pub mod check_constraint;
pub mod clone_read;
pub mod clone_write;
pub(crate) mod cluster_array_dispatch;
pub mod ddl;
pub mod metering;
pub mod panic_isolation;
pub mod plan_admission;
pub mod plan_util;
pub mod planning_overrides;
pub mod quota_admission;
pub mod response_payload;
pub mod retry;
pub mod returning;
pub mod session;
pub mod sql;
pub mod write_admission;

pub use panic_isolation::{ConnectionFutureOutcome, isolate_connection_future};
