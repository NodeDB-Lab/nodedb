// SPDX-License-Identifier: BUSL-1.1

//! Local execution of incoming `ExecuteRequest` / `ExecuteStreamRequest` RPCs.

mod backup_cut;
pub mod executor;
mod metadata_applied;
mod plan_decode;
mod read_leg;
mod request_validation;
mod stream_events;
mod stream_scope;
mod support;
mod surrogate_binds;
mod tenant_marks;

pub use executor::LocalPlanExecutor;
