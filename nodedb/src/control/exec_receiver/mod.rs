// SPDX-License-Identifier: BUSL-1.1

//! Local execution of incoming `ExecuteRequest` / `ExecuteStreamRequest` RPCs.

mod backup_cut;
pub mod executor;
mod plan_decode;
mod request_validation;
mod support;
mod tenant_marks;

pub use executor::LocalPlanExecutor;
