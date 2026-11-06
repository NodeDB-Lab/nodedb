// SPDX-License-Identifier: BUSL-1.1

//! Async Data-Plane dispatch for DDL, DSL, and system-initiated work.
//!
//! Two doors, and the type system says which one a caller took: a
//! [`CloneCheckedTask`](crate::control::server::shared::clone_write::CloneCheckedTask)
//! for work a user asked for — clone-checked and authorized in one step, so
//! neither hook can be skipped — or a [`SystemTask`] naming why no user exists.

mod dispatch;
mod system_task;

pub(crate) use dispatch::{
    dispatch_authorized, dispatch_committed_apply, dispatch_system, dispatch_system_by,
    dispatch_system_response_with_source,
};
pub(crate) use system_task::{SystemReason, SystemTask};
