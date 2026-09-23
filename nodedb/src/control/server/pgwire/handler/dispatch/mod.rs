// SPDX-License-Identifier: BUSL-1.1

//! Core dispatch mechanics: single-task dispatch, Raft replication, and local Data Plane submission.
//!
//! Split by concern:
//! - [`entry`]: the public dispatch entry points and the write-HLC
//!   bookkeeping wrapper around them.
//! - [`routing`]: the per-task routing decision — freeze/mirror checks,
//!   orchestrated DML, exchange resolution, and the replicated-vs-local
//!   choice.
//! - [`replicated`]: proposing a write to Raft and shaping the response once
//!   it applies.
//! - [`local`]: dispatching a task directly to the local Data Plane, with or
//!   without a per-task WAL append.
//! - [`authorize`]: the task-authorization helper and the CRDT
//!   admission-gate check shared by every routing path.

mod authorize;
mod entry;
mod local;
mod replicated;
mod routing;
