// SPDX-License-Identifier: BUSL-1.1

//! `/v1/query`: materialized (buffer-then-respond) SQL execution.
//!
//! `request.rs` holds the entry point through admission; `shape.rs` runs
//! the per-task dispatch loop and shapes each task's response; `encode.rs`
//! maps errors onto the HTTP surface.

mod encode;
mod request;
mod shape;

pub use request::query;
