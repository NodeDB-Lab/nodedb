// SPDX-License-Identifier: BUSL-1.1

//! `/v1/query`: materialized (buffer-then-respond) SQL execution.
//!
//! - `request.rs`: the entry point through admission and the shared atomic
//!   routes.
//! - `shape.rs`: the per-task dispatch loop.
//! - `orchestrated.rs`: the plans that run on the Control Plane instead of
//!   the gateway route, cluster array ops among them.
//! - `append.rs`: shaping each task's answer into JSON rows, and the
//!   per-task metering helper.
//! - `encode.rs`: maps errors onto the HTTP surface.

mod append;
mod encode;
mod orchestrated;
mod request;
mod shape;

pub use request::query;
