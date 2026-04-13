//! `CREATE COLLECTION` DDL — split by concern.
//!
//! - [`handler`] — the `create_collection` pgwire entry point
//! - [`register`] — Data Plane `DocumentOp::Register` dispatch
//!   (leader-side + applier-side variants)
//! - [`enforcement`] — parse WITH BALANCED / materialized_sum /
//!   generated-column helpers
//!
//! Public API stays at `super::create::*` via the re-exports
//! below — no callers outside this file need to change.

pub mod enforcement;
pub mod handler;
pub mod register;

#[cfg(test)]
mod tests;

pub use handler::create_collection;
pub use register::{dispatch_register_from_stored, dispatch_register_if_needed};
