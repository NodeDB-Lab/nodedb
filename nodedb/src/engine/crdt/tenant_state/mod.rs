// SPDX-License-Identifier: BUSL-1.1

//! Per-tenant CRDT engine state.
//!
//! Manages the loro-backed CRDT state, constraint validation, and dead-letter
//! queue for a single tenant. Lives on the Data Plane (one per tenant per core).

pub mod apply;
pub mod apply_validated;
pub mod constraints;
pub mod core;
pub mod dead_letters;
pub mod doc_mutate;
pub mod history;
pub mod list_ops;
pub mod policy;
pub mod preview;
pub mod rows;
mod snapshot_io;
mod snapshot_restore;
pub mod validate;

pub use apply_validated::{DeltaSigningAdmission, ValidatedApplyOutcome};
pub use core::TenantCrdtEngine;
