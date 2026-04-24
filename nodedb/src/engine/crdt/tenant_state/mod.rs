//! Per-tenant CRDT engine state.
//!
//! Manages the loro-backed CRDT state, constraint validation, and dead-letter
//! queue for a single tenant. Lives on the Data Plane (one per tenant per core).

pub mod core;
pub mod history;
pub mod policy;

#[cfg(test)]
mod tests;

pub use core::TenantCrdtEngine;
