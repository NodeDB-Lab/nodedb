// SPDX-License-Identifier: BUSL-1.1

//! Column-level redaction: mask or pseudonymize fields based on role.
//!
//! Evaluated after RLS (row filtering), before result delivery.
//! Supports static masks (`'***@***.com'`) and hash pseudonymization
//! (`hash(email)` — joinable but not readable).
//!
//! Layout:
//! - [`types`] — `RedactionPolicy`, `RedactionRule`, `RedactionMode` data
//!   shapes.
//! - [`store`] — `RedactionStore` in-memory CRUD.
//! - [`apply`] — the shared rule-application logic: whole-document and
//!   per-SELECT-row.
//! - [`replication`] — helpers used by the `CatalogEntry` applier to sync
//!   replicated policies into the in-memory store.

pub mod apply;
pub mod replication;
pub mod snapshot_rules;
pub mod store;
pub mod types;

pub use store::RedactionStore;
pub use types::{
    HASH_REDACTION_TEXT_BYTES, MAX_REDACTION_MASK_BYTES, RedactionMode, RedactionPolicy,
    RedactionRule,
};
