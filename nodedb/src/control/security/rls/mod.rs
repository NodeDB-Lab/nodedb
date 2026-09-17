// SPDX-License-Identifier: BUSL-1.1

//! Row-Level Security (RLS) policies.
//!
//! RLS adds per-row access control: predicates injected into physical
//! plans as mandatory filters. Not bypassable by application code.
//!
//! Layout:
//! - [`types`] — `RlsPolicy`, `PolicyType` data shapes.
//! - [`compile`] — `USING` text → typed `RlsPredicate` against the
//!   collection's declared columns.
//! - [`store`] — `RlsPolicyStore` in-memory CRUD + query methods.
//! - [`eval`] — read/write predicate evaluation on `RlsPolicyStore`
//!   (including `$auth.*` substitution).
//! - [`replication`] — helpers used by the `CatalogEntry` applier
//!   to sync replicated policies into the in-memory store.
//! - [`namespace`] — namespace-scoped `check_namespace_authz` helper.

pub mod compile;
pub mod eval;
pub mod namespace;
pub mod replication;
pub mod store;
pub mod types;

pub use eval::admit_compiled_write_image;
pub use namespace::check_namespace_authz;
pub use store::RlsPolicyStore;
pub use types::{PolicyType, RlsPolicy};
