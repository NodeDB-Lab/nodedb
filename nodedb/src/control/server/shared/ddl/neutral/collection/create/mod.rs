// SPDX-License-Identifier: BUSL-1.1

//! `CREATE COLLECTION` / `CREATE TABLE` DDL — split by concern.
//!
//! Relocated from `pgwire::ddl::collection::create` (now deleted):
//! - [`build`] — the shared `build_and_persist` body + `Variant`
//! - [`build_column_defaults`] — DDL-time column `DEFAULT` gate for `build`
//! - [`build_flags`] — name / flag validation for `build`
//! - [`build_primary_engine`] — vector-primary resolution for `build`
//! - [`build_post_create`] — post-create side effects for `build`
//! - [`engine_option`] — `WITH (engine='...')` parsing/validation
//! - [`handler`] — the `create_collection` entry point
//! - [`table`] — the `create_table` entry point
//! - [`request`] — `CreateCollectionRequest`
//!
//! `build_column_defaults`, `build_flags`, `build_primary_engine`, and
//! `build_post_create` are
//! internal to [`build`] — declared here (siblings must be declared by the
//! parent module, not by `build` itself) but scoped no wider than `build`
//! needs them.

pub mod build;
pub(in crate::control::server::shared::ddl::neutral::collection::create) mod build_column_defaults;
pub(in crate::control::server::shared::ddl::neutral::collection::create) mod build_flags;
pub(in crate::control::server::shared::ddl::neutral::collection::create) mod build_post_create;
pub(in crate::control::server::shared::ddl::neutral::collection::create) mod build_primary_engine;
pub mod engine_option;
pub mod handler;
pub mod request;
pub mod table;

pub use handler::create_collection;
pub use request::CreateCollectionRequest;
pub use table::create_table;
