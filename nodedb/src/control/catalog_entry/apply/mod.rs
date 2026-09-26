// SPDX-License-Identifier: BUSL-1.1

//! Synchronous host-side application of a [`CatalogEntry`] to
//! `SystemCatalog` redb — dispatched by DDL family.
//!
//! [`apply_to`] is an exhaustive match routing each variant to a typed
//! function in a per-family sibling file, one match arm per variant.

pub mod alert_rule;
pub mod api_key;
pub mod auth_user;
pub mod change_stream;
pub mod checkpoint;
pub mod collection;
pub mod column_stats;
pub mod consumer_group;
pub mod continuous_aggregate;
pub mod custom_type;
pub mod database;
mod dispatch;
pub mod function;
pub mod index_registry;
pub mod local;
pub mod materialized_view;
pub mod oidc_provider;
pub mod outcome;
pub mod owner;
pub mod permission;
pub mod procedure;
pub mod quota;
pub mod redaction;
pub mod retention_policy;
pub mod rls;
pub mod role;
pub mod schedule;
pub mod scope_grant;
pub mod scope_quota;
pub mod sequence;
pub mod streaming_materialized_view;
pub mod synonym_group;
pub mod tenant;
pub mod topic;
pub mod trigger;
pub mod user;
pub mod vector;
pub mod wal_tombstone;

pub use dispatch::apply_to;
pub use outcome::ApplyOutcome;
