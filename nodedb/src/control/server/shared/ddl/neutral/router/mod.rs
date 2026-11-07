// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL router.
//!
//! [`try_dispatch`] recognizes the migrated families and routes to them; every
//! other statement returns `None` so the transitional pgwire delegation in the
//! parent `super::super::dispatch` handles it.

mod dispatch;
mod helpers;
mod route_tokens;
mod string_admin;
mod string_engine_ops;
mod string_introspection;
mod string_schema;
mod string_streaming;
mod string_versioning;
mod sync_wait;
mod typed_auth;
mod typed_automation;
mod typed_collection;
mod typed_database;
mod typed_misc;
mod typed_policy;
mod typed_streamview;

pub use dispatch::try_dispatch;
