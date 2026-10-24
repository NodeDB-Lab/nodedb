// SPDX-License-Identifier: BUSL-1.1

//! Post-apply side effects for a [`CatalogEntry`] — dispatched by
//! DDL family.
//!
//! Split into two phases so readers of `applied_index` observe a
//! consistent view:
//!
//! - [`apply_post_apply_side_effects_sync`] (in `sync`) runs the
//!   synchronous in-memory cache updates **inline** on the raft
//!   applier thread, BEFORE the metadata applier bumps the
//!   `AppliedIndexWatcher`.
//! - [`run_post_apply_async_side_effects`] (in `async_dispatch`)
//!   runs the async work — on **every node** (leader and followers)
//!   so each node's local Data Plane observes catalog mutations
//!   symmetrically. The metadata applier awaits it before it
//!   advances past the entry.

// Per-family modules (existing).
pub mod alert_rule;
pub mod api_key;
pub mod array;
pub mod auth_user;
pub mod change_stream;
pub mod collection;
pub mod consumer_group;
pub mod continuous_aggregate;
pub mod custom_type;
pub mod database;
pub mod function;
pub mod materialized_view;
pub mod oidc_provider;
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

// Orchestration modules.
mod async_dispatch;
pub(crate) mod gateway_invalidation;
mod sync;

pub(crate) use async_dispatch::array::{
    delete_on_every_core as drop_array_on_every_core,
    open_on_every_core as open_array_on_every_core,
};
pub(crate) use async_dispatch::collection::{
    ReclaimFailure, clear_before_recreate, reclaim_collection_storage,
};
pub(crate) use async_dispatch::continuous_aggregate::{
    RegisterFailure as ContinuousAggregateRegisterFailure,
    delete_async as unregister_continuous_aggregate,
    finish_boot_registration_on_every_core as finish_continuous_aggregate_boot_registration,
    register_def_on_every_core as register_continuous_aggregate_def_on_every_core,
    register_on_every_core as register_continuous_aggregate_on_every_core,
    unregister_on_every_core as unregister_continuous_aggregate_on_every_core,
};
pub(crate) use async_dispatch::crdt_compact::{compact_async, drain_pending_compactions};
pub use async_dispatch::run_post_apply_async_side_effects;
pub(crate) use async_dispatch::spatial_ref_sys::install_on_every_core as install_custom_crs_set;
pub(crate) use async_dispatch::synonym_group::delete_async as remove_synonym_group;
pub(crate) use async_dispatch::synonym_group::put_async as install_synonym_group;
pub(crate) use async_dispatch::vector::delete_async as remove_vector_index_params;
pub(crate) use async_dispatch::vector::longest_core_wait as vector_install_longest_core_wait;
pub(crate) use async_dispatch::vector::put_async as install_vector_index_params;
pub use sync::apply_post_apply_side_effects_sync;
