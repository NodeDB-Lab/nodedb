//! In-memory registry ⇔ redb verification.
//!
//! Each submodule holds a single verifier for one registry
//! family. A verifier compares the redb truth against the
//! current in-memory state using the registry's snapshot/list
//! methods, reports divergences, and repairs by re-loading
//! from redb into the same registry (swap-in fresh).
//!
//! The top-level dispatcher lives in [`run`] to respect the
//! `mod.rs = pub mod + pub use` house rule.

pub mod alert;
pub mod api_keys;
pub mod blacklist;
pub mod change_stream;
pub mod consumer_group;
pub mod credential;
pub mod diff;
pub mod materialized_view;
pub mod permissions;
pub mod retention_policy;
pub mod rls_policy;
pub mod roles;
pub mod run;
pub mod schedule;
pub mod triggers;

pub use run::verify_registries;
