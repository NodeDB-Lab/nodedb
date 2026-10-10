// SPDX-License-Identifier: Apache-2.0

//! Minimal feature-gated fail-point framework for crash-injection tests.
//!
//! When the `failpoints` Cargo feature is OFF (the default and what release
//! builds compile with), `fail_point!` expands to nothing at runtime.
//!
//! When the feature is ON, each invocation looks up its scope and name in a
//! process-wide registry. A point evaluated with scope `Node(n)` fires the
//! action armed for `(Node(n), name)`, else the one armed for `(Any, name)`.
//! A point evaluated with scope `Any` fires only the `(Any, name)` action.
//! The actions are listed on [`FailAction`].
//!
//! An in-process cluster test runs several nodes in one process, so they
//! share the registry. The scope arms one node's point and leaves the
//! others running.
//!
//! Tests arm actions with `set` / `clear` or the RAII [`FailGuard`]. A
//! spawned server reads its actions once from [`FAILPOINTS_ENV`].
//!
//! Feature gating is per-crate: the macros expand under the *calling* crate's
//! `failpoints` feature, so every crate that injects a fail point declares its
//! own `failpoints = ["nodedb-types/failpoints"]`.

#[cfg(feature = "failpoints")]
pub mod action;
#[cfg(feature = "failpoints")]
pub mod env;
#[cfg(feature = "failpoints")]
pub mod guard;
pub mod macros;
#[cfg(feature = "failpoints")]
pub mod registry;
pub mod scope;

#[cfg(feature = "failpoints")]
pub use action::FailAction;
#[cfg(feature = "failpoints")]
pub use env::FAILPOINTS_ENV;
#[cfg(feature = "failpoints")]
pub use guard::FailGuard;
#[cfg(feature = "failpoints")]
pub use registry::{clear, eval, eval_fail, lookup, set};
pub use scope::FailScope;
