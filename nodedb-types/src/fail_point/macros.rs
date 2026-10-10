// SPDX-License-Identifier: Apache-2.0

//! The `fail_point!` and `fail_point_err!` injection macros.
//!
//! Each takes the scope the point is evaluated in as its first argument. A
//! call site with a node identity passes `FailScope::Node(id)`. The form
//! without a scope evaluates in `FailScope::Any`, for code with no node
//! identity. Without the calling crate's `failpoints` feature, a scoped form
//! only type-checks its scope and evaluates no name.

/// Inject a fail point. Does nothing at runtime without the `failpoints`
/// feature.
///
/// Usage in production code:
///   `nodedb_types::fail_point!(scope, "calvin_static::during_overlay_stage");`
///   `nodedb_types::fail_point!("wal::roll_before_seal");` (no node identity)
///
/// Tests opt in by enabling the feature and arming actions:
///   `FailGuard::for_node(node_id, "calvin_static::during_overlay_stage",
///                        FailAction::Panic);`
#[macro_export]
macro_rules! fail_point {
    ($name:expr) => {
        #[cfg(feature = "failpoints")]
        $crate::fail_point::eval($crate::fail_point::FailScope::Any, $name);
    };
    ($scope:expr, $name:expr) => {
        #[cfg(feature = "failpoints")]
        $crate::fail_point::eval($scope, $name);
        #[cfg(not(feature = "failpoints"))]
        let _: $crate::fail_point::FailScope = $scope;
    };
}

/// Inject a fail point that can abort the enclosing function with an error.
///
/// `$map` receives the detail string armed with `FailAction::Fail` and
/// returns the error value to propagate, so each crate injects its own error
/// type without the framework knowing about it:
///
/// ```ignore
/// fail_point_err!("wal::flush_out_of_space", |_| WalError::OutOfSpace {
///     context: "WAL segment append (failpoint)",
/// });
/// fail_point_err!(FailScope::Node(node_id), "cut_floor::before_persist", |detail| {
///     Error::Internal { detail }
/// });
/// ```
///
/// Does nothing at runtime without the `failpoints` feature.
#[macro_export]
macro_rules! fail_point_err {
    ($name:expr, $map:expr) => {
        #[cfg(feature = "failpoints")]
        if let Some(detail) =
            $crate::fail_point::eval_fail($crate::fail_point::FailScope::Any, $name)
        {
            return Err(($map)(detail));
        }
    };
    ($scope:expr, $name:expr, $map:expr) => {
        #[cfg(feature = "failpoints")]
        if let Some(detail) = $crate::fail_point::eval_fail($scope, $name) {
            return Err(($map)(detail));
        }
        #[cfg(not(feature = "failpoints"))]
        let _: $crate::fail_point::FailScope = $scope;
    };
}
