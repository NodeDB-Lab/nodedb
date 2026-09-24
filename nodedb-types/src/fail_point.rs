// SPDX-License-Identifier: Apache-2.0

//! Minimal feature-gated fail-point framework for crash-injection tests.
//!
//! When the `failpoints` Cargo feature is OFF (the default and what release
//! builds compile with), `fail_point!(name)` expands to nothing — zero
//! runtime cost.
//!
//! When the feature is ON, each invocation looks up `name` in a process-wide
//! registry. If a [`FailAction`] has been installed for that name, the action
//! fires:
//!   - `Panic`: panic immediately with a message naming the fail point.
//!   - `Sleep(d)`: sleep for `d`, useful for race-condition tests.
//!   - `Fail(detail)`: make the injected call site return an error. Only
//!     [`fail_point_err!`] can honour this — the call site supplies the
//!     mapping into its own error type, so no crate has to know about
//!     anyone else's.
//!
//! The framework is deliberately tiny — no fail-rs dep, no parsing of env
//! vars, no list of probabilities. Tests install actions explicitly via
//! `set` / `clear` and must clean up in their own teardown (or use
//! `FailGuard` for RAII cleanup).
//!
//! Feature gating is per-crate: the macros expand under the *calling* crate's
//! `failpoints` feature, so every crate that injects a fail point declares its
//! own `failpoints = ["nodedb-types/failpoints"]`.

#[cfg(feature = "failpoints")]
mod imp {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};
    use std::time::Duration;

    /// Action a fail point performs when triggered.
    #[derive(Debug, Clone)]
    pub enum FailAction {
        /// Panic with a message that names the fail-point.
        Panic,
        /// Kill the whole process immediately via `abort()`.
        ///
        /// `Panic` only unwinds the current task, which a supervising runtime
        /// absorbs — no use for simulating a crash in a spawned server. This
        /// is the action a process-kill harness arms.
        Abort,
        /// Sleep for the given duration; execution then continues normally.
        Sleep(Duration),
        /// Return an error from the injected call site, carrying this detail.
        /// Ignored by bare `fail_point!` — use `fail_point_err!`.
        Fail(String),
    }

    /// Environment variable read once, the first time any fail point is
    /// evaluated. Lets a test arm injections in a *spawned* process — the
    /// in-process `set` API cannot reach a server the test only supervises.
    ///
    /// Format: comma-separated `name=action`, where action is `panic`,
    /// `sleep(<millis>)`, or `fail(<detail>)`. For example:
    /// `NODEDB_FAILPOINTS='checkpoint::after_marker_before_truncate=panic'`
    pub const FAILPOINTS_ENV: &str = "NODEDB_FAILPOINTS";

    static REGISTRY: LazyLock<Mutex<HashMap<String, FailAction>>> =
        LazyLock::new(|| Mutex::new(parse_env(std::env::var(FAILPOINTS_ENV).ok().as_deref())));

    pub(super) fn parse_env(spec: Option<&str>) -> HashMap<String, FailAction> {
        let mut actions = HashMap::new();
        let Some(spec) = spec else {
            return actions;
        };
        for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let Some((name, action)) = entry.split_once('=') else {
                panic!("{FAILPOINTS_ENV} entry {entry:?} is not `name=action`");
            };
            let action = match action.trim() {
                "panic" => FailAction::Panic,
                "abort" => FailAction::Abort,
                rest if rest.starts_with("sleep(") && rest.ends_with(')') => {
                    let millis =
                        rest["sleep(".len()..rest.len() - 1]
                            .parse()
                            .unwrap_or_else(|_| {
                                panic!("{FAILPOINTS_ENV} entry {entry:?} has a non-numeric sleep")
                            });
                    FailAction::Sleep(Duration::from_millis(millis))
                }
                rest if rest.starts_with("fail(") && rest.ends_with(')') => {
                    FailAction::Fail(rest["fail(".len()..rest.len() - 1].to_string())
                }
                other => panic!("{FAILPOINTS_ENV} entry {entry:?} has unknown action {other:?}"),
            };
            actions.insert(name.trim().to_string(), action);
        }
        actions
    }

    /// Install an action for a named fail point. Subsequent
    /// `fail_point!(name)` invocations will fire the action.
    pub fn set(name: &str, action: FailAction) {
        REGISTRY
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(name.to_string(), action);
    }

    /// Remove any installed action for the named fail point.
    pub fn clear(name: &str) {
        REGISTRY
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(name);
    }

    /// Look up the installed action for a fail point (None if not set).
    pub fn lookup(name: &str) -> Option<FailAction> {
        REGISTRY
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(name)
            .cloned()
    }

    /// Evaluate the installed action for a fail point. Used by the
    /// `fail_point!` macro; not intended to be called directly.
    pub fn eval(name: &str) {
        if let Some(action) = lookup(name) {
            match action {
                FailAction::Panic => panic!("fail_point fired: {name}"),
                FailAction::Abort => abort(name),
                FailAction::Sleep(d) => std::thread::sleep(d),
                // A caller that cannot return an error must not silently
                // swallow the injection: an armed failpoint that does nothing
                // makes a test pass for the wrong reason.
                FailAction::Fail(detail) => {
                    panic!(
                        "fail_point {name} installed Fail({detail}) but the call site cannot return an error — use fail_point_err!"
                    )
                }
            }
        }
    }

    /// Kill the process at a fail point. Flushes the reason first — an
    /// unexplained SIGABRT in a test log is indistinguishable from a real bug.
    fn abort(name: &str) -> ! {
        eprintln!("fail_point aborting process: {name}");
        std::process::abort()
    }

    /// Evaluate a fail point that can return an error, yielding the detail
    /// string when the installed action is [`FailAction::Fail`]. Used by the
    /// `fail_point_err!` macro; not intended to be called directly.
    pub fn eval_fail(name: &str) -> Option<String> {
        match lookup(name) {
            Some(FailAction::Fail(detail)) => Some(detail),
            Some(FailAction::Panic) => panic!("fail_point fired: {name}"),
            Some(FailAction::Abort) => abort(name),
            Some(FailAction::Sleep(d)) => {
                std::thread::sleep(d);
                None
            }
            None => None,
        }
    }

    /// RAII guard that clears a fail point on drop. Use to keep tests
    /// from leaking installed actions across cases.
    pub struct FailGuard {
        name: String,
    }

    impl FailGuard {
        pub fn install(name: &str, action: FailAction) -> Self {
            set(name, action);
            Self {
                name: name.to_string(),
            }
        }

        /// Arm a fail point to return an error carrying `detail`.
        pub fn fail(name: &str, detail: &str) -> Self {
            Self::install(name, FailAction::Fail(detail.to_string()))
        }
    }

    impl Drop for FailGuard {
        fn drop(&mut self) {
            clear(&self.name);
        }
    }
}

#[cfg(feature = "failpoints")]
pub use imp::{FAILPOINTS_ENV, FailAction, FailGuard, clear, eval, eval_fail, lookup, set};

/// Inject a fail point. Expands to nothing without the `failpoints` feature.
///
/// Usage in production code:
///   `nodedb_types::fail_point!("calvin_static::during_overlay_stage");`
///
/// Tests opt in by enabling the feature and installing actions:
///   `fail_point::set("calvin_static::during_overlay_stage",
///                    fail_point::FailAction::Panic);`
#[macro_export]
macro_rules! fail_point {
    ($name:expr) => {
        #[cfg(feature = "failpoints")]
        $crate::fail_point::eval($name);
    };
}

/// Inject a fail point that can abort the enclosing function with an error.
///
/// `$map` receives the detail string installed with [`FailAction::Fail`] and
/// returns the error value to propagate, so each crate injects its own error
/// type without the framework knowing about it:
///
/// ```ignore
/// fail_point_err!("wal::flush_out_of_space", |_| WalError::OutOfSpace {
///     context: "WAL segment append (failpoint)",
/// });
/// ```
///
/// Expands to nothing without the `failpoints` feature.
#[macro_export]
macro_rules! fail_point_err {
    ($name:expr, $map:expr) => {
        #[cfg(feature = "failpoints")]
        if let Some(detail) = $crate::fail_point::eval_fail($name) {
            return Err(($map)(detail));
        }
    };
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use super::*;

    #[test]
    fn unset_fail_point_is_noop() {
        eval("nodedb::test::unset");
        assert_eq!(eval_fail("nodedb::test::unset"), None);
    }

    #[test]
    #[should_panic(expected = "fail_point fired: nodedb::test::panic_target")]
    fn set_panic_fires() {
        let _g = FailGuard::install("nodedb::test::panic_target", FailAction::Panic);
        eval("nodedb::test::panic_target");
    }

    #[test]
    fn fail_action_yields_its_detail() {
        let _g = FailGuard::fail("nodedb::test::fail_target", "disk full");
        assert_eq!(
            eval_fail("nodedb::test::fail_target"),
            Some("disk full".to_string())
        );
    }

    #[test]
    #[should_panic(expected = "cannot return an error")]
    fn fail_action_at_an_infallible_call_site_is_loud() {
        let _g = FailGuard::fail("nodedb::test::fail_at_infallible", "nope");
        eval("nodedb::test::fail_at_infallible");
    }

    #[test]
    fn env_spec_parses_every_action() {
        let actions = super::imp::parse_env(Some(
            "a::panic=panic, b::sleep=sleep(25), c::fail=fail(disk full)",
        ));
        assert!(matches!(actions.get("a::panic"), Some(FailAction::Panic)));
        assert!(matches!(
            actions.get("b::sleep"),
            Some(FailAction::Sleep(d)) if *d == std::time::Duration::from_millis(25)
        ));
        assert!(matches!(
            actions.get("c::fail"),
            Some(FailAction::Fail(detail)) if detail == "disk full"
        ));
    }

    #[test]
    fn empty_env_spec_arms_nothing() {
        assert!(super::imp::parse_env(None).is_empty());
        assert!(super::imp::parse_env(Some("")).is_empty());
    }

    #[test]
    #[should_panic(expected = "unknown action")]
    fn malformed_env_spec_is_loud() {
        // A typo that silently armed nothing would make a crash test pass
        // without ever injecting the crash.
        super::imp::parse_env(Some("a::b=explode"));
    }

    #[test]
    fn fail_guard_clears_on_drop() {
        {
            let _g = FailGuard::install("nodedb::test::guard_clear", FailAction::Panic);
            assert!(lookup("nodedb::test::guard_clear").is_some());
        }
        assert!(lookup("nodedb::test::guard_clear").is_none());
    }
}
