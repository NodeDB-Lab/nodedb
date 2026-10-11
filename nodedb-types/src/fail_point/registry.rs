// SPDX-License-Identifier: Apache-2.0

//! The process-wide registry of armed fail points, and their evaluation.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, MutexGuard};

use super::action::FailAction;
use super::env::{FAILPOINTS_ENV, parse_env};
use super::scope::FailScope;

/// Armed actions by fail point name, then by scope.
pub(crate) type ArmedActions = HashMap<String, HashMap<FailScope, FailAction>>;

static REGISTRY: LazyLock<Mutex<ArmedActions>> =
    LazyLock::new(|| Mutex::new(parse_env(std::env::var(FAILPOINTS_ENV).ok().as_deref())));

fn registry() -> MutexGuard<'static, ArmedActions> {
    REGISTRY.lock().unwrap_or_else(|p| p.into_inner())
}

/// Arm `name` for `scope`. A later point evaluated in a matching scope fires
/// `action`.
pub fn set(scope: FailScope, name: &str, action: FailAction) {
    registry()
        .entry(name.to_string())
        .or_default()
        .insert(scope, action);
}

/// Disarm `name` for `scope`. Actions armed for other scopes stay.
pub fn clear(scope: FailScope, name: &str) {
    let mut armed = registry();
    if let Some(scopes) = armed.get_mut(name) {
        scopes.remove(&scope);
        if scopes.is_empty() {
            armed.remove(name);
        }
    }
}

/// The action a point `name` evaluated in `scope` fires, if any.
///
/// `Node(n)` takes the action armed for `Node(n)`, else the one armed for
/// `Any`. `Any` takes only the `Any` action.
pub fn lookup(scope: FailScope, name: &str) -> Option<FailAction> {
    let armed = registry();
    let scopes = armed.get(name)?;
    scopes
        .get(&scope)
        .or_else(|| scopes.get(&FailScope::Any))
        .cloned()
}

/// Evaluate point `name` in `scope`. Used by the `fail_point!` macro; not
/// intended to be called directly.
pub fn eval(scope: FailScope, name: &str) {
    if let Some(action) = lookup(scope, name) {
        match action {
            FailAction::Panic => panic!("fail_point fired: {name} ({scope})"),
            FailAction::Abort => abort(scope, name),
            FailAction::Sleep(d) => std::thread::sleep(d),
            // A caller that cannot return an error must not silently
            // swallow the injection: an armed failpoint that does nothing
            // makes a test pass for the wrong reason.
            FailAction::Fail(detail) => panic!(
                "fail_point {name} ({scope}) installed Fail({detail}) but the call site cannot \
                 return an error — use fail_point_err!"
            ),
            // Blocking the thread would stall every task that shares it.
            FailAction::WaitForFile(path) => panic!(
                "fail_point {name} ({scope}) installed WaitForFile({}) but the call site is \
                 synchronous — only an async call site can park",
                path.display()
            ),
        }
    }
}

/// Evaluate point `name` in `scope` at a call site that can return an error.
/// Yields the detail when the action is [`FailAction::Fail`]. Used by the
/// `fail_point_err!` macro; not intended to be called directly.
pub fn eval_fail(scope: FailScope, name: &str) -> Option<String> {
    match lookup(scope, name) {
        Some(FailAction::Fail(detail)) => Some(detail),
        Some(FailAction::Panic) => panic!("fail_point fired: {name} ({scope})"),
        Some(FailAction::Abort) => abort(scope, name),
        Some(FailAction::Sleep(d)) => {
            std::thread::sleep(d);
            None
        }
        Some(FailAction::WaitForFile(path)) => panic!(
            "fail_point {name} ({scope}) installed WaitForFile({}) but the call site is \
             synchronous — only an async call site can park",
            path.display()
        ),
        None => None,
    }
}

/// Kill the process at a fail point. Flushes the reason first — an
/// unexplained SIGABRT in a test log is indistinguishable from a real bug.
fn abort(scope: FailScope, name: &str) -> ! {
    eprintln!("fail_point aborting process: {name} ({scope})");
    std::process::abort()
}

#[cfg(test)]
mod tests {
    use super::super::guard::FailGuard;
    use super::*;

    #[test]
    fn unset_fail_point_is_noop() {
        eval(FailScope::Any, "nodedb::test::unset");
        assert_eq!(eval_fail(FailScope::Node(1), "nodedb::test::unset"), None);
    }

    #[test]
    #[should_panic(expected = "fail_point fired: nodedb::test::panic_target")]
    fn set_panic_fires() {
        let _g = FailGuard::install("nodedb::test::panic_target", FailAction::Panic);
        eval(FailScope::Any, "nodedb::test::panic_target");
    }

    #[test]
    fn fail_action_yields_its_detail() {
        let _g = FailGuard::fail("nodedb::test::fail_target", "disk full");
        assert_eq!(
            eval_fail(FailScope::Any, "nodedb::test::fail_target"),
            Some("disk full".to_string())
        );
    }

    #[test]
    #[should_panic(expected = "cannot return an error")]
    fn fail_action_at_an_infallible_call_site_is_loud() {
        let _g = FailGuard::fail("nodedb::test::fail_at_infallible", "nope");
        eval(FailScope::Any, "nodedb::test::fail_at_infallible");
    }

    #[test]
    #[should_panic(expected = "only an async call site can park")]
    fn a_file_gate_at_a_synchronous_call_site_is_loud() {
        let _g = FailGuard::install(
            "nodedb::test::gate_at_sync",
            FailAction::WaitForFile(std::path::PathBuf::from("/nonexistent")),
        );
        eval(FailScope::Any, "nodedb::test::gate_at_sync");
    }

    #[test]
    fn an_any_action_fires_on_every_node() {
        let _g = FailGuard::fail("nodedb::test::any_scope", "every node");
        for scope in [FailScope::Any, FailScope::Node(1), FailScope::Node(2)] {
            assert_eq!(
                eval_fail(scope, "nodedb::test::any_scope"),
                Some("every node".to_string()),
                "{scope}"
            );
        }
    }

    #[test]
    fn a_node_action_fires_only_on_its_node() {
        let _g = FailGuard::for_node(
            2,
            "nodedb::test::node_scope",
            FailAction::Fail("node two".to_string()),
        );
        assert_eq!(
            eval_fail(FailScope::Node(2), "nodedb::test::node_scope"),
            Some("node two".to_string())
        );
        assert_eq!(
            eval_fail(FailScope::Node(1), "nodedb::test::node_scope"),
            None
        );
        assert_eq!(eval_fail(FailScope::Any, "nodedb::test::node_scope"), None);
    }

    #[test]
    fn a_node_action_wins_over_an_any_action() {
        let _any = FailGuard::fail("nodedb::test::both_scopes", "any");
        let _node = FailGuard::for_node(
            3,
            "nodedb::test::both_scopes",
            FailAction::Fail("node three".to_string()),
        );
        assert_eq!(
            eval_fail(FailScope::Node(3), "nodedb::test::both_scopes"),
            Some("node three".to_string())
        );
        assert_eq!(
            eval_fail(FailScope::Node(4), "nodedb::test::both_scopes"),
            Some("any".to_string())
        );
    }

    #[test]
    fn clearing_one_scope_keeps_the_other() {
        let _any = FailGuard::fail("nodedb::test::clear_one", "any");
        {
            let _node = FailGuard::for_node(
                5,
                "nodedb::test::clear_one",
                FailAction::Fail("node five".to_string()),
            );
        }
        assert_eq!(
            eval_fail(FailScope::Node(5), "nodedb::test::clear_one"),
            Some("any".to_string())
        );
    }

    #[test]
    fn fail_guard_clears_on_drop() {
        {
            let _g = FailGuard::install("nodedb::test::guard_clear", FailAction::Panic);
            assert!(lookup(FailScope::Any, "nodedb::test::guard_clear").is_some());
        }
        assert!(lookup(FailScope::Any, "nodedb::test::guard_clear").is_none());
    }
}
