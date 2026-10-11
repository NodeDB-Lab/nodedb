// SPDX-License-Identifier: Apache-2.0

//! The environment spec that arms fail points in a spawned process.

use std::time::Duration;

use super::action::FailAction;
use super::registry::ArmedActions;
use super::scope::FailScope;

/// Environment variable read once, the first time any fail point is
/// evaluated. Lets a test arm injections in a *spawned* process — the
/// in-process `set` API cannot reach a server the test only supervises.
///
/// Format: comma-separated `name=action` or `name@node<N>=action`. The
/// first arms every node, the second only node `N`. The action is `panic`,
/// `abort`, `sleep(<millis>)`, `fail(<detail>)`, or `wait_file(<path>)`.
/// For example:
/// `NODEDB_FAILPOINTS='checkpoint::after_marker_before_truncate=panic'`
pub const FAILPOINTS_ENV: &str = "NODEDB_FAILPOINTS";

/// Parse a [`FAILPOINTS_ENV`] spec. Panics on a malformed entry: a typo that
/// armed nothing would let a crash test pass without injecting the crash.
pub(crate) fn parse_env(spec: Option<&str>) -> ArmedActions {
    let mut actions = ArmedActions::new();
    let Some(spec) = spec else {
        return actions;
    };
    for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let Some((target, action)) = entry.split_once('=') else {
            panic!(
                "{FAILPOINTS_ENV} entry {entry:?} is not `name=action` or `name@node<N>=action`"
            );
        };
        let (name, scope) = parse_target(entry, target.trim());
        let action = parse_action(entry, action.trim());
        actions
            .entry(name.to_string())
            .or_default()
            .insert(scope, action);
    }
    actions
}

/// Split `name@node<N>` into the name and `Node(N)`. A target with no `@`
/// is scoped to every node.
fn parse_target<'a>(entry: &str, target: &'a str) -> (&'a str, FailScope) {
    let (name, scope) = match target.rsplit_once('@') {
        None => (target, FailScope::Any),
        Some((name, scope)) => {
            let node = scope
                .trim()
                .strip_prefix("node")
                .and_then(|id| id.parse().ok())
                .unwrap_or_else(|| {
                    panic!("{FAILPOINTS_ENV} entry {entry:?} has scope {scope:?}, not `node<N>`")
                });
            (name.trim(), FailScope::Node(node))
        }
    };
    if name.is_empty() {
        panic!("{FAILPOINTS_ENV} entry {entry:?} names no fail point");
    }
    (name, scope)
}

fn parse_action(entry: &str, action: &str) -> FailAction {
    match action {
        "panic" => FailAction::Panic,
        "abort" => FailAction::Abort,
        rest if rest.starts_with("sleep(") && rest.ends_with(')') => {
            let millis = rest["sleep(".len()..rest.len() - 1]
                .parse()
                .unwrap_or_else(|_| {
                    panic!("{FAILPOINTS_ENV} entry {entry:?} has a non-numeric sleep")
                });
            FailAction::Sleep(Duration::from_millis(millis))
        }
        rest if rest.starts_with("fail(") && rest.ends_with(')') => {
            FailAction::Fail(rest["fail(".len()..rest.len() - 1].to_string())
        }
        rest if rest.starts_with("wait_file(") && rest.ends_with(')') => FailAction::WaitForFile(
            std::path::PathBuf::from(&rest["wait_file(".len()..rest.len() - 1]),
        ),
        other => panic!("{FAILPOINTS_ENV} entry {entry:?} has unknown action {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn armed<'a>(
        actions: &'a ArmedActions,
        scope: FailScope,
        name: &str,
    ) -> Option<&'a FailAction> {
        actions.get(name).and_then(|scopes| scopes.get(&scope))
    }

    #[test]
    fn env_spec_parses_every_action() {
        let actions = parse_env(Some(
            "a::panic=panic, b::sleep=sleep(25), c::fail=fail(disk full)",
        ));
        assert!(matches!(
            armed(&actions, FailScope::Any, "a::panic"),
            Some(FailAction::Panic)
        ));
        assert!(matches!(
            armed(&actions, FailScope::Any, "b::sleep"),
            Some(FailAction::Sleep(d)) if *d == Duration::from_millis(25)
        ));
        assert!(matches!(
            armed(&actions, FailScope::Any, "c::fail"),
            Some(FailAction::Fail(detail)) if detail == "disk full"
        ));
    }

    #[test]
    fn env_spec_parses_a_file_gate() {
        let actions = parse_env(Some("d::gate=wait_file(/tmp/release-d)"));
        assert!(matches!(
            armed(&actions, FailScope::Any, "d::gate"),
            Some(FailAction::WaitForFile(path)) if path == std::path::Path::new("/tmp/release-d")
        ));
    }

    #[test]
    fn env_spec_scopes_an_entry_to_one_node() {
        let actions = parse_env(Some("e::hold@node3=abort, e::hold=panic"));
        assert!(matches!(
            armed(&actions, FailScope::Node(3), "e::hold"),
            Some(FailAction::Abort)
        ));
        assert!(matches!(
            armed(&actions, FailScope::Any, "e::hold"),
            Some(FailAction::Panic)
        ));
        assert!(armed(&actions, FailScope::Node(2), "e::hold").is_none());
    }

    #[test]
    fn a_node_scope_leaves_the_action_path_alone() {
        let actions = parse_env(Some("f::gate@node1=wait_file(/tmp/a@b=c)"));
        assert!(matches!(
            armed(&actions, FailScope::Node(1), "f::gate"),
            Some(FailAction::WaitForFile(path)) if path == std::path::Path::new("/tmp/a@b=c")
        ));
    }

    #[test]
    fn empty_env_spec_arms_nothing() {
        assert!(parse_env(None).is_empty());
        assert!(parse_env(Some("")).is_empty());
    }

    #[test]
    #[should_panic(expected = "unknown action")]
    fn malformed_env_spec_is_loud() {
        parse_env(Some("a::b=explode"));
    }

    #[test]
    #[should_panic(expected = "not `node<N>`")]
    fn a_scope_that_is_not_a_node_is_loud() {
        parse_env(Some("a::b@core1=panic"));
    }

    #[test]
    #[should_panic(expected = "not `node<N>`")]
    fn a_node_scope_without_an_id_is_loud() {
        parse_env(Some("a::b@node=panic"));
    }

    #[test]
    #[should_panic(expected = "names no fail point")]
    fn a_scope_with_no_name_is_loud() {
        parse_env(Some("@node1=panic"));
    }
}
