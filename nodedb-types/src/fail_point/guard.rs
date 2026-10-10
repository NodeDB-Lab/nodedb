// SPDX-License-Identifier: Apache-2.0

//! RAII arming of a fail point for one test.

use super::action::FailAction;
use super::registry::{clear, set};
use super::scope::FailScope;

/// Arms a fail point and disarms it on drop. Use to keep tests from
/// leaking armed actions across cases.
pub struct FailGuard {
    scope: FailScope,
    name: String,
}

impl FailGuard {
    /// Arm `name` on every node.
    pub fn install(name: &str, action: FailAction) -> Self {
        Self::arm(FailScope::Any, name, action)
    }

    /// Arm `name` on node `node_id` only. Every other node runs past it.
    pub fn for_node(node_id: u64, name: &str, action: FailAction) -> Self {
        Self::arm(FailScope::Node(node_id), name, action)
    }

    /// Arm `name` on every node to return an error carrying `detail`.
    pub fn fail(name: &str, detail: &str) -> Self {
        Self::install(name, FailAction::Fail(detail.to_string()))
    }

    fn arm(scope: FailScope, name: &str, action: FailAction) -> Self {
        set(scope, name, action);
        Self {
            scope,
            name: name.to_string(),
        }
    }
}

impl Drop for FailGuard {
    fn drop(&mut self) {
        clear(self.scope, &self.name);
    }
}
