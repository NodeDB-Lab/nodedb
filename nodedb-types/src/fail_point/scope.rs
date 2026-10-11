// SPDX-License-Identifier: Apache-2.0

//! The node scope of a fail point.

use std::fmt;

/// The nodes an armed fail point applies to, and the node a point is
/// evaluated on.
///
/// Compiled without the `failpoints` feature too, so a call site can name
/// its scope in every build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailScope {
    /// Every node. Code with no node identity evaluates its points here.
    Any,
    /// The node with this id.
    Node(u64),
}

impl fmt::Display for FailScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => f.write_str("any node"),
            Self::Node(id) => write!(f, "node {id}"),
        }
    }
}
