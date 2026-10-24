// SPDX-License-Identifier: Apache-2.0

//! DDL statement AST — re-export surface.

pub mod auth;
pub mod collection;
pub mod maintenance;
pub mod types;
pub mod wrapper;

pub use auth::*;
pub use collection::*;
pub use maintenance::*;
pub use types::*;
pub use wrapper::*;

// Cross-module re-exports preserved at the `statement::` path for
// callers that imported these types via the pre-split surface.
pub use super::alter_ops::{AlterCollectionOp, AlterRoleOp, AlterUserOp};
pub use super::graph_types::{GraphDirection, GraphEdgeTuple, GraphProperties};
