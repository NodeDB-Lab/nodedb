// SPDX-License-Identifier: BUSL-1.1

//! The fail-point test API.
//!
//! An in-process cluster shares one fail-point registry across its nodes.
//! [`FailGuard::for_node`] arms a point on one node, and
//! [`FailGuard::install`] arms it on every node.

pub use nodedb_types::fail_point::{FailAction, FailGuard, FailScope};
