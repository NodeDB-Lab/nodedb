// SPDX-License-Identifier: BUSL-1.1

//! `GatewayVersionSet` — deterministic ordered set of (collection, version)
//! pairs used as a plan cache key and as the payload for
//! `DescriptorVersionEntry` in `ExecuteRequest`. Collected from a
//! `PhysicalPlan` by walking every variant and extracting the collection
//! name.

mod keys;
mod plan_keys;
mod set;

pub use keys::{permission_tree_version_key, rls_version_key};
pub use plan_keys::touched_collections;
pub use set::GatewayVersionSet;
