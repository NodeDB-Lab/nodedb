// SPDX-License-Identifier: BUSL-1.1

//! `GatewayVersionSet` — deterministic ordered set of (collection, version)
//! pairs used as a plan cache key and as the payload for
//! `DescriptorVersionEntry` in `ExecuteRequest`. Collected from a
//! `PhysicalPlan` by walking every variant and extracting the collection
//! name.

mod array_keys;
mod keys;
mod plan_keys;
mod provider_keys;
mod set;

pub use array_keys::{array_names, array_version, with_arrays};
pub use keys::{
    array_of_key, array_version_key, continuous_aggregate_of_key, continuous_aggregate_version_key,
    permission_tree_version_key, rls_version_key,
};
pub use plan_keys::touched_collections;
pub use provider_keys::{continuous_aggregate_version, provider_names, with_continuous_aggregates};
pub use set::{GatewayVersionSet, catalog_name, collection_version};
