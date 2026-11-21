// SPDX-License-Identifier: BUSL-1.1

//! Routed-surrogate-exchange: the leader-side cluster hook that assigns or
//! returns the authoritative surrogate of a batch of `(collection, pk)` keys,
//! and the coordinator-side helpers that route each batch to the home
//! vShard's leader, or assign locally when this node IS the leader.

pub mod authority;
mod batch;
pub mod home_bridge;
pub mod hook;
pub mod resolve;
pub mod unbound;

pub use home_bridge::RoutedHomeAuthority;
pub use hook::RegistryAssignRemoteSurrogate;
pub use resolve::{
    assign_surrogate_routed, assign_surrogates_routed, lookup_surrogate_routed,
    lookup_surrogates_routed,
};
