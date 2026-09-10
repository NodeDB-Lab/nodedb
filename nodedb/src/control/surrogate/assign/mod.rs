// SPDX-License-Identifier: BUSL-1.1

//! CP-side surrogate assigner: resolves `(collection, pk_bytes)` to a
//! stable `Surrogate` and owns the cross-node HiLo reservation path.

pub(super) mod cluster_reserve;
pub mod core;

pub(crate) use core::fresh_identity_string;
pub use core::{SurrogateAssigner, SurrogateRegistryHandle};
