// SPDX-License-Identifier: Apache-2.0

//! Index writer wiring.

mod document;
mod maintenance;
mod scope;
mod state;

pub(crate) use scope::{memtable_collection_prefix, memtable_key, memtable_tenant_prefix};
pub use state::FtsIndex;
