// SPDX-License-Identifier: Apache-2.0

//! Central memory governor.
//!
//! The governor owns all budget levels and enforces a four-layer hierarchy:
//! global ceiling → per-database → per-tenant → per-engine.
//! Every subsystem that wants to allocate significant memory must go through
//! the governor.

mod config;
mod core;
mod global_counter;
mod metrics;
mod quotas;
mod reserve;
#[cfg(test)]
mod test_support;

pub use config::GovernorConfig;
pub use core::MemoryGovernor;
pub use global_counter::GlobalCounter;
pub use metrics::{DatabaseUsage, EngineSnapshot};
