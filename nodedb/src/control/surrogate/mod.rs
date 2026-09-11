// SPDX-License-Identifier: BUSL-1.1

//! Surrogate registry — global, monotonic, WAL-durable allocator for
//! every row's stable cross-engine identity.
//!
//! See `nodedb-types/src/surrogate.rs` for the value type. This module
//! owns allocation, checkpoint persistence, boot-time recovery, and
//! the CP-side `assign_surrogate` plumbing wired through `SharedState`.

pub mod assign;
pub mod bootstrap;
pub mod persist;
pub mod physical_impl;
pub mod registry;
pub mod wal_appender;

pub use assign::{SurrogateAssigner, SurrogateRegistryHandle};
pub use bootstrap::bootstrap_registry;
pub use persist::{SURROGATE_HWM, SurrogateHwmPersist, SystemCatalogHwm};
pub use registry::{
    FLUSH_ELAPSED_THRESHOLD, FLUSH_OPS_THRESHOLD, RESERVE_BATCH_SIZE, SurrogateAllocError,
    SurrogatePromotionError, SurrogateRegistry, SurrogateRegistryMode,
};
pub use wal_appender::{NoopWalAppender, SurrogateWalAppender, WalSurrogateAppender};
