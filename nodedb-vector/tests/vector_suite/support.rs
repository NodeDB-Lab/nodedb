// SPDX-License-Identifier: Apache-2.0

//! Shared test fixtures for the `vector_suite` integration test binary.
//!
//! Not reachable from `src/`'s inline `#[cfg(test)]` modules — those compile
//! into the library crate, while everything under `tests/` compiles as a
//! separate crate against the public API.

use std::sync::Arc;

use nodedb_mem::{EngineId, EngineLimits, GovernorConfig, MemoryGovernor, ScopedMemory};
use nodedb_types::{DatabaseId, TenantId};
use nodedb_vector::{BuildRequest, BuiltSegment, build_segment};

/// A scope backed by a governor whose ceiling covers every engine's limit.
pub fn test_memory() -> ScopedMemory {
    let per_engine = usize::MAX / EngineId::ALL.len();
    let governor = Arc::new(
        MemoryGovernor::new(GovernorConfig {
            global_ceiling: per_engine * EngineId::ALL.len(),
            engine_limits: EngineLimits::uniform(per_engine),
        })
        .expect("test governor"),
    );
    ScopedMemory::new(
        governor,
        DatabaseId::DEFAULT,
        TenantId::new(0),
        EngineId::Vector,
    )
}

/// Build `req` on the calling thread with a fixed seed. A codec error fails
/// the test: a fixture build carries the codec its plan names.
pub fn build_now(req: BuildRequest) -> BuiltSegment {
    let built = build_segment(7, req).expect("test segment build");
    if let Err(e) = &built.codec {
        panic!("test codec build: {e}");
    }
    built
}
