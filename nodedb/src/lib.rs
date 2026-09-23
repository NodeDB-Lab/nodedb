// SPDX-License-Identifier: BUSL-1.1

//! NodeDB server core: pgwire / HTTP / native transports, the SQL planner
//! integration, the SPSC bridge to the Data Plane, all storage engines,
//! and the Event Plane (triggers / CDC / scheduler).
//!
//! This crate is the heart of the Origin (cloud and single-node) deployment
//! mode. The binary entry point is `src/main.rs`; the library entry point
//! exposes the modules below for embedding scenarios that want to drive the
//! server from another process. Most external users should depend on
//! `nodedb-client` instead.

pub mod bootstrap;
pub mod bridge;
pub mod config;
pub mod control;
pub mod ctl;
pub mod data;
pub mod diag;
pub mod engine;
pub mod error;
mod error_classify;
mod error_from;
mod error_from_data_plane;
pub mod event;
// The fail-point framework lives in `nodedb-types` so crates below this one
// (`nodedb-wal` in particular) inject into the same process-wide registry.
// `nodedb_types::fail_point` resolves in both the type namespace (the module,
// giving `crate::fail_point::FailAction` / `::set` / `::FailGuard`, ...) and
// the macro namespace (`crate::fail_point!` / `crate::fail_point_err!`), so
// this single re-export covers both.
pub use nodedb_types::{fail_point, fail_point_err};
pub mod memory;
pub mod query;
pub mod storage;
pub mod types;
pub mod util;
pub mod version;
pub mod wal;

pub use config::{EngineConfig, ServerConfig};
pub use error::{DispatchCapacityScope, Error, OllpExhaustedCause, Result};
pub use nodedb_types::error::{ErrorCode, NodeDbError, NodeDbResult};
pub use types::{DocumentId, Lsn, ReadConsistency, RequestId, TenantId, VShardId};
