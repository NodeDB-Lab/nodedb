// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT — module root.
//!
//! Submodule wiring only. All restore orchestrator logic lives in
//! [`orchestrate`]; each engine's re-issue lives in its own submodule; the
//! section decoding lives in `sections`.

pub mod columnar_reissue;
pub(crate) mod crdt_reissue;
mod durable;
pub(crate) mod guard;
mod kv_reissue;
mod orchestrate;
mod quorum;
mod redo_reissue;
mod sections;
pub mod timeseries_reissue;
pub mod vector_reissue;

pub use orchestrate::{RestoreStats, restore_tenant};
