// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT — module root.
//!
//! Submodule wiring only. All restore orchestrator logic lives in
//! [`orchestrate`]; each engine's re-issue lives in its own submodule; the
//! section decoding lives in `sections`; the destination databases are
//! resolved in `databases`; `target` maps a source collection name to its
//! destination database.

mod array_reissue;
pub(crate) mod bind_conflicts;
pub mod columnar_reissue;
pub(crate) mod crdt_reissue;
mod cut;
mod databases;
mod durable;
pub(crate) mod guard;
mod orchestrate;
mod quorum;
mod redo_reissue;
pub(in crate::control::backup) mod sections;
mod surrogate_floor;
mod target;
pub mod timeseries_partition;
pub mod timeseries_reissue;
mod validate;
pub mod vector_reissue;

pub(crate) use orchestrate::reissue_mirror_capture;
pub use orchestrate::{CollectionRows, RestoreStats, reissue_into_database, restore_tenant};
