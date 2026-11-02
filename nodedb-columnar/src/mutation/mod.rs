// SPDX-License-Identifier: Apache-2.0

//! Columnar mutation engine: coordinates PK index, delete bitmaps, row
//! version stamps and chains, memtable, and WAL records for full
//! INSERT/UPDATE/DELETE.
//!
//! The MutationEngine is the single point of coordination for all
//! columnar write operations. It produces WAL records that must be
//! persisted before the mutation is considered durable.

pub mod batch;
pub mod engine;
pub mod flush;
pub mod history;
pub mod install;
pub mod purge;
pub mod snapshot;
pub mod truncate;
pub mod versioning;
pub mod visibility;
pub mod write;

pub use batch::{BatchConflict, BatchRow};
pub use engine::{MutationEngine, MutationResult};
pub use history::StoredVersion;
pub use install::InstallRow;
pub use snapshot::{ColumnDataSnapshot, ColumnarEngineSnapshot};
pub use truncate::TruncatedRows;
