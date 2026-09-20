// SPDX-License-Identifier: Apache-2.0

//! Columnar mutation engine: coordinates PK index, delete bitmaps,
//! memtable, and WAL records for full INSERT/UPDATE/DELETE.
//!
//! The MutationEngine is the single point of coordination for all
//! columnar write operations. It produces WAL records that must be
//! persisted before the mutation is considered durable.

pub mod engine;
pub mod flush;
pub mod snapshot;
pub mod truncate;
pub mod write;

pub use engine::{MutationEngine, MutationResult};
pub use snapshot::{ColumnDataSnapshot, ColumnarEngineSnapshot};
pub use truncate::TruncatedRows;
