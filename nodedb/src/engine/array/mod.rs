//! NodeDB Array Engine — LSM-style storage on top of `nodedb-array` segments.
//!
//! Lives in the Data Plane: `!Send`, no tokio. Persistence routes through
//! the [`wal::ArrayWalAppender`] trait, which Origin wires to the real
//! group-committed WAL writer. Recovery replays WAL records past the
//! last `ArrayFlush` watermark; flushed segments are durable on disk and
//! mmap'd by the segment store on open.

pub mod compact;
pub mod compaction;
pub mod engine;
pub mod flush;
pub mod memtable;
pub mod read;
pub mod recovery;
pub mod store;
#[cfg(test)]
mod test_support;
pub mod wal;
pub mod write;

pub use engine::{ArrayEngine, ArrayEngineConfig};
pub use wal::{ArrayDeletePayload, ArrayFlushPayload, ArrayPutPayload};
