//! Columnar mutation engine: coordinates PK index, delete bitmaps,
//! memtable, and WAL records for full INSERT/UPDATE/DELETE.
//!
//! The MutationEngine is the single point of coordination for all
//! columnar write operations. It produces WAL records that must be
//! persisted before the mutation is considered durable.

pub mod engine;
pub mod flush;
pub mod write;

#[cfg(test)]
mod tests;

pub use engine::{MutationEngine, MutationResult};
