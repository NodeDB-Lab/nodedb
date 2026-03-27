//! KV engine — hash-indexed O(1) point lookups with typed value fields.
//!
//! This module will contain the per-core hash table, incremental rehash,
//! expiry wheel, slab allocator, and secondary index maintenance.
//! Currently a placeholder for the engine module registration;
//! implementation follows in subsequent batches.
