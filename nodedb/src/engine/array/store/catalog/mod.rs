// SPDX-License-Identifier: BUSL-1.1

//! Per-array LSM store — manifest, memtable, open segment handles.
//!
//! Each [`ArrayStore`] manages one array's directory. The engine in
//! `engine.rs` keeps a `HashMap<ArrayId, ArrayStore>`. Stores are
//! Data-Plane only (`!Send`-compatible — no atomics, no shared mutability).

mod error;
mod newest;
mod resolve;
mod scan;
mod segments;
mod store;
mod versions;

pub use error::{ArrayStoreError, CellVersion};
pub use store::ArrayStore;
