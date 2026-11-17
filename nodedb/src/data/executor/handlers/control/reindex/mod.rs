// SPDX-License-Identifier: BUSL-1.1

//! REINDEX of a collection's HNSW, full-text and CSR indexes.
//!
//! - `dispatch`: the `MetaOp::RebuildIndex` handler: starts the rebuilds.
//! - `fts`, `csr`: start a rebuild on its own OS thread and cut it over on
//!   the owning core with every write made during the build replayed. The
//!   full-text index DDL starts its rebuilds through `fts` too.
//! - `pending`: in-flight rebuilds, polled every tick.
//! - `hold`: the failpoint that holds a rebuild thread in tests.
//! - `waiter`: holds a plain REINDEX's answer until its cutovers.

pub mod csr;
pub mod dispatch;
pub mod fts;
pub mod hold;
pub mod pending;
pub mod waiter;

pub use pending::{PendingReindex, RebuildTarget};
pub use waiter::ReindexWaiter;
