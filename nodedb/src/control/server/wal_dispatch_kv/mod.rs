// SPDX-License-Identifier: BUSL-1.1

//! WAL append for KV engine operations.

pub mod append;
pub mod append_resolved;
pub mod encode;

pub use append::wal_append_kv_op;
