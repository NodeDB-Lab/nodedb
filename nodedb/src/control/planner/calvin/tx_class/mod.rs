// SPDX-License-Identifier: BUSL-1.1

//! `TxClass` construction for Calvin dispatch.
//!
//! Builds the replicated transaction descriptor (`TxClass`) from a physical
//! task slice: the per-engine write set (`EngineKeySet` — document / vector
//! surrogates, KV raw keys, graph-edge identity + routing homes, array tile
//! vShards) plus the
//! msgpack-encoded plans. The builders split by shape (static, predicted,
//! read-dependent) and participant floor (strict multi-vshard vs the
//! single-vshard opt-in):
//!
//! - [`build_static_tx_class`] / [`build_single_vshard_tx_class`] — every
//!   write key is known upfront.
//! - [`build_predicted_tx_class`] / [`build_single_vshard_predicted_tx_class`]
//!   — the OLLP collection's write set comes from reconnaissance-predicted
//!   surrogates; all other tasks use static extraction.
//! - [`build_read_dependent_tx_class`] — a cross-shard write whose values
//!   depend on rows of another vShard. Its passive vShards broadcast those
//!   rows to its active vShards through their data-group logs.

pub mod predicted_builder;
pub mod read_dependent_builder;
pub mod shared;
pub mod static_builder;
pub mod write_keys;

pub use predicted_builder::{build_predicted_tx_class, build_single_vshard_predicted_tx_class};
pub use read_dependent_builder::{PassiveReads, build_read_dependent_tx_class};
pub use static_builder::{build_single_vshard_tx_class, build_static_tx_class};
