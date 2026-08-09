// SPDX-License-Identifier: Apache-2.0

//! `NodeDbError` constructors, split by category.
//!
//! Each submodule provides `impl NodeDbError` blocks; Rust merges all of
//! them into the type. Categories:
//! - [`write_path`] — 1000-range write-time errors + accounting/type enforcement.
//! - [`read_query_auth`] — 1100 read path, 1200 query, 2000 auth.
//! - [`sync_infra`] — 3000 sync, 4000 storage, 4200 serialization, 5000 config,
//!   6000 cluster, 7000 memory, 8000 encryption, 9000 internal/bridge/dispatch.
//! - [`from_sqlstate`] — rebuild a typed error from a SQLSTATE sent by a peer.

pub mod from_sqlstate;
pub mod mirror;
pub mod move_tenant;
pub mod read_query_auth;
pub mod sync_infra;
pub mod write_path;
