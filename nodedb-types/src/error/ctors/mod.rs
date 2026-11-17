// SPDX-License-Identifier: Apache-2.0

//! `NodeDbError` constructors, split by category.
//!
//! Each submodule provides `impl NodeDbError` blocks; Rust merges all of
//! them into the type. Categories:
//! - [`write_path`] — 1000-range write-time errors + accounting/type enforcement.
//! - [`read_query_auth`] — 1100 read path, 1200 query, 2000 auth.
//! - [`data_exception`] — 1200-range data exceptions (arithmetic, text, date/time, type).
//! - [`sync_infra`] — 3000 sync, 4000 storage, 4200 serialization, 5000 config,
//!   6000 cluster, 7000 memory, 8000 encryption, 9000 internal/bridge/dispatch.
//! - [`from_wire`] — rebuilds a typed error from a numeric code decoded off
//!   the wire, for the receiving side that no longer holds the context.
//! - [`backup`] — 1800-range backup/restore envelope errors.
//! - [`remote_code`] — `RemoteTyped` reconstruction for cluster RPC replies.

pub mod backup;
pub mod data_exception;
pub mod from_wire;
pub mod mirror;
pub mod move_tenant;
pub mod read_query_auth;
pub mod remote_code;
pub mod sync_infra;
pub mod write_path;
