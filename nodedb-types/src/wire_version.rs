// SPDX-License-Identifier: Apache-2.0

//! Single source of truth for the `WIRE_FORMAT_VERSION` constant
//! shared between every crate that needs to stamp or interpret it.
//!
//! This is the *cluster-wide* wire format version, distinct from:
//! - `nodedb_cluster::wire::WIRE_VERSION` (the binary frame layout
//!   version of the `VShardEnvelope`),
//! - the RPC frame header version in
//!   `nodedb_cluster::rpc_codec::header` (a private constant of that
//!   module).
//!
//! Bump this when the SPSC bridge, WAL, or RPC payload schemas change
//! in a way that requires a coordinated upgrade. Readers MUST reject
//! messages stamped with a version other than their own.

/// Cluster-wide wire format version. Stamped on every `NodeInfo` and
/// returned by `nodedb::version::WIRE_FORMAT_VERSION` (a re-export).
pub const WIRE_FORMAT_VERSION: u16 = 8;

/// Minimum wire format version this build can read. Equal to
/// `WIRE_FORMAT_VERSION`: floor == ceiling, no backward compat window.
pub const MIN_WIRE_FORMAT_VERSION: u16 = WIRE_FORMAT_VERSION;

// Compile-time invariants — these constants must satisfy:
//   - MIN_WIRE_FORMAT_VERSION <= WIRE_FORMAT_VERSION
//   - WIRE_FORMAT_VERSION > 0
const _: () = assert!(MIN_WIRE_FORMAT_VERSION <= WIRE_FORMAT_VERSION);
const _: () = assert!(WIRE_FORMAT_VERSION > 0);
