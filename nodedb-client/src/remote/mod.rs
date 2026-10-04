// SPDX-License-Identifier: Apache-2.0

//! `NodeDbRemote` — pgwire client that translates `NodeDb` trait calls
//! into SQL/DSL and sends them to the NodeDB Origin.
//!
//! Split into per-concern files: connection lifecycle and trait impl in
//! `client`, SQL/param translation seams in `sql`. Search-hit rows decode
//! through the crate-level `row_decode` module. Each file holds the unit
//! tests for the code it owns.

pub mod client;
mod sql;

pub use client::NodeDbRemote;
