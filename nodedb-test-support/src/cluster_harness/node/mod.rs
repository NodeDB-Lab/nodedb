// SPDX-License-Identifier: BUSL-1.1

//! Single cluster node: struct, spawn/shutdown lifecycle, and a suite
//! of local-state inspector methods used by integration tests.

pub mod graph;
pub mod inspect;
pub mod lifecycle;
pub mod native_client;
pub mod raw_pgwire;

pub use lifecycle::TestClusterNode;
