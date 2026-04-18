//! Single cluster node: struct, spawn/shutdown lifecycle, and a suite
//! of local-state inspector methods used by integration tests.

pub mod inspect;
pub mod lifecycle;

pub use lifecycle::TestClusterNode;
