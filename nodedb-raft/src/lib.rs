// SPDX-License-Identifier: BUSL-1.1

//! Raft consensus primitives: leader election, log replication, snapshots,
//! membership change (joint consensus), and snapshot framing.
//!
//! This crate provides the algorithm only — transport, persistence, and
//! state-machine application are the consumer's responsibility. It is
//! consumed by `nodedb-cluster` (Multi-Raft per vShard for replicated
//! collections) and by `nodedb` (single-group Raft for the metadata
//! catalog and the cross-engine surrogate counter).

pub mod error;
#[cfg(feature = "failpoints")]
mod fail_gate;
pub mod log;
pub mod message;
pub mod node;
pub mod snapshot_framing;
pub mod state;
pub mod storage;
pub mod transport;

#[cfg(test)]
pub(crate) mod test_support;

pub use error::{RaftError, Result};
pub use log::RaftLog;
pub use message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    LogEntry, PreVoteRequest, PreVoteResponse, RequestVoteRequest, RequestVoteResponse,
    TimeoutNowRequest,
};
pub use node::{RaftNode, ReadIndexProbe, ReadIndexStatus, Ready, StalenessVerdict};
pub use snapshot_framing::{
    SNAPSHOT_FORMAT_VERSION, SNAPSHOT_MAGIC, SnapshotEngineId, SnapshotFramingError,
    decode_snapshot_chunk, encode_snapshot_chunk,
};
pub use state::{HardState, NodeRole, PeerRole};
pub use storage::LogStorage;
pub use transport::RaftTransport;
