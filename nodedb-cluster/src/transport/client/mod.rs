// SPDX-License-Identifier: BUSL-1.1

//! Outbound Raft RPC transport.
//!
//! [`NexarTransport`] implements [`RaftTransport`] — serialize the request,
//! open a QUIC bidi stream, send the frame, read the response frame, and
//! deserialize. Connection pooling is automatic (cached per peer, replaced
//! on stale).
//!
//! [`RaftTransport`]: nodedb_raft::transport::RaftTransport

pub mod pool;
pub mod raft_impl;
pub mod send;
pub mod serve;
pub mod sever;
pub mod shuffle_push;
pub mod transport;

pub use shuffle_push::ShufflePushStream;
pub use transport::{NexarTransport, TransportPeerSnapshot};
