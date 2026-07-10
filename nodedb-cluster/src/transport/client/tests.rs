// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for [`NexarTransport`]: end-to-end RPC roundtrips, concurrent
//! fan-out, connection reuse, and unreachable-peer errors.
//!
//! [`NexarTransport`]: super::NexarTransport

use std::sync::Arc;
use std::time::Duration;

use nodedb_raft::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    LogEntry, RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest,
};
use nodedb_raft::transport::RaftTransport;

use crate::error::{ClusterError, Result};
use crate::rpc_codec::RaftRpc;
use crate::transport::credentials::TransportCredentials;
use crate::transport::rpc_handler::RaftRpcHandler;

use super::NexarTransport;

/// Mock handler that returns fixed responses for testing.
struct EchoHandler;

impl RaftRpcHandler for EchoHandler {
    async fn handle_rpc(&self, rpc: RaftRpc) -> Result<RaftRpc> {
        match rpc {
            RaftRpc::AppendEntriesRequest(req) => {
                Ok(RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                    term: req.term,
                    success: true,
                    last_log_index: req.prev_log_index + req.entries.len() as u64,
                }))
            }
            RaftRpc::RequestVoteRequest(req) => {
                Ok(RaftRpc::RequestVoteResponse(RequestVoteResponse {
                    term: req.term,
                    vote_granted: true,
                }))
            }
            RaftRpc::InstallSnapshotRequest(req) => {
                Ok(RaftRpc::InstallSnapshotResponse(InstallSnapshotResponse {
                    term: req.term,
                }))
            }
            other => Err(ClusterError::Transport {
                detail: format!("unexpected request: {other:?}"),
            }),
        }
    }

    async fn handle_rpc_streaming(
        &self,
        _req: crate::rpc_codec::ExecuteRequest,
        mut sink: impl crate::forward::ChunkSink,
    ) -> Option<crate::rpc_codec::TypedClusterError> {
        // Emit three deterministic chunks then clean EOF. Exercises the
        // multi-frame chunk/end envelope path end-to-end over loopback QUIC.
        for i in 0u64..3 {
            if let Err(_e) = sink.send_chunk(vec![i as u8; 4], i + 100).await {
                // Coordinator gone — stop producing, no terminal frame.
                return None;
            }
        }
        None
    }

    async fn on_shuffle_request(&self, _req: crate::rpc_codec::ShufflePushRequest) {}

    async fn on_shuffle_chunk(
        &self,
        _shuffle_id: u64,
        _part: u32,
        _side: u8,
        _payload: Vec<u8>,
    ) -> Result<()> {
        Ok(())
    }

    async fn on_shuffle_end(
        &self,
        _shuffle_id: u64,
        _part: u32,
        _side: u8,
        _error: Option<crate::rpc_codec::TypedClusterError>,
    ) {
    }

    async fn on_shuffle_produce(
        &self,
        _req: crate::rpc_codec::ShuffleProduceRequest,
    ) -> Option<crate::rpc_codec::TypedClusterError> {
        None
    }

    async fn on_shuffle_consume(
        &self,
        _req: crate::rpc_codec::ShuffleConsumeRequest,
    ) -> crate::rpc_codec::ShuffleConsumeResponse {
        crate::rpc_codec::ShuffleConsumeResponse {
            rows: Vec::new(),
            error: None,
        }
    }

    async fn on_shuffle_aggregate(
        &self,
        _req: crate::rpc_codec::ShuffleAggregateConsumeRequest,
    ) -> crate::rpc_codec::ShuffleAggregateConsumeResponse {
        crate::rpc_codec::ShuffleAggregateConsumeResponse {
            rows: Vec::new(),
            error: None,
        }
    }

    async fn on_assign_surrogate(
        &self,
        _req: crate::rpc_codec::AssignSurrogateRequest,
    ) -> crate::rpc_codec::AssignSurrogateResponse {
        crate::rpc_codec::AssignSurrogateResponse {
            surrogate: 0,
            error: None,
        }
    }

    async fn on_submit_calvin_txn(
        &self,
        _req: crate::rpc_codec::SubmitCalvinTxnRequest,
    ) -> crate::rpc_codec::SubmitCalvinTxnResponse {
        crate::rpc_codec::SubmitCalvinTxnResponse {
            error: None,
            payload_bytes: None,
        }
    }

    async fn on_submit_calvin_inbox(
        &self,
        _req: crate::rpc_codec::SubmitCalvinInboxRequest,
    ) -> crate::rpc_codec::SubmitCalvinInboxResponse {
        crate::rpc_codec::SubmitCalvinInboxResponse {
            inbox_seq: 0,
            epoch: 0,
            position: 0,
            participants: 0,
            error: None,
        }
    }

    async fn on_timeout_now(&self, _req: TimeoutNowRequest) {}
}

fn make_transport(node_id: u64) -> NexarTransport {
    NexarTransport::new(
        node_id,
        "127.0.0.1:0".parse().unwrap(),
        TransportCredentials::Insecure,
    )
    .unwrap()
}

#[tokio::test]
async fn append_entries_roundtrip() {
    let server = Arc::new(make_transport(1));
    let client = Arc::new(make_transport(2));

    client.register_peer(1, server.local_addr());

    let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let srv = server.clone();
    let serve_task = tokio::spawn(async move {
        srv.serve(handler, shutdown_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let req = AppendEntriesRequest {
        term: 5,
        leader_id: 2,
        prev_log_index: 10,
        prev_log_term: 4,
        entries: vec![
            LogEntry {
                term: 5,
                index: 11,
                data: b"cmd1".to_vec(),
            },
            LogEntry {
                term: 5,
                index: 12,
                data: b"cmd2".to_vec(),
            },
        ],
        leader_commit: 10,
        group_id: 7,
    };

    let resp = client.append_entries(1, req).await.unwrap();
    assert_eq!(resp.term, 5);
    assert!(resp.success);
    assert_eq!(resp.last_log_index, 12);

    shutdown_tx.send(true).unwrap();
    serve_task.abort();
}

#[tokio::test]
async fn execute_stream_roundtrip() {
    use futures::StreamExt;

    let server = Arc::new(make_transport(1));
    let client = Arc::new(make_transport(2));

    client.register_peer(1, server.local_addr());

    let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let srv = server.clone();
    let serve_task = tokio::spawn(async move {
        srv.serve(handler, shutdown_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let req = RaftRpc::ExecuteStreamRequest(crate::rpc_codec::ExecuteRequest {
        plan_bytes: b"plan".to_vec(),
        tenant_id: 1,
        database_id: 0,
        deadline_remaining_ms: 5000,
        trace_id: [0u8; 16],
        descriptor_versions: vec![],
        txn_id: None,
    });

    let stream = client.send_rpc_stream(1, req).await.unwrap();
    futures::pin_mut!(stream);

    let mut chunks: Vec<(Vec<u8>, u64)> = Vec::new();
    while let Some(item) = stream.next().await {
        chunks.push(item.unwrap());
    }

    // EchoHandler emits three chunks then a clean EOF.
    assert_eq!(chunks.len(), 3, "expected three streamed chunks");
    for (i, (payload, lsn)) in chunks.iter().enumerate() {
        assert_eq!(payload, &vec![i as u8; 4]);
        assert_eq!(*lsn, i as u64 + 100);
    }

    shutdown_tx.send(true).unwrap();
    serve_task.abort();
}

#[tokio::test]
async fn request_vote_roundtrip() {
    let server = Arc::new(make_transport(1));
    let client = Arc::new(make_transport(2));

    client.register_peer(1, server.local_addr());

    let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let srv = server.clone();
    tokio::spawn(async move {
        srv.serve(handler, shutdown_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let req = RequestVoteRequest {
        term: 10,
        candidate_id: 2,
        last_log_index: 100,
        last_log_term: 9,
        group_id: 3,
    };

    let resp = client.request_vote(1, req).await.unwrap();
    assert_eq!(resp.term, 10);
    assert!(resp.vote_granted);
}

#[tokio::test]
async fn install_snapshot_roundtrip() {
    let server = Arc::new(make_transport(1));
    let client = Arc::new(make_transport(2));

    client.register_peer(1, server.local_addr());

    let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let srv = server.clone();
    tokio::spawn(async move {
        srv.serve(handler, shutdown_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let req = InstallSnapshotRequest {
        term: 7,
        leader_id: 2,
        last_included_index: 500,
        last_included_term: 6,
        offset: 0,
        data: vec![0xAB; 4096],
        done: true,
        group_id: 0,
        total_size: 0,
    };

    let resp = client.install_snapshot(1, req).await.unwrap();
    assert_eq!(resp.term, 7);
}

#[tokio::test]
async fn concurrent_rpcs() {
    let server = Arc::new(make_transport(1));
    let client = Arc::new(make_transport(2));

    client.register_peer(1, server.local_addr());

    let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let srv = server.clone();
    tokio::spawn(async move {
        srv.serve(handler, shutdown_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut handles = Vec::new();
    for i in 0..10u64 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let req = AppendEntriesRequest {
                term: i,
                leader_id: 2,
                prev_log_index: i * 10,
                prev_log_term: i.saturating_sub(1),
                entries: vec![],
                leader_commit: i * 10,
                group_id: 0,
            };
            let resp = c.append_entries(1, req).await.unwrap();
            assert_eq!(resp.term, i);
            assert!(resp.success);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn connection_reuse() {
    let server = Arc::new(make_transport(1));
    let client = Arc::new(make_transport(2));

    client.register_peer(1, server.local_addr());

    let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let srv = server.clone();
    tokio::spawn(async move {
        srv.serve(handler, shutdown_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    for _ in 0..2 {
        let req = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            group_id: 0,
        };
        client.request_vote(1, req).await.unwrap();
    }

    let peers = client.peers.read().unwrap();
    assert_eq!(peers.len(), 1);
}

#[tokio::test]
async fn unregistered_peer_fails() {
    let client = make_transport(1);

    let req = AppendEntriesRequest {
        term: 1,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
        group_id: 0,
    };

    let err = client.append_entries(99, req).await.unwrap_err();
    assert!(
        err.to_string().contains("not reachable"),
        "expected unreachable error: {err}"
    );
}

#[tokio::test]
async fn heartbeat_roundtrip() {
    let server = Arc::new(make_transport(1));
    let client = Arc::new(make_transport(2));

    client.register_peer(1, server.local_addr());

    let handler: Arc<EchoHandler> = Arc::new(EchoHandler);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let srv = server.clone();
    tokio::spawn(async move {
        srv.serve(handler, shutdown_rx).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let req = AppendEntriesRequest {
        term: 3,
        leader_id: 2,
        prev_log_index: 50,
        prev_log_term: 2,
        entries: vec![],
        leader_commit: 50,
        group_id: 0,
    };

    let resp = client.append_entries(1, req).await.unwrap();
    assert_eq!(resp.term, 3);
    assert!(resp.success);
    assert_eq!(resp.last_log_index, 50);
}
