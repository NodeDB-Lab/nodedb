//! Outbound Raft RPC transport.
//!
//! [`NexarTransport`] implements [`RaftTransport`] — serialize the request,
//! open a QUIC bidi stream, send the frame, read the response frame, and
//! deserialize. Connection pooling is automatic (cached per peer, replaced
//! on stale).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nodedb_raft::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use nodedb_raft::transport::RaftTransport;
use tracing::{debug, info, warn};

use crate::error::{ClusterError, Result};
use crate::rpc_codec::{self, RaftRpc};

use super::config::{self, SNI_HOSTNAME};
use super::server::{self, RaftRpcHandler};

/// QUIC-based Raft transport.
///
/// Implements [`RaftTransport`] for outbound RPCs and provides [`serve`]
/// for inbound RPC handling. Thread-safe — wrap in `Arc` for shared use.
pub struct NexarTransport {
    node_id: u64,
    listener: nexar::TransportListener,
    client_config: quinn::ClientConfig,
    /// Cached connections to peers. Stale connections are replaced on next use.
    peers: RwLock<HashMap<u64, quinn::Connection>>,
    /// Known peer addresses for connection establishment.
    peer_addrs: RwLock<HashMap<u64, SocketAddr>>,
    rpc_timeout: Duration,
}

impl NexarTransport {
    /// Create a new transport bound to the given address.
    pub fn new(node_id: u64, listen_addr: SocketAddr) -> Result<Self> {
        Self::with_timeout(node_id, listen_addr, config::DEFAULT_RPC_TIMEOUT)
    }

    /// Create a new transport with a custom RPC timeout.
    pub fn with_timeout(
        node_id: u64,
        listen_addr: SocketAddr,
        rpc_timeout: Duration,
    ) -> Result<Self> {
        let server_config = config::make_raft_server_config()?;
        let listener = nexar::TransportListener::bind_with_config(listen_addr, server_config)
            .map_err(|e| ClusterError::Transport {
                detail: format!("bind {listen_addr}: {e}"),
            })?;
        let client_config = config::make_raft_client_config()?;

        info!(
            node_id,
            addr = %listener.local_addr(),
            "raft transport bound"
        );

        Ok(Self {
            node_id,
            listener,
            client_config,
            peers: RwLock::new(HashMap::new()),
            peer_addrs: RwLock::new(HashMap::new()),
            rpc_timeout,
        })
    }

    /// The local address this transport is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr()
    }

    /// This node's ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Register a peer's address for outbound connections.
    pub fn register_peer(&self, node_id: u64, addr: SocketAddr) {
        let mut addrs = self.peer_addrs.write().unwrap_or_else(|p| p.into_inner());
        addrs.insert(node_id, addr);
        debug!(node_id, %addr, "peer registered");
    }

    /// Run the inbound RPC accept loop until shutdown.
    ///
    /// For each incoming connection, spawns a task that accepts bidi streams
    /// and dispatches RPCs to the handler.
    pub async fn serve<H: RaftRpcHandler>(
        &self,
        handler: Arc<H>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        info!(node_id = self.node_id, addr = %self.local_addr(), "raft RPC server started");

        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    match result {
                        Ok(conn) => {
                            let peer = conn.remote_address();
                            debug!(%peer, "accepted raft connection");
                            let h = handler.clone();
                            tokio::spawn(async move {
                                if let Err(e) = server::handle_connection(conn, h).await {
                                    debug!(%peer, error = %e, "raft connection ended");
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = %e, "raft accept failed");
                        }
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(node_id = self.node_id, "raft RPC server shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Get an existing connection to a peer, or establish a new one.
    async fn get_or_connect(&self, target: u64) -> Result<quinn::Connection> {
        // Check cache — fast path.
        {
            let peers = self.peers.read().unwrap_or_else(|p| p.into_inner());
            if let Some(conn) = peers.get(&target) {
                if conn.close_reason().is_none() {
                    return Ok(conn.clone());
                }
            }
        }

        // Resolve address.
        let addr = {
            let addrs = self.peer_addrs.read().unwrap_or_else(|p| p.into_inner());
            addrs
                .get(&target)
                .copied()
                .ok_or(ClusterError::NodeUnreachable { node_id: target })?
        };

        // Connect.
        let conn = self
            .listener
            .endpoint()
            .connect_with(self.client_config.clone(), addr, SNI_HOSTNAME)
            .map_err(|e| ClusterError::Transport {
                detail: format!("connect to node {target} at {addr}: {e}"),
            })?
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("handshake with node {target} at {addr}: {e}"),
            })?;

        debug!(target, %addr, "connected to peer");

        // Cache (harmless race: last writer wins, both connections valid).
        let mut peers = self.peers.write().unwrap_or_else(|p| p.into_inner());
        peers.insert(target, conn.clone());
        Ok(conn)
    }

    /// Send an RPC to a peer and await the response.
    async fn send_rpc(&self, target: u64, rpc: RaftRpc) -> Result<RaftRpc> {
        let frame = rpc_codec::encode(&rpc)?;
        let conn = self.get_or_connect(target).await?;

        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| ClusterError::Transport {
            detail: format!("open_bi to node {target}: {e}"),
        })?;

        // Write request.
        send.write_all(&frame)
            .await
            .map_err(|e| ClusterError::Transport {
                detail: format!("write to node {target}: {e}"),
            })?;
        send.finish().map_err(|e| ClusterError::Transport {
            detail: format!("finish send to node {target}: {e}"),
        })?;

        // Read response with timeout.
        let response_frame = tokio::time::timeout(self.rpc_timeout, server::read_frame(&mut recv))
            .await
            .map_err(|_| ClusterError::Transport {
                detail: format!(
                    "RPC timeout ({}ms) to node {target}",
                    self.rpc_timeout.as_millis()
                ),
            })??;

        rpc_codec::decode(&response_frame)
    }
}

fn to_raft_err(e: ClusterError) -> nodedb_raft::RaftError {
    nodedb_raft::RaftError::Transport {
        detail: e.to_string(),
    }
}

impl RaftTransport for NexarTransport {
    async fn append_entries(
        &self,
        target: u64,
        req: AppendEntriesRequest,
    ) -> nodedb_raft::Result<AppendEntriesResponse> {
        let resp = self
            .send_rpc(target, RaftRpc::AppendEntriesRequest(req))
            .await
            .map_err(to_raft_err)?;
        match resp {
            RaftRpc::AppendEntriesResponse(r) => Ok(r),
            other => Err(nodedb_raft::RaftError::Transport {
                detail: format!("expected AppendEntriesResponse, got {other:?}"),
            }),
        }
    }

    async fn request_vote(
        &self,
        target: u64,
        req: RequestVoteRequest,
    ) -> nodedb_raft::Result<RequestVoteResponse> {
        let resp = self
            .send_rpc(target, RaftRpc::RequestVoteRequest(req))
            .await
            .map_err(to_raft_err)?;
        match resp {
            RaftRpc::RequestVoteResponse(r) => Ok(r),
            other => Err(nodedb_raft::RaftError::Transport {
                detail: format!("expected RequestVoteResponse, got {other:?}"),
            }),
        }
    }

    async fn install_snapshot(
        &self,
        target: u64,
        req: InstallSnapshotRequest,
    ) -> nodedb_raft::Result<InstallSnapshotResponse> {
        let resp = self
            .send_rpc(target, RaftRpc::InstallSnapshotRequest(req))
            .await
            .map_err(to_raft_err)?;
        match resp {
            RaftRpc::InstallSnapshotResponse(r) => Ok(r),
            other => Err(nodedb_raft::RaftError::Transport {
                detail: format!("expected InstallSnapshotResponse, got {other:?}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_raft::message::LogEntry;

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
    }

    fn make_transport(node_id: u64) -> NexarTransport {
        NexarTransport::new(node_id, "127.0.0.1:0".parse().unwrap()).unwrap()
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
}
