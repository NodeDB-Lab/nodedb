use sonic_rs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, instrument, warn};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::bridge::physical_plan::{CrdtOp, DocumentOp, GraphOp, VectorOp};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};

/// Maximum frame size: 16 MiB.
const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// A client session on the Control Plane.
///
/// Each accepted TCP connection gets its own `Session`. The session handles
/// protocol framing, request parsing, and dispatching via the shared state.
/// This is `Send + Sync` — runs on the Tokio thread pool.
///
/// ## Wire Protocol (length-prefixed binary)
///
/// ```text
/// Request frame:  [4 bytes: payload_len (big-endian u32)] [payload_len bytes: JSON body]
/// Response frame: [4 bytes: payload_len (big-endian u32)] [payload_len bytes: JSON body]
/// ```
///
/// Request JSON:
/// ```json
/// {
///   "op": "point_get" | "vector_search" | "range_scan" | "crdt_read" | "crdt_apply",
///   "tenant_id": 1,
///   "collection": "users",
///   "document_id": "doc-1",    // for point_get, crdt_read, crdt_apply
///   "query_vector": [0.1, ...], // for vector_search
///   "top_k": 10,                // for vector_search
///   "field": "age",             // for range_scan
///   "limit": 100,               // for range_scan
///   "delta": "base64...",       // for crdt_apply
///   "peer_id": 12345            // for crdt_apply
/// }
/// ```
///
/// Response JSON:
/// ```json
/// {
///   "request_id": 1,
///   "status": "ok" | "error",
///   "payload": "base64...",
///   "watermark_lsn": 42,
///   "error_code": null | "deadline_exceeded" | ...
/// }
/// ```
use super::conn_stream::ConnStream;

pub struct Session {
    stream: ConnStream,
    peer_addr: SocketAddr,
    next_request_id: AtomicU64,
    state: Arc<SharedState>,
    auth_mode: crate::config::auth::AuthMode,
    /// Bound after auth handshake. None until first frame is processed.
    identity: Option<crate::control::security::identity::AuthenticatedIdentity>,
}

impl Session {
    fn with_stream(
        stream: ConnStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: crate::config::auth::AuthMode,
    ) -> Self {
        Self {
            stream,
            peer_addr,
            next_request_id: AtomicU64::new(1),
            state,
            auth_mode,
            identity: None,
        }
    }

    /// Create a session from a plain TCP stream.
    pub fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: crate::config::auth::AuthMode,
    ) -> Self {
        Self::with_stream(ConnStream::plain(stream), peer_addr, state, auth_mode)
    }

    /// Create a session from a TLS-wrapped stream.
    pub fn new_tls(
        stream: tokio_rustls::server::TlsStream<TcpStream>,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: crate::config::auth::AuthMode,
    ) -> Self {
        Self::with_stream(ConnStream::tls(stream), peer_addr, state, auth_mode)
    }

    /// Allocate a monotonically increasing request ID for this connection.
    fn next_request_id(&self) -> RequestId {
        RequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Run the session loop: read frames, parse, dispatch, respond.
    #[instrument(skip(self), fields(peer = %self.peer_addr))]
    pub async fn run(mut self) -> crate::Result<()> {
        let idle_timeout_secs = self.state.idle_timeout_secs();
        loop {
            // Read length prefix with idle timeout.
            let mut len_buf = [0u8; 4];
            let read_result: std::io::Result<usize> = if idle_timeout_secs > 0 {
                match tokio::time::timeout(
                    Duration::from_secs(idle_timeout_secs),
                    self.stream.read_exact(&mut len_buf),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        debug!("session idle timeout ({}s)", idle_timeout_secs);
                        return Ok(());
                    }
                }
            } else {
                self.stream.read_exact(&mut len_buf).await
            };
            match read_result {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("client disconnected");
                    return Ok(());
                }
                Err(e) => return Err(e.into()),
            }

            let payload_len = u32::from_be_bytes(len_buf);
            if payload_len > MAX_FRAME_SIZE {
                warn!(payload_len, "frame too large, closing connection");
                return Err(crate::Error::BadRequest {
                    detail: format!("frame size {payload_len} exceeds maximum {MAX_FRAME_SIZE}"),
                });
            }

            // Read payload.
            let mut payload = vec![0u8; payload_len as usize];
            self.stream.read_exact(&mut payload).await?;

            // Parse and dispatch.
            let request_id = self.next_request_id();
            match self.handle_frame(request_id, &payload).await {
                Ok(response_bytes) => {
                    // Write length-prefixed response.
                    let resp_len = (response_bytes.len() as u32).to_be_bytes();
                    self.stream.write_all(&resp_len).await?;
                    self.stream.write_all(&response_bytes).await?;
                }
                Err(e) => {
                    // Send error response.
                    let error_json = format!(r#"{{"status":"error","error":"{e}"}}"#);
                    let resp_len = (error_json.len() as u32).to_be_bytes();
                    self.stream.write_all(&resp_len).await?;
                    self.stream.write_all(error_json.as_bytes()).await?;
                }
            }
        }
    }

    /// Parse a request frame and dispatch to the Data Plane.
    async fn handle_frame(
        &mut self,
        request_id: RequestId,
        payload: &[u8],
    ) -> crate::Result<Vec<u8>> {
        // Parse the JSON request body.
        let body: serde_json::Value =
            sonic_rs::from_slice(payload).map_err(|e| crate::Error::BadRequest {
                detail: format!("invalid JSON: {e}"),
            })?;

        let op = body["op"]
            .as_str()
            .ok_or_else(|| crate::Error::BadRequest {
                detail: "missing 'op' field".into(),
            })?;

        // Auth handshake: must be first frame.
        if op == "auth" {
            let identity = super::session_auth::authenticate(
                &self.state,
                &self.auth_mode,
                &body,
                &self.peer_addr.to_string(),
            )?;
            let resp = format!(
                r#"{{"status":"ok","username":"{}","tenant_id":{}}}"#,
                identity.username,
                identity.tenant_id.as_u32()
            );
            self.identity = Some(identity);
            return Ok(resp.into_bytes());
        }

        // All other ops require auth. In trust mode, auto-authenticate on first frame.
        if self.identity.is_none() {
            if self.auth_mode == crate::config::auth::AuthMode::Trust {
                self.identity = Some(super::session_auth::trust_identity(
                    &self.state,
                    "anonymous",
                ));
            } else {
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: r#"not authenticated. Send {"op":"auth",...} first."#.into(),
                });
            }
        }

        let identity = match self.identity.as_ref() {
            Some(id) => id,
            None => {
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: TenantId::new(0),
                    resource: "not authenticated".into(),
                });
            }
        };

        // Tenant from authenticated identity, not from client payload.
        let tenant_id = identity.tenant_id;

        let collection = body["collection"].as_str().unwrap_or("default").to_string();

        // Determine vShard from collection + document_id for data locality.
        let vshard_key = body["document_id"].as_str().unwrap_or(&collection);
        let vshard_id = VShardId::from_key(vshard_key.as_bytes());

        let plan = match op {
            "point_get" => {
                let document_id = body["document_id"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'document_id'".into(),
                    })?
                    .to_string();
                let pk_bytes = document_id.as_bytes().to_vec();
                let surrogate = self
                    .state
                    .surrogate_assigner
                    .lookup(&collection, &pk_bytes)?
                    .unwrap_or(nodedb_types::Surrogate::ZERO);
                PhysicalPlan::Document(DocumentOp::PointGet {
                    collection,
                    document_id,
                    surrogate,
                    pk_bytes,
                    rls_filters: Vec::new(),
                    system_as_of_ms: None,
                    valid_at_ms: None,
                })
            }
            "vector_search" => {
                let query_vector: Vec<f32> = body["query_vector"]
                    .as_array()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'query_vector'".into(),
                    })?
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                let top_k = body["top_k"].as_u64().unwrap_or(10) as usize;
                PhysicalPlan::Vector(VectorOp::Search {
                    collection,
                    query_vector,
                    top_k,
                    ef_search: 0,
                    filter_bitmap: None,
                    field_name: String::new(),
                    rls_filters: Vec::new(),
                })
            }
            "range_scan" => {
                let field = body["field"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'field'".into(),
                    })?
                    .to_string();
                let limit = body["limit"].as_u64().unwrap_or(100) as usize;
                PhysicalPlan::Document(DocumentOp::RangeScan {
                    collection,
                    field,
                    lower: None,
                    upper: None,
                    limit,
                })
            }
            "crdt_read" => {
                let document_id = body["document_id"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'document_id'".into(),
                    })?
                    .to_string();
                PhysicalPlan::Crdt(CrdtOp::Read {
                    collection,
                    document_id,
                })
            }
            "crdt_apply" => {
                let document_id = body["document_id"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'document_id'".into(),
                    })?
                    .to_string();
                let delta_b64 = body["delta"]
                    .as_str()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'delta'".into(),
                    })?;
                // Decode base64 delta. For now accept raw bytes if not valid base64.
                let delta = delta_b64.as_bytes().to_vec();
                let peer_id = body["peer_id"].as_u64().unwrap_or(0);
                let surrogate = self
                    .state
                    .surrogate_assigner
                    .assign(&collection, document_id.as_bytes())?;
                PhysicalPlan::Crdt(CrdtOp::Apply {
                    collection,
                    document_id,
                    delta,
                    peer_id,
                    mutation_id: 0,
                    surrogate,
                })
            }
            "graph_rag_fusion" => {
                let query_vector: Vec<f32> = body["query_vector"]
                    .as_array()
                    .ok_or_else(|| crate::Error::BadRequest {
                        detail: "missing 'query_vector'".into(),
                    })?
                    .iter()
                    .filter_map(|v| v.as_f64().map(|f| f as f32))
                    .collect();
                let vector_top_k = body["vector_top_k"].as_u64().unwrap_or(20) as usize;
                let edge_label = body["edge_label"].as_str().map(String::from);
                let direction_str = body["direction"].as_str().unwrap_or("out");
                let direction = match direction_str {
                    "in" => crate::engine::graph::edge_store::Direction::In,
                    "both" => crate::engine::graph::edge_store::Direction::Both,
                    _ => crate::engine::graph::edge_store::Direction::Out,
                };
                let expansion_depth = body["expansion_depth"].as_u64().unwrap_or(2) as usize;
                let final_top_k = body["final_top_k"].as_u64().unwrap_or(10) as usize;
                let vector_k = body["vector_k"].as_f64().unwrap_or(60.0);
                let graph_k = body["graph_k"].as_f64().unwrap_or(10.0);
                PhysicalPlan::Graph(GraphOp::RagFusion {
                    collection,
                    query_vector,
                    vector_top_k,
                    edge_label,
                    direction,
                    expansion_depth,
                    final_top_k,
                    rrf_k: (vector_k, graph_k),
                    vector_field: body["vector_field"].as_str().unwrap_or("").to_string(),
                    options: Default::default(),
                })
            }
            "alter_collection_policy" => {
                let policy = &body["policy"];
                if policy.is_null() {
                    return Err(crate::Error::BadRequest {
                        detail: "missing 'policy' field".into(),
                    });
                }
                let policy_json =
                    sonic_rs::to_string(policy).map_err(|e| crate::Error::BadRequest {
                        detail: format!("invalid policy JSON: {e}"),
                    })?;
                PhysicalPlan::Crdt(CrdtOp::SetPolicy {
                    collection,
                    policy_json,
                })
            }
            _ => {
                return Err(crate::Error::BadRequest {
                    detail: format!("unknown op: {op}"),
                });
            }
        };

        let request = Request {
            request_id,
            tenant_id,
            vshard_id,
            plan,
            deadline: Instant::now()
                + Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        };

        // Register for response routing before dispatching.
        let rx = self.state.tracker.register_oneshot(request_id);

        // Dispatch to Data Plane via SPSC.
        match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request)?,
            Err(poisoned) => poisoned.into_inner().dispatch(request)?,
        };

        // Await response from Data Plane (routed back via the response poller).
        let response = tokio::time::timeout(
            Duration::from_secs(self.state.tuning.network.default_deadline_secs),
            rx,
        )
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
        .map_err(|_| crate::Error::Dispatch {
            detail: "response channel closed".into(),
        })?;

        // Serialize response to JSON.
        let status_str = match response.status {
            Status::Ok => "ok",
            Status::Partial => "partial",
            Status::Error => "error",
        };

        let payload_b64 = if response.payload.is_empty() {
            String::new()
        } else {
            // Return raw payload as lossy UTF-8 for now.
            String::from_utf8_lossy(&response.payload).into_owned()
        };

        let error_code_str = response.error_code.as_ref().map(|ec| format!("{ec:?}"));

        let resp_json = format!(
            r#"{{"request_id":{},"status":"{}","payload":"{}","watermark_lsn":{},"error_code":{}}}"#,
            response.request_id.as_u64(),
            status_str,
            payload_b64,
            response.watermark_lsn.as_u64(),
            error_code_str
                .as_ref()
                .map(|s| format!("\"{s}\""))
                .unwrap_or_else(|| "null".to_string()),
        );

        Ok(resp_json.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::wal::WalManager;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// End-to-end test: client -> session -> dispatcher -> core_loop -> response -> client.
    #[tokio::test]
    async fn full_request_response_roundtrip() {
        // Set up infrastructure.
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());

        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal);

        // Start a Data Plane core in a background thread.
        let data_side = data_sides.into_iter().next().unwrap();
        let core_dir = dir.path().to_path_buf();
        let (core_stop_tx, core_stop_rx) = std::sync::mpsc::channel::<()>();
        let core_handle = tokio::task::spawn_blocking(move || {
            let mut core = CoreLoop::open(
                0,
                data_side.request_rx,
                data_side.response_tx,
                &core_dir,
                std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            )
            .unwrap();
            while matches!(
                core_stop_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ) {
                core.tick();
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        // Start response poller.
        let shared_poller = Arc::clone(&shared);
        let (poller_stop_tx, mut poller_stop_rx) = tokio::sync::watch::channel(false);
        let poller_handle = tokio::spawn(async move {
            loop {
                shared_poller.poll_and_route_responses();
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    _ = poller_stop_rx.changed() => break,
                }
            }
        });

        // Bind a test listener.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn session handler.
        let shared_session = Arc::clone(&shared);
        let session_handle = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let session = Session::new(
                stream,
                peer_addr,
                shared_session,
                crate::config::auth::AuthMode::Trust,
            );
            session.run().await
        });

        // Connect as a client and send a request.
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        let request_json =
            r#"{"op":"point_get","tenant_id":1,"collection":"users","document_id":"u1"}"#;
        let len = (request_json.len() as u32).to_be_bytes();
        client.write_all(&len).await.unwrap();
        client.write_all(request_json.as_bytes()).await.unwrap();

        // Read response.
        let mut resp_len_buf = [0u8; 4];
        client.read_exact(&mut resp_len_buf).await.unwrap();
        let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        client.read_exact(&mut resp_buf).await.unwrap();

        let resp_str = String::from_utf8(resp_buf).unwrap();
        // Document doesn't exist, so we get NotFound — but the roundtrip works.
        assert!(
            resp_str.contains(r#""status""#),
            "expected a valid response, got: {resp_str}"
        );
        assert!(
            resp_str.contains(r#""request_id""#),
            "expected request_id in response, got: {resp_str}"
        );

        // Clean up: signal background tasks to stop.
        drop(client);
        let _ = session_handle.await;
        let _ = poller_stop_tx.send(true);
        let _ = poller_handle.await;
        let _ = core_stop_tx.send(());
        let _ = core_handle.await;
    }
}
