//! Native protocol session: the run loop that reads frames, routes
//! by opcode, and writes responses.
//!
//! Replaces the legacy JSON-only `Session` with auto-detection of
//! JSON vs MessagePack and full SQL/DDL/transaction support.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tracing::{debug, instrument};

use nodedb_types::protocol::{NativeResponse, OpCode, RequestFields};

use crate::config::auth::AuthMode;
use crate::control::planner::context::QueryContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::conn_stream::ConnStream;
use crate::control::server::pgwire::session::SessionStore;
use crate::control::state::SharedState;

use super::codec::{self, FrameFormat};
use super::dispatch::{self, DispatchCtx};

/// A client session on the native binary protocol.
///
/// Auto-detects JSON vs MessagePack on the first frame. Supports all
/// operations: auth, SQL, DDL, transactions, direct Data Plane ops.
pub struct NativeSession {
    stream: ConnStream,
    peer_addr: SocketAddr,
    state: Arc<SharedState>,
    auth_mode: AuthMode,
    identity: Option<AuthenticatedIdentity>,
    format: Option<FrameFormat>,
    query_ctx: QueryContext,
    sessions: SessionStore,
}

impl NativeSession {
    fn with_stream(
        stream: ConnStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
    ) -> Self {
        let query_ctx = QueryContext::with_catalog(
            Arc::clone(&state.credentials),
            1, // default tenant for name resolution
        );
        Self {
            stream,
            peer_addr,
            state,
            auth_mode,
            identity: None,
            format: None,
            query_ctx,
            sessions: SessionStore::new(),
        }
    }

    /// Create a session from a plain TCP stream.
    pub fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
    ) -> Self {
        Self::with_stream(ConnStream::plain(stream), peer_addr, state, auth_mode)
    }

    /// Create a session from a TLS-wrapped stream.
    pub fn new_tls(
        stream: tokio_rustls::server::TlsStream<TcpStream>,
        peer_addr: SocketAddr,
        state: Arc<SharedState>,
        auth_mode: AuthMode,
    ) -> Self {
        Self::with_stream(ConnStream::tls(stream), peer_addr, state, auth_mode)
    }

    /// Run the session loop: read frames, route by opcode, write responses.
    #[instrument(skip(self), fields(peer = %self.peer_addr))]
    pub async fn run(mut self) -> crate::Result<()> {
        let idle_timeout_secs = self.state.idle_timeout_secs();

        loop {
            // Read a frame with idle timeout.
            let payload = if idle_timeout_secs > 0 {
                match tokio::time::timeout(
                    Duration::from_secs(idle_timeout_secs),
                    codec::read_frame(&mut self.stream),
                )
                .await
                {
                    Ok(result) => result?,
                    Err(_) => {
                        debug!("session idle timeout ({}s)", idle_timeout_secs);
                        return Ok(());
                    }
                }
            } else {
                codec::read_frame(&mut self.stream).await?
            };

            let payload = match payload {
                Some(p) => p,
                None => return Ok(()), // clean EOF
            };

            // Auto-detect format on first frame.
            if self.format.is_none() {
                self.format = Some(FrameFormat::detect(payload[0]));
            }
            // Safety: format is always Some after the check above.
            let Some(format) = self.format else {
                return Err(crate::Error::BadRequest {
                    detail: "internal error: format detection failed".into(),
                });
            };

            // Decode and handle.
            let response = match codec::decode_request(&payload, format) {
                Ok(req) => self.handle_request(req).await,
                Err(e) => NativeResponse::error(0, "42601", format!("{e}")),
            };

            // Encode and write response.
            let resp_bytes = codec::encode_response(&response, format)?;
            codec::write_frame(&mut self.stream, &resp_bytes).await?;
        }
    }

    /// Route a decoded request to the appropriate handler.
    async fn handle_request(
        &mut self,
        req: nodedb_types::protocol::NativeRequest,
    ) -> NativeResponse {
        let seq = req.seq;
        let op = req.op;

        // Auth handling.
        if op == OpCode::Auth {
            return self.handle_auth(seq, &req.fields);
        }

        // Ping requires no auth.
        if op == OpCode::Ping {
            return dispatch::handle_ping(seq);
        }

        // All other ops require authentication.
        if self.identity.is_none() {
            if self.auth_mode == AuthMode::Trust {
                self.identity = Some(super::super::session_auth::trust_identity(
                    &self.state,
                    "anonymous",
                ));
            } else {
                return NativeResponse::error(
                    seq,
                    "28000",
                    "not authenticated. Send Auth request first.",
                );
            }
        }

        let identity = match self.identity.as_ref() {
            Some(id) => id,
            None => {
                return NativeResponse::error(seq, "28000", "not authenticated");
            }
        };

        let ctx = DispatchCtx {
            state: &self.state,
            identity,
            query_ctx: &self.query_ctx,
            sessions: &self.sessions,
            peer_addr: &self.peer_addr,
        };

        let RequestFields::Text(fields) = &req.fields;

        match op {
            // SQL: full DataFusion pipeline.
            OpCode::Sql | OpCode::Ddl => {
                let sql = match &fields.sql {
                    Some(s) => s.as_str(),
                    None => return NativeResponse::error(seq, "42601", "missing 'sql' field"),
                };
                dispatch::handle_sql(&ctx, seq, sql).await
            }

            // Session parameters.
            OpCode::Set => {
                let key = match &fields.key {
                    Some(k) => k.as_str(),
                    None => {
                        // Also support SET via sql field: "SET key = value"
                        if let Some(sql) = &fields.sql {
                            return dispatch::handle_sql(&ctx, seq, sql).await;
                        }
                        return NativeResponse::error(seq, "42601", "missing 'key' field");
                    }
                };
                let value = fields.value.as_deref().unwrap_or("");
                dispatch::handle_set(&ctx, seq, key, value)
            }
            OpCode::Show => {
                let key = match &fields.key {
                    Some(k) => k.as_str(),
                    None => {
                        if let Some(sql) = &fields.sql {
                            return dispatch::handle_sql(&ctx, seq, sql).await;
                        }
                        return NativeResponse::error(seq, "42601", "missing 'key' field");
                    }
                };
                dispatch::handle_show(&ctx, seq, key)
            }
            OpCode::Reset => {
                let key = match &fields.key {
                    Some(k) => k.as_str(),
                    None => return NativeResponse::error(seq, "42601", "missing 'key' field"),
                };
                dispatch::handle_reset(&ctx, seq, key)
            }

            // Transaction control.
            OpCode::Begin => dispatch::handle_begin(&ctx, seq),
            OpCode::Commit => dispatch::handle_commit(&ctx, seq).await,
            OpCode::Rollback => dispatch::handle_rollback(&ctx, seq),

            // Explain.
            OpCode::Explain => {
                let sql = match &fields.sql {
                    Some(s) => s.as_str(),
                    None => return NativeResponse::error(seq, "42601", "missing 'sql' field"),
                };
                dispatch::handle_sql(&ctx, seq, &format!("EXPLAIN {sql}")).await
            }

            // Direct Data Plane operations.
            OpCode::PointGet
            | OpCode::PointPut
            | OpCode::PointDelete
            | OpCode::VectorSearch
            | OpCode::RangeScan
            | OpCode::CrdtRead
            | OpCode::CrdtApply
            | OpCode::GraphRagFusion
            | OpCode::AlterCollectionPolicy
            | OpCode::GraphHop
            | OpCode::GraphNeighbors
            | OpCode::GraphPath
            | OpCode::GraphSubgraph
            | OpCode::EdgePut
            | OpCode::EdgeDelete
            | OpCode::TextSearch
            | OpCode::HybridSearch => dispatch::handle_direct_op(&ctx, seq, op, fields).await,

            // Batch ops: dispatch via SQL for now.
            OpCode::VectorBatchInsert | OpCode::DocumentBatchInsert => NativeResponse::error(
                seq,
                "0A000",
                "batch operations via native protocol not yet implemented; use SQL INSERT",
            ),

            // Copy from file.
            OpCode::CopyFrom => {
                let sql = match &fields.sql {
                    Some(s) => s.as_str(),
                    None => return NativeResponse::error(seq, "42601", "missing 'sql' field"),
                };
                dispatch::handle_sql(&ctx, seq, sql).await
            }

            // Auth/Ping handled above.
            OpCode::Auth | OpCode::Ping => unreachable!(),
        }
    }

    /// Handle authentication request.
    fn handle_auth(&mut self, seq: u64, fields: &RequestFields) -> NativeResponse {
        let auth = match fields {
            RequestFields::Text(f) => match &f.auth {
                Some(a) => a,
                None => {
                    return NativeResponse::error(seq, "28000", "missing 'auth' field");
                }
            },
        };

        match dispatch::handle_auth(
            &self.state,
            &self.auth_mode,
            auth,
            &self.peer_addr.to_string(),
        ) {
            Ok(identity) => {
                let resp = NativeResponse::auth_ok(
                    seq,
                    identity.username.clone(),
                    identity.tenant_id.as_u32(),
                );
                self.identity = Some(identity);
                resp
            }
            Err(e) => NativeResponse::error(seq, "28P01", format!("{e}")),
        }
    }
}
