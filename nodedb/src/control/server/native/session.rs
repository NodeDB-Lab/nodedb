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

use nodedb_types::protocol::{
    MAX_FRAME_SIZE, NativeResponse, OpCode, RequestFields, ResponseStatus,
};

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
    auth_context: Option<crate::control::security::auth_context::AuthContext>,
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
        let query_ctx = QueryContext::for_state(&state, 1); // default tenant
        Self {
            stream,
            peer_addr,
            state,
            auth_mode,
            identity: None,
            auth_context: None,
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
            let Some(format) = self.format else {
                return Err(crate::Error::BadRequest {
                    detail: "format detection failed after first frame".into(),
                });
            };

            // Decode and handle.
            let response = match codec::decode_request(&payload, format) {
                Ok(req) => self.handle_request(req).await,
                Err(e) => NativeResponse::error(0, "42601", format!("{e}")),
            };

            // Encode and write response — chunk if it exceeds frame limit.
            let resp_bytes = codec::encode_response(&response, format)?;
            if resp_bytes.len() <= MAX_FRAME_SIZE as usize {
                codec::write_frame(&mut self.stream, &resp_bytes).await?;
            } else {
                // Response too large for a single frame — split rows.
                let frames = chunk_large_response(response, format)?;
                for frame in &frames {
                    codec::write_frame(&mut self.stream, frame).await?;
                }
            }
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
                let trust_id = super::super::session_auth::trust_identity(&self.state, "anonymous");
                self.auth_context = Some(super::super::session_auth::build_auth_context(&trust_id));
                self.identity = Some(trust_id);
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

        // Build a default AuthContext if not yet set (shouldn't happen but be safe).
        let default_auth_ctx;
        let auth_ctx = match self.auth_context.as_ref() {
            Some(ctx) => ctx,
            None => {
                default_auth_ctx = super::super::session_auth::build_auth_context(identity);
                &default_auth_ctx
            }
        };

        let ctx = DispatchCtx {
            state: &self.state,
            identity,
            auth_context: auth_ctx,
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
            | OpCode::HybridSearch
            | OpCode::SpatialScan
            | OpCode::TimeseriesScan
            | OpCode::TimeseriesIngest
            | OpCode::KvScan
            | OpCode::KvExpire
            | OpCode::KvPersist
            | OpCode::KvGetTtl
            | OpCode::KvBatchGet
            | OpCode::KvBatchPut
            | OpCode::KvFieldGet
            | OpCode::KvFieldSet
            | OpCode::DocumentUpdate
            | OpCode::DocumentScan
            | OpCode::DocumentUpsert
            | OpCode::DocumentBulkUpdate
            | OpCode::DocumentBulkDelete
            | OpCode::VectorInsert
            | OpCode::VectorMultiSearch
            | OpCode::VectorDelete
            | OpCode::GraphAlgo
            | OpCode::GraphMatch
            | OpCode::ColumnarScan
            | OpCode::ColumnarInsert
            | OpCode::RecursiveScan
            | OpCode::DocumentTruncate
            | OpCode::DocumentEstimateCount
            | OpCode::DocumentInsertSelect
            | OpCode::DocumentRegister
            | OpCode::DocumentDropIndex
            | OpCode::KvRegisterIndex
            | OpCode::KvDropIndex
            | OpCode::KvTruncate
            | OpCode::VectorSetParams => dispatch::handle_direct_op(&ctx, seq, op, fields).await,

            // Batch ops: direct Data Plane dispatch.
            OpCode::VectorBatchInsert | OpCode::DocumentBatchInsert => {
                dispatch::handle_direct_op(&ctx, seq, op, fields).await
            }

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
                self.auth_context = Some(super::super::session_auth::build_auth_context(&identity));
                self.identity = Some(identity);
                resp
            }
            Err(e) => NativeResponse::error(seq, "28P01", format!("{e}")),
        }
    }
}

/// Split a large `NativeResponse` into multiple encoded frames that each
/// fit within `MAX_FRAME_SIZE`. Intermediate frames use `Partial` status;
/// the last frame uses the original status.
///
/// Only responses with `rows` data are split. Error or auth responses
/// that somehow exceed the frame limit are returned as-is (best effort).
fn chunk_large_response(
    response: NativeResponse,
    format: codec::FrameFormat,
) -> crate::Result<Vec<Vec<u8>>> {
    let rows = match response.rows {
        Some(ref rows) if !rows.is_empty() => rows,
        _ => {
            // No rows to split — send as-is (shouldn't happen, but safe).
            return Ok(vec![codec::encode_response(&response, format)?]);
        }
    };

    // Estimate how many rows per chunk: target ~12 MiB per frame (75% of max)
    // to leave headroom for envelope overhead.
    let target_size = (MAX_FRAME_SIZE as usize) * 3 / 4;
    let total_rows = rows.len();

    // Estimate per-row size from a sample of the first rows.
    let sample_resp = NativeResponse {
        seq: response.seq,
        status: ResponseStatus::Ok,
        columns: response.columns.clone(),
        rows: Some(rows[..total_rows.min(100)].to_vec()),
        rows_affected: None,
        watermark_lsn: response.watermark_lsn,
        error: None,
        auth: None,
    };
    let sample_bytes = codec::encode_response(&sample_resp, format)?;
    let sample_count = total_rows.min(100);
    let per_row_estimate = if sample_count > 0 {
        sample_bytes.len() / sample_count
    } else {
        256 // fallback
    };

    let rows_per_chunk = if per_row_estimate > 0 {
        (target_size / per_row_estimate).max(1)
    } else {
        1000
    };

    let mut frames = Vec::new();
    let chunks: Vec<_> = rows.chunks(rows_per_chunk).collect();
    let last_idx = chunks.len().saturating_sub(1);

    for (i, chunk) in chunks.iter().enumerate() {
        let is_last = i == last_idx;
        let frame_resp = NativeResponse {
            seq: response.seq,
            status: if is_last {
                response.status
            } else {
                ResponseStatus::Partial
            },
            columns: if i == 0 {
                response.columns.clone()
            } else {
                None
            },
            rows: Some(chunk.to_vec()),
            rows_affected: if is_last {
                response.rows_affected
            } else {
                None
            },
            watermark_lsn: response.watermark_lsn,
            error: if is_last {
                response.error.clone()
            } else {
                None
            },
            auth: None,
        };
        frames.push(codec::encode_response(&frame_resp, format)?);
    }

    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;

    #[test]
    fn chunk_large_response_splits_rows() {
        // Build a response with 100 rows, each ~200 bytes when serialized.
        let columns = vec!["id".to_string(), "data".to_string()];
        let rows: Vec<Vec<Value>> = (0..100)
            .map(|i| {
                vec![
                    Value::Integer(i),
                    Value::String(format!("row-data-{i}-padding-{}", "x".repeat(150))),
                ]
            })
            .collect();

        let response = NativeResponse {
            seq: 1,
            status: ResponseStatus::Ok,
            columns: Some(columns),
            rows: Some(rows),
            rows_affected: None,
            watermark_lsn: 42,
            error: None,
            auth: None,
        };

        let frames = chunk_large_response(response, codec::FrameFormat::MessagePack).unwrap();

        // With 100 rows of ~200 bytes each (~20KB total), this should fit in
        // one frame (MAX_FRAME_SIZE = 16MB). Test with a scenario that forces splitting.
        assert!(!frames.is_empty());

        // Decode each frame and verify structure.
        for (i, frame) in frames.iter().enumerate() {
            let resp: NativeResponse = rmp_serde::from_slice(frame).unwrap();
            assert!(resp.rows.is_some());
            if i < frames.len() - 1 {
                assert_eq!(resp.status, ResponseStatus::Partial);
            } else {
                assert_eq!(resp.status, ResponseStatus::Ok);
            }
        }
    }

    #[test]
    fn chunk_large_response_no_rows_passthrough() {
        let response = NativeResponse {
            seq: 1,
            status: ResponseStatus::Ok,
            columns: None,
            rows: None,
            rows_affected: Some(5),
            watermark_lsn: 42,
            error: None,
            auth: None,
        };

        let frames = chunk_large_response(response, codec::FrameFormat::MessagePack).unwrap();
        assert_eq!(
            frames.len(),
            1,
            "no-rows response should pass through as-is"
        );
    }

    #[test]
    fn chunk_large_response_preserves_all_rows() {
        // Create a response that's guaranteed to exceed MAX_FRAME_SIZE.
        // Each row ~200 bytes * 100K rows = ~20MB > 16MB limit.
        let columns = vec!["id".to_string(), "value".to_string()];
        let row_count = 100_000;
        let rows: Vec<Vec<Value>> = (0..row_count)
            .map(|i| {
                vec![
                    Value::Integer(i),
                    Value::String(format!("v{i}-{}", "p".repeat(150))),
                ]
            })
            .collect();

        let response = NativeResponse {
            seq: 42,
            status: ResponseStatus::Ok,
            columns: Some(columns.clone()),
            rows: Some(rows),
            rows_affected: None,
            watermark_lsn: 99,
            error: None,
            auth: None,
        };

        let frames = chunk_large_response(response, codec::FrameFormat::MessagePack).unwrap();
        assert!(frames.len() > 1, "should produce multiple frames");

        // Reassemble all rows from frames (simulating client behavior).
        let mut total_rows: Vec<Vec<Value>> = Vec::new();
        for frame in &frames {
            let resp: NativeResponse = rmp_serde::from_slice(frame).unwrap();
            if let Some(rows) = resp.rows {
                total_rows.extend(rows);
            }
        }
        assert_eq!(total_rows.len(), row_count as usize);

        // First frame should have columns.
        let first: NativeResponse = rmp_serde::from_slice(&frames[0]).unwrap();
        assert_eq!(first.columns, Some(columns));
        assert_eq!(first.status, ResponseStatus::Partial);

        // Last frame should have Ok status.
        let last: NativeResponse = rmp_serde::from_slice(frames.last().unwrap()).unwrap();
        assert_eq!(last.status, ResponseStatus::Ok);

        // Each frame should be <= MAX_FRAME_SIZE.
        for frame in &frames {
            assert!(
                frame.len() <= MAX_FRAME_SIZE as usize,
                "frame size {} exceeds MAX_FRAME_SIZE {}",
                frame.len(),
                MAX_FRAME_SIZE
            );
        }
    }
}
