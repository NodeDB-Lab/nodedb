// SPDX-License-Identifier: Apache-2.0

//! Single TCP connection to a NodeDB server over the native binary protocol.
//!
//! Handles MessagePack framing, request/response correlation via sequence
//! numbers, authentication, and optional TLS encryption.

mod stream;
mod tls;

use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{
    AuthMethod, CAP_COLUMNAR, CAP_CRDT, CAP_FTS, CAP_GRAPHRAG, CAP_SPATIAL, CAP_STREAMING,
    CAP_TIMESERIES, ErrorPayload, FRAME_HEADER_LEN, HelloAckFrame, HelloFrame, Limits,
    MAX_FRAME_SIZE, NativeRequest, NativeResponse, OpCode, PROTO_VERSION, RequestFields,
    ResponseStatus, TextFields,
};
use nodedb_types::result::QueryResult;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use stream::ConnStream;
pub use tls::TlsConfig;
use tls::build_tls_client_config;

/// A single connection to a NodeDB server using the native binary protocol.
pub struct NativeConnection {
    stream: ConnStream,
    seq: AtomicU64,
    authenticated: bool,
    /// Protocol version negotiated during the handshake (0 = handshake not performed).
    pub proto_version: u16,
    /// Capability bits advertised by the server in `HelloAckFrame`.
    pub capabilities: u64,
    /// Human-readable server version string from `HelloAckFrame`.
    pub server_version: String,
    /// Per-operation limits from `HelloAckFrame`.
    pub limits: Limits,
}

impl NativeConnection {
    /// Connect to a NodeDB server at the given address (plain TCP).
    pub async fn connect(addr: &str) -> NodeDbResult<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| NodeDbError::sync_connection_failed(format!("connect to {addr}: {e}")))?;
        let mut conn = Self {
            stream: ConnStream::Plain(stream),
            seq: AtomicU64::new(1),
            authenticated: false,
            proto_version: 0,
            capabilities: 0,
            server_version: String::new(),
            limits: Limits::default(),
        };
        conn.perform_client_handshake().await?;
        Ok(conn)
    }

    /// Connect to a NodeDB server with TLS.
    pub async fn connect_tls(addr: &str, tls: &TlsConfig) -> NodeDbResult<Self> {
        let tcp = TcpStream::connect(addr)
            .await
            .map_err(|e| NodeDbError::sync_connection_failed(format!("connect to {addr}: {e}")))?;

        let config = build_tls_client_config(tls)?;
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));

        let server_name = tls
            .server_name
            .as_deref()
            .unwrap_or_else(|| addr.split(':').next().unwrap_or("localhost"));

        let sni = tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| {
                NodeDbError::sync_connection_failed(format!(
                    "invalid server name '{server_name}': {e}"
                ))
            })?;

        let tls_stream = connector.connect(sni, tcp).await.map_err(|e| {
            NodeDbError::sync_connection_failed(format!("TLS handshake failed: {e}"))
        })?;

        let mut conn = Self {
            stream: ConnStream::Tls(Box::new(tls_stream)),
            seq: AtomicU64::new(1),
            authenticated: false,
            proto_version: 0,
            capabilities: 0,
            server_version: String::new(),
            limits: Limits::default(),
        };
        conn.perform_client_handshake().await?;
        Ok(conn)
    }

    /// Perform the native protocol handshake.
    pub async fn perform_client_handshake(&mut self) -> NodeDbResult<()> {
        let client_caps = CAP_STREAMING
            | CAP_GRAPHRAG
            | CAP_FTS
            | CAP_CRDT
            | CAP_SPATIAL
            | CAP_TIMESERIES
            | CAP_COLUMNAR;

        let hello = HelloFrame {
            proto_min: 1,
            proto_max: PROTO_VERSION,
            capabilities: client_caps,
        };

        let payload = hello.encode();
        self.stream.write_all(&payload).await.map_err(io_err)?;
        self.stream.flush().await.map_err(io_err)?;

        let mut magic_buf = [0u8; 4];
        self.stream
            .read_exact(&mut magic_buf)
            .await
            .map_err(io_err)?;

        let magic = u32::from_be_bytes(magic_buf);

        if magic == nodedb_types::protocol::HELLO_ERROR_MAGIC_U32 {
            let mut header = [0u8; 2];
            self.stream.read_exact(&mut header).await.map_err(io_err)?;
            let msg_len = header[1] as usize;
            let mut msg_bytes = vec![0u8; msg_len];
            self.stream
                .read_exact(&mut msg_bytes)
                .await
                .map_err(io_err)?;

            let code = match header[0] {
                0 => nodedb_types::protocol::HelloErrorCode::BadMagic,
                1 => nodedb_types::protocol::HelloErrorCode::VersionMismatch,
                _ => nodedb_types::protocol::HelloErrorCode::Malformed,
            };
            let message = String::from_utf8_lossy(&msg_bytes).into_owned();
            return Err(NodeDbError::handshake_failed(code, message));
        }

        if magic != nodedb_types::protocol::HELLO_ACK_MAGIC {
            return Err(NodeDbError::internal(format!(
                "HelloAck magic mismatch: expected {:#010x}, got {:#010x}",
                nodedb_types::protocol::HELLO_ACK_MAGIC,
                magic,
            )));
        }

        let mut fixed_rest = [0u8; 11];
        self.stream
            .read_exact(&mut fixed_rest)
            .await
            .map_err(io_err)?;
        let sv_len = fixed_rest[10] as usize;
        let var_len = sv_len + 1 + 7 * 5;
        let mut var_buf = vec![0u8; var_len];
        self.stream.read_exact(&mut var_buf).await.map_err(io_err)?;

        let mut ack_buf = Vec::with_capacity(4 + 11 + var_len);
        ack_buf.extend_from_slice(&magic_buf);
        ack_buf.extend_from_slice(&fixed_rest);
        ack_buf.extend_from_slice(&var_buf);

        let ack = HelloAckFrame::decode(&ack_buf)
            .ok_or_else(|| NodeDbError::internal("failed to decode HelloAckFrame from server"))?;

        self.proto_version = ack.proto_version;
        self.capabilities = ack.capabilities;
        self.server_version = ack.server_version;
        self.limits = ack.limits;

        Ok(())
    }

    /// Authenticate with the server.
    ///
    /// `database` — optional target database name. When set it is sent in
    /// the auth frame so the server can bind the connection's database
    /// context at handshake time (equivalent to `psql -d <name>`).
    pub async fn authenticate(
        &mut self,
        method: AuthMethod,
        database: Option<&str>,
    ) -> NodeDbResult<()> {
        let resp = self
            .send(
                OpCode::Auth,
                TextFields {
                    auth: Some(method),
                    database: database.map(|s| s.to_string()),
                    ..Default::default()
                },
            )
            .await?;

        if resp.status == ResponseStatus::Error {
            let msg = resp
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "auth failed".into());
            return Err(NodeDbError::authorization_denied(msg));
        }

        self.authenticated = true;
        Ok(())
    }

    /// Send a ping and await the pong.
    pub async fn ping(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Ping, TextFields::default()).await?;
        if resp.status == ResponseStatus::Error {
            return Err(NodeDbError::internal("ping failed"));
        }
        Ok(())
    }

    /// Whether this connection has been authenticated.
    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    /// Execute a SQL query and return the result.
    pub async fn execute_sql(&mut self, sql: &str) -> NodeDbResult<QueryResult> {
        let resp = self
            .send(
                OpCode::Sql,
                TextFields {
                    sql: Some(sql.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        response_to_query_result(resp)
    }

    /// Execute a SQL query with bound parameters and return the result.
    ///
    /// Bound parameters travel through `TextFields::sql_params` as a
    /// `Vec<Value>` — zerompk's generic array encoding serialises each
    /// element via `Value`'s hand-rolled `ToMessagePack` impl so the
    /// canonical scalar variants round-trip without a lossy JSON step.
    /// The server inlines each value as a SQL literal before planning,
    /// so `$1`, `$2`, … placeholders resolve to the caller's values.
    /// Empty `params` routes through the same opcode but omits the
    /// field, equivalent to `execute_sql`.
    pub async fn execute_sql_with_params(
        &mut self,
        sql: &str,
        params: &[nodedb_types::Value],
    ) -> NodeDbResult<QueryResult> {
        let sql_params = if params.is_empty() {
            None
        } else {
            Some(params.to_vec())
        };
        let resp = self
            .send(
                OpCode::Sql,
                TextFields {
                    sql: Some(sql.to_string()),
                    sql_params,
                    ..Default::default()
                },
            )
            .await?;
        response_to_query_result(resp)
    }

    /// Execute a DDL command.
    pub async fn execute_ddl(&mut self, sql: &str) -> NodeDbResult<QueryResult> {
        let resp = self
            .send(
                OpCode::Ddl,
                TextFields {
                    sql: Some(sql.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        response_to_query_result(resp)
    }

    /// Begin a transaction.
    pub async fn begin(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Begin, TextFields::default()).await?;
        check_error(resp)
    }

    /// Commit the current transaction.
    pub async fn commit(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Commit, TextFields::default()).await?;
        check_error(resp)
    }

    /// Rollback the current transaction.
    pub async fn rollback(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Rollback, TextFields::default()).await?;
        check_error(resp)
    }

    /// Set a session parameter.
    pub async fn set_parameter(&mut self, key: &str, value: &str) -> NodeDbResult<()> {
        let resp = self
            .send(
                OpCode::Set,
                TextFields {
                    key: Some(key.to_string()),
                    value: Some(value.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        check_error(resp)
    }

    /// Show a session parameter.
    pub async fn show_parameter(&mut self, key: &str) -> NodeDbResult<String> {
        let resp = self
            .send(
                OpCode::Show,
                TextFields {
                    key: Some(key.to_string()),
                    ..Default::default()
                },
            )
            .await?;
        if resp.status == ResponseStatus::Error {
            let msg = resp
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "show failed".into());
            return Err(NodeDbError::internal(msg));
        }
        let value = resp
            .rows
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.into_iter().next())
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        Ok(value)
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a request and read the response.
    pub(crate) async fn send(
        &mut self,
        op: OpCode,
        fields: TextFields,
    ) -> NodeDbResult<NativeResponse> {
        let req_seq = self.next_seq();
        let req = NativeRequest {
            op,
            seq: req_seq,
            fields: RequestFields::Text(fields),
        };

        let payload = zerompk::to_msgpack_vec(&req)
            .map_err(|e| NodeDbError::serialization("msgpack", format!("request encode: {e}")))?;

        let len = payload.len() as u32;
        self.stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(io_err)?;
        self.stream.write_all(&payload).await.map_err(io_err)?;
        self.stream.flush().await.map_err(io_err)?;

        let mut combined_rows: Vec<Vec<nodedb_types::Value>> = Vec::new();
        let mut partial_columns: Option<Vec<String>> = None;

        let final_resp = loop {
            let mut len_buf = [0u8; FRAME_HEADER_LEN];
            self.stream.read_exact(&mut len_buf).await.map_err(io_err)?;
            let resp_len = u32::from_be_bytes(len_buf);
            if resp_len > MAX_FRAME_SIZE {
                return Err(NodeDbError::internal(format!(
                    "response frame too large: {resp_len}"
                )));
            }

            let mut resp_buf = vec![0u8; resp_len as usize];
            self.stream
                .read_exact(&mut resp_buf)
                .await
                .map_err(io_err)?;

            let resp: NativeResponse = zerompk::from_msgpack(&resp_buf).map_err(|e| {
                NodeDbError::serialization("msgpack", format!("response decode: {e}"))
            })?;

            if resp.seq != req_seq {
                // A fan-out query for a preceding request can leave stale
                // trailing frames on the wire after that request's terminal
                // frame was already returned to its caller. Discard any
                // frame that doesn't belong to this request rather than
                // misattributing it — never surface another request's rows
                // or status as this request's response.
                tracing::warn!(
                    expected_seq = req_seq,
                    got_seq = resp.seq,
                    "native connection: discarding stale response frame"
                );
                continue;
            }

            if resp.status == ResponseStatus::Partial {
                if partial_columns.is_none() {
                    partial_columns = resp.columns;
                }
                if let Some(rows) = resp.rows {
                    combined_rows.extend(rows);
                }
                continue;
            }

            // The terminal frame owns status and all terminal metadata. In
            // particular, never turn a stream error into success merely
            // because earlier partial rows were received.
            if resp.status == ResponseStatus::Error {
                break resp;
            }
            let mut terminal = resp;
            if let Some(rows) = terminal.rows.take() {
                combined_rows.extend(rows);
            }
            if !combined_rows.is_empty() {
                terminal.rows = Some(combined_rows);
            }
            if terminal.columns.is_none() {
                terminal.columns = partial_columns;
            }
            break terminal;
        };

        Ok(final_resp)
    }
}

fn io_err(e: std::io::Error) -> NodeDbError {
    NodeDbError::sync_connection_failed(format!("I/O: {e}"))
}

/// Rebuild the typed error a server error frame describes.
///
/// The frame carries the server's own SQLSTATE alongside the message; reading
/// only the message would discard the one machine-matchable fact in it and
/// leave every server-side failure indistinguishable from an internal fault.
/// A frame with no payload at all has no classification to recover, so it
/// falls back to `fallback` as an internal error.
fn error_from_frame(error: Option<ErrorPayload>, fallback: &str) -> NodeDbError {
    error.map_or_else(
        || NodeDbError::internal(fallback),
        |e| NodeDbError::from_sqlstate(&e.code, &e.message),
    )
}

fn check_error(resp: NativeResponse) -> NodeDbResult<()> {
    if resp.status == ResponseStatus::Error {
        return Err(error_from_frame(resp.error, "unknown error"));
    }
    Ok(())
}

fn response_to_query_result(resp: NativeResponse) -> NodeDbResult<QueryResult> {
    if resp.status == ResponseStatus::Error {
        return Err(error_from_frame(resp.error, "query failed"));
    }
    Ok(QueryResult {
        columns: resp.columns.unwrap_or_default(),
        rows: resp.rows.unwrap_or_default(),
        rows_affected: resp.rows_affected.unwrap_or(0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::{
        CAP_MSGPACK, CAP_STREAMING, HELLO_ACK_MAGIC, HELLO_MAGIC, HelloAckFrame,
    };

    #[test]
    fn response_to_query_result_ok() {
        let resp = NativeResponse::from_query_result(
            1,
            QueryResult {
                columns: vec!["x".into()],
                rows: vec![vec![nodedb_types::Value::Integer(42)]],
                rows_affected: 0,
            },
            0,
        );
        let qr = response_to_query_result(resp).unwrap();
        assert_eq!(qr.columns, vec!["x"]);
        assert_eq!(qr.rows[0][0].as_i64(), Some(42));
    }

    /// An error frame must surface the server's classification, not just its
    /// prose: asserting only on the message is what let the SQLSTATE be
    /// dropped here unnoticed.
    #[test]
    fn response_to_query_result_error() {
        let resp = NativeResponse::error(1, "42P01", "not found");
        let err = response_to_query_result(resp).unwrap_err();

        assert!(format!("{err}").contains("not found"));
        assert_eq!(
            err.code(),
            nodedb_types::error::ErrorCode::COLLECTION_NOT_FOUND
        );
        assert!(err.is_not_found());
        assert!(!err.is_internal());
    }

    /// The same reconstruction happens on the non-query path, which has its
    /// own copy of the error branch.
    #[test]
    fn check_error_reconstructs_sqlstate() {
        let resp = NativeResponse::error(1, "42501", "authorization denied on t");
        let err = check_error(resp).unwrap_err();

        assert!(err.is_auth_denied());
        assert_eq!(err.message(), "authorization denied on t");
    }

    /// Retriability is the behavioural consequence of the classification: a
    /// serialization failure the client folds to `Internal` reads as
    /// permanent, so a retrying caller gives up on a transaction that would
    /// have succeeded.
    #[test]
    fn serialization_failure_frame_is_retriable() {
        let resp = NativeResponse::error(1, "40001", "transaction aborted");
        let err = response_to_query_result(resp).unwrap_err();

        assert!(err.is_retriable());
    }

    /// Non-regression: a SQLSTATE with no mapped variant behaves exactly as
    /// every server error did before the mapping existed.
    #[test]
    fn unmapped_sqlstate_frame_stays_internal() {
        let resp = NativeResponse::error(1, "XX000", "boom");
        let err = response_to_query_result(resp).unwrap_err();

        assert!(err.is_internal());
        assert!(format!("{err}").contains("boom"));
    }

    /// An error frame with no payload has no SQLSTATE to recover, so it keeps
    /// the call site's own fallback text.
    #[test]
    fn error_frame_without_payload_uses_fallback() {
        let mut resp = NativeResponse::error(1, "42P01", "not found");
        resp.error = None;
        let err = response_to_query_result(resp).unwrap_err();

        assert!(err.is_internal());
        assert!(format!("{err}").contains("query failed"));
    }

    #[test]
    fn check_error_ok() {
        let resp = NativeResponse::ok(1);
        assert!(check_error(resp).is_ok());
    }

    #[test]
    fn check_error_fail() {
        let resp = NativeResponse::error(1, "XX000", "boom");
        assert!(check_error(resp).is_err());
    }

    #[tokio::test]
    async fn client_handshake_succeeds_when_versions_match() {
        use nodedb_types::protocol::HelloAckFrame;
        use tokio::io::{AsyncWriteExt, duplex};

        let (mut server_half, mut client_half) = duplex(4096);

        let server_task = tokio::spawn(async move {
            let mut hello_buf = [0u8; HelloFrame::WIRE_SIZE];
            tokio::io::AsyncReadExt::read_exact(&mut server_half, &mut hello_buf)
                .await
                .unwrap();
            let magic =
                u32::from_be_bytes([hello_buf[0], hello_buf[1], hello_buf[2], hello_buf[3]]);
            assert_eq!(magic, HELLO_MAGIC, "client sent correct HelloFrame magic");

            let ack = HelloAckFrame {
                proto_version: 1,
                capabilities: CAP_STREAMING | CAP_MSGPACK,
                server_version: "NodeDB/test".into(),
                limits: Limits::default(),
            };
            server_half.write_all(&ack.encode()).await.unwrap();
            server_half.flush().await.unwrap();
        });

        let result = handshake_on_duplex(&mut client_half).await;
        server_task.await.unwrap();

        assert!(result.is_ok(), "expected Ok, got {result:?}");
        let (proto_version, server_version) = result.unwrap();
        assert_eq!(proto_version, 1);
        assert!(server_version.contains("NodeDB"));
    }

    #[tokio::test]
    async fn client_handshake_returns_typed_error_on_version_mismatch() {
        use nodedb_types::protocol::{HelloErrorCode, HelloErrorFrame};
        use tokio::io::{AsyncWriteExt, duplex};

        let (mut server_half, mut client_half) = duplex(4096);

        let server_task = tokio::spawn(async move {
            let mut hello_buf = [0u8; HelloFrame::WIRE_SIZE];
            tokio::io::AsyncReadExt::read_exact(&mut server_half, &mut hello_buf)
                .await
                .unwrap();
            let err_frame = HelloErrorFrame {
                code: HelloErrorCode::VersionMismatch,
                message: "version mismatch".into(),
            };
            server_half.write_all(&err_frame.encode()).await.unwrap();
            server_half.flush().await.unwrap();
        });

        let result = handshake_on_duplex(&mut client_half).await;
        server_task.await.unwrap();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            format!("{err}").contains("version mismatch") || format!("{err}").contains("handshake")
        );
    }

    /// Drive the client-side handshake on a raw `AsyncRead + AsyncWrite` stream (for testing).
    async fn handshake_on_duplex<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
        stream: &mut S,
    ) -> NodeDbResult<(u16, String)> {
        use tokio::io::AsyncWriteExt;

        let client_caps = CAP_STREAMING | CAP_MSGPACK;
        let hello = HelloFrame {
            proto_min: 1,
            proto_max: PROTO_VERSION,
            capabilities: client_caps,
        };

        let payload = hello.encode();
        stream.write_all(&payload).await.map_err(io_err)?;
        stream.flush().await.map_err(io_err)?;

        let mut magic_buf = [0u8; 4];
        stream.read_exact(&mut magic_buf).await.map_err(io_err)?;

        let magic = u32::from_be_bytes(magic_buf);

        if magic == nodedb_types::protocol::HELLO_ERROR_MAGIC_U32 {
            let mut header = [0u8; 2];
            stream.read_exact(&mut header).await.map_err(io_err)?;
            let msg_len = header[1] as usize;
            let mut msg_bytes = vec![0u8; msg_len];
            stream.read_exact(&mut msg_bytes).await.map_err(io_err)?;
            let code = match header[0] {
                0 => nodedb_types::protocol::HelloErrorCode::BadMagic,
                1 => nodedb_types::protocol::HelloErrorCode::VersionMismatch,
                _ => nodedb_types::protocol::HelloErrorCode::Malformed,
            };
            let message = String::from_utf8_lossy(&msg_bytes).into_owned();
            return Err(NodeDbError::handshake_failed(code, message));
        }

        if magic != HELLO_ACK_MAGIC {
            return Err(NodeDbError::internal(format!(
                "HelloAck magic mismatch: {magic:#010x}"
            )));
        }

        let mut fixed_rest = [0u8; 11];
        stream.read_exact(&mut fixed_rest).await.map_err(io_err)?;
        let sv_len = fixed_rest[10] as usize;
        let var_len = sv_len + 1 + 7 * 5;
        let mut var_buf = vec![0u8; var_len];
        stream.read_exact(&mut var_buf).await.map_err(io_err)?;

        let mut ack_buf = Vec::with_capacity(4 + 11 + var_len);
        ack_buf.extend_from_slice(&magic_buf);
        ack_buf.extend_from_slice(&fixed_rest);
        ack_buf.extend_from_slice(&var_buf);

        let ack = HelloAckFrame::decode(&ack_buf)
            .ok_or_else(|| NodeDbError::internal("failed to decode HelloAckFrame"))?;
        Ok((ack.proto_version, ack.server_version))
    }
}
