//! Single TCP connection to a NodeDB server over the native binary protocol.
//!
//! Handles MessagePack framing, request/response correlation via sequence
//! numbers, and authentication.

use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{
    AuthMethod, FRAME_HEADER_LEN, MAX_FRAME_SIZE, NativeRequest, NativeResponse, OpCode,
    RequestFields, ResponseStatus, TextFields,
};
use nodedb_types::result::QueryResult;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A single connection to a NodeDB server using the native binary protocol.
pub struct NativeConnection {
    stream: TcpStream,
    seq: AtomicU64,
    authenticated: bool,
}

impl NativeConnection {
    /// Connect to a NodeDB server at the given address.
    pub async fn connect(addr: &str) -> NodeDbResult<Self> {
        let stream =
            TcpStream::connect(addr)
                .await
                .map_err(|e| NodeDbError::SyncConnectionFailed {
                    detail: format!("connect to {addr}: {e}"),
                })?;
        Ok(Self {
            stream,
            seq: AtomicU64::new(1),
            authenticated: false,
        })
    }

    /// Authenticate with the server.
    pub async fn authenticate(&mut self, method: AuthMethod) -> NodeDbResult<()> {
        let resp = self
            .send(
                OpCode::Auth,
                TextFields {
                    auth: Some(method),
                    ..Default::default()
                },
            )
            .await?;

        if resp.status == ResponseStatus::Error {
            let msg = resp
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "auth failed".into());
            return Err(NodeDbError::AuthorizationDenied { resource: msg });
        }

        self.authenticated = true;
        Ok(())
    }

    /// Send a ping and await the pong.
    pub async fn ping(&mut self) -> NodeDbResult<()> {
        let resp = self.send(OpCode::Ping, TextFields::default()).await?;
        if resp.status == ResponseStatus::Error {
            return Err(NodeDbError::Internal {
                detail: "ping failed".into(),
            });
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
            return Err(NodeDbError::Internal { detail: msg });
        }
        let value = resp
            .rows
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.into_iter().next())
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        Ok(value)
    }

    // ─── Low-level transport ────────────────────────────────────

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a request and read the response.
    pub(crate) async fn send(
        &mut self,
        op: OpCode,
        fields: TextFields,
    ) -> NodeDbResult<NativeResponse> {
        let req = NativeRequest {
            op,
            seq: self.next_seq(),
            fields: RequestFields::Text(fields),
        };

        // Encode request as MessagePack.
        let payload = rmp_serde::to_vec_named(&req).map_err(|e| NodeDbError::Serialization {
            format: "msgpack".into(),
            detail: format!("request encode: {e}"),
        })?;

        // Write length-prefixed frame.
        let len = payload.len() as u32;
        self.stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(io_err)?;
        self.stream.write_all(&payload).await.map_err(io_err)?;
        self.stream.flush().await.map_err(io_err)?;

        // Read response frame.
        let mut len_buf = [0u8; FRAME_HEADER_LEN];
        self.stream.read_exact(&mut len_buf).await.map_err(io_err)?;
        let resp_len = u32::from_be_bytes(len_buf);
        if resp_len > MAX_FRAME_SIZE {
            return Err(NodeDbError::Internal {
                detail: format!("response frame too large: {resp_len}"),
            });
        }

        let mut resp_buf = vec![0u8; resp_len as usize];
        self.stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(io_err)?;

        // Decode response.
        rmp_serde::from_slice(&resp_buf).map_err(|e| NodeDbError::Serialization {
            format: "msgpack".into(),
            detail: format!("response decode: {e}"),
        })
    }
}

fn io_err(e: std::io::Error) -> NodeDbError {
    NodeDbError::SyncConnectionFailed {
        detail: format!("I/O: {e}"),
    }
}

fn check_error(resp: NativeResponse) -> NodeDbResult<()> {
    if resp.status == ResponseStatus::Error {
        let msg = resp
            .error
            .map(|e| e.message)
            .unwrap_or_else(|| "unknown error".into());
        return Err(NodeDbError::Internal { detail: msg });
    }
    Ok(())
}

fn response_to_query_result(resp: NativeResponse) -> NodeDbResult<QueryResult> {
    if resp.status == ResponseStatus::Error {
        let msg = resp
            .error
            .map(|e| e.message)
            .unwrap_or_else(|| "query failed".into());
        return Err(NodeDbError::Internal { detail: msg });
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

    #[test]
    fn response_to_query_result_error() {
        let resp = NativeResponse::error(1, "42P01", "not found");
        let err = response_to_query_result(resp).unwrap_err();
        assert!(format!("{err}").contains("not found"));
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
}
