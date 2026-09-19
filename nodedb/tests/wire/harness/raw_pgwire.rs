// SPDX-License-Identifier: BUSL-1.1

//! Raw PostgreSQL v3 wire protocol connection, for tests that need bytes
//! the `tokio_postgres` client does not expose (command tag words, column
//! type OIDs) rather than its decoded `SimpleQueryMessage`s.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// PostgreSQL protocol version 3.0, sent in every `StartupMessage`.
const PROTOCOL_V3: i32 = 196_608; // 0x0003_0000

/// A raw simple-query pgwire connection: startup handshake done,
/// `ReadyForQuery` consumed.
pub struct RawPgConn {
    stream: TcpStream,
}

impl RawPgConn {
    /// Connect, complete a trust-mode `StartupMessage` for `user`/`database`,
    /// and drain replies through the first `ReadyForQuery`.
    pub async fn connect(port: u16, user: &str, database: &str) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect to pgwire port");

        let mut params = Vec::new();
        params.extend_from_slice(b"user\0");
        params.extend_from_slice(user.as_bytes());
        params.push(0);
        params.extend_from_slice(b"database\0");
        params.extend_from_slice(database.as_bytes());
        params.push(0);
        params.push(0); // parameter-list terminator

        let total = 4 + 4 + params.len();
        let mut startup = Vec::new();
        startup.extend_from_slice(&(total as i32).to_be_bytes());
        startup.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
        startup.extend_from_slice(&params);

        let mut conn = Self { stream };
        conn.stream.write_all(&startup).await.expect("send startup");

        // Trust mode sends AuthenticationOk, ParameterStatus*, and
        // BackendKeyData before the first ReadyForQuery.
        loop {
            let (tag, body) = conn.read_message().await;
            match tag {
                b'Z' => break,
                b'E' => panic!("startup error: {}", String::from_utf8_lossy(&body)),
                _ => {}
            }
        }
        conn
    }

    /// Read exactly one backend message: a 1-byte type tag followed by an
    /// i32 length (which counts itself but not the tag). Returns
    /// `(tag, body)` where `body` excludes the 4-byte length prefix.
    pub async fn read_message(&mut self) -> (u8, Vec<u8>) {
        let mut tag = [0u8; 1];
        self.stream
            .read_exact(&mut tag)
            .await
            .expect("read message tag");
        let mut len_buf = [0u8; 4];
        self.stream
            .read_exact(&mut len_buf)
            .await
            .expect("read message length");
        let len = i32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; len - 4];
        self.stream
            .read_exact(&mut body)
            .await
            .expect("read message body");
        (tag[0], body)
    }

    /// Send a Simple Query (`Q`) and read every reply up to (not including)
    /// the terminating `ReadyForQuery`. A backend `ErrorResponse` panics —
    /// callers that expect the query to fail must not route it through
    /// here.
    pub async fn simple_query(&mut self, sql: &str) -> Vec<(u8, Vec<u8>)> {
        let mut qbody = sql.as_bytes().to_vec();
        qbody.push(0);
        let mut qmsg = vec![b'Q'];
        qmsg.extend_from_slice(&((4 + qbody.len()) as i32).to_be_bytes());
        qmsg.extend_from_slice(&qbody);
        self.stream.write_all(&qmsg).await.expect("send query");

        let mut messages = Vec::new();
        loop {
            let (tag, body) = self.read_message().await;
            match tag {
                b'Z' => break,
                b'E' => panic!("query error: {}", String::from_utf8_lossy(&body)),
                _ => messages.push((tag, body)),
            }
        }
        messages
    }
}

/// `CommandComplete` (`C`) tag strings in `messages`, in wire order.
pub fn command_tags(messages: &[(u8, Vec<u8>)]) -> Vec<String> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'C')
        .map(|(_, body)| {
            let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
            String::from_utf8_lossy(&body[..end]).into_owned()
        })
        .collect()
}
