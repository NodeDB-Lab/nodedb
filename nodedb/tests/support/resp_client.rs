// SPDX-License-Identifier: BUSL-1.1

//! Minimal RESP (Redis protocol) client for out-of-process tests.
//!
//! Deliberately the same encode/parse approach as
//! `tests/resp_row_level_security.rs`'s in-process RESP client: a raw
//! `TcpStream`, array-of-bulk-strings command encoding, and a reply parser
//! covering the five RESP2 type prefixes those tests already exercise. This
//! copy talks to a real spawned `nodedb` binary's RESP port instead of an
//! in-process listener, so the crash and wire harnesses can drive `AUTH` /
//! `SELECT` / data commands the same way a real Redis client would.

#![allow(dead_code)] // Not every test binary exercises every reply variant.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A RESP reply, decoded far enough for these tests to assert on it.
#[derive(Debug, PartialEq, Eq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<String>),
    Array(Vec<Reply>),
}

impl Reply {
    pub fn is_error(&self) -> bool {
        matches!(self, Reply::Error(_))
    }
}

pub struct RespClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl RespClient {
    pub async fn connect(addr: std::net::SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("RESP connect");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    /// Send a command as a RESP array and return the decoded reply.
    pub async fn cmd(&mut self, args: &[&str]) -> Reply {
        let mut out = format!("*{}\r\n", args.len());
        for a in args {
            out.push_str(&format!("${}\r\n{a}\r\n", a.len()));
        }
        self.stream
            .write_all(out.as_bytes())
            .await
            .expect("RESP write");
        self.read_reply().await
    }

    async fn read_reply(&mut self) -> Reply {
        loop {
            if let Some((reply, consumed)) = parse(&self.buf) {
                self.buf.drain(..consumed);
                return reply;
            }
            let mut chunk = vec![0u8; 8192];
            let n = tokio::time::timeout(Duration::from_secs(10), self.stream.read(&mut chunk))
                .await
                .expect("RESP read timed out")
                .expect("RESP read");
            assert!(n > 0, "RESP connection closed mid-reply");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

/// Authenticate as `user` and select `collection` on a fresh RESP connection.
pub async fn session(
    addr: std::net::SocketAddr,
    user: &str,
    password: &str,
    collection: &str,
) -> RespClient {
    let mut client = RespClient::connect(addr).await;
    let auth = client.cmd(&["AUTH", user, password]).await;
    assert!(!auth.is_error(), "RESP AUTH as {user} failed: {auth:?}");
    let select = client.cmd(&["SELECT", collection]).await;
    assert!(!select.is_error(), "SELECT {collection} failed: {select:?}");
    client
}

/// Parse one reply from `buf`, returning it with the number of bytes consumed.
/// `None` means the buffer holds an incomplete reply.
fn parse(buf: &[u8]) -> Option<(Reply, usize)> {
    let line_end = buf.windows(2).position(|w| w == b"\r\n")?;
    let line = std::str::from_utf8(&buf[1..line_end]).ok()?.to_string();
    let head = *buf.first()?;
    let after_line = line_end + 2;
    match head {
        b'+' => Some((Reply::Simple(line), after_line)),
        b'-' => Some((Reply::Error(line), after_line)),
        b':' => Some((Reply::Integer(line.parse().ok()?), after_line)),
        b'$' => {
            let len: i64 = line.parse().ok()?;
            if len < 0 {
                return Some((Reply::Bulk(None), after_line));
            }
            let len = len as usize;
            if buf.len() < after_line + len + 2 {
                return None;
            }
            let body = String::from_utf8_lossy(&buf[after_line..after_line + len]).into_owned();
            Some((Reply::Bulk(Some(body)), after_line + len + 2))
        }
        b'*' => {
            let count: i64 = line.parse().ok()?;
            if count < 0 {
                return Some((Reply::Array(Vec::new()), after_line));
            }
            let mut items = Vec::new();
            let mut offset = after_line;
            for _ in 0..count {
                let (item, used) = parse(&buf[offset..])?;
                items.push(item);
                offset += used;
            }
            Some((Reply::Array(items), offset))
        }
        _ => None,
    }
}
