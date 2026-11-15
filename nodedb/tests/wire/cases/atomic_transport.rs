// SPDX-License-Identifier: BUSL-1.1

//! Four ways to send one request of several statements, for the cells that
//! check the same request on every transport.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};

use super::wire_support::{post, ws_frame};
use crate::harness::TestServer;

/// A transport that carries a request of several statements.
#[derive(Clone, Copy, Debug)]
pub(super) enum Transport {
    Http,
    Ndjson,
    Ws,
    Native,
}

/// The answer to one request.
pub(super) struct Sent {
    /// Whether the transport answered an error.
    pub(super) errored: bool,
    /// The answer as text: the body, the frame or the serialized response.
    pub(super) text: String,
}

impl Transport {
    /// [`Self::send`] as a status and a body, the shape `TestServer::http_query`
    /// answers: 200 when the request succeeds, 400 when it errors. HTTP keeps
    /// its own status.
    pub(super) async fn query(self, server: &TestServer, sql: &str) -> (u16, String) {
        if matches!(self, Self::Http) {
            return server.http_query(sql).await;
        }
        let sent = self.send(server, sql).await;
        (if sent.errored { 400 } else { 200 }, sent.text)
    }

    /// Send `sql` as one request.
    pub(super) async fn send(self, server: &TestServer, sql: &str) -> Sent {
        match self {
            Self::Http => {
                let (status, text) = post(server, "/v1/query", sql).await;
                Sent {
                    errored: status != 200,
                    text: format!("{status}: {text}"),
                }
            }
            Self::Ndjson => {
                let (status, text) = post(server, "/v1/query/stream", sql).await;
                Sent {
                    errored: status != 200 || text.contains("\"error\""),
                    text: format!("{status}: {text}"),
                }
            }
            Self::Ws => {
                let frame = ws_frame(server, sql).await;
                Sent {
                    errored: frame.get("error").is_some(),
                    text: with_decoded_bytes(&frame),
                }
            }
            Self::Native => {
                let mut stream = open_trust_session(server.native_port, "nodedb").await;
                let response = send_sql(&mut stream, 2, sql).await;
                Sent {
                    errored: response.error.is_some(),
                    text: serde_json::to_string(&response)
                        .unwrap_or_else(|e| format!("unserializable response: {e}")),
                }
            }
        }
    }
}

/// The frame as text, followed by every array of bytes in it read as UTF-8.
/// WebSocket RPC answers a vector hit as the bytes of its document.
fn with_decoded_bytes(frame: &serde_json::Value) -> String {
    fn walk(value: &serde_json::Value, out: &mut String) {
        match value {
            serde_json::Value::Array(items) => {
                let bytes: Option<Vec<u8>> = items
                    .iter()
                    .map(|item| item.as_u64().and_then(|n| u8::try_from(n).ok()))
                    .collect();
                match bytes {
                    Some(bytes) if bytes.len() > 2 => {
                        out.push(' ');
                        out.push_str(&String::from_utf8_lossy(&bytes));
                    }
                    _ => items.iter().for_each(|item| walk(item, out)),
                }
            }
            serde_json::Value::Object(map) => map.values().for_each(|item| walk(item, out)),
            _ => {}
        }
    }
    let mut text = frame.to_string();
    walk(frame, &mut text);
    text
}
