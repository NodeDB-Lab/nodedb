// SPDX-License-Identifier: BUSL-1.1

//! Row-level security and collection scoping over the RESP (Redis) protocol.
//!
//! Every RESP data command funnels through one dispatch pair, so the protocol
//! either honours row-level security for all of its commands or for none of
//! them. These tests pin the contract:
//!
//! * Reads that can carry a filter (`GET`, `KEYS`, `SCAN`) must have the read
//!   policy applied before the value leaves the server.
//! * Reads with no storage pushdown slot (`MGET`, `HGET`) filter post-fetch: an
//!   excluded row reads back as absent, indistinguishable from a missing key,
//!   so the reply cannot be used to probe for rows the caller may not read.
//! * `SELECT` must refuse the internal `_system` collection at the point the
//!   client names it.
//! * A failed re-`AUTH` must leave the session's established identity intact
//!   rather than half-applying the new principal.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use nodedb::control::server::resp::listener::RespListener;
use nodedb_test_support::pgwire_harness::TestServer;

const PASSWORD: &str = "probe-secret-99";

// ── Minimal RESP client ────────────────────────────────────────────────────

/// A RESP reply, decoded far enough for these tests to assert on it.
#[derive(Debug, PartialEq, Eq)]
enum Reply {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<String>),
    Array(Vec<Reply>),
}

impl Reply {
    fn is_error(&self) -> bool {
        matches!(self, Reply::Error(_))
    }
}

struct RespClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl RespClient {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("RESP connect");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    /// Send a command as a RESP array and return the decoded reply.
    async fn cmd(&mut self, args: &[&str]) -> Reply {
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

// ── Harness ────────────────────────────────────────────────────────────────

/// Bind a RESP listener onto the running server's shared state.
async fn start_resp_listener(server: &TestServer) -> std::net::SocketAddr {
    let listener = RespListener::bind("127.0.0.1:0".parse().expect("loopback addr"))
        .await
        .expect("RESP bind");
    let addr = listener.addr();
    let shared = Arc::clone(&server.shared);
    let gate = Arc::clone(&shared.startup);
    let (bus, _rx) = nodedb::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
    tokio::spawn(async move {
        let _ = listener
            .run(
                shared,
                Arc::new(tokio::sync::Semaphore::new(32)),
                None,
                gate,
                bus,
            )
            .await;
    });
    addr
}

/// Create a KV collection plus a non-superuser principal able to use it.
async fn seed(server: &TestServer, collection: &str, user: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, val TEXT) \
             WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
}

/// Authenticate as `user` and select `collection`.
async fn session(addr: std::net::SocketAddr, user: &str, collection: &str) -> RespClient {
    let mut client = RespClient::connect(addr).await;
    let auth = client.cmd(&["AUTH", user, PASSWORD]).await;
    assert!(!auth.is_error(), "RESP AUTH as {user} failed: {auth:?}");
    let select = client.cmd(&["SELECT", collection]).await;
    assert!(!select.is_error(), "SELECT {collection} failed: {select:?}");
    client
}

/// Add the read policy that restricts rows to the authenticated principal.
async fn create_owner_policy(server: &TestServer, policy: &str, collection: &str) {
    server
        .exec(&format!(
            "CREATE RLS POLICY {policy} ON {collection} FOR READ USING (owner = $auth.id)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create policy {policy}: {e}"));
}

/// A policy-excluded row must read back as absent, never as an error: an error
/// distinguishable from "no such key" is itself a probe for keys the caller may
/// not read.
fn assert_absent_not_error(reply: &Reply, command: &str) {
    assert!(
        !reply.is_error(),
        "{command} returned an error for a policy-excluded row instead of \
         reporting it absent: {reply:?}"
    );
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// `GET` carries an `rls_filters` slot, so the read policy must be evaluated
/// before the stored value is returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_applies_row_level_security() {
    let server = TestServer::start().await;
    seed(&server, "resp_rls_get", "resp_get_user").await;
    let addr = start_resp_listener(&server).await;

    let mut client = session(addr, "resp_get_user", "resp_rls_get").await;
    let set = client.cmd(&["SET", "k1", "classified"]).await;
    assert!(!set.is_error(), "SET failed: {set:?}");

    create_owner_policy(&server, "resp_get_policy", "resp_rls_get").await;

    let got = client.cmd(&["GET", "k1"]).await;
    assert_eq!(
        got,
        Reply::Bulk(None),
        "GET returned a value the read policy excludes"
    );
}

/// Baseline for the two scan commands, with no policy in play: the keys a
/// client stored are the keys it gets back. `KEYS` and `SCAN` decode the KV
/// scan payload themselves instead of going through the shared response codec,
/// so this pins the payload contract independently of any policy behaviour —
/// a scan that cannot decode its own result cannot be said to filter it either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_lists_stored_keys() {
    let server = TestServer::start().await;
    seed(&server, "resp_keys_plain", "resp_keys_plain_user").await;
    let addr = start_resp_listener(&server).await;

    let mut client = session(addr, "resp_keys_plain_user", "resp_keys_plain").await;
    assert!(!client.cmd(&["SET", "k1", "v1"]).await.is_error());

    let keys = client.cmd(&["KEYS", "*"]).await;
    assert_eq!(
        keys,
        Reply::Array(vec![Reply::Bulk(Some("k1".into()))]),
        "KEYS did not return the stored key"
    );
}

/// `SCAN` shares the scan plan with `KEYS`; same baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_lists_stored_keys() {
    let server = TestServer::start().await;
    seed(&server, "resp_scan_plain", "resp_scan_plain_user").await;
    let addr = start_resp_listener(&server).await;

    let mut client = session(addr, "resp_scan_plain_user", "resp_scan_plain").await;
    assert!(!client.cmd(&["SET", "k1", "v1"]).await.is_error());

    let scan = client.cmd(&["SCAN", "0"]).await;
    assert_eq!(
        scan,
        Reply::Array(vec![
            Reply::Bulk(Some("0".into())),
            Reply::Array(vec![Reply::Bulk(Some("k1".into()))]),
        ]),
        "SCAN did not return the stored key"
    );
}

/// `KEYS` is a filterable scan; a policy-excluded key must not be listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_applies_row_level_security() {
    let server = TestServer::start().await;
    seed(&server, "resp_rls_keys", "resp_keys_user").await;
    let addr = start_resp_listener(&server).await;

    let mut client = session(addr, "resp_keys_user", "resp_rls_keys").await;
    assert!(!client.cmd(&["SET", "k1", "classified"]).await.is_error());

    create_owner_policy(&server, "resp_keys_policy", "resp_rls_keys").await;

    let keys = client.cmd(&["KEYS", "*"]).await;
    assert_eq!(
        keys,
        Reply::Array(Vec::new()),
        "KEYS listed a key the read policy excludes"
    );
}

/// `SCAN` shares the scan plan with `KEYS` and must behave identically.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_applies_row_level_security() {
    let server = TestServer::start().await;
    seed(&server, "resp_rls_scan", "resp_scan_user").await;
    let addr = start_resp_listener(&server).await;

    let mut client = session(addr, "resp_scan_user", "resp_rls_scan").await;
    assert!(!client.cmd(&["SET", "k1", "classified"]).await.is_error());

    create_owner_policy(&server, "resp_scan_policy", "resp_rls_scan").await;

    let scan = client.cmd(&["SCAN", "0"]).await;
    assert_eq!(
        scan,
        Reply::Array(vec![
            Reply::Bulk(Some("0".into())),
            Reply::Array(Vec::new()),
        ]),
        "SCAN listed a key the read policy excludes"
    );
}

/// `MGET` maps to a batch get, which filters post-fetch: an excluded key comes
/// back as a nil element, exactly like a key that does not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mget_reports_policy_excluded_keys_as_absent() {
    let server = TestServer::start().await;
    seed(&server, "resp_rls_mget", "resp_mget_user").await;
    let addr = start_resp_listener(&server).await;

    let mut client = session(addr, "resp_mget_user", "resp_rls_mget").await;
    assert!(!client.cmd(&["SET", "k1", "classified"]).await.is_error());

    create_owner_policy(&server, "resp_mget_policy", "resp_rls_mget").await;

    let reply = client.cmd(&["MGET", "k1", "k2"]).await;
    assert_absent_not_error(&reply, "MGET");
    assert_eq!(
        reply,
        Reply::Array(vec![Reply::Bulk(None), Reply::Bulk(None)]),
        "MGET returned a value the read policy excludes"
    );
}

/// `HGET` maps to a field get, which filters the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hget_reports_policy_excluded_rows_as_absent() {
    let server = TestServer::start().await;
    seed(&server, "resp_rls_hget", "resp_hget_user").await;
    let addr = start_resp_listener(&server).await;

    let mut client = session(addr, "resp_hget_user", "resp_rls_hget").await;
    assert!(
        !client
            .cmd(&["HSET", "k1", "field", "classified"])
            .await
            .is_error()
    );

    create_owner_policy(&server, "resp_hget_policy", "resp_rls_hget").await;

    let reply = client.cmd(&["HGET", "k1", "field"]).await;
    assert_absent_not_error(&reply, "HGET");
    assert_eq!(
        reply,
        Reply::Bulk(None),
        "HGET returned a field from a row the read policy excludes"
    );
}

/// `HSET` on a key that was never inserted creates the row: the RESP
/// hash-set family is unaffected by SQL `UPDATE`'s absent-key no-op rule.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hset_on_absent_key_creates_the_row() {
    let server = TestServer::start().await;
    seed(&server, "resp_hset_create", "resp_hset_user").await;
    let addr = start_resp_listener(&server).await;

    let mut client = session(addr, "resp_hset_user", "resp_hset_create").await;
    let reply = client.cmd(&["HSET", "fresh", "val", "created"]).await;
    assert!(
        !reply.is_error(),
        "HSET on an absent key must succeed: {reply:?}"
    );

    let rows = server
        .query_rows("SELECT val FROM resp_hset_create WHERE id = 'fresh'")
        .await
        .unwrap_or_else(|e| panic!("read back created row: {e}"));
    assert_eq!(
        rows,
        vec![vec!["created".to_string()]],
        "HSET on an absent key must create the row"
    );
}

/// `SELECT` writes client input straight into the session's collection slot.
/// The internal catalog collection must be refused where the client names it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_rejects_the_system_collection() {
    let server = TestServer::start().await;
    seed(&server, "resp_select_guard", "resp_select_user").await;
    let addr = start_resp_listener(&server).await;

    let mut client = RespClient::connect(addr).await;
    assert!(
        !client
            .cmd(&["AUTH", "resp_select_user", PASSWORD])
            .await
            .is_error()
    );

    let reply = client.cmd(&["SELECT", "_system"]).await;
    assert!(
        reply.is_error(),
        "SELECT accepted the internal _system collection: {reply:?}"
    );
}

/// A rejected re-`AUTH` must not disturb the identity the session already
/// holds: the tenant and principal move together or not at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_reauth_leaves_the_established_identity_intact() {
    let server = TestServer::start().await;
    seed(&server, "resp_reauth", "resp_reauth_user").await;
    server
        .exec("CREATE USER resp_other_user PASSWORD 'a-different-secret-77'")
        .await
        .expect("create second user");
    let addr = start_resp_listener(&server).await;

    let mut client = session(addr, "resp_reauth_user", "resp_reauth").await;
    assert!(!client.cmd(&["SET", "k1", "value"]).await.is_error());

    let failed = client
        .cmd(&["AUTH", "resp_other_user", "wrong-password"])
        .await;
    assert!(failed.is_error(), "AUTH with a bad password succeeded");

    // The original principal is still the session's identity, and still works.
    let got = client.cmd(&["GET", "k1"]).await;
    assert_eq!(
        got,
        Reply::Bulk(Some("value".into())),
        "a failed re-AUTH disturbed the session's established identity"
    );
}
