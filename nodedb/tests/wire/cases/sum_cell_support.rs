// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers of the materialized-sum cells: the sum post-condition, the
//! parked-gate helpers, the request transports and the period lock fixture.
//! The fixture itself comes from `materialized_sum_cross_shard_expanders`.

#![allow(dead_code, unused_imports)]

use std::collections::BTreeMap;

use futures::{SinkExt, StreamExt};
use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use crate::harness::TestServer;

// Main's own fixture helpers (`pub(super)` in that module).
#[cfg(feature = "failpoints")]
pub(super) use super::materialized_sum_cross_shard_expanders::FIXTURE_CORES;
pub(super) use super::materialized_sum_cross_shard_expanders::{
    INSERT_SELECT, balance, feed, sum_fixture, sum_fixture_on,
};

/// The sum's source collection. Private on main, so named again here.
pub(super) const POSTINGS: &str = "ptv_post";

/// The SQLSTATE of a serialization conflict.
pub(super) const SERIALIZATION_FAILURE: &str = "40001";

/// Assert that a native `response` either committed or aborted with a
/// serialization conflict.
pub(super) fn assert_native_commit_or_conflict(
    response: &nodedb_types::protocol::NativeResponse,
    what: &str,
) {
    if let Some(error) = &response.error {
        assert_eq!(
            error.code, SERIALIZATION_FAILURE,
            "{what} must commit or abort with a serialization conflict, got {}: {}",
            error.code, error.message
        );
    }
}

/// The sum of the `acc` postings, read from the source rows.
pub(super) async fn source_sum(server: &TestServer) -> i64 {
    server
        .query_text(&format!(
            "SELECT amount FROM {POSTINGS} WHERE account_id = 'acc'"
        ))
        .await
        .expect("read the source postings")
        .iter()
        .map(|amount| amount.parse::<i64>().expect("a numeric amount"))
        .sum()
}

// ---------------------------------------------------------------- sum pair

/// The collections and columns of one materialized sum.
#[derive(Clone, Copy, Debug)]
pub(super) struct SumPair {
    pub(super) target: &'static str,
    pub(super) balance: &'static str,
    pub(super) source: &'static str,
    pub(super) join: &'static str,
    pub(super) value: &'static str,
}

impl SumPair {
    /// `ptv_acct.balance` adds `ptv_post.amount` by `ptv_post.account_id`.
    pub(super) const POSTINGS: Self = Self {
        target: "ptv_acct",
        balance: "balance",
        source: "ptv_post",
        join: "account_id",
        value: "amount",
    };
}

/// A target row whose balance differs from the sum of its source rows.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Drift {
    pub(super) id: String,
    pub(super) balance: f64,
    pub(super) sum: f64,
}

impl std::fmt::Display for Drift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: balance {} against source sum {}",
            self.id, self.balance, self.sum
        )
    }
}

fn number(text: &str) -> Result<f64, String> {
    if text.is_empty() {
        return Ok(0.0);
    }
    text.parse::<f64>()
        .map_err(|e| format!("not a number {text:?}: {e}"))
}

impl TestServer {
    /// The branch harness checked the pair after every statement. Main's
    /// harness has no hook for that, so this does nothing. Cells that call it
    /// still check the pair at their end.
    pub(super) fn watch_sum(&self, _pair: SumPair) {}

    /// The target rows of `pair` whose balance differs from the sum of the
    /// source rows that name them.
    pub(super) async fn sum_drift(&self, pair: SumPair) -> Result<Vec<Drift>, String> {
        let balances = self
            .query_rows(&format!("SELECT id, {} FROM {}", pair.balance, pair.target))
            .await?;
        let postings = self
            .query_rows(&format!(
                "SELECT {}, {} FROM {}",
                pair.join, pair.value, pair.source
            ))
            .await?;
        let mut sums: BTreeMap<String, f64> = BTreeMap::new();
        for row in &postings {
            let [key, value] = row.as_slice() else {
                return Err(format!("two source columns expected, got {row:?}"));
            };
            if !key.is_empty() {
                *sums.entry(key.clone()).or_default() += number(value)?;
            }
        }
        let mut drift = Vec::new();
        for row in &balances {
            let [id, balance] = row.as_slice() else {
                return Err(format!("two target columns expected, got {row:?}"));
            };
            let balance = number(balance)?;
            let sum = sums.get(id).copied().unwrap_or(0.0);
            if (balance - sum).abs() > 1e-9 {
                drift.push(Drift {
                    id: id.clone(),
                    balance,
                    sum,
                });
            }
        }
        Ok(drift)
    }

    /// Panic when a balance of `pair` differs from its source sum.
    pub(super) async fn assert_sum_invariant(&self, pair: SumPair, after: &str) {
        match self.sum_drift(pair).await {
            Ok(drift) if drift.is_empty() => {}
            Ok(drift) => panic!(
                "the balance must equal the source sum after {after}: {}",
                drift
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            Err(e) => panic!("read the sum post-condition after {after}: {e}"),
        }
    }
}

// ------------------------------------------------------------- wire helpers

/// Run `sql` and panic with the server error text on an error.
pub(super) async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// The first column of every row `sql` reads.
pub(super) async fn column(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("read {sql}: {e}"))
}

/// The rows of `collection`, ordered by key. Each row holds `id` and `v`.
pub(super) async fn rows(server: &TestServer, collection: &str) -> Vec<Vec<String>> {
    server
        .query_rows(&format!("SELECT id, v FROM {collection} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

/// The sorted ids of `collection`.
pub(super) async fn ids(server: &TestServer, collection: &str) -> Vec<String> {
    let mut ids = server
        .query_text(&format!("SELECT id FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"));
    ids.sort();
    ids
}

/// Create `collection` with a text key `id` and a text column `v`.
pub(super) async fn create(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, v TEXT)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
}

/// POST `sql` to `path`. Returns the status and the body.
pub(super) async fn post(server: &TestServer, path: &str, sql: &str) -> (u16, String) {
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}{path}", server.http_port))
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {path} {sql}: {e}"));
    let status = response.status().as_u16();
    (status, response.text().await.unwrap_or_default())
}

/// Send `sql` as WebSocket RPC `query` on a new socket. Returns the frame
/// that answers it.
pub(super) async fn ws_frame(server: &TestServer, sql: &str) -> Value {
    let url = format!("ws://127.0.0.1:{}/v1/ws", server.http_port);
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("connect the WebSocket RPC endpoint");
    let request = serde_json::json!({ "id": 1, "method": "query", "params": { "sql": sql } });
    ws.send(Message::Text(request.to_string().into()))
        .await
        .unwrap_or_else(|e| panic!("send {sql}: {e}"));
    loop {
        let message = tokio::time::timeout(std::time::Duration::from_secs(30), ws.next())
            .await
            .unwrap_or_else(|_| panic!("no answer to {sql}"))
            .unwrap_or_else(|| panic!("the socket closed before answering {sql}"))
            .unwrap_or_else(|e| panic!("read the answer to {sql}: {e}"));
        let Message::Text(text) = message else {
            continue;
        };
        let frame: Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("a JSON frame for {sql}: {e}: {text}"));
        if frame["id"] == 1 {
            return frame;
        }
    }
}

/// The Data Plane core of collection `name` on a server of `cores` cores.
pub(super) fn core_of(name: &str, cores: usize) -> usize {
    nodedb_types::CollectionKey::from_bare(nodedb_types::id::DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32() as usize
        % cores
}

/// A name that starts with `prefix` and hashes to another core than `first`.
pub(super) fn name_on_another_core(first: &str, prefix: &str, cores: usize) -> String {
    (0..)
        .map(|n| format!("{prefix}{n}"))
        .find(|name| core_of(name, cores) != core_of(first, cores))
        .expect("a name on the other core")
}

// --------------------------------------------------------------- transports

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
    pub(super) errored: bool,
    pub(super) text: String,
}

impl Transport {
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
                    text: frame.to_string(),
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

// -------------------------------------------------------------- parked gates

/// The gate a recon write parks at before it submits its Calvin transaction.
#[cfg(feature = "failpoints")]
const RECON_GATE: &str = "recon::before_submit";

/// A server whose recon writes park at `recon::before_submit` until
/// `release` exists, with the sum fixture created. The gate is armed on return.
#[cfg(feature = "failpoints")]
pub(super) async fn parked_fixture(release: &std::path::Path) -> TestServer {
    let server = parked_fixture_at(RECON_GATE, release).await;
    arm_gate(release);
    server
}

/// A server with `gate` armed as `wait_file(release)`, and the sum fixture
/// created. The gate is open until the caller calls [`arm_gate`].
#[cfg(feature = "failpoints")]
pub(super) async fn parked_fixture_at(gate: &str, release: &std::path::Path) -> TestServer {
    std::fs::write(release, b"open").expect("open the gate for the fixture");
    let spec = format!("{gate}=wait_file({})", release.display());
    let server = TestServer::start_multicores_with_failpoints(FIXTURE_CORES, &spec).await;
    sum_fixture_on(server).await
}

/// Close the gate that waits on `release`: the next statement to reach it parks.
#[cfg(feature = "failpoints")]
pub(super) fn arm_gate(release: &std::path::Path) {
    clear_parked_marker(release);
    std::fs::remove_file(release).expect("arm the gate");
}

/// Remove the marker a pass through the open gate on `release` left.
#[cfg(feature = "failpoints")]
pub(super) fn clear_parked_marker(release: &std::path::Path) {
    if let Err(e) = std::fs::remove_file(parked_marker(release))
        && e.kind() != std::io::ErrorKind::NotFound
    {
        panic!("clear the marker an open pass left: {e}");
    }
}

/// `release` with `.parked` appended: the marker a gate writes on arrival.
#[cfg(feature = "failpoints")]
pub(super) fn parked_marker(release: &std::path::Path) -> std::path::PathBuf {
    let mut marker = release.as_os_str().to_owned();
    marker.push(".parked");
    std::path::PathBuf::from(marker)
}

/// Wait until a write reports parked at `recon::before_submit`.
#[cfg(feature = "failpoints")]
pub(super) async fn wait_parked(release: &std::path::Path, what: &str) {
    wait_parked_at(RECON_GATE, release, what).await;
}

/// Wait until a statement reports parked at `gate`, armed on `release`.
#[cfg(feature = "failpoints")]
pub(super) async fn wait_parked_at(gate: &str, release: &std::path::Path, what: &str) {
    let marker = parked_marker(release);
    for _ in 0..600 {
        if marker.exists() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the {what} never reached {gate}");
}

// -------------------------------------------------------------- period lock

/// A period collection, an entries collection and the period lock between them.
pub(super) async fn period_setup(server: &TestServer, periods: &str, entries: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {periods} (period_key TEXT PRIMARY KEY, status TEXT) \
             WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE COLLECTION {entries} (id TEXT PRIMARY KEY, fiscal_period TEXT, amount INT) \
             WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "ALTER COLLECTION {entries} ADD PERIOD LOCK ON fiscal_period \
             REFERENCES {periods}(period_key) STATUS status \
             ALLOW WRITE WHEN status IN ('OPEN')"
        ))
        .await
        .unwrap();
}

/// Insert the period row `key` with `status`.
pub(super) async fn seed_period(server: &TestServer, periods: &str, key: &str, status: &str) {
    server
        .exec(&format!(
            "INSERT INTO {periods} (period_key, status) VALUES ('{key}', '{status}')"
        ))
        .await
        .unwrap();
}
