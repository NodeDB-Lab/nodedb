// SPDX-License-Identifier: BUSL-1.1

//! A spent `HARD` scope quota refuses a write that commits through Calvin, as
//! it refuses the same write on the direct path.
//!
//! A Calvin route dispatches no task alone, so the per-task admission never
//! runs for it. The route admits the whole task set before it submits, over
//! native and over pgwire.

use nodedb::control::security::metering::config::MeteringConfig;
use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::id::DatabaseId;
use tokio::net::TcpStream;

const SCOPE: &str = "quota:calvin_writes";
const ONE: &str = "cq_one";

fn vshard_of(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// A collection name on another vShard than [`ONE`].
fn two() -> String {
    (0..)
        .map(|n| format!("cq_two_{n}"))
        .find(|name| vshard_of(name) != vshard_of(ONE))
        .expect("a name on another vShard")
}

/// The numeric id of the harness user, which a scope grant names.
async fn harness_user_id(server: &TestServer) -> String {
    let rows = server.query_rows("SHOW USERS").await.expect("SHOW USERS");
    let row = rows
        .iter()
        .find(|row| row.iter().any(|field| field == "nodedb"))
        .unwrap_or_else(|| panic!("the harness user is listed: {rows:?}"));
    row.iter()
        .find(|field| field.parse::<u64>().is_ok())
        .unwrap_or_else(|| panic!("a numeric id in {row:?}"))
        .clone()
}

/// Write single rows on the direct path until the quota refuses one. Returns
/// how many were admitted.
async fn spend_the_cap(stream: &mut TcpStream) -> usize {
    for attempt in 0..20u64 {
        let response = send_sql(
            stream,
            10 + attempt,
            &format!("INSERT INTO {ONE} (id, n) VALUES ('spend_{attempt}', 1)"),
        )
        .await;
        if let Some(error) = response.error {
            assert_eq!(error.code, "53400", "{}", error.message);
            return usize::try_from(attempt).expect("an attempt count");
        }
    }
    panic!("a HARD quota of 1 token refuses a write within 20 writes");
}

/// The quota refuses a one-vShard write whose `RETURNING` list can fail, and
/// a request over two vShards. Neither stores a row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spent_hard_quota_refuses_a_calvin_write() {
    let server = TestServer::start_with_metering(MeteringConfig {
        enabled: true,
        ..Default::default()
    })
    .await;
    let two = two();
    for collection in [ONE, two.as_str()] {
        server
            .exec(&format!(
                "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, n INT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    }
    // The request over two vShards commits through Calvin on this harness:
    // with no quota yet, it commits and the sequencer admits a transaction.
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let before = admitted_txns(&server).await;
    let response = send_sql(&mut stream, 2, &two_vshard_insert(&two, "probe")).await;
    assert!(response.error.is_none(), "{:?}", response.error);
    assert!(
        admitted_txns(&server).await > before,
        "the request over two vShards commits through Calvin"
    );

    let user_id = harness_user_id(&server).await;
    for sql in [
        format!("DEFINE SCOPE '{SCOPE}' AS WRITE ON {ONE}, WRITE ON {two}"),
        format!("GRANT SCOPE '{SCOPE}' TO USER '{user_id}'"),
        format!("DEFINE QUOTA ON SCOPE '{SCOPE}' MAX 1 TOKENS PER 3600 SECONDS ENFORCEMENT HARD"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let admitted = spend_the_cap(&mut stream).await;

    for (seq, sql) in [
        format!("INSERT INTO {ONE} (id, n) VALUES ('calvin_one', 5) RETURNING id, n * 2"),
        two_vshard_insert(&two, "calvin_two"),
    ]
    .into_iter()
    .enumerate()
    {
        let seq = 100 + u64::try_from(seq).expect("an index");
        let response = send_sql(&mut stream, seq, &sql).await;
        let error = response
            .error
            .unwrap_or_else(|| panic!("the spent quota refuses: {sql}"));
        assert_eq!(error.code, "53400", "{sql}: {}", error.message);
        assert!(
            error.message.contains("quota exceeded"),
            "{}",
            error.message
        );
    }

    let stored = server
        .query_text(&format!("SELECT id FROM {ONE}"))
        .await
        .expect("read the first collection");
    assert_eq!(
        stored.len(),
        admitted + 1,
        "the probe and the admitted writes only: {stored:?}"
    );
    let stored = server
        .query_text(&format!("SELECT id FROM {two}"))
        .await
        .expect("read the second collection");
    assert_eq!(stored, ["probe"], "the probe only");
}

/// One native request that inserts row `id` into [`ONE`] and into `two`.
fn two_vshard_insert(two: &str, id: &str) -> String {
    format!(
        "INSERT INTO {ONE} (id, n) VALUES ('{id}', 1); \
         INSERT INTO {two} (id, n) VALUES ('{id}', 1)"
    )
}

/// The `admitted` count of Calvin transactions the sequencer reports on
/// `/metrics`.
async fn admitted_txns(server: &TestServer) -> u64 {
    let body = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/metrics", server.http_port))
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET /metrics: {e}"))
        .text()
        .await
        .unwrap_or_default();
    body.lines()
        .find_map(|line| {
            line.strip_prefix("nodedb_sequencer_admitted_txns_total{outcome=\"admitted\"} ")
                .and_then(|value| value.trim().parse().ok())
        })
        .unwrap_or_else(|| panic!("no admitted sequencer count in /metrics: {body}"))
}

/// The postings collection of the materialized sum, and its accounts.
const POSTINGS: &str = "ptv_post";
const ACCOUNTS: &str = "ptv_acct";

/// A pgwire update of a sum source whose target lives on another vShard
/// commits through Calvin. The spent quota refuses it with `53400`, and
/// neither the posting nor the balance moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spent_hard_quota_refuses_a_pgwire_calvin_write() {
    assert_ne!(
        vshard_of(POSTINGS),
        vshard_of(ACCOUNTS),
        "the sum spans two vShards"
    );
    let server = TestServer::start_with_metering(MeteringConfig {
        enabled: true,
        ..Default::default()
    })
    .await;
    for sql in [
        format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
        format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ),
        format!(
            "ALTER COLLECTION {ACCOUNTS} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {POSTINGS} \
             ON {POSTINGS}.account_id = {ACCOUNTS}.id VALUE {POSTINGS}.amount"
        ),
        format!("INSERT INTO {ACCOUNTS} (id, owner, balance) VALUES ('acc', 'alice', '0')"),
        format!("INSERT INTO {POSTINGS} (id, account_id, amount) VALUES ('e0', 'acc', '10')"),
        format!(
            "CREATE COLLECTION {ONE} (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let user_id = harness_user_id(&server).await;
    for sql in [
        format!("DEFINE SCOPE '{SCOPE}' AS WRITE ON {ONE}, WRITE ON {POSTINGS}"),
        format!("GRANT SCOPE '{SCOPE}' TO USER '{user_id}'"),
        format!("DEFINE QUOTA ON SCOPE '{SCOPE}' MAX 1 TOKENS PER 3600 SECONDS ENFORCEMENT HARD"),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    spend_the_cap(&mut stream).await;

    let error = server
        .exec(&format!(
            "UPDATE {POSTINGS} SET amount = '1' WHERE id = 'e0'"
        ))
        .await
        .expect_err("the spent quota refuses the Calvin write");
    assert!(error.contains("(SQLSTATE 53400)"), "{error}");
    assert!(error.contains("quota exceeded"), "{error}");

    let rows = server
        .query_rows(&format!("SELECT amount FROM {POSTINGS} WHERE id = 'e0'"))
        .await
        .expect("read the posting");
    assert_eq!(rows, [["10"]], "the posting keeps its amount");
    let rows = server
        .query_rows(&format!("SELECT balance FROM {ACCOUNTS} WHERE id = 'acc'"))
        .await
        .expect("read the balance");
    assert_eq!(rows, [["10"]], "the balance keeps its value");
}
