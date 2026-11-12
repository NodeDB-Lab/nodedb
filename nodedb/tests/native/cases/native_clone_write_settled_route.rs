// SPDX-License-Identifier: BUSL-1.1

//! A settled write on a `Shadowed` clone answers what the per-task route
//! answers.
//!
//! The source database holds a cross-shard materialized sum. `CLONE DATABASE`
//! copies the binding into the clone. A predicate write of the clone's sum
//! source that moves no balance stamps a settlement read on one vShard. That
//! set commits through Calvin, not through the per-task loop and its clone
//! check. The per-task loop refuses a predicate write on a `Shadowed` clone
//! with SQLSTATE 55006, so the settled route must refuse it too, over pgwire
//! and over native.

use nodedb_test_support::native_harness::{do_handshake, send_request, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{AuthMethod, HelloFrame, OpCode};
use tokio::net::TcpStream;

const SOURCE_DB: &str = "cws_src";
const CLONE_DB: &str = "cws_tgt";
/// The sum's target collection.
const ACCOUNTS: &str = "cws_acct";
/// The sum's source collection.
const POSTINGS: &str = "cws_post";
/// A collection with no sum. A write to it takes the per-task route.
const PLAIN: &str = "cws_plain";
/// The SQLSTATE of `CloneWriteRequiresMaterialize`.
const CLONE_WRITE_REFUSED: &str = "55006";
/// A predicate write of the sum source that moves no balance.
const SETTLED_WRITE: &str = "UPDATE cws_post SET note = 'x' WHERE account_id = 'acc'";
/// The same write shape on the collection with no sum.
const PLAIN_WRITE: &str = "UPDATE cws_plain SET v = 'x' WHERE v = 'a'";

async fn exec(srv: &TestServer, sql: &str) {
    srv.exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
}

/// The SQLSTATE `sql` fails with over pgwire, or `None` when it succeeds.
async fn pg_sqlstate(srv: &TestServer, sql: &str) -> Option<String> {
    match srv.client.simple_query(sql).await {
        Ok(_) => None,
        Err(error) => Some(
            error
                .as_db_error()
                .unwrap_or_else(|| panic!("{sql} failed without a database error: {error:?}"))
                .code()
                .code()
                .to_owned(),
        ),
    }
}

/// Seed a source database with a cross-shard sum and a plain collection,
/// clone it, and make the clone's sum binding visible to the planner.
async fn clone_fixture() -> TestServer {
    let srv = TestServer::start().await;
    exec(&srv, &format!("CREATE DATABASE {SOURCE_DB}")).await;
    exec(&srv, &format!("USE DATABASE {SOURCE_DB}")).await;
    exec(
        &srv,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &srv,
        &format!(
            "CREATE COLLECTION {POSTINGS} \
             (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT, note TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &srv,
        &format!(
            "ALTER COLLECTION {ACCOUNTS} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {POSTINGS} \
             ON {POSTINGS}.account_id = {ACCOUNTS}.id VALUE {POSTINGS}.amount"
        ),
    )
    .await;
    exec(
        &srv,
        &format!("CREATE COLLECTION {PLAIN} (id TEXT PRIMARY KEY, v TEXT)"),
    )
    .await;
    exec(
        &srv,
        &format!("INSERT INTO {ACCOUNTS} (id, owner, balance) VALUES ('acc', 'alice', '0')"),
    )
    .await;
    exec(
        &srv,
        &format!(
            "INSERT INTO {POSTINGS} (id, account_id, amount, note) \
             VALUES ('p1', 'acc', '5', 'n')"
        ),
    )
    .await;
    exec(
        &srv,
        &format!("INSERT INTO {PLAIN} (id, v) VALUES ('r1', 'a')"),
    )
    .await;

    exec(&srv, "USE DATABASE default").await;
    exec(&srv, &format!("CLONE DATABASE {CLONE_DB} FROM {SOURCE_DB}")).await;
    // The sum index rebuilds on a schema version bump. `CLONE DATABASE` does
    // not bump it, so a DDL that does makes the clone's binding visible.
    exec(&srv, "CREATE SEQUENCE cws_bump START 1 INCREMENT 1").await;

    assert_sum_pair_is_cross_shard(&srv);
    srv
}

/// The premise: in the clone database the sum pair spans two vShards, so the
/// settlement stamps a read.
fn assert_sum_pair_is_cross_shard(srv: &TestServer) {
    let clone_id = srv
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(CLONE_DB)
        .expect("read clone database id")
        .expect("clone database exists");
    let vshard = |name: &str| {
        nodedb_types::CollectionKey::from_bare(clone_id, name)
            .vshard()
            .as_u32()
    };
    assert_ne!(
        vshard(ACCOUNTS),
        vshard(POSTINGS),
        "the sum pair must span two vShards in the clone; rename the collections"
    );
}

/// The source row the clone reads through to is untouched.
async fn assert_source_untouched(srv: &TestServer) {
    exec(srv, &format!("USE DATABASE {SOURCE_DB}")).await;
    let rows = srv
        .query_rows(&format!("SELECT note FROM {POSTINGS} WHERE id = 'p1'"))
        .await
        .expect("read source row");
    assert_eq!(
        rows,
        vec![vec!["n".to_owned()]],
        "a write on the clone must never change its source: {rows:?}"
    );
}

/// A native session authenticated straight into the clone database.
async fn clone_session(srv: &TestServer) -> TcpStream {
    let username = srv
        .shared
        .credentials
        .configured_trust_superuser()
        .expect("read configured trust superuser")
        .expect("harness runs in AuthMode::Trust");
    let addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (mut stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_request(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Trust { username }),
            database: Some(CLONE_DB.into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        auth.status,
        ResponseStatus::Ok,
        "native session must authenticate into the clone database: {auth:?}"
    );
    stream
}

/// pgwire: the settled predicate write answers 55006, as the same shape on a
/// collection with no sum does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_settled_predicate_write_on_shadowed_clone_is_refused() {
    let srv = clone_fixture().await;
    exec(&srv, &format!("USE DATABASE {CLONE_DB}")).await;

    assert_eq!(
        pg_sqlstate(&srv, PLAIN_WRITE).await.as_deref(),
        Some(CLONE_WRITE_REFUSED),
        "the per-task route refuses a predicate write on a Shadowed clone"
    );
    assert_eq!(
        pg_sqlstate(&srv, SETTLED_WRITE).await.as_deref(),
        Some(CLONE_WRITE_REFUSED),
        "the settled route must refuse the same write shape"
    );

    assert_source_untouched(&srv).await;
    srv.graceful_shutdown().await;
}

/// Native SQL: the settled predicate write answers 55006, as the same shape
/// on a collection with no sum does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_settled_predicate_write_on_shadowed_clone_is_refused() {
    let srv = clone_fixture().await;
    let mut stream = clone_session(&srv).await;

    for (seq, sql, route) in [(2, PLAIN_WRITE, "per-task"), (3, SETTLED_WRITE, "settled")] {
        let resp = send_sql(&mut stream, seq, sql).await;
        assert_eq!(
            resp.status,
            ResponseStatus::Error,
            "the {route} route must refuse {sql} on a Shadowed clone: {resp:?}"
        );
        let code = resp.error.map(|error| error.code);
        assert_eq!(
            code.as_deref(),
            Some(CLONE_WRITE_REFUSED),
            "the {route} route must answer 55006 for {sql}"
        );
    }
    drop(stream);

    assert_source_untouched(&srv).await;
    srv.graceful_shutdown().await;
}
