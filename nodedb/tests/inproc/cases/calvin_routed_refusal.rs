// SPDX-License-Identifier: BUSL-1.1

//! A contended write that routes through Calvin answers its stage's refusal.
//!
//! A predicate UPDATE whose collection key is held routes to the Calvin
//! scheduler. Its stage refuses the slice with a typed Data-Plane error. The
//! client must see that error's SQLSTATE, the one a direct write renders,
//! never a bare transaction rollback.
//!
//! A routed write keeps its `RETURNING` contract: its participant answers
//! the frozen rows. Only a list that cannot fail reaches the gate. A list
//! that can fail commits through Calvin ahead of it, and an error in it
//! refuses the write.
//!
//! Each test holds the collection key in the vShard's real lock table, runs
//! the UPDATE over pgwire, waits until the write gate routed it, then frees
//! the key the way a write-gate guard does.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nodedb::control::cluster::calvin::scheduler::lock_manager::{
    AcquireOutcome, LockKey, LockManager, LockMode, TxnId,
};
use nodedb::control::server::shared::write_admission::cp_routed_to_calvin;
use nodedb::types::DatabaseId;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{CollectionKey, QualifiedCollection};

/// How long the gate may take to route the UPDATE.
const ROUTE_DEADLINE: Duration = Duration::from_secs(30);

/// How long a vShard's scheduler may take to start after the server's start.
/// Its reconcile pass runs every 500 ms.
const SCHEDULER_DEADLINE: Duration = Duration::from_secs(30);

/// A holder id in the reserved autocommit band that no real write mints.
fn holder() -> TxnId {
    TxnId::new(TxnId::AUTOCOMMIT_EPOCH, u32::MAX)
}

/// Create `name` with a declared key and seed rows `k1` / `k2`.
async fn seeded(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, v TEXT)"
        ))
        .await
        .expect("create collection");
    server
        .exec(&format!(
            "INSERT INTO {name} (id, v) VALUES ('k1', 'one'), ('k2', 'two')"
        ))
        .await
        .expect("seed rows");
}

/// The lock table the write gate probes for a write to `qualified`.
///
/// The gate homes a collection write on `CollectionKey::from_qualified` of
/// the plan's collection, and probes that vShard's table. A scheduler
/// registers its table when its reconcile pass starts it, which can follow
/// the server's start. The wait ends once the table is there.
async fn gate_lock_table(
    server: &TestServer,
    qualified: &QualifiedCollection,
) -> (u32, Arc<Mutex<LockManager>>) {
    let vshard = CollectionKey::from_qualified(DatabaseId::DEFAULT, qualified)
        .expect("a default-database collection name")
        .vshard()
        .as_u32();
    let started = Instant::now();
    loop {
        let table = server
            .shared
            .calvin
            .lock_managers
            .lock()
            .expect("lock managers")
            .get(&vshard)
            .cloned();
        if let Some(table) = table {
            return (vshard, table);
        }
        assert!(
            started.elapsed() < SCHEDULER_DEADLINE,
            "no Calvin scheduler started for vShard {vshard} of {}",
            qualified.as_str()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Run `sql` while `name`'s collection key is held, and return the SQLSTATE
/// it fails with. Panics when the gate never routes the write.
async fn routed_sqlstate(server: &TestServer, name: &str, sql: &str) -> String {
    let Err(error) = routed(server, name, sql).await else {
        panic!("the routed write must be refused, but it ran: {sql}");
    };
    error
        .as_db_error()
        .unwrap_or_else(|| panic!("expected a DbError from {sql}, got: {error}"))
        .code()
        .code()
        .to_string()
}

/// Run `sql` while `name`'s collection key is held, so the write gate routes
/// it through Calvin, and return its answer. Panics when the gate never
/// routes the write.
async fn routed(
    server: &TestServer,
    name: &str,
    sql: &str,
) -> Result<Vec<tokio_postgres::SimpleQueryMessage>, tokio_postgres::Error> {
    let qualified = QualifiedCollection::new(DatabaseId::DEFAULT, name);
    let (vshard, lock_manager) = gate_lock_table(server, &qualified).await;
    let key = LockKey::Collection {
        collection: Arc::from(qualified.as_str()),
    };
    assert_eq!(
        lock_manager
            .lock()
            .expect("lock table")
            .acquire(holder(), BTreeMap::from([(key, LockMode::Exclusive)]))
            .outcome,
        AcquireOutcome::Ready,
        "the test holds the collection key"
    );
    let routed_before = cp_routed_to_calvin();

    let release = async {
        let started = Instant::now();
        while cp_routed_to_calvin() == routed_before {
            assert!(
                started.elapsed() < ROUTE_DEADLINE,
                "the gate never routed the contended write: {sql}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Free the key, then hand the promoted waiters to the scheduler, as
        // a write-gate guard's drop does.
        let promoted = lock_manager.lock().expect("lock table").release(holder());
        if !promoted.is_empty() {
            server
                .shared
                .calvin
                .promotion_senders
                .lock()
                .expect("promotion senders")
                .get(&vshard)
                .expect("the scheduler registered its promotion channel")
                .send(promoted)
                .expect("the scheduler takes its promoted waiters");
        }
    };
    let (result, ()) = tokio::join!(server.client.simple_query(sql), release);
    result
}

async fn assert_rows_unchanged(server: &TestServer, name: &str) {
    let ids = server
        .query_text(&format!("SELECT id FROM {name} ORDER BY id"))
        .await
        .expect("scan ids");
    assert_eq!(ids, vec!["k1".to_string(), "k2".to_string()]);
}

/// A routed predicate UPDATE that changes a primary key answers `23000`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_routed_primary_key_change_answers_an_integrity_violation() {
    let server = TestServer::start().await;
    let name = "routed_pk_change";
    seeded(&server, name).await;

    let state = routed_sqlstate(
        &server,
        name,
        &format!("UPDATE {name} SET id = 'moved' WHERE v = 'one'"),
    )
    .await;
    assert_eq!(
        state, "23000",
        "a routed primary-key change is an integrity violation"
    );
    assert_rows_unchanged(&server, name).await;
}

/// A routed predicate UPDATE that nulls a declared key answers `23502`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_routed_null_primary_key_answers_a_not_null_violation() {
    let server = TestServer::start().await;
    let name = "routed_pk_null";
    seeded(&server, name).await;

    let state = routed_sqlstate(
        &server,
        name,
        &format!("UPDATE {name} SET id = NULLIF(v, v) WHERE v = 'one'"),
    )
    .await;
    assert_eq!(
        state, "23502",
        "a routed NULL primary key is a not_null_violation"
    );
    assert_rows_unchanged(&server, name).await;
}

/// Create `name` with rows `k1` (n = 0) and `k2` (n = 2).
async fn seeded_with_n(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, v TEXT, n INT)"
        ))
        .await
        .expect("create collection");
    server
        .exec(&format!(
            "INSERT INTO {name} (id, v, n) VALUES ('k1', 'one', 0), ('k2', 'two', 2)"
        ))
        .await
        .expect("seed rows");
}

/// The `v` of every row of `name`, by id.
async fn values(server: &TestServer, name: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT v FROM {name} ORDER BY id"))
        .await
        .expect("scan values")
}

/// The `id`, `v` and `s` of every row a `RETURNING` answer holds.
fn returned_rows(
    messages: &[tokio_postgres::SimpleQueryMessage],
) -> Vec<(Option<&str>, Option<&str>, Option<&str>)> {
    messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some((row.get("id"), row.get("v"), row.try_get("s").ok().flatten()))
            }
            _ => None,
        })
        .collect()
}

/// A routed `RETURNING` write answers the rows its participant froze. Only a
/// list that cannot fail reaches the gate: a list that can fail commits
/// through Calvin ahead of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_routed_returning_write_answers_its_frozen_rows() {
    let server = TestServer::start().await;
    let name = "routed_returning";
    seeded_with_n(&server, name).await;

    let messages = routed(
        &server,
        name,
        &format!("UPDATE {name} SET v = 'uno' WHERE id = 'k1' RETURNING id, v"),
    )
    .await
    .expect("the routed RETURNING write runs");
    assert_eq!(returned_rows(&messages), [(Some("k1"), Some("uno"), None)]);
    assert_eq!(values(&server, name).await, ["uno", "two"]);
}

/// A `RETURNING` list that can fail commits through Calvin, so the client
/// gets the rows its participant froze. The list's `nextval` draws once per
/// row, at the participant, and the session takes the value it drew.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_returning_write_with_an_accessor_answers_its_frozen_rows() {
    let server = TestServer::start().await;
    let name = "frozen_returning";
    seeded_with_n(&server, name).await;
    server
        .exec("CREATE SEQUENCE frozen_returning_seq")
        .await
        .expect("create sequence");

    let messages = server
        .client
        .simple_query(&format!(
            "UPDATE {name} SET v = 'uno' WHERE n = 0 \
             RETURNING id, v, nextval('frozen_returning_seq') AS s"
        ))
        .await
        .expect("the RETURNING write runs");
    assert_eq!(
        returned_rows(&messages),
        [(Some("k1"), Some("uno"), Some("1"))]
    );
    assert_eq!(values(&server, name).await, ["uno", "two"]);
    assert_eq!(
        server
            .query_text("SELECT currval('frozen_returning_seq')")
            .await
            .expect("currval"),
        ["1"],
        "the session takes the value the participant drew"
    );
    assert_eq!(
        server
            .query_text("SELECT nextval('frozen_returning_seq')")
            .await
            .expect("nextval"),
        ["2"],
        "the list drew one value"
    );
}

/// An error in a `RETURNING` list refuses the write before it commits: the
/// list can fail, so the write commits through Calvin, whose participant
/// evaluates it before it votes. The client gets the error's SQLSTATE and
/// the row is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_returning_error_refuses_the_write() {
    let server = TestServer::start().await;
    let name = "returning_error";
    seeded_with_n(&server, name).await;

    let error = server
        .client
        .simple_query(&format!(
            "UPDATE {name} SET v = 'uno' WHERE n = 0 RETURNING id, 10 / n AS q"
        ))
        .await
        .map(|_| ())
        .expect_err("the list divides by zero");
    let state = error
        .as_db_error()
        .unwrap_or_else(|| panic!("a DbError, got: {error}"))
        .code()
        .code()
        .to_string();
    assert_eq!(state, "22012", "division_by_zero");
    assert_eq!(values(&server, name).await, ["one", "two"]);
}
