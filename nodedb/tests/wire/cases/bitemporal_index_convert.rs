// SPDX-License-Identifier: BUSL-1.1

//! Secondary indexes and CONVERT on a bitemporal collection.
//!
//! - `DROP INDEX` removes the versioned index entries, so a later
//!   `CREATE INDEX` on the same field starts clean.
//! - An `AS OF SYSTEM TIME` read with an indexed equality reads the versioned
//!   index at its cutoff, so it returns the rows as they stood then. That
//!   holds for an index created after the rows changed, too.
//! - `CONVERT` rewrites every version, so the current read and the history
//!   read return the same rows after it. A key-value target keeps no valid
//!   time and purges its versions at the cleanup horizon, so it is refused
//!   with nothing moved.

use crate::harness::TestServer;

/// The wall-clock millisecond strictly between the writes before and after
/// it.
async fn instant_between_writes() -> i64 {
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before epoch")
        .as_millis();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    i64::try_from(now).expect("millisecond clock fits i64")
}

/// `(id, kind)` of every row `sql` returns, sorted by id.
async fn id_kind_rows(srv: &TestServer, sql: &str) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = srv
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"))
        .into_iter()
        .map(|row| (row[0].clone(), row[1].clone()))
        .collect();
    rows.sort();
    rows
}

fn rows(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(id, kind)| (id.to_string(), kind.to_string()))
        .collect()
}

async fn insert(srv: &TestServer, name: &str, id: &str, kind: &str) {
    srv.exec(&format!(
        "INSERT INTO {name} {{ id: '{id}', kind: '{kind}' }}"
    ))
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreated_index_holds_only_the_live_rows() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION bt_idx_drop WITH (bitemporal=true)")
        .await
        .unwrap();
    srv.exec("CREATE INDEX bt_idx_drop_kind ON bt_idx_drop(kind)")
        .await
        .unwrap();
    insert(&srv, "bt_idx_drop", "a", "x").await;
    insert(&srv, "bt_idx_drop", "b", "y").await;
    srv.exec("DROP INDEX bt_idx_drop_kind").await.unwrap();

    // Writes with no index: a stale entry from the first index names the
    // old values of a and b.
    srv.exec("UPDATE bt_idx_drop SET kind = 'z' WHERE id = 'a'")
        .await
        .unwrap();
    srv.exec("DELETE FROM bt_idx_drop WHERE id = 'b'")
        .await
        .unwrap();
    insert(&srv, "bt_idx_drop", "c", "x").await;
    srv.exec("CREATE INDEX bt_idx_drop_kind ON bt_idx_drop(kind)")
        .await
        .unwrap();

    let lookup =
        |kind: &'static str| format!("SELECT id, kind FROM bt_idx_drop WHERE kind = '{kind}'");
    assert_eq!(id_kind_rows(&srv, &lookup("x")).await, rows(&[("c", "x")]));
    assert_eq!(id_kind_rows(&srv, &lookup("z")).await, rows(&[("a", "z")]));
    assert!(id_kind_rows(&srv, &lookup("y")).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn as_of_with_an_indexed_equality_reads_the_versions() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION bt_idx_asof WITH (bitemporal=true)")
        .await
        .unwrap();
    srv.exec("CREATE INDEX bt_idx_asof_kind ON bt_idx_asof(kind)")
        .await
        .unwrap();
    insert(&srv, "bt_idx_asof", "a", "x").await;
    let before = instant_between_writes().await;
    srv.exec("UPDATE bt_idx_asof SET kind = 'y' WHERE id = 'a'")
        .await
        .unwrap();

    let as_of = |kind: &str| {
        format!("SELECT id, kind FROM bt_idx_asof AS OF SYSTEM TIME {before} WHERE kind = '{kind}'")
    };
    assert_eq!(
        id_kind_rows(&srv, &as_of("x")).await,
        rows(&[("a", "x")]),
        "AS OF before the UPDATE finds the old value"
    );
    assert!(id_kind_rows(&srv, &as_of("y")).await.is_empty());
    assert_eq!(
        id_kind_rows(&srv, "SELECT id, kind FROM bt_idx_asof WHERE kind = 'y'").await,
        rows(&[("a", "y")])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn as_of_before_the_index_was_created_reads_the_versions() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION bt_idx_late WITH (bitemporal=true)")
        .await
        .unwrap();
    insert(&srv, "bt_idx_late", "a", "x").await;
    insert(&srv, "bt_idx_late", "b", "y").await;
    insert(&srv, "bt_idx_late", "c", "x").await;
    let t1 = instant_between_writes().await;
    srv.exec("UPDATE bt_idx_late SET kind = 'z' WHERE id = 'a'")
        .await
        .unwrap();
    srv.exec("DELETE FROM bt_idx_late WHERE id = 'c'")
        .await
        .unwrap();
    let t2 = instant_between_writes().await;
    insert(&srv, "bt_idx_late", "d", "x").await;
    // The backfill writes each version's entries at that version's time.
    srv.exec("CREATE INDEX bt_idx_late_kind ON bt_idx_late(kind)")
        .await
        .unwrap();

    let as_of = |at: i64, kind: &str| {
        format!("SELECT id, kind FROM bt_idx_late AS OF SYSTEM TIME {at} WHERE kind = '{kind}'")
    };
    assert_eq!(
        id_kind_rows(&srv, &as_of(t1, "x")).await,
        rows(&[("a", "x"), ("c", "x")]),
        "before the update and the delete, a and c hold x"
    );
    assert_eq!(
        id_kind_rows(&srv, &as_of(t1, "y")).await,
        rows(&[("b", "y")])
    );
    assert!(
        id_kind_rows(&srv, &as_of(t2, "x")).await.is_empty(),
        "after the update and the delete, before d, no row holds x"
    );
    assert_eq!(
        id_kind_rows(&srv, &as_of(t2, "z")).await,
        rows(&[("a", "z")])
    );
    assert_eq!(
        id_kind_rows(&srv, "SELECT id, kind FROM bt_idx_late WHERE kind = 'x'").await,
        rows(&[("d", "x")])
    );
}

/// A semi-join whose subquery side is an indexed equality on a bitemporal
/// collection. Its bitmap prefilter reads the versioned index at the
/// statement's cutoff, so the side holds the rows as they stood then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn as_of_join_side_with_an_indexed_equality_reads_the_versions() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION bt_cust WITH (bitemporal=true)")
        .await
        .unwrap();
    srv.exec("CREATE INDEX bt_cust_tier ON bt_cust(tier)")
        .await
        .unwrap();
    srv.exec("CREATE COLLECTION bt_orders WITH (bitemporal=true)")
        .await
        .unwrap();
    srv.exec("INSERT INTO bt_cust { id: 'c1', tier: 'gold' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO bt_cust { id: 'c2', tier: 'silver' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO bt_orders { id: 'o1', cust: 'c1' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO bt_orders { id: 'o2', cust: 'c2' }")
        .await
        .unwrap();
    let before = instant_between_writes().await;
    srv.exec("UPDATE bt_cust SET tier = 'silver' WHERE id = 'c1'")
        .await
        .unwrap();
    srv.exec("UPDATE bt_cust SET tier = 'gold' WHERE id = 'c2'")
        .await
        .unwrap();

    let gold_orders = |as_of: &str| {
        format!(
            "SELECT id, cust FROM bt_orders {as_of} \
             WHERE cust IN (SELECT id FROM bt_cust WHERE tier = 'gold')"
        )
    };
    assert_eq!(
        id_kind_rows(&srv, &gold_orders(&format!("AS OF SYSTEM TIME {before}"))).await,
        rows(&[("o1", "c1")]),
        "before the updates, c1 was the gold customer"
    );
    assert_eq!(
        id_kind_rows(&srv, &gold_orders("")).await,
        rows(&[("o2", "c2")]),
        "now c2 is the gold customer"
    );
}

/// The current rows and the rows `AS OF SYSTEM TIME` `before` of `name`.
async fn reads(
    srv: &TestServer,
    name: &str,
    before: i64,
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let current = id_kind_rows(srv, &format!("SELECT id, kind FROM {name}")).await;
    let as_of = id_kind_rows(
        srv,
        &format!("SELECT id, kind FROM {name} AS OF SYSTEM TIME {before}"),
    )
    .await;
    (current, as_of)
}

/// Seed `a` (`x`), `b` (`y`), `c` (`x`), then update `b` and delete `c`.
/// Returns an instant before the update and the delete.
async fn seed_history(srv: &TestServer, name: &str) -> i64 {
    insert(srv, name, "a", "x").await;
    insert(srv, name, "b", "y").await;
    insert(srv, name, "c", "x").await;
    let before = instant_between_writes().await;
    srv.exec(&format!("UPDATE {name} SET kind = 'z' WHERE id = 'b'"))
        .await
        .unwrap();
    srv.exec(&format!("DELETE FROM {name} WHERE id = 'c'"))
        .await
        .unwrap();
    before
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_strict_keeps_every_version() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION bt_conv_strict WITH (bitemporal=true)")
        .await
        .unwrap();
    let before = seed_history(&srv, "bt_conv_strict").await;
    let expected = reads(&srv, "bt_conv_strict", before).await;
    assert_eq!(expected.0, rows(&[("a", "x"), ("b", "z")]));
    assert_eq!(expected.1, rows(&[("a", "x"), ("b", "y"), ("c", "x")]));

    srv.exec(
        "CONVERT COLLECTION bt_conv_strict TO document_strict \
         (id TEXT PRIMARY KEY, kind TEXT)",
    )
    .await
    .unwrap();
    assert_eq!(reads(&srv, "bt_conv_strict", before).await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_schemaless_keeps_every_version() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION bt_conv_doc (id STRING PRIMARY KEY, kind STRING) \
         WITH (engine='document_strict', bitemporal=true)",
    )
    .await
    .unwrap();
    let before = seed_history(&srv, "bt_conv_doc").await;
    let expected = reads(&srv, "bt_conv_doc", before).await;
    assert_eq!(expected.0, rows(&[("a", "x"), ("b", "z")]));
    assert_eq!(expected.1, rows(&[("a", "x"), ("b", "y"), ("c", "x")]));

    srv.exec("CONVERT COLLECTION bt_conv_doc TO document_schemaless")
        .await
        .unwrap();
    assert_eq!(reads(&srv, "bt_conv_doc", before).await, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn convert_to_kv_is_refused_with_nothing_moved() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION bt_conv_kv WITH (bitemporal=true)")
        .await
        .unwrap();
    let before = seed_history(&srv, "bt_conv_kv").await;
    let expected = reads(&srv, "bt_conv_kv", before).await;

    srv.expect_error(
        "CONVERT COLLECTION bt_conv_kv TO kv (id TEXT PRIMARY KEY, kind TEXT)",
        "a key-value collection keeps no valid time and purges its row versions at the \
         cleanup horizon",
    )
    .await;
    assert_eq!(reads(&srv, "bt_conv_kv", before).await, expected);
}
