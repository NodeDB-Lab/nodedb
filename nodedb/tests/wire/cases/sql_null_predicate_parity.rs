// SPDX-License-Identifier: BUSL-1.1

//! One predicate, one row set — on every execution path.
//!
//! A schemaless document stores an explicitly-NULL field as a null value and
//! an omitted field as no field at all. SQL draws no distinction between the
//! two: `f IS NULL` matches both. The scan, aggregate, DELETE, and UPDATE
//! paths each resolve the field themselves, so each is checked against the
//! same fixture, and against the others.

use crate::harness::TestServer;

/// Two rows carry no value for `note`: one stores an explicit NULL, the other
/// omits the column entirely. A third row carries a real value.
async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (id INT PRIMARY KEY, note TEXT, v TEXT)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, note, v) VALUES (1, NULL, 'explicit-null')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed explicit null: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, v) VALUES (2, 'omitted-field')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed omitted field: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {collection} (id, note, v) VALUES (3, 'present', 'has-value')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed present value: {e}"));
}

/// The single scalar a `count(*)` query answered with.
async fn count_of(server: &TestServer, sql: &str) -> i64 {
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("count query failed: {e}"));
    assert_eq!(rows.len(), 1, "count must answer one row, got: {rows:?}");
    rows[0]
        .trim()
        .parse::<i64>()
        .unwrap_or_else(|e| panic!("count must be an integer, got {:?}: {e}", rows[0]))
}

/// An absent field and an explicit NULL are both NULL, and the aggregate path
/// must agree with the scan path on how many rows that is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_and_count_agree_on_is_null() {
    let server = TestServer::start().await;
    seed(&server, "np_count").await;

    let scanned = server
        .query_text("SELECT v FROM np_count WHERE note IS NULL")
        .await
        .expect("scan IS NULL");
    assert_eq!(
        scanned.len(),
        2,
        "an absent field and an explicit NULL are both NULL: {scanned:?}"
    );

    let counted = count_of(&server, "SELECT count(*) FROM np_count WHERE note IS NULL").await;
    assert_eq!(
        counted, 2,
        "the aggregate path must count the same rows the scan path returns"
    );
}

/// The complement of the same predicate, on the same fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_and_count_agree_on_is_not_null() {
    let server = TestServer::start().await;
    seed(&server, "np_not_null").await;

    let scanned = server
        .query_text("SELECT v FROM np_not_null WHERE note IS NOT NULL")
        .await
        .expect("scan IS NOT NULL");
    assert_eq!(
        scanned.len(),
        1,
        "only the row with a stored value is NOT NULL: {scanned:?}"
    );

    let counted = count_of(
        &server,
        "SELECT count(*) FROM np_not_null WHERE note IS NOT NULL",
    )
    .await;
    assert_eq!(
        counted, 1,
        "the aggregate path must count the same rows the scan path returns"
    );
}

/// The two halves must sum to the collection — a row cannot be neither NULL
/// nor NOT NULL on the path that counts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn is_null_and_is_not_null_counts_partition_the_collection() {
    let server = TestServer::start().await;
    seed(&server, "np_partition").await;

    let total = count_of(&server, "SELECT count(*) FROM np_partition").await;
    let nulls = count_of(
        &server,
        "SELECT count(*) FROM np_partition WHERE note IS NULL",
    )
    .await;
    let non_nulls = count_of(
        &server,
        "SELECT count(*) FROM np_partition WHERE note IS NOT NULL",
    )
    .await;

    assert_eq!(total, 3, "the fixture stores three rows");
    assert_eq!(
        nulls + non_nulls,
        total,
        "IS NULL ({nulls}) and IS NOT NULL ({non_nulls}) must partition the {total} rows"
    );
}

/// DELETE reports the rows it removed. That number is the same predicate's
/// row set, so it must equal what the aggregate path counted beforehand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_where_is_null_removes_exactly_the_counted_rows() {
    let server = TestServer::start().await;
    seed(&server, "np_delete").await;

    let counted = count_of(&server, "SELECT count(*) FROM np_delete WHERE note IS NULL").await;

    server
        .exec("DELETE FROM np_delete WHERE note IS NULL")
        .await
        .expect("delete IS NULL");

    let remaining = server
        .query_text("SELECT v FROM np_delete")
        .await
        .expect("scan after delete");
    assert_eq!(
        remaining,
        vec!["has-value".to_string()],
        "only the row with a stored value survives: {remaining:?}"
    );
    assert_eq!(
        counted,
        3 - remaining.len() as i64,
        "count(*) must have predicted how many rows the DELETE removed"
    );
}

/// UPDATE resolves the same predicate again; every row it touched must be a
/// row the aggregate path counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_where_is_null_touches_exactly_the_counted_rows() {
    let server = TestServer::start().await;
    seed(&server, "np_update").await;

    let counted = count_of(&server, "SELECT count(*) FROM np_update WHERE note IS NULL").await;

    server
        .exec("UPDATE np_update SET v = 'touched' WHERE note IS NULL")
        .await
        .expect("update IS NULL");

    let touched = server
        .query_text("SELECT id FROM np_update WHERE v = 'touched'")
        .await
        .expect("scan touched rows");
    assert_eq!(
        touched.len(),
        2,
        "both the explicit-NULL and the absent-field row are updated: {touched:?}"
    );
    assert_eq!(
        counted,
        touched.len() as i64,
        "count(*) must have predicted how many rows the UPDATE touched"
    );
}

/// A collection with no declared primary key stores rows carrying no `id`
/// field at all. `id IS NULL` is then true of every row, on both paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_and_count_agree_on_is_null_over_the_identity_column() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION np_identity (v TEXT)")
        .await
        .expect("create np_identity");
    server
        .exec("INSERT INTO np_identity (v) VALUES ('first')")
        .await
        .expect("seed first");
    server
        .exec("INSERT INTO np_identity (v) VALUES ('second')")
        .await
        .expect("seed second");

    let scanned = server
        .query_text("SELECT v FROM np_identity WHERE id IS NULL")
        .await
        .expect("scan id IS NULL");
    let counted = count_of(&server, "SELECT count(*) FROM np_identity WHERE id IS NULL").await;

    assert_eq!(
        counted,
        scanned.len() as i64,
        "the aggregate path counted {counted} rows where the scan path returned {}: {scanned:?}",
        scanned.len()
    );
}
