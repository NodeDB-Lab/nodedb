// SPDX-License-Identifier: BUSL-1.1

//! In-transaction DML on a vector-primary collection (`primary='vector'`)
//! is staged at statement time: it answers the real command tag, raises
//! constraint errors at the statement, and is visible to the same
//! transaction's point reads, scans, and vector searches (read-your-own-
//! writes). COMMIT makes it durable; ROLLBACK and `ROLLBACK TO SAVEPOINT`
//! discard it.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in `sql`'s
/// response. A buffered write answers a bare `OK`, which carries no count.
async fn affected(server: &TestServer, sql: &str) -> Option<u64> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

/// The SQLSTATE `sql` fails with.
async fn sqlstate(server: &TestServer, sql: &str) -> String {
    match server.client.simple_query(sql).await {
        Ok(_) => panic!("expected an error from: {sql}"),
        Err(e) => e
            .as_db_error()
            .unwrap_or_else(|| panic!("expected a DbError from: {sql}; got {e:?}"))
            .code()
            .code()
            .to_string(),
    }
}

async fn create(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

fn insert_sql(name: &str, id: &str, vec: &str, owner: &str) -> String {
    format!("INSERT INTO {name} (id, vec, owner) VALUES ('{id}', ARRAY[{vec}], '{owner}')")
}

async fn insert(server: &TestServer, name: &str, id: &str, vec: &str, owner: &str) {
    server
        .exec(&insert_sql(name, id, vec, owner))
        .await
        .unwrap_or_else(|e| panic!("insert {id} into {name}: {e}"));
}

/// Ids in nearest-first order for `query`, with an optional `WHERE`, on the
/// transaction's own connection.
async fn nearest(server: &TestServer, name: &str, query: &str, filter: &str) -> Vec<String> {
    let where_clause = if filter.is_empty() {
        String::new()
    } else {
        format!(" WHERE {filter}")
    };
    server
        .query_rows(&format!(
            "SELECT id FROM {name}{where_clause} \
             ORDER BY vector_distance(vec, ARRAY[{query}]) LIMIT 10"
        ))
        .await
        .unwrap_or_else(|e| panic!("search {name}: {e}"))
        .into_iter()
        .map(|r| r[0].clone())
        .collect()
}

/// `id`, `owner` pairs of the point read `WHERE id = '<id>'`.
async fn point(server: &TestServer, name: &str, id: &str) -> Vec<(String, String)> {
    server
        .query_rows(&format!("SELECT id, owner FROM {name} WHERE id = '{id}'"))
        .await
        .unwrap_or_else(|e| panic!("point read {id} from {name}: {e}"))
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone()))
        .collect()
}

/// Ids of every row a full scan returns, sorted.
async fn scan_ids(server: &TestServer, name: &str) -> Vec<String> {
    let mut ids: Vec<String> = server
        .query_rows(&format!("SELECT id FROM {name}"))
        .await
        .unwrap_or_else(|e| panic!("scan {name}: {e}"))
        .into_iter()
        .map(|r| r[0].clone())
        .collect();
    ids.sort();
    ids
}

fn pair(id: &str, owner: &str) -> Vec<(String, String)> {
    vec![(id.to_string(), owner.to_string())]
}

/// BEGIN; INSERT answers `INSERT 0 1`; the same transaction's point read,
/// scan, and search see the row ranked by true distance; COMMIT keeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_is_visible_in_txn_and_durable_after_commit() {
    let server = TestServer::start().await;
    create(&server, "vpt_ins").await;
    insert(&server, "vpt_ins", "base", "0.0, 0.0, 1.0", "alice").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(
            &server,
            &insert_sql("vpt_ins", "staged", "1.0, 0.0, 0.0", "bob")
        )
        .await,
        Some(1),
        "in-tx INSERT must report 1 row at the statement, not a bare OK"
    );
    assert_eq!(
        point(&server, "vpt_ins", "staged").await,
        pair("staged", "bob")
    );
    assert_eq!(
        scan_ids(&server, "vpt_ins").await,
        vec!["base".to_string(), "staged".to_string()]
    );
    assert_eq!(
        nearest(&server, "vpt_ins", "1.0, 0.0, 0.0", "").await,
        vec!["staged".to_string(), "base".to_string()],
        "the staged vector ranks first by true distance"
    );
    assert_eq!(
        nearest(&server, "vpt_ins", "0.0, 0.0, 1.0", "").await,
        vec!["base".to_string(), "staged".to_string()]
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        point(&server, "vpt_ins", "staged").await,
        pair("staged", "bob")
    );
    assert_eq!(
        nearest(&server, "vpt_ins", "1.0, 0.0, 0.0", "").await,
        vec!["staged".to_string(), "base".to_string()]
    );
}

/// ROLLBACK discards the staged row from the point read, the scan, and the
/// search.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_discards_staged_insert() {
    let server = TestServer::start().await;
    create(&server, "vpt_rb").await;
    insert(&server, "vpt_rb", "base", "0.0, 0.0, 1.0", "alice").await;

    server.exec("BEGIN").await.unwrap();
    insert(&server, "vpt_rb", "staged", "1.0, 0.0, 0.0", "bob").await;
    assert_eq!(
        point(&server, "vpt_rb", "staged").await,
        pair("staged", "bob")
    );
    server.exec("ROLLBACK").await.unwrap();

    assert_eq!(point(&server, "vpt_rb", "staged").await, Vec::new());
    assert_eq!(scan_ids(&server, "vpt_rb").await, vec!["base".to_string()]);
    assert_eq!(
        nearest(&server, "vpt_rb", "1.0, 0.0, 0.0", "").await,
        vec!["base".to_string()]
    );
}

/// A duplicate primary key is refused at the statement with 23505, whether
/// the row lives in base or was staged earlier in the same transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_primary_key_raises_23505_at_the_statement() {
    let server = TestServer::start().await;
    create(&server, "vpt_dup").await;
    insert(&server, "vpt_dup", "r1", "1.0, 0.0, 0.0", "alice").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        sqlstate(
            &server,
            &insert_sql("vpt_dup", "r1", "0.0, 1.0, 0.0", "bob")
        )
        .await,
        "23505",
        "a base row's key is a duplicate inside the transaction"
    );
    server.exec("ROLLBACK").await.unwrap();
    assert_eq!(point(&server, "vpt_dup", "r1").await, pair("r1", "alice"));

    server.exec("BEGIN").await.unwrap();
    insert(&server, "vpt_dup", "r2", "0.0, 1.0, 0.0", "bob").await;
    assert_eq!(
        sqlstate(
            &server,
            &insert_sql("vpt_dup", "r2", "0.0, 0.0, 1.0", "carol")
        )
        .await,
        "23505",
        "a key staged earlier in the transaction is a duplicate"
    );
    server.exec("ROLLBACK").await.unwrap();
    assert_eq!(point(&server, "vpt_dup", "r2").await, Vec::new());
    assert_eq!(
        nearest(&server, "vpt_dup", "1.0, 0.0, 0.0", "").await,
        vec!["r1".to_string()],
        "the refused inserts must write nothing"
    );
}

/// `ON CONFLICT DO NOTHING` on a staged key reports 0 and keeps the staged
/// row; `UPSERT` of a staged key reports `UPSERT 1` and the last value wins
/// in the transaction and at COMMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_of_a_staged_key_replaces_and_last_value_wins() {
    let server = TestServer::start().await;
    create(&server, "vpt_ups").await;

    server.exec("BEGIN").await.unwrap();
    insert(&server, "vpt_ups", "r1", "1.0, 0.0, 0.0", "alice").await;
    assert_eq!(
        affected(
            &server,
            "INSERT INTO vpt_ups (id, vec, owner) VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'bob') \
             ON CONFLICT DO NOTHING",
        )
        .await,
        Some(0)
    );
    assert_eq!(point(&server, "vpt_ups", "r1").await, pair("r1", "alice"));
    assert_eq!(
        affected(
            &server,
            "UPSERT INTO vpt_ups (id, vec, owner) VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'carol')",
        )
        .await,
        Some(1)
    );
    assert_eq!(point(&server, "vpt_ups", "r1").await, pair("r1", "carol"));
    assert_eq!(
        nearest(&server, "vpt_ups", "0.0, 1.0, 0.0", "").await,
        vec!["r1".to_string()],
        "one row, at its replaced vector"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(point(&server, "vpt_ups", "r1").await, pair("r1", "carol"));
    assert_eq!(
        nearest(&server, "vpt_ups", "0.0, 1.0, 0.0", "owner = 'carol'").await,
        vec!["r1".to_string()]
    );
    assert_eq!(
        nearest(&server, "vpt_ups", "0.0, 1.0, 0.0", "owner = 'alice'").await,
        Vec::<String>::new(),
        "the replaced payload must not survive COMMIT"
    );
}

/// `ON CONFLICT (id) DO UPDATE SET` inside the transaction merges into the
/// base row and answers the resolved `UPDATE 1` tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_do_update_patches_the_base_row_in_txn() {
    let server = TestServer::start().await;
    create(&server, "vpt_patch").await;
    insert(&server, "vpt_patch", "r1", "1.0, 0.0, 0.0", "alice").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(
            &server,
            "INSERT INTO vpt_patch (id, vec, owner) VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'bob') \
             ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner",
        )
        .await,
        Some(1)
    );
    assert_eq!(point(&server, "vpt_patch", "r1").await, pair("r1", "bob"));
    server.exec("COMMIT").await.unwrap();
    assert_eq!(point(&server, "vpt_patch", "r1").await, pair("r1", "bob"));
    assert_eq!(
        nearest(&server, "vpt_patch", "0.0, 1.0, 0.0", "owner = 'bob'").await,
        vec!["r1".to_string()]
    );
}

/// A same-transaction DELETE hides a base row from the point read, the
/// scan, and the search, reports 1, and a re-delete reports 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_hides_the_base_row_in_txn() {
    let server = TestServer::start().await;
    create(&server, "vpt_del").await;
    insert(&server, "vpt_del", "r1", "1.0, 0.0, 0.0", "alice").await;
    insert(&server, "vpt_del", "r2", "0.0, 1.0, 0.0", "bob").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(&server, "DELETE FROM vpt_del WHERE id = 'r1'").await,
        Some(1)
    );
    assert_eq!(
        affected(&server, "DELETE FROM vpt_del WHERE id = 'r1'").await,
        Some(0),
        "a re-delete inside the transaction affects nothing"
    );
    assert_eq!(point(&server, "vpt_del", "r1").await, Vec::new());
    assert_eq!(scan_ids(&server, "vpt_del").await, vec!["r2".to_string()]);
    assert_eq!(
        nearest(&server, "vpt_del", "1.0, 0.0, 0.0", "").await,
        vec!["r2".to_string()],
        "the staged tombstone hides the base node"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(point(&server, "vpt_del", "r1").await, Vec::new());
    assert_eq!(
        nearest(&server, "vpt_del", "1.0, 0.0, 0.0", "").await,
        vec!["r2".to_string()]
    );
}

/// A predicate DELETE counts base rows and rows staged earlier in the same
/// transaction, and a staged row is deletable by its key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn predicate_delete_counts_base_and_staged_rows() {
    let server = TestServer::start().await;
    create(&server, "vpt_pdel").await;
    insert(&server, "vpt_pdel", "r1", "1.0, 0.0, 0.0", "alice").await;
    insert(&server, "vpt_pdel", "r2", "0.0, 0.0, 1.0", "bob").await;

    server.exec("BEGIN").await.unwrap();
    insert(&server, "vpt_pdel", "r3", "0.0, 1.0, 0.0", "alice").await;
    assert_eq!(
        affected(&server, "DELETE FROM vpt_pdel WHERE owner = 'alice'").await,
        Some(2),
        "one base row and one staged row match"
    );
    assert_eq!(scan_ids(&server, "vpt_pdel").await, vec!["r2".to_string()]);
    assert_eq!(
        nearest(&server, "vpt_pdel", "0.0, 1.0, 0.0", "").await,
        vec!["r2".to_string()]
    );
    server.exec("COMMIT").await.unwrap();
    assert_eq!(scan_ids(&server, "vpt_pdel").await, vec!["r2".to_string()]);
}

/// UPDATE of the vector inside the transaction re-ranks the search; a
/// payload update moves the row across a payload filter; both report
/// `UPDATE 1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_re_ranks_and_moves_payload_in_txn() {
    let server = TestServer::start().await;
    create(&server, "vpt_upd").await;
    insert(&server, "vpt_upd", "r1", "1.0, 0.0, 0.0", "alice").await;
    insert(&server, "vpt_upd", "r2", "0.0, 1.0, 0.0", "bob").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(
            &server,
            "UPDATE vpt_upd SET vec = ARRAY[0.0, 0.0, 1.0] WHERE id = 'r1'"
        )
        .await,
        Some(1)
    );
    assert_eq!(
        nearest(&server, "vpt_upd", "0.0, 0.0, 1.0", "").await,
        vec!["r1".to_string(), "r2".to_string()],
        "the re-embedded row ranks at its new vector"
    );
    assert_eq!(
        affected(
            &server,
            "UPDATE vpt_upd SET owner = 'carol' WHERE id = 'r1'"
        )
        .await,
        Some(1)
    );
    assert_eq!(point(&server, "vpt_upd", "r1").await, pair("r1", "carol"));
    assert_eq!(
        nearest(&server, "vpt_upd", "0.0, 0.0, 1.0", "owner = 'carol'").await,
        vec!["r1".to_string()]
    );
    assert_eq!(
        nearest(&server, "vpt_upd", "0.0, 0.0, 1.0", "owner = 'alice'").await,
        Vec::<String>::new(),
        "the staged payload no longer matches the old owner"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(point(&server, "vpt_upd", "r1").await, pair("r1", "carol"));
    assert_eq!(
        nearest(&server, "vpt_upd", "0.0, 0.0, 1.0", "").await,
        vec!["r1".to_string(), "r2".to_string()]
    );
}

/// A payload filter excludes a staged row of another owner and admits a
/// staged row of the filtered owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_filter_applies_to_staged_rows() {
    let server = TestServer::start().await;
    create(&server, "vpt_filt").await;
    insert(&server, "vpt_filt", "base", "0.0, 0.0, 1.0", "alice").await;

    server.exec("BEGIN").await.unwrap();
    insert(&server, "vpt_filt", "other", "1.0, 0.0, 0.0", "bob").await;
    insert(&server, "vpt_filt", "same", "0.9, 0.1, 0.0", "alice").await;
    assert_eq!(
        nearest(&server, "vpt_filt", "1.0, 0.0, 0.0", "owner = 'alice'").await,
        vec!["same".to_string(), "base".to_string()],
        "the staged row of another owner is excluded"
    );
    assert_eq!(
        nearest(&server, "vpt_filt", "1.0, 0.0, 0.0", "owner = 'bob'").await,
        vec!["other".to_string()]
    );
    server.exec("ROLLBACK").await.unwrap();
}

/// `ROLLBACK TO SAVEPOINT` reverts a staged vector insert made after the
/// savepoint and keeps one made before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_to_savepoint_reverts_staged_insert() {
    let server = TestServer::start().await;
    create(&server, "vpt_sp").await;

    server.exec("BEGIN").await.unwrap();
    insert(&server, "vpt_sp", "before", "1.0, 0.0, 0.0", "alice").await;
    server.exec("SAVEPOINT s1").await.unwrap();
    insert(&server, "vpt_sp", "after", "0.0, 1.0, 0.0", "bob").await;
    assert_eq!(
        scan_ids(&server, "vpt_sp").await,
        vec!["after".to_string(), "before".to_string()]
    );
    server.exec("ROLLBACK TO SAVEPOINT s1").await.unwrap();

    assert_eq!(
        point(&server, "vpt_sp", "before").await,
        pair("before", "alice")
    );
    assert_eq!(point(&server, "vpt_sp", "after").await, Vec::new());
    assert_eq!(
        nearest(&server, "vpt_sp", "0.0, 1.0, 0.0", "").await,
        vec!["before".to_string()],
        "the post-savepoint vector must not rank after ROLLBACK TO"
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        scan_ids(&server, "vpt_sp").await,
        vec!["before".to_string()]
    );
}

/// Statement tags inside the transaction carry real counts: `INSERT 0 1`,
/// `UPDATE 1`, `DELETE 1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn statement_tags_carry_real_counts_in_txn() {
    let server = TestServer::start().await;
    create(&server, "vpt_tags").await;
    insert(&server, "vpt_tags", "r1", "1.0, 0.0, 0.0", "alice").await;

    server.exec("BEGIN").await.unwrap();
    assert_eq!(
        affected(
            &server,
            &insert_sql("vpt_tags", "r2", "0.0, 1.0, 0.0", "bob")
        )
        .await,
        Some(1)
    );
    assert_eq!(
        affected(
            &server,
            "UPDATE vpt_tags SET owner = 'carol' WHERE id = 'r1'"
        )
        .await,
        Some(1)
    );
    assert_eq!(
        affected(&server, "DELETE FROM vpt_tags WHERE id = 'r2'").await,
        Some(1)
    );
    assert_eq!(
        affected(&server, "DELETE FROM vpt_tags WHERE id = 'missing'").await,
        Some(0)
    );
    server.exec("COMMIT").await.unwrap();

    assert_eq!(point(&server, "vpt_tags", "r1").await, pair("r1", "carol"));
    assert_eq!(point(&server, "vpt_tags", "r2").await, Vec::new());
}
