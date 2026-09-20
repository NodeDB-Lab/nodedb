// SPDX-License-Identifier: BUSL-1.1

//! DML correctness on a vector-primary collection (`primary='vector'`).
//!
//! A vector-primary row is keyed by its declared primary key: the key binds
//! the surrogate, the surrogate keys the HNSW node and the payload sidecar.
//! `INSERT` refuses a duplicate key, `ON CONFLICT DO NOTHING` skips it,
//! `UPSERT` / `ON CONFLICT DO UPDATE` replace it, and `DELETE` / `UPDATE`
//! reach the node, the payload bitmaps, and the sidecar together — so a
//! search, a payload pre-filter, and a point read all agree afterwards.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Every `CommandComplete` count in `sql`'s response, in wire order.
async fn command_tags(server: &TestServer, sql: &str) -> Vec<u64> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(n),
            _ => None,
        })
        .collect()
}

/// The row count of the single command tag `sql` answers with.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let tags = command_tags(server, sql).await;
    assert_eq!(
        tags.len(),
        1,
        "one statement answers one tag: {sql} -> {tags:?}"
    );
    tags[0]
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

async fn insert(server: &TestServer, name: &str, id: &str, vec: &str, owner: &str) {
    server
        .exec(&format!(
            "INSERT INTO {name} (id, vec, owner) VALUES ('{id}', ARRAY[{vec}], '{owner}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id} into {name}: {e}"));
}

/// Ids in nearest-first order for `query`, with an optional `WHERE`.
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

/// A row keys on its primary key: the point read finds it, and a duplicate
/// key is refused with `unique_violation` while the stored vector still wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_primary_key_insert_raises_unique_violation() {
    let server = TestServer::start().await;
    create(&server, "vp_dup").await;
    insert(&server, "vp_dup", "r1", "1.0, 0.0, 0.0", "alice").await;
    assert_eq!(
        point(&server, "vp_dup", "r1").await,
        vec![("r1".to_string(), "alice".to_string())],
        "the point read must resolve the row by its primary key"
    );

    let code = sqlstate(
        &server,
        "INSERT INTO vp_dup (id, vec, owner) VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'bob')",
    )
    .await;
    assert_eq!(code, "23505");

    assert_eq!(
        nearest(&server, "vp_dup", "1.0, 0.0, 0.0", "").await,
        vec!["r1".to_string()],
        "exactly one row, the original, is searchable"
    );
    assert_eq!(
        point(&server, "vp_dup", "r1").await,
        vec![("r1".to_string(), "alice".to_string())],
        "the refused insert must not touch the stored row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_do_nothing_skips_the_existing_row() {
    let server = TestServer::start().await;
    create(&server, "vp_skip").await;
    insert(&server, "vp_skip", "r1", "1.0, 0.0, 0.0", "alice").await;

    let count = affected(
        &server,
        "INSERT INTO vp_skip (id, vec, owner) VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'bob') \
         ON CONFLICT DO NOTHING",
    )
    .await;
    assert_eq!(count, 0);
    assert_eq!(
        point(&server, "vp_skip", "r1").await,
        vec![("r1".to_string(), "alice".to_string())]
    );
    assert_eq!(
        nearest(&server, "vp_skip", "0.0, 1.0, 0.0", "").await,
        vec!["r1".to_string()],
        "one live node, the original"
    );
}

/// `UPSERT INTO` replaces the row: the new vector ranks, the old node is
/// gone, and the payload reads back as replaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_replaces_the_vector_and_the_payload() {
    let server = TestServer::start().await;
    create(&server, "vp_upsert").await;
    insert(&server, "vp_upsert", "r1", "1.0, 0.0, 0.0", "alice").await;
    insert(&server, "vp_upsert", "r2", "0.0, 0.0, 1.0", "carol").await;

    let count = affected(
        &server,
        "UPSERT INTO vp_upsert (id, vec, owner) VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'bob')",
    )
    .await;
    assert_eq!(count, 1);

    assert_eq!(
        nearest(&server, "vp_upsert", "0.0, 1.0, 0.0", "").await,
        vec!["r1".to_string(), "r2".to_string()],
        "the new vector ranks first"
    );
    // Near the OLD vector, r2 (orthogonal to both) must rank ahead of nothing
    // but r1's new position: the old node is gone, so the result set still
    // holds exactly two ids and r1 does not come back as an exact hit.
    let near_old = nearest(&server, "vp_upsert", "1.0, 0.0, 0.0", "").await;
    assert_eq!(near_old.len(), 2, "no leaked node: {near_old:?}");
    assert_eq!(
        point(&server, "vp_upsert", "r1").await,
        vec![("r1".to_string(), "bob".to_string())]
    );
    assert_eq!(
        nearest(&server, "vp_upsert", "0.0, 1.0, 0.0", "owner = 'alice'").await,
        Vec::<String>::new(),
        "the old payload bitmap entry must be gone"
    );
    assert_eq!(
        nearest(&server, "vp_upsert", "0.0, 1.0, 0.0", "owner = 'bob'").await,
        vec!["r1".to_string()]
    );
}

/// `ON CONFLICT (id) DO UPDATE SET` patches the payload and replaces the
/// vector with the proposed row's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_do_update_patches_the_payload() {
    let server = TestServer::start().await;
    create(&server, "vp_patch").await;
    insert(&server, "vp_patch", "r1", "1.0, 0.0, 0.0", "alice").await;

    server
        .exec(
            "INSERT INTO vp_patch (id, vec, owner) VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'bob') \
             ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner",
        )
        .await
        .expect("ON CONFLICT DO UPDATE on a vector-primary collection");

    assert_eq!(
        point(&server, "vp_patch", "r1").await,
        vec![("r1".to_string(), "bob".to_string())]
    );
    assert_eq!(
        nearest(&server, "vp_patch", "0.0, 1.0, 0.0", "owner = 'bob'").await,
        vec!["r1".to_string()],
        "the vector is replaced and the bitmap follows the patched payload"
    );
    assert_eq!(
        nearest(&server, "vp_patch", "1.0, 0.0, 0.0", "")
            .await
            .len(),
        1,
        "one live node after the patch"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn point_delete_removes_node_sidecar_and_bitmap() {
    let server = TestServer::start().await;
    create(&server, "vp_del").await;
    insert(&server, "vp_del", "r1", "1.0, 0.0, 0.0", "alice").await;
    insert(&server, "vp_del", "r2", "0.0, 1.0, 0.0", "bob").await;

    assert_eq!(
        affected(&server, "DELETE FROM vp_del WHERE id = 'r1'").await,
        1
    );
    assert_eq!(
        nearest(&server, "vp_del", "1.0, 0.0, 0.0", "").await,
        vec!["r2".to_string()],
        "the deleted node must not score"
    );
    assert_eq!(
        nearest(&server, "vp_del", "1.0, 0.0, 0.0", "owner = 'alice'").await,
        Vec::<String>::new(),
        "the deleted row's bitmap entry must be gone"
    );
    assert_eq!(point(&server, "vp_del", "r1").await, Vec::new());
    assert_eq!(
        server
            .query_rows("SELECT id FROM vp_del")
            .await
            .expect("scan")
            .len(),
        1,
        "the sidecar row must be gone from the scan"
    );
    assert_eq!(
        affected(&server, "DELETE FROM vp_del WHERE id = 'r1'").await,
        0,
        "a re-delete affects nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn predicate_delete_removes_only_matching_rows() {
    let server = TestServer::start().await;
    create(&server, "vp_pdel").await;
    insert(&server, "vp_pdel", "r1", "1.0, 0.0, 0.0", "alice").await;
    insert(&server, "vp_pdel", "r2", "0.0, 1.0, 0.0", "alice").await;
    insert(&server, "vp_pdel", "r3", "0.0, 0.0, 1.0", "bob").await;

    assert_eq!(
        affected(&server, "DELETE FROM vp_pdel WHERE owner = 'alice'").await,
        2
    );
    assert_eq!(
        nearest(&server, "vp_pdel", "1.0, 0.0, 0.0", "").await,
        vec!["r3".to_string()]
    );
    assert_eq!(
        point(&server, "vp_pdel", "r3").await,
        vec![("r3".to_string(), "bob".to_string())]
    );
    assert_eq!(point(&server, "vp_pdel", "r1").await, Vec::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_of_the_vector_re_ranks_the_row() {
    let server = TestServer::start().await;
    create(&server, "vp_upd").await;
    insert(&server, "vp_upd", "r1", "1.0, 0.0, 0.0", "alice").await;
    insert(&server, "vp_upd", "r2", "0.0, 1.0, 0.0", "bob").await;
    assert_eq!(
        nearest(&server, "vp_upd", "0.0, 0.0, 1.0", "").await.len(),
        2
    );

    assert_eq!(
        affected(
            &server,
            "UPDATE vp_upd SET vec = ARRAY[0.0, 0.0, 1.0] WHERE id = 'r1'"
        )
        .await,
        1
    );
    let ranked = nearest(&server, "vp_upd", "0.0, 0.0, 1.0", "").await;
    assert_eq!(ranked, vec!["r1".to_string(), "r2".to_string()]);
    assert_eq!(
        nearest(&server, "vp_upd", "0.0, 0.0, 1.0", "owner = 'alice'").await,
        vec!["r1".to_string()],
        "the payload bitmap survives a re-embed"
    );
    assert_eq!(
        point(&server, "vp_upd", "r1").await,
        vec![("r1".to_string(), "alice".to_string())]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_of_a_payload_field_moves_the_prefilter() {
    let server = TestServer::start().await;
    create(&server, "vp_pupd").await;
    insert(&server, "vp_pupd", "r1", "1.0, 0.0, 0.0", "alice").await;
    insert(&server, "vp_pupd", "r2", "0.0, 1.0, 0.0", "bob").await;

    assert_eq!(
        affected(
            &server,
            "UPDATE vp_pupd SET owner = 'carol' WHERE id = 'r1'"
        )
        .await,
        1
    );
    assert_eq!(
        nearest(&server, "vp_pupd", "1.0, 0.0, 0.0", "owner = 'carol'").await,
        vec!["r1".to_string()]
    );
    assert_eq!(
        nearest(&server, "vp_pupd", "1.0, 0.0, 0.0", "owner = 'alice'").await,
        Vec::<String>::new(),
        "the old bitmap entry must be gone"
    );
    assert_eq!(
        nearest(&server, "vp_pupd", "1.0, 0.0, 0.0", "").await,
        vec!["r1".to_string(), "r2".to_string()],
        "the vector is unchanged"
    );
    assert_eq!(
        point(&server, "vp_pupd", "r1").await,
        vec![("r1".to_string(), "carol".to_string())]
    );

    // A predicate update reaches every matching row.
    assert_eq!(
        affected(
            &server,
            "UPDATE vp_pupd SET owner = 'dave' WHERE owner = 'bob'"
        )
        .await,
        1
    );
    assert_eq!(
        nearest(&server, "vp_pupd", "0.0, 1.0, 0.0", "owner = 'dave'").await,
        vec!["r2".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_from_and_merge_are_refused() {
    let server = TestServer::start().await;
    create(&server, "vp_shape").await;
    insert(&server, "vp_shape", "r1", "1.0, 0.0, 0.0", "alice").await;
    server
        .exec("CREATE COLLECTION vp_src (id TEXT PRIMARY KEY, owner TEXT)")
        .await
        .expect("create source");
    server
        .exec("INSERT INTO vp_src (id, owner) VALUES ('r1', 'zed')")
        .await
        .expect("insert source");

    server
        .expect_error(
            "UPDATE vp_shape SET owner = vp_src.owner FROM vp_src WHERE vp_shape.id = vp_src.id",
            "not supported on vector-primary collection",
        )
        .await;
    server
        .expect_error(
            "MERGE INTO vp_shape t USING vp_src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET owner = s.owner",
            "not supported on vector-primary collection",
        )
        .await;
    assert_eq!(
        point(&server, "vp_shape", "r1").await,
        vec![("r1".to_string(), "alice".to_string())],
        "a refused statement must not touch the row"
    );
}
