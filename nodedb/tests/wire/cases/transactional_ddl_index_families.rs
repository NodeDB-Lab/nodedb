// SPDX-License-Identifier: BUSL-1.1

//! Index DDL inside an explicit transaction, one family at a time: document
//! `CREATE`/`DROP INDEX`, KV `CREATE`/`DROP SORTED INDEX`, and
//! `CREATE`/`ALTER`/`DROP VECTOR INDEX`.
//!
//! Each family is visible to later statements of its own transaction,
//! applies at COMMIT, and leaves nothing behind on ROLLBACK: neither the
//! catalog rows nor the engine state (a backfilled index, a sorted-index
//! tree, a declared vector dimension).

use crate::harness::TestServer;

async fn indexes(server: &TestServer) -> Vec<String> {
    server.query_text("SHOW INDEXES").await.unwrap()
}

async fn document_collection(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, region TEXT) \
             WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {name} (id, region) VALUES ('a', 'eu')"
        ))
        .await
        .unwrap();
}

async fn eu_rows(server: &TestServer, name: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT id FROM {name} WHERE region = 'eu'"))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_index_created_in_a_transaction_is_visible_then_committed() {
    let server = TestServer::start().await;
    document_collection(&server, "txn_fam_doc_c").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE INDEX txn_fam_doc_idx ON txn_fam_doc_c(region)")
        .await
        .unwrap();
    assert!(
        indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_doc_idx"),
        "the transaction lists its own index"
    );
    server.exec("COMMIT").await.unwrap();

    assert!(
        indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_doc_idx")
    );
    assert_eq!(
        eu_rows(&server, "txn_fam_doc_c").await,
        vec!["a".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_index_rolled_back_leaves_nothing() {
    let server = TestServer::start().await;
    document_collection(&server, "txn_fam_doc_rb").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE INDEX txn_fam_doc_rb_idx ON txn_fam_doc_rb(region)")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    assert!(
        !indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_doc_rb_idx")
    );
    // The name and the engine slot are free: the same index builds again.
    server
        .exec("CREATE INDEX txn_fam_doc_rb_idx ON txn_fam_doc_rb(region)")
        .await
        .expect("a rolled-back CREATE INDEX leaves the name free");
    assert_eq!(
        eu_rows(&server, "txn_fam_doc_rb").await,
        vec!["a".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_index_dropped_then_rolled_back_keeps_its_entries() {
    let server = TestServer::start().await;
    document_collection(&server, "txn_fam_doc_drop").await;
    server
        .exec("CREATE INDEX txn_fam_doc_drop_idx ON txn_fam_doc_drop(region)")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DROP INDEX txn_fam_doc_drop_idx")
        .await
        .unwrap();
    assert!(
        !indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_doc_drop_idx"),
        "the transaction no longer lists the index it dropped"
    );
    server.exec("ROLLBACK").await.unwrap();

    assert!(
        indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_doc_drop_idx")
    );
    assert_eq!(
        eu_rows(&server, "txn_fam_doc_drop").await,
        vec!["a".to_string()],
        "the index keeps its entries: the purge never ran"
    );
}

async fn kv_board(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id STRING PRIMARY KEY, score INT) WITH (engine='kv')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("INSERT INTO {name} {{ id: 'p1', score: 10 }}"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorted_index_created_in_a_transaction_is_visible_then_committed() {
    let server = TestServer::start().await;
    kv_board(&server, "txn_fam_sorted").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE SORTED INDEX txn_fam_sorted_idx ON txn_fam_sorted (score DESC) KEY id")
        .await
        .unwrap();
    assert!(
        indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_sorted_idx")
    );
    server.exec("COMMIT").await.unwrap();

    server
        .query_text("SELECT SORTED_COUNT(txn_fam_sorted_idx)")
        .await
        .expect("the committed sorted index has its tree");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorted_index_rolled_back_leaves_nothing() {
    let server = TestServer::start().await;
    kv_board(&server, "txn_fam_sorted_rb").await;

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE SORTED INDEX txn_fam_sorted_rb_idx ON txn_fam_sorted_rb (score DESC) KEY id")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    assert!(
        !indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_sorted_rb_idx")
    );
    server
        .expect_error(
            "SELECT SORTED_COUNT(txn_fam_sorted_rb_idx)",
            "does not exist",
        )
        .await;
    server
        .exec("CREATE SORTED INDEX txn_fam_sorted_rb_idx ON txn_fam_sorted_rb (score DESC) KEY id")
        .await
        .expect("a rolled-back CREATE SORTED INDEX leaves no tree behind");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sorted_index_dropped_then_rolled_back_keeps_its_tree() {
    let server = TestServer::start().await;
    kv_board(&server, "txn_fam_sorted_drop").await;
    server
        .exec("CREATE SORTED INDEX txn_fam_sorted_drop_idx ON txn_fam_sorted_drop (score DESC) KEY id")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DROP SORTED INDEX txn_fam_sorted_drop_idx")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    server
        .query_text("SELECT SORTED_COUNT(txn_fam_sorted_drop_idx)")
        .await
        .expect("the tree survives a rolled-back DROP SORTED INDEX");
}

async fn insert_vector(server: &TestServer, coll: &str, id: &str, v: &[f32]) -> Result<(), String> {
    let arr = v
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",");
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, embedding) VALUES ('{id}', ARRAY[{arr}])"
        ))
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_index_created_and_altered_in_a_transaction_commits() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION txn_fam_vec").await.unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX txn_fam_vec_idx ON txn_fam_vec METRIC l2 DIM 3")
        .await
        .unwrap();
    server
        .exec("ALTER VECTOR INDEX txn_fam_vec_idx ON txn_fam_vec SET (m = 32)")
        .await
        .expect("ALTER resolves the index this transaction created");
    server.exec("COMMIT").await.unwrap();

    assert!(
        indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_vec_idx")
    );
    insert_vector(&server, "txn_fam_vec", "v1", &[1.0, 0.0, 0.0])
        .await
        .expect("a vector of the committed dimension inserts");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_index_rolled_back_leaves_no_declared_dimension() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION txn_fam_vec_rb")
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX txn_fam_vec_rb_idx ON txn_fam_vec_rb METRIC l2 DIM 3")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    assert!(
        !indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_vec_rb_idx")
    );
    // A 5-wide vector would be refused against a lingering DIM 3 declaration.
    insert_vector(&server, "txn_fam_vec_rb", "v1", &[1.0, 0.0, 0.0, 0.0, 0.0])
        .await
        .expect("the rolled-back index declared no dimension");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_index_dropped_then_rolled_back_still_serves() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION txn_fam_vec_drop")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX txn_fam_vec_drop_idx ON txn_fam_vec_drop METRIC l2 DIM 3")
        .await
        .unwrap();
    insert_vector(&server, "txn_fam_vec_drop", "v1", &[1.0, 0.0, 0.0])
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("DROP VECTOR INDEX txn_fam_vec_drop_idx")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();

    assert!(
        indexes(&server)
            .await
            .iter()
            .any(|n| n == "txn_fam_vec_drop_idx")
    );
    let nearest = server
        .query_text(
            "SELECT id FROM txn_fam_vec_drop \
             ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0]) LIMIT 1",
        )
        .await
        .unwrap();
    assert_eq!(nearest, vec!["v1".to_string()]);
}
