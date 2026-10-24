// SPDX-License-Identifier: BUSL-1.1

//! RESTORE carries node labels and their history.
//!
//! A backup holds each node's current label set and every label version. A
//! fresh server restores both. A MATCH with a label predicate reads the
//! current labels. The same MATCH `AS OF SYSTEM TIME` before a label change
//! reads the labels the node carried then. A restart reads the same.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::backup_support::{drain_backup, push_restore};
use crate::harness::TestServer;

const TENANT: u64 = 1;
const COLLECTION: &str = "rnl_people";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

/// Let the wall clock move past the last write's millisecond.
async fn tick() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// The sources of `knows` edges whose source carries `label`, at `as_of`.
async fn labeled(server: &TestServer, label: &str, as_of: Option<i64>) -> Vec<String> {
    let cut = as_of.map_or(String::new(), |ms| format!(" AS OF SYSTEM TIME {ms}"));
    let sql = format!("MATCH (a:{label})-[:knows]->(b) IN '{COLLECTION}'{cut} RETURN a");
    server
        .query_text(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// alice carries `Admin` now and carried `Person` at `cut`.
async fn assert_labels(server: &TestServer, cut: i64, stage: &str) {
    assert_eq!(
        labeled(server, "Admin", None).await,
        vec!["alice".to_string()],
        "{stage}: alice is an Admin now"
    );
    assert!(
        labeled(server, "Person", None).await.is_empty(),
        "{stage}: alice is no Person now"
    );
    assert_eq!(
        labeled(server, "Person", Some(cut)).await,
        vec!["alice".to_string()],
        "{stage}: alice was a Person at the cut"
    );
    assert!(
        labeled(server, "Admin", Some(cut)).await.is_empty(),
        "{stage}: alice was no Admin at the cut"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_node_keeps_its_labels_and_its_label_history() {
    let source = TestServer::start().await;
    for sql in [
        format!("CREATE COLLECTION {COLLECTION} WITH (engine='document_schemaless')"),
        format!("INSERT INTO {COLLECTION} {{ id: 'alice' }}"),
        format!("INSERT INTO {COLLECTION} {{ id: 'bob' }}"),
        format!("GRAPH INSERT EDGE IN '{COLLECTION}' FROM 'alice' TO 'bob' TYPE 'knows'"),
        "GRAPH LABEL 'alice' AS 'Person'".to_string(),
    ] {
        source
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    tick().await;
    let cut = now_ms();
    tick().await;
    for sql in [
        "GRAPH UNLABEL 'alice' AS 'Person'",
        "GRAPH LABEL 'alice' AS 'Admin'",
    ] {
        source
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_labels(&source, cut, "on the source").await;
    let backup = drain_backup(&source.client, TENANT)
        .await
        .expect("take the backup");

    let target = TestServer::start().await;
    push_restore(&target.client, TENANT, backup)
        .await
        .unwrap_or_else(|e| panic!("restore: {e}"));
    assert_labels(&target, cut, "after the restore").await;

    let (target, dir) = target.take_dir();
    target.graceful_shutdown().await;
    let (target, _dir) = TestServer::open_on_path(dir).await;
    assert_labels(&target, cut, "after a restart").await;
}
