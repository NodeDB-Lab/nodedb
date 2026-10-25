// SPDX-License-Identifier: BUSL-1.1

//! A bitemporal version written outside Calvin has one system time on every
//! replica.
//!
//! A single-key autocommit write replicates through its data group's Raft
//! log. The group's leader stamps the entry with an ordinal of its clock as
//! it admits it, and every replica installs that ordinal. Each node's own
//! stored document version keys and columnar `_ts_system` cells then agree
//! ordinal for ordinal. After the leader stops, the new leader answers an
//! `AS OF` at each version's system time with the rows the old leader
//! answered.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb::types::TenantDataSnapshot;
use nodedb_types::{DatabaseId, TenantId};

use crate::common::cluster_harness::{TestCluster, TestClusterNode, read_once_a_leader_exists};

const DOCS: &str = "brs_docs";
const COLS: &str = "brs_cols";
const UPDATED: &str = "brs_updated";
const DELETED: &str = "brs_deleted";
const TENANT: u64 = 1;

/// Every cell of every row `sql` returns through `node`, one string per row,
/// sorted.
pub(super) async fn rows(node: &TestClusterNode, sql: &str) -> Vec<String> {
    let messages = read_once_a_leader_exists(
        sql,
        Duration::from_secs(30),
        Duration::from_millis(100),
        || node.client.simple_query(sql),
    )
    .await;
    let mut out: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("NULL"))
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// Every version key of `DOCS` the cores of `node` store. The key ends in
/// the version's raw system-time ordinal.
fn doc_version_keys(snapshots: &[TenantDataSnapshot]) -> BTreeSet<String> {
    let marker = format!(":{DOCS}:");
    snapshots
        .iter()
        .flat_map(|snap| snap.documents_versioned.iter().map(|(key, _)| key))
        .filter(|key| key.contains(&marker))
        .cloned()
        .collect()
}

/// Every row of columnar collection `coll` the cores of `node` hold in
/// their memtables, `_ts_system` ordinal included.
fn columnar_rows(snapshots: &[TenantDataSnapshot], coll: &str) -> BTreeSet<String> {
    let suffix = format!(":{coll}");
    let mut out = BTreeSet::new();
    for (key, bytes) in snapshots
        .iter()
        .flat_map(|snap| snap.columnar_engines.iter())
        .filter(|(key, _)| key.ends_with(&suffix))
    {
        let snap: nodedb_columnar::ColumnarEngineSnapshot =
            zerompk::from_msgpack(bytes).unwrap_or_else(|e| panic!("{key}: decode: {e}"));
        let (engine, _segments, _surrogates) = nodedb_columnar::MutationEngine::from_snapshot(snap)
            .unwrap_or_else(|e| panic!("{key}: rebuild: {e}"));
        let ts_idx = engine
            .schema()
            .ts_system_idx()
            .expect("a bitemporal schema has _ts_system");
        for scanned in engine.scan_memtable_rows_with_surrogates() {
            let (_, values) = scanned.unwrap_or_else(|e| panic!("{key}: row: {e}"));
            let ts = match values.get(ts_idx) {
                Some(nodedb_types::Value::Integer(ts)) => *ts,
                other => panic!("{key}: no _ts_system ordinal: {other:?}"),
            };
            out.insert(format!("{values:?} ts={ts}"));
        }
    }
    out
}

/// The audit log of `coll` through `node`, and for each distinct system
/// time in it the rows an `AS OF` read at it returns. The audit log of a
/// bitemporal columnar collection holds its tombstone versions too.
pub(super) async fn cut_view(
    node: &TestClusterNode,
    coll: &str,
    cols: &str,
) -> Vec<(String, Vec<String>)> {
    let stamps: BTreeSet<String> = rows(
        node,
        &format!("SELECT _ts_system FROM {coll} AS OF SYSTEM TIME NULL"),
    )
    .await
    .into_iter()
    .collect();
    let mut view = Vec::with_capacity(stamps.len());
    for stamp in stamps {
        let at = rows(
            node,
            &format!("SELECT {cols} FROM {coll} AS OF SYSTEM TIME {stamp}"),
        )
        .await;
        view.push((stamp, at));
    }
    view
}

/// The node id of the leader of `coll`'s data group, as `node` sees it.
fn group_leader(node: &TestClusterNode, coll: &str) -> u64 {
    let vshard = nodedb_cluster::routing::vshard_for_collection(
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, coll),
    );
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let group = routing.group_for_vshard(vshard).expect("vShard group");
    routing
        .group_info(group)
        .map(|info| info.leader)
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_version_written_outside_calvin_has_one_system_time_on_every_replica() {
    let mut cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    for ddl in [
        format!(
            "CREATE COLLECTION {DOCS} (id STRING PRIMARY KEY, value STRING) \
             WITH (engine='document_schemaless', bitemporal=true)"
        ),
        format!(
            "CREATE COLLECTION {COLS} (id BIGINT PRIMARY KEY, v BIGINT) \
             WITH (engine='columnar', bitemporal=true)"
        ),
    ] {
        cluster
            .exec_ddl_on_any_leader(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }

    // Single-key autocommit writes from a node that need not lead either
    // group: each replicates as one data-group entry.
    let writer = &cluster.nodes[1];
    for sql in [
        format!("INSERT INTO {DOCS} (id, value) VALUES ('k1', 'draft')"),
        format!("INSERT INTO {COLS} (id, v) VALUES (1, 10)"),
        format!("UPDATE {DOCS} SET value = 'final' WHERE id = 'k1'"),
        // A declared primary key refuses a second INSERT of a current key,
        // bitemporal or not: the second version comes from an UPDATE.
        format!("UPDATE {COLS} SET v = 20 WHERE id = 1"),
        format!("INSERT INTO {DOCS} (id, value) VALUES ('k2', 'other')"),
    ] {
        writer
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    // Each replica's own stored versions carry the same raw ordinals.
    let mut doc_keys = Vec::new();
    let mut col_rows = Vec::new();
    for node in &cluster.nodes {
        let snapshots = node.stored_tenant_snapshots(TenantId::new(TENANT)).await;
        doc_keys.push((node.node_id, doc_version_keys(&snapshots)));
        col_rows.push((node.node_id, columnar_rows(&snapshots, COLS)));
    }
    let (first_node, first_docs) = &doc_keys[0];
    assert_eq!(
        first_docs.len(),
        3,
        "node {first_node}: three document versions"
    );
    for (node_id, keys) in &doc_keys[1..] {
        assert_eq!(
            keys, first_docs,
            "node {node_id}: every document version keys the system time node {first_node} keys"
        );
    }
    let (first_node, first_cols) = &col_rows[0];
    assert_eq!(
        first_cols.len(),
        2,
        "node {first_node}: two columnar versions"
    );
    for (node_id, cells) in &col_rows[1..] {
        assert_eq!(
            cells, first_cols,
            "node {node_id}: every columnar version stamps the _ts_system node {first_node} stamps"
        );
    }

    // Every node reads its own replica, and answers each cut alike.
    for node in &cluster.nodes {
        node.client
            .simple_query("SET default_read_consistency = 'eventual'")
            .await
            .unwrap_or_else(|e| panic!("node {}: eventual reads: {e}", node.node_id));
    }
    let docs_before = cut_view(&cluster.nodes[0], DOCS, "id, value").await;
    let cols_before = cut_view(&cluster.nodes[0], COLS, "id, v").await;
    assert!(!docs_before.is_empty() && !cols_before.is_empty());
    for node in &cluster.nodes[1..] {
        assert_eq!(cut_view(node, DOCS, "id, value").await, docs_before);
        assert_eq!(cut_view(node, COLS, "id, v").await, cols_before);
    }

    // Stop the documents group's leader. The new leader serves each cut from
    // the ordinals it installed as a follower.
    let leader = group_leader(&cluster.nodes[0], DOCS);
    assert!(leader != 0, "the documents group has a leader");
    let index = cluster
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .expect("leader node present");
    let _stopped = cluster.stop_member(index).await.expect("stop the leader");
    for node in &cluster.nodes {
        node.client
            .simple_query("SET default_read_consistency = 'strong'")
            .await
            .unwrap_or_else(|e| panic!("node {}: strong reads: {e}", node.node_id));
        assert_eq!(
            cut_view(node, DOCS, "id, value").await,
            docs_before,
            "node {} after the leader stopped: the document cuts answer as before",
            node.node_id
        );
        assert_eq!(
            cut_view(node, COLS, "id, v").await,
            cols_before,
            "node {} after the leader stopped: the columnar cuts answer as before",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

/// Every stored `_ts_system` ordinal of columnar collection `coll` on
/// `node`, tombstone versions included, ascending.
pub(super) async fn stored_stamps(node: &TestClusterNode, coll: &str) -> Vec<i64> {
    let snapshots = node.stored_tenant_snapshots(TenantId::new(TENANT)).await;
    let mut stamps: Vec<i64> = columnar_rows(&snapshots, coll)
        .into_iter()
        .map(|row| {
            row.rsplit("ts=")
                .next()
                .and_then(|ts| ts.parse().ok())
                .unwrap_or_else(|| panic!("row without an ordinal: {row}"))
        })
        .collect();
    stamps.sort_unstable();
    stamps
}

/// Two UPDATEs of one bitemporal columnar key land two new versions. With
/// the insert, the key holds three versions under three distinct ascending
/// system times, the same raw ordinals on every replica, and an `AS OF` at
/// each version's `_ts_system` returns that version.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn each_columnar_update_lands_a_version_with_its_own_system_time() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {UPDATED} (id BIGINT PRIMARY KEY, v BIGINT) \
             WITH (engine='columnar', bitemporal=true)"
        ))
        .await
        .expect("CREATE COLLECTION");
    let writer = &cluster.nodes[2];
    for sql in [
        format!("INSERT INTO {UPDATED} (id, v) VALUES (1, 10)"),
        format!("UPDATE {UPDATED} SET v = 20 WHERE id = 1"),
        format!("UPDATE {UPDATED} SET v = 30 WHERE id = 1"),
    ] {
        writer
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        // Distinct milliseconds, so each version has its own cut.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let first = stored_stamps(&cluster.nodes[0], UPDATED).await;
    assert_eq!(first.len(), 3, "three versions: {first:?}");
    assert!(
        first.windows(2).all(|pair| pair[0] < pair[1]),
        "distinct ascending system times: {first:?}"
    );
    for node in &cluster.nodes[1..] {
        assert_eq!(
            stored_stamps(node, UPDATED).await,
            first,
            "node {}: every version stamps the ordinal node {} stamps",
            node.node_id,
            cluster.nodes[0].node_id
        );
    }

    for node in &cluster.nodes {
        node.client
            .simple_query("SET default_read_consistency = 'eventual'")
            .await
            .unwrap_or_else(|e| panic!("node {}: eventual reads: {e}", node.node_id));
        let view = cut_view(node, UPDATED, "id, v").await;
        let values: Vec<Vec<String>> = view.into_iter().map(|(_, at)| at).collect();
        assert_eq!(
            values,
            vec![
                vec!["1|10".to_string()],
                vec!["1|20".to_string()],
                vec!["1|30".to_string()],
            ],
            "node {}: each cut returns its version",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

/// A DELETE of a bitemporal columnar key closes it with a tombstone version.
/// The key holds the inserted version and the tombstone under two ascending
/// system times, the same raw ordinals on every replica. A current read
/// returns no row. The audit log holds both versions: an `AS OF` at the
/// insert's system time returns the inserted row, and one at the
/// tombstone's returns none.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_columnar_delete_closes_its_key_with_its_own_system_time() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {DELETED} (id BIGINT PRIMARY KEY, v BIGINT) \
             WITH (engine='columnar', bitemporal=true)"
        ))
        .await
        .expect("CREATE COLLECTION");
    let writer = &cluster.nodes[2];
    for sql in [
        format!("INSERT INTO {DELETED} (id, v) VALUES (1, 10)"),
        format!("DELETE FROM {DELETED} WHERE id = 1"),
    ] {
        writer
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        // Distinct milliseconds, so the insert has a cut before the close.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let first = stored_stamps(&cluster.nodes[0], DELETED).await;
    assert_eq!(first.len(), 2, "the version and its tombstone: {first:?}");
    assert!(
        first[0] < first[1],
        "the tombstone stamps after the version: {first:?}"
    );
    for node in &cluster.nodes[1..] {
        assert_eq!(
            stored_stamps(node, DELETED).await,
            first,
            "node {}: every version stamps the ordinal node {} stamps",
            node.node_id,
            cluster.nodes[0].node_id
        );
    }

    for node in &cluster.nodes {
        node.client
            .simple_query("SET default_read_consistency = 'eventual'")
            .await
            .unwrap_or_else(|e| panic!("node {}: eventual reads: {e}", node.node_id));
        assert_eq!(
            rows(node, &format!("SELECT id, v FROM {DELETED}")).await,
            Vec::<String>::new(),
            "node {}: a current read excludes the closed key",
            node.node_id
        );
        let view = cut_view(node, DELETED, "id, v").await;
        assert_eq!(
            view.into_iter().map(|(_, at)| at).collect::<Vec<_>>(),
            vec![vec!["1|10".to_string()], Vec::new()],
            "node {}: a cut before the close returns the closed version, and a cut at the \
             close returns none",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
