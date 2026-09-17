// SPDX-License-Identifier: BUSL-1.1

//! End-to-end distributed shuffle-JOIN test (E4b-2).
//!
//! Brings up a 3-node cluster, creates two distributed collections homed on
//! DIFFERENT vShards (so their rows land on different nodes), inserts rows with
//! overlapping join keys, then runs a real SQL inner join with the permanent
//! force-shuffle override engaged via the session var
//! `SET nodedb.force_shuffle_join = on`. The override makes the planner emit a
//! whole-join `Exchange{Shuffle}` instead of the default broadcast-build plan;
//! the coordinator resolver fans producers to each side's owner, repartitions
//! rows on the join key to the part-owners, runs the node-local grace join on
//! each part, and merges the results.
//!
//! The assertion is correctness: the shuffle join must return EXACTLY the same
//! inner-join result a non-shuffle join returns (computed independently in the
//! test). A baseline (no-override) join is run first so a regression in either
//! path is distinguishable.

use std::time::Duration;

use nodedb_cluster::routing::vshard_for_collection;
use nodedb_types::DatabaseId;

use crate::common::cluster_harness::{TestCluster, wait_for};

/// The instant [`a_shuffle_join_renders_a_time_key_as_the_stored_instant`]
/// stores in its timeseries collection.
const EARLY: &str = "2020-03-05 10:00:00";
/// `EARLY` as a declared `TIMESTAMP` time key renders it. The engine stores
/// 1583402400000 epoch milliseconds and emits the cell as a typed instant,
/// which the pgwire encoder writes as ISO-8601 UTC.
const EARLY_ISO: &str = "2020-03-05T10:00:00.000000Z";

/// Run `sql` and collect the `id` column of every returned data row, sorted, so
/// the result is order-independent for equality assertions.
async fn collect_ids(client: &tokio_postgres::Client, sql: &str, col: &str) -> Vec<String> {
    let msgs = client.simple_query(sql).await.expect("simple_query");
    let mut ids: Vec<String> = msgs
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(col).map(|s| s.to_string()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids
}

/// Count data rows returned by `sql`.
async fn count_rows(client: &tokio_postgres::Client, sql: &str) -> usize {
    client
        .simple_query(sql)
        .await
        .expect("simple_query")
        .into_iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn distributed_shuffle_join_matches_inner_join() {
    // Two collection names that hash to DIFFERENT vShards so the build and probe
    // sides are genuinely homed on different routes (and likely different
    // nodes) — the cross-node path the shuffle join exists to handle.
    const LEFT: &str = "orders";
    const RIGHT: &str = "customers";
    assert_ne!(
        vshard_for_collection(DatabaseId::DEFAULT, LEFT),
        vshard_for_collection(DatabaseId::DEFAULT, RIGHT),
        "test collections must hash to different vShards to exercise cross-node shuffle"
    );

    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION customers \
             (id TEXT PRIMARY KEY, name TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION customers");
    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION orders \
             (id TEXT PRIMARY KEY, cust TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION orders");

    wait_for(
        "all 3 nodes see both collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 2)
        },
    )
    .await;

    // 4 customers: c1..c4.
    cluster.nodes[0]
        .client
        .simple_query(
            "INSERT INTO customers (id, name) VALUES \
             ('c1', 'alice'), ('c2', 'bob'), ('c3', 'carol'), ('c4', 'dave')",
        )
        .await
        .expect("insert customers");

    // 7 orders: o1..o6 match c1..c3 (cust in c1..c3), o7 has a non-matching
    // customer key. Spread across both join keys so the hash partitioning
    // genuinely distributes rows across parts.
    cluster.nodes[0]
        .client
        .simple_query(
            "INSERT INTO orders (id, cust) VALUES \
             ('o1', 'c1'), ('o2', 'c1'), ('o3', 'c2'), \
             ('o4', 'c2'), ('o5', 'c3'), ('o6', 'c3'), \
             ('o7', 'zz')",
        )
        .await
        .expect("insert orders");

    // Wait until every node sees the full local row counts so replication has
    // completed before asserting the cross-node join.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees all customer rows"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM customers"))
                });
                n >= 4
            },
        )
        .await;
        wait_for(
            &format!("node {idx} sees all order rows"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, "SELECT id FROM orders"))
                });
                n >= 7
            },
        )
        .await;
    }

    // The expected inner join: o1..o6 match (cust in c1..c3); o7 does not.
    let expected: Vec<String> = vec![
        "o1".into(),
        "o2".into(),
        "o3".into(),
        "o4".into(),
        "o5".into(),
        "o6".into(),
    ];

    // Alias the projected column explicitly so the result column name is stable
    // regardless of how each join path qualifies its output columns.
    let join_sql = "SELECT o.id AS oid FROM orders o JOIN customers c ON o.cust = c.id";

    // Baseline (default broadcast/local path, no override) — establishes the
    // correct answer and proves the non-shuffle path still works.
    let baseline = collect_ids(&cluster.nodes[0].client, join_sql, "oid").await;
    assert_eq!(
        baseline, expected,
        "baseline (non-shuffle) join must return the 6 matching orders; got {baseline:?}"
    );

    // Engage the permanent force-shuffle override on node 1's session, then run
    // the SAME join. The plan cache is bypassed while the override is set, so
    // this re-plans into an Exchange{Shuffle} and drives the cross-node grace
    // hash join. Use an explicit small partition count to exercise multi-part
    // fan-out and merge.
    cluster.nodes[1]
        .client
        .simple_query("SET nodedb.force_shuffle_join = on")
        .await
        .expect("SET nodedb.force_shuffle_join");
    cluster.nodes[1]
        .client
        .simple_query("SET nodedb.shuffle_num_parts = 4")
        .await
        .expect("SET nodedb.shuffle_num_parts");

    let shuffled = collect_ids(&cluster.nodes[1].client, join_sql, "oid").await;
    assert_eq!(
        shuffled, expected,
        "distributed shuffle join must return the SAME 6 matching orders as the \
         baseline inner join; got {shuffled:?}"
    );

    // Turning the override off again must fall back to the default plan and
    // still return the correct result (the cache was not poisoned with the
    // shuffle plan).
    cluster.nodes[1]
        .client
        .simple_query("SET nodedb.force_shuffle_join = off")
        .await
        .expect("SET nodedb.force_shuffle_join = off");
    let after = collect_ids(&cluster.nodes[1].client, join_sql, "oid").await;
    assert_eq!(
        after, expected,
        "join after disabling the override must still return the 6 matches; got {after:?}"
    );

    cluster.shutdown().await;
}

/// A timeseries `TIMESTAMP` time key projected through a shuffle join denotes
/// the stored instant, the same as a direct read does. The shuffle path
/// repartitions rows through its own coordinator-side encode/decode hop, so
/// it is a route to the cell distinct from the local join and the direct
/// `SELECT` both.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_shuffle_join_renders_a_time_key_as_the_stored_instant() {
    const LEFT: &str = "ts_shuffle_events";
    const RIGHT: &str = "ts_shuffle_hosts";
    assert_ne!(
        vshard_for_collection(DatabaseId::DEFAULT, LEFT),
        vshard_for_collection(DatabaseId::DEFAULT, RIGHT),
        "test collections must hash to different vShards to exercise cross-node shuffle"
    );

    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {LEFT} \
             (captured_at TIMESTAMP TIME_KEY, host TEXT, v FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .expect("CREATE COLLECTION for the timeseries side");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {RIGHT} (id TEXT PRIMARY KEY, region TEXT) \
             WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION for the document side");

    wait_for(
        "all 3 nodes see both collections",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 2)
        },
    )
    .await;

    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {LEFT} (captured_at, host, v) VALUES ('{EARLY}', 'h1', 1.5)"
        ))
        .await
        .expect("insert timeseries row");
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {RIGHT} (id, region) VALUES ('h1', 'eu')"
        ))
        .await
        .expect("insert document row");

    for (idx, node) in cluster.nodes.iter().enumerate() {
        wait_for(
            &format!("node {idx} sees the timeseries row"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(count_rows(
                        &node.client,
                        &format!("SELECT host FROM {LEFT}"),
                    ))
                });
                n >= 1
            },
        )
        .await;
        wait_for(
            &format!("node {idx} sees the document row"),
            Duration::from_secs(15),
            Duration::from_millis(50),
            || {
                let n = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(count_rows(&node.client, &format!("SELECT id FROM {RIGHT}")))
                });
                n >= 1
            },
        )
        .await;
    }

    cluster.nodes[1]
        .client
        .simple_query("SET nodedb.force_shuffle_join = on")
        .await
        .expect("SET nodedb.force_shuffle_join");
    cluster.nodes[1]
        .client
        .simple_query("SET nodedb.shuffle_num_parts = 4")
        .await
        .expect("SET nodedb.shuffle_num_parts");

    let join_sql = format!(
        "SELECT {LEFT}.captured_at FROM {LEFT} \
         INNER JOIN {RIGHT} ON {LEFT}.host = {RIGHT}.id"
    );
    let msgs = cluster.nodes[1]
        .client
        .simple_query(&join_sql)
        .await
        .expect("a shuffle join over a time key must succeed");
    let values: Vec<String> = msgs
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect();

    assert_eq!(values.len(), 1, "one event matches one host: {values:?}");
    assert_eq!(
        values[0], EARLY_ISO,
        "a shuffle-joined time key must denote {EARLY}: got {values:?}"
    );

    cluster.shutdown().await;
}
