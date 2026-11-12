// SPDX-License-Identifier: BUSL-1.1

//! `SUM` over an `INT` column through the distributed shuffle merge is
//! `bigint`, and its binary-format cell is an 8-byte integer.
//!
//! A 4-core single-node-calvin node has a cluster transport and the shuffle
//! hooks installed. With `nodedb.force_shuffle_agg` on, a GROUP BY lowers to
//! `Exchange{ShuffleAggregate}`: partial states are produced, staged, and
//! merged by the shuffle consumer. `EXPLAIN` shows that plan before the
//! aggregate runs, so the result comes from the shuffle merge.

use std::time::Duration;

use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::types::Type;

use crate::common::cluster_harness::{TestClusterNode, wait_for};
use crate::common::occ_shuffle::{pg_detail, sequencer_leader};

/// The `QUERY PLAN` rows of `EXPLAIN sql`, joined.
async fn explain_text(client: &tokio_postgres::Client, sql: &str) -> String {
    client
        .simple_query(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN {sql}: {}", pg_detail(&e)))
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => row.get("QUERY PLAN").map(str::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_merged_sum_of_int_is_binary_bigint() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let node = TestClusterNode::spawn_single_node_calvin_on_path(4, data_dir.path().to_path_buf())
        .await
        .expect("spawn single-node-calvin server");
    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || sequencer_leader(&node) == node.node_id,
    )
    .await;

    node.client
        .simple_query(
            "CREATE COLLECTION shuffle_int_sum (id TEXT PRIMARY KEY, k TEXT, i INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap_or_else(|e| panic!("create: {}", pg_detail(&e)));
    wait_for(
        "the collection is visible",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 1,
    )
    .await;
    node.client
        .simple_query(
            "INSERT INTO shuffle_int_sum (id, k, i) VALUES \
             ('r1', 'a', 1), ('r2', 'a', 2), ('r3', 'b', 2147483647), ('r4', 'b', 2147483647)",
        )
        .await
        .unwrap_or_else(|e| panic!("insert: {}", pg_detail(&e)));

    for set in [
        "SET nodedb.force_shuffle_agg = on",
        "SET nodedb.shuffle_agg_num_parts = 2",
    ] {
        node.client
            .simple_query(set)
            .await
            .unwrap_or_else(|e| panic!("{set}: {}", pg_detail(&e)));
    }

    let sql = "SELECT k, SUM(i) AS s FROM shuffle_int_sum GROUP BY k";
    let plan = explain_text(&node.client, sql).await;
    assert!(
        plan.contains("ShuffleAggregate"),
        "the forced aggregate must plan the shuffle merge: {plan}"
    );

    let statement = node
        .client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("prepare: {}", pg_detail(&e)));
    assert_eq!(statement.columns()[1].type_(), &Type::INT8);
    let rows = node
        .client
        .query(&statement, &[])
        .await
        .unwrap_or_else(|e| panic!("shuffle aggregate: {}", pg_detail(&e)));
    let mut sums: Vec<(String, i64)> = rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
        .collect();
    sums.sort();
    assert_eq!(
        sums,
        [("a".to_string(), 3), ("b".to_string(), 4_294_967_294)],
        "the shuffle merge totals integer input exactly, past the integer range"
    );
}
