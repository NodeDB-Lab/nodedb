// SPDX-License-Identifier: BUSL-1.1

//! A `MOVE TENANT` retried after its cutover failed past the re-issue leaves
//! each moved row in the target exactly once.
//!
//! Columnar and timeseries ingests append. The fail point
//! `move_tenant::cutover::before_proposal` fails the first cutover after
//! every row is re-issued into the target and before the catalog proposal.
//! The retry re-issues the same rows again. The re-issue clears each
//! append-only target collection first, so the counts stay exact.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb_test_support::fail_point::FailGuard;

use crate::common;
use common::cluster_harness::TestCluster;
use common::cluster_harness::shared_steps::{db_detail, use_database};

const SOURCE: &str = "mt_rt_src";
const TARGET: &str = "mt_rt_tgt";
const MOVED_TENANT: &str = "mt_rt_owner";
const COLUMNAR: &str = "mt_rt_cols";
const TIMESERIES: &str = "mt_rt_ts";
const ROWS: usize = 4;
const FAULT: &str = "move_tenant::cutover::before_proposal";

async fn create_collections(cluster: &TestCluster, database: &str) {
    use_database(cluster, database).await;
    for ddl in [
        format!(
            "CREATE COLLECTION {COLUMNAR} COLUMNS (id TEXT, region TEXT, ts BIGINT) WITH (engine='columnar')"
        ),
        format!(
            "CREATE COLLECTION {TIMESERIES} \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
             WITH (engine='timeseries')"
        ),
    ] {
        cluster
            .exec_ddl_on_any_leader(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl} in {database}: {e}"));
    }
}

async fn count(cluster: &TestCluster, node_idx: usize, collection: &str) -> String {
    let sql = format!("SELECT COUNT(*) FROM {collection}");
    let messages = cluster.nodes[node_idx]
        .client
        .simple_query(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} on node {node_idx}: {}", db_detail(&e)));
    messages
        .into_iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql} on node {node_idx} returned no row"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_move_holds_each_append_only_row_once() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    for database in [SOURCE, TARGET] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DATABASE {database}: {e}"));
    }
    create_collections(&cluster, SOURCE).await;
    create_collections(&cluster, TARGET).await;

    use_database(&cluster, SOURCE).await;
    for i in 0..ROWS {
        let ts = (i + 1) * 1000;
        for sql in [
            format!("INSERT INTO {COLUMNAR} (id, region, ts) VALUES ('c{i}', 'r{i}', {ts})"),
            format!(
                "INSERT INTO {TIMESERIES} (id, ts, metric, value) VALUES ('t{i}', {ts}, 'cpu', 1.0)"
            ),
        ] {
            cluster.nodes[0]
                .client
                .simple_query(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {}", db_detail(&e)));
        }
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    use_database(&cluster, "default").await;
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE TENANT {MOVED_TENANT} ID 78"))
        .await
        .unwrap_or_else(|e| panic!("CREATE TENANT {MOVED_TENANT}: {e}"));
    let move_sql = format!("MOVE TENANT {MOVED_TENANT} FROM {SOURCE} TO {TARGET}");

    // First attempt: every row reaches the target, then the cutover fails.
    {
        let _fault = FailGuard::fail(FAULT, "injected before the cutover proposal");
        let refused = cluster.nodes[0].exec(&move_sql).await;
        assert!(
            refused.is_err(),
            "the first MOVE TENANT must fail at {FAULT}"
        );
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;
    use_database(&cluster, TARGET).await;
    for collection in [COLUMNAR, TIMESERIES] {
        assert_eq!(
            count(&cluster, 0, collection).await,
            ROWS.to_string(),
            "{TARGET}.{collection} must hold the rows the failed attempt re-issued"
        );
    }

    // Retry: the re-issue replaces each target collection's rows.
    use_database(&cluster, "default").await;
    cluster
        .exec_ddl_on_any_leader(&move_sql)
        .await
        .unwrap_or_else(|e| panic!("retried {move_sql}: {e}"));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    use_database(&cluster, TARGET).await;
    for node_idx in 0..cluster.nodes.len() {
        for collection in [COLUMNAR, TIMESERIES] {
            assert_eq!(
                count(&cluster, node_idx, collection).await,
                ROWS.to_string(),
                "{TARGET}.{collection} on node {node_idx} must hold each row once"
            );
        }
    }

    cluster.shutdown().await;
}
