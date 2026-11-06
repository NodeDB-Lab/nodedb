// SPDX-License-Identifier: BUSL-1.1

//! A bootstrap keeps each engine's system-time stamps.
//!
//! Every row exists before `MIRROR DATABASE` runs, so a re-issue that stamped its
//! own time leaves the mirror empty at the cut. Each engine with a historical
//! reader answers the same at the cut on both clusters, and differently from its
//! current state, so the probe cannot pass on a read that ignores the cut.
//!
//! KV and vector have no historical SQL reader. For those two the bootstrap's own
//! verification is the proof. It hashes each KV version under its start ordinal
//! and each vector node under its span, and the mirror reaches `Following` only
//! once that verification passed. The test checks the capture held their rows,
//! so the check was not empty.

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nodedb::control::backup::capture::capture_collections;
use nodedb_types::DatabaseId;

use super::mirror_bootstrap_fixture::{
    await_following, await_mirror_reads, boot_mirror_loops, link_pki, mirror_settings, read_mirror,
    rows, serve_source, use_database,
};
use crate::common::cluster_harness::TestCluster;

const SOURCE_DB: &str = "mirror_boot_engines";
const MIRROR_DB: &str = "mirror_boot_copy";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

const DDL: &[&str] = &[
    "CREATE COLLECTION b_docs (id STRING PRIMARY KEY, value STRING) \
     WITH (engine='document_schemaless', bitemporal=true)",
    "CREATE COLLECTION b_cols (id BIGINT PRIMARY KEY, v BIGINT) \
     WITH (engine='columnar', bitemporal=true)",
    "CREATE COLLECTION b_spat COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
     WITH (engine='spatial')",
    "CREATE COLLECTION b_kv (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
    "CREATE COLLECTION b_ts (ts BIGINT TIME_KEY, host TEXT, value FLOAT) \
     WITH (engine='timeseries')",
    "CREATE COLLECTION b_vec (id TEXT PRIMARY KEY, emb VECTOR(3)) WITH (engine='vector', \
     primary='vector', vector_field='emb', dim=3, m=16, ef_construction=100)",
    "CREATE COLLECTION b_graph",
    "CREATE ARRAY b_arr DIMS (x INT64 [0..15]) ATTRS (v INT64) TILE_EXTENTS (16) \
     CELL_ORDER ROW_MAJOR",
];

const BEFORE_CUT: &[&str] = &[
    "INSERT INTO b_docs (id, value) VALUES ('d1', 'first')",
    "INSERT INTO b_docs (id, value) VALUES ('d2', 'kept')",
    "INSERT INTO b_cols (id, v) VALUES (1, 10)",
    "INSERT INTO b_cols (id, v) VALUES (2, 5)",
    "INSERT INTO b_spat (id, location, name) VALUES \
     ('p1', '{\"type\":\"Point\",\"coordinates\":[-122.4,37.8]}', 'early')",
    "INSERT INTO b_kv (key, n) VALUES ('k1', 1)",
    "INSERT INTO b_ts (ts, host, value) VALUES (1000, 'early', 1.5)",
    "INSERT INTO b_vec (id, emb) VALUES ('v1', ARRAY[1.0,0.0,0.0])",
    "GRAPH INSERT EDGE IN 'b_graph' FROM 'a' TO 'b' TYPE 'K'",
    "GRAPH INSERT EDGE IN 'b_graph' FROM 'c' TO 'd' TYPE 'K'",
    "GRAPH LABEL 'a' AS 'Hot'",
    "GRAPH LABEL 'c' AS 'Hot'",
    "INSERT INTO ARRAY b_arr COORDS (0) VALUES (10)",
    "SELECT ARRAY_FLUSH('b_arr')",
];

const AFTER_CUT: &[&str] = &[
    "UPDATE b_docs SET value = 'second' WHERE id = 'd1'",
    "UPDATE b_cols SET v = 20 WHERE id = 1",
    "INSERT INTO b_spat (id, location, name) VALUES \
     ('p2', '{\"type\":\"Point\",\"coordinates\":[2.3,48.8]}', 'late')",
    "INSERT INTO b_kv (key, n) VALUES ('k2', 2)",
    "INSERT INTO b_ts (ts, host, value) VALUES (2000, 'late', 2.5)",
    "INSERT INTO b_vec (id, emb) VALUES ('v2', ARRAY[0.0,1.0,0.0])",
    "GRAPH INSERT EDGE IN 'b_graph' FROM 'a' TO 'e' TYPE 'K'",
    "GRAPH UNLABEL 'c' AS 'Hot'",
    "INSERT INTO ARRAY b_arr COORDS (0) VALUES (99)",
    "SELECT ARRAY_FLUSH('b_arr')",
];

/// One engine's read at `AS OF SYSTEM TIME`, or its current state with `None`.
fn engine_reads(cut: Option<i64>) -> Vec<(&'static str, String)> {
    let at = cut.map_or(String::new(), |ms| format!(" AS OF SYSTEM TIME {ms}"));
    vec![
        ("documents", format!("SELECT id, value FROM b_docs{at}")),
        ("columnar", format!("SELECT id, v FROM b_cols{at}")),
        ("spatial", format!("SELECT id, name FROM b_spat{at}")),
        ("timeseries", format!("SELECT host FROM b_ts{at}")),
        (
            "edges",
            format!("MATCH (a)-[:K]->(b) IN 'b_graph'{at} RETURN a, b"),
        ),
        (
            "labels",
            format!("MATCH (a:Hot)-[:K]->(b) IN 'b_graph'{at} RETURN a, b"),
        ),
        (
            "arrays",
            format!("SELECT * FROM ARRAY_SLICE('b_arr', '{{x: [0, 0]}}', ['v'], 10){at}"),
        ),
    ]
}

/// Each `_ts_system` the source stamped, with what a read at that stamp answers.
async fn stamp_reads(source: &TestCluster, coll: &str, cols: &str) -> Vec<(String, Vec<String>)> {
    let node = &source.nodes[0];
    let stamps: BTreeSet<String> = rows(
        node,
        &format!("SELECT _ts_system FROM {coll} AS OF SYSTEM TIME NULL"),
    )
    .await
    .into_iter()
    .collect();
    let mut reads = Vec::with_capacity(stamps.len());
    for stamp in stamps {
        let at = rows(
            node,
            &format!("SELECT {cols} FROM {coll} AS OF SYSTEM TIME {stamp}"),
        )
        .await;
        reads.push((stamp, at));
    }
    reads
}

/// The mirror stores KV and vector rows, so the bootstrap's verification saw them.
async fn assert_capture_holds_kv_and_vectors(mirror: &TestCluster, database: DatabaseId) {
    let shared = &mirror.nodes[0].shared;
    let tenant = shared
        .credentials
        .catalog()
        .load_all_collections(database)
        .expect("catalog")
        .into_iter()
        .find(|collection| collection.name == "b_kv")
        .expect("the catalog image carried the KV collection")
        .tenant_id;
    let names: BTreeSet<String> = ["b_kv".to_string(), "b_vec".to_string()].into();
    let snap = capture_collections(shared, tenant, database, &names, false)
        .await
        .expect("capture the mirror");
    assert!(!snap.kv_tables.is_empty(), "the mirror holds KV versions");
    assert!(
        !snap.vectors.is_empty() || !snap.vector_versions.is_empty(),
        "the mirror holds vector versions"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_bootstrap_keeps_every_engines_system_time_stamps() {
    let source = TestCluster::spawn_three().await.expect("source cluster");
    let mirror = TestCluster::spawn_three().await.expect("mirror cluster");

    source
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {SOURCE_DB}"))
        .await
        .expect("create database");
    use_database(&source, SOURCE_DB).await;
    for ddl in DDL {
        source
            .exec_ddl_on_any_leader(ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    for sql in BEFORE_CUT {
        source.nodes[0]
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        // Distinct milliseconds, so each version has its own stamp.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    for sql in AFTER_CUT {
        source.nodes[0]
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    source
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;

    let at_cut = engine_reads(Some(cut));
    let current = engine_reads(None);
    let mut at_cut_rows = Vec::with_capacity(at_cut.len());
    let mut current_rows = Vec::with_capacity(current.len());
    for ((engine, cut_sql), (_, now_sql)) in at_cut.iter().zip(&current) {
        let then = rows(&source.nodes[0], cut_sql).await;
        let now = rows(&source.nodes[0], now_sql).await;
        assert!(
            !then.is_empty(),
            "{engine}: the source holds rows at the cut"
        );
        assert_ne!(then, now, "{engine}: the probe tells the cut from now");
        at_cut_rows.push(then);
        current_rows.push(now);
    }
    let doc_stamps = stamp_reads(&source, "b_docs", "id, value").await;
    let col_stamps = stamp_reads(&source, "b_cols", "id, v").await;
    assert!(doc_stamps.len() >= 3, "three document versions");
    assert!(col_stamps.len() >= 3, "three columnar versions");
    let kv_rows = rows(&source.nodes[0], "SELECT key, n FROM b_kv").await;
    let vector_rows = rows(&source.nodes[0], "SELECT id FROM b_vec").await;

    let pki = link_pki();
    let seeds = serve_source(&source, &pki, &[SOURCE_DB]);
    boot_mirror_loops(&mirror, &mirror_settings(&pki, &seeds));
    mirror
        .exec_ddl_on_any_leader(&format!(
            "MIRROR DATABASE {MIRROR_DB} FROM source.{SOURCE_DB}"
        ))
        .await
        .expect("MIRROR DATABASE");
    let database = await_following(&mirror, MIRROR_DB).await;
    read_mirror(&mirror, MIRROR_DB).await;

    for ((_, cut_sql), then) in at_cut.iter().zip(&at_cut_rows) {
        await_mirror_reads(&mirror, cut_sql, then).await;
    }
    for ((_, now_sql), now) in current.iter().zip(&current_rows) {
        await_mirror_reads(&mirror, now_sql, now).await;
    }
    for (coll, cols, reads) in [
        ("b_docs", "id, value", &doc_stamps),
        ("b_cols", "id, v", &col_stamps),
    ] {
        for (stamp, at) in reads {
            await_mirror_reads(
                &mirror,
                &format!("SELECT {cols} FROM {coll} AS OF SYSTEM TIME {stamp}"),
                at,
            )
            .await;
        }
    }
    await_mirror_reads(&mirror, "SELECT key, n FROM b_kv", &kv_rows).await;
    await_mirror_reads(&mirror, "SELECT id FROM b_vec", &vector_rows).await;
    assert_capture_holds_kv_and_vectors(&mirror, database).await;

    mirror.shutdown().await;
    source.shutdown().await;
}
