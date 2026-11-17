// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill WAL-durability regressions for the cross-engine
//! overlays: Graph (edges + node labels), Full-Text Search, and Spatial.
//! Graph and FTS sit atop a base document collection, so `kill -9` must
//! leave the overlay index (CSR adjacency, inverted index) queryable, not
//! just the document rows. The spatial case covers a geometry field on a
//! document collection, served by `execute_spatial_scan` reading `sparse`.

mod crash_harness;

use crash_harness::CrashHarness;

#[tokio::test(flavor = "multi_thread")]
async fn graph_edges_survive_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();

    h.exec("CREATE COLLECTION crash_graph_edges").await;
    h.exec("GRAPH INSERT EDGE IN 'crash_graph_edges' FROM 'a' TO 'b' TYPE 'knows'")
        .await;

    // Live sanity BEFORE the crash: the edge is traversable pre-restart, so
    // any post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col(
            "MATCH (x)-[:knows]->(y) IN 'crash_graph_edges' RETURN x, y",
            "y",
        )
        .await;
    assert_eq!(
        live,
        vec!["b".to_string()],
        "graph edge must be traversable BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "MATCH (x)-[:knows]->(y) IN 'crash_graph_edges' RETURN x, y",
            "y",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["b".to_string()],
        "graph edge did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_node_labels_survive_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();

    h.exec("CREATE COLLECTION crash_graph_labels").await;
    h.exec("INSERT INTO crash_graph_labels { id: 'alice', name: 'Alice' }")
        .await;
    h.exec("INSERT INTO crash_graph_labels { id: 'bob', name: 'Bob' }")
        .await;
    h.exec("GRAPH INSERT EDGE IN 'crash_graph_labels' FROM 'alice' TO 'bob' TYPE 'knows'")
        .await;
    h.exec("GRAPH LABEL 'alice' AS 'Person'").await;
    h.exec("GRAPH LABEL 'bob' AS 'Person'").await;

    // Live sanity BEFORE the crash: the labeled MATCH works pre-restart, so
    // any post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col("MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b", "b")
        .await;
    assert_eq!(
        live,
        vec!["bob".to_string()],
        "labeled MATCH must work BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col("MATCH (a:Person)-[:knows]->(b:Person) RETURN a, b", "b")
        .await;
    assert_eq!(
        recovered,
        vec!["bob".to_string()],
        "graph node labels did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fts_index_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();

    h.exec("CREATE COLLECTION crash_fts WITH (engine='document_schemaless')")
        .await;
    h.exec("CREATE SEARCH INDEX ON crash_fts (body)").await;
    h.exec("INSERT INTO crash_fts { id: 'd1', body: 'The quick brown fox' }")
        .await;

    // Live sanity BEFORE the crash: the FTS match works pre-restart, so any
    // post-restart failure is attributable to recovery, not test setup.
    let live = h
        .query_col(
            "SELECT id FROM crash_fts WHERE text_match(body, 'fox')",
            "id",
        )
        .await;
    assert_eq!(
        live,
        vec!["d1".to_string()],
        "FTS match must work BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "SELECT id FROM crash_fts WHERE text_match(body, 'fox')",
            "id",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["d1".to_string()],
        "FTS index did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn spatial_document_geometry_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready();

    // A document collection, not `engine='spatial'` (columnar-family):
    // geometry lives as a plain field, no declared spatial index.
    h.exec("CREATE COLLECTION crash_geo_doc WITH (engine='document_schemaless')")
        .await;
    h.exec(
        "INSERT INTO crash_geo_doc (id, location, name) \
         VALUES ('p1', ST_Point(-73.9857, 40.7580), 'Times Square')",
    )
    .await;
    h.exec(
        "INSERT INTO crash_geo_doc (id, location, name) \
         VALUES ('p2', ST_Point(2.3522, 48.8566), 'Paris')",
    )
    .await;

    // Rows within ~5 km of Times Square: p1 matches, Paris does not.
    let q = "SELECT name FROM crash_geo_doc WHERE \
             ST_DWithin(location, '{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}', 5000)";

    // Live sanity BEFORE the crash: the spatial predicate works pre-restart, so
    // any post-restart failure is attributable to recovery, not test setup.
    let live = h.query_col(q, "name").await;
    assert_eq!(
        live,
        vec!["Times Square".to_string()],
        "document spatial predicate must work BEFORE the crash (test-setup sanity): {live:?}"
    );

    h.kill_9();
    h.reopen();

    // The geometry documents survive in redb and WAL replay re-applies their
    // `Put`s on boot, so the same predicate must still match the same row.
    let recovered = h.query_col(q, "name").await;
    assert_eq!(
        recovered,
        vec!["Times Square".to_string()],
        "document spatial predicate did not survive kill -9 + WAL replay (got {recovered:?})"
    );
}
