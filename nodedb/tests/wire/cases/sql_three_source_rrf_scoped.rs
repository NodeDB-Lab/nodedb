// SPDX-License-Identifier: BUSL-1.1

//! Three-source RRF fusion over named columns and residual filters: the text
//! leg reads the column `bm25_score` names, the vector leg the column
//! `vector_distance` names, and a WHERE predicate restricts every leg,
//! the graph walk included.

use crate::harness::TestServer;

/// Layout over a named vector column `emb`:
///   s1 (vector = query, title "plain", body "alpha") --hop--> s2
///   s2 (vector far, title "alpha", body "plain", tag "drop")
///   s3 (vector far, title "plain", body "plain")
async fn create_scoped_triple_collection(server: &TestServer, name: &str) {
    server
        .exec(&format!("CREATE COLLECTION {name}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "CREATE VECTOR INDEX idx_{name}_emb ON {name} (emb) METRIC cosine DIM 3"
        ))
        .await
        .unwrap();
    for (id, tag, title, body, emb) in [
        ("s1", "keep", "plain", "alpha", "1.0, 0.0, 0.0"),
        ("s2", "drop", "alpha", "plain", "-1.0, 0.0, 0.0"),
        ("s3", "keep", "plain", "plain", "0.0, -1.0, 0.0"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO {name} (id, tag, title, body, emb) \
                 VALUES ('{id}', '{tag}', '{title}', '{body}', ARRAY[{emb}])"
            ))
            .await
            .unwrap();
    }
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{name}' FROM 's1' TO 's2' TYPE 'hop'"
        ))
        .await
        .unwrap();
}

/// The text leg reads the title index only: `s2` holds "alpha" in its title,
/// `s1` only in its body. With a dominant text weight `s2` ranks first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_triple_text_leg_reads_only_its_column() {
    let server = TestServer::start().await;
    create_scoped_triple_collection(&server, "t3_scoped").await;

    let rows = server
        .query_rows(
            "SELECT id, \
             rrf_score(\
               vector_distance(emb, ARRAY[0.0, 0.0, 1.0]), \
               bm25_score(title, 'alpha'), \
               graph_score(id, 's3', depth => 1, label => 'hop'), \
               10000.0, 1.0, 10000.0 \
             ) AS score \
             FROM t3_scoped ORDER BY score DESC LIMIT 10",
        )
        .await
        .expect("a field-scoped three-source search must succeed");
    assert!(!rows.is_empty(), "the search must fuse rows");
    assert_eq!(rows[0][0], "s2", "the title match leads: {rows:?}");
}

/// A WHERE predicate restricts every leg, the graph walk included: `s2` is
/// reachable from `s1` but its tag fails the filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_triple_where_filter_restricts_every_leg() {
    let server = TestServer::start().await;
    create_scoped_triple_collection(&server, "t3_filtered").await;

    let rows = server
        .query_rows(
            "SELECT id, \
             rrf_score(\
               vector_distance(emb, ARRAY[1.0, 0.0, 0.0]), \
               bm25_score(title, 'alpha'), \
               graph_score(id, 's1', depth => 1, label => 'hop') \
             ) AS score \
             FROM t3_filtered WHERE tag = 'keep' LIMIT 10",
        )
        .await
        .expect("a filtered three-source search must succeed");
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(!ids.is_empty(), "the kept rows fuse: {rows:?}");
    assert!(
        !ids.contains(&"s2"),
        "a row the filter excludes never fuses: {rows:?}"
    );
}

/// The vector leg searches the column `vector_distance` names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rrf_score_triple_vector_leg_reads_the_named_column() {
    let server = TestServer::start().await;
    create_scoped_triple_collection(&server, "t3_named_vec").await;

    let rows = server
        .query_rows(
            "SELECT id, \
             rrf_score(\
               vector_distance(emb, ARRAY[1.0, 0.0, 0.0]), \
               bm25_score(title, 'absent'), \
               graph_score(id, 's3', depth => 1, label => 'hop'), \
               1.0, 10000.0, 10000.0 \
             ) AS score \
             FROM t3_named_vec ORDER BY score DESC LIMIT 10",
        )
        .await
        .expect("a three-source search over a named vector column must succeed");
    assert!(!rows.is_empty(), "the emb index must answer the vector leg");
    assert_eq!(rows[0][0], "s1", "emb ranks s1 first: {rows:?}");
}
