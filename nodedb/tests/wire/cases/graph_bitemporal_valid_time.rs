// SPDX-License-Identifier: BUSL-1.1

//! Edge valid time (`VALID_TIME ... UNTIL ...`), `AS OF VALID TIME` on graph
//! reads, historical GraphRAG fusion, and the `AS OF` forms a graph
//! statement refuses.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::harness::TestServer;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

fn has(blob: &str, node: &str) -> bool {
    blob.contains(&format!("\"{node}\""))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_time_interval_filters_as_of_valid_time_reads() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vt_org")
        .await
        .expect("create");
    server
        .exec(
            "GRAPH INSERT EDGE IN 'vt_org' FROM 'alice' TO 'bob' TYPE 'reports_to' \
             VALID_TIME '2020-01-01T00:00:00Z' UNTIL '2021-01-01T00:00:00Z'",
        )
        .await
        .expect("insert closed interval");
    server
        .exec(
            "GRAPH INSERT EDGE IN 'vt_org' FROM 'alice' TO 'carol' TYPE 'reports_to' \
             VALID_TIME '2021-01-01T00:00:00Z'",
        )
        .await
        .expect("insert open interval");

    let mid_2020 = server
        .query_text(
            "GRAPH NEIGHBORS IN 'vt_org' OF 'alice' AS OF VALID TIME '2020-06-01T00:00:00Z'",
        )
        .await
        .expect("neighbors valid in 2020")
        .join("");
    assert!(
        has(&mid_2020, "bob") && !has(&mid_2020, "carol"),
        "{mid_2020}"
    );

    let mid_2022 = server
        .query_text(
            "GRAPH NEIGHBORS IN 'vt_org' OF 'alice' AS OF VALID TIME '2022-06-01T00:00:00Z'",
        )
        .await
        .expect("neighbors valid in 2022")
        .join("");
    assert!(
        !has(&mid_2022, "bob") && has(&mid_2022, "carol"),
        "{mid_2022}"
    );

    // `UNTIL` is exclusive: the boundary instant belongs to the next edge.
    let boundary = server
        .query_text(
            "GRAPH NEIGHBORS IN 'vt_org' OF 'alice' AS OF VALID TIME '2021-01-01T00:00:00Z'",
        )
        .await
        .expect("neighbors at the boundary")
        .join("");
    assert!(
        !has(&boundary, "bob") && has(&boundary, "carol"),
        "{boundary}"
    );

    // Without `AS OF VALID TIME` both edges are current versions.
    let all = server
        .query_text("GRAPH NEIGHBORS IN 'vt_org' OF 'alice'")
        .await
        .expect("current neighbors")
        .join("");
    assert!(has(&all, "bob") && has(&all, "carol"), "{all}");

    // The axes combine: valid in 2020, as the database held it now.
    let both = server
        .query_text(&format!(
            "MATCH (x)-[:reports_to]->(y) IN 'vt_org' AS OF SYSTEM TIME {} \
             AS OF VALID TIME '2020-06-01T00:00:00Z' RETURN y",
            now_ms()
        ))
        .await
        .expect("match on both axes");
    assert_eq!(both, vec!["bob".to_string()]);

    let traverse = server
        .query_text(
            "GRAPH TRAVERSE FROM 'alice' IN 'vt_org' DEPTH 1 \
             AS OF VALID TIME '2022-06-01T00:00:00Z'",
        )
        .await
        .expect("traverse valid in 2022")
        .join("");
    assert!(
        !has(&traverse, "bob") && has(&traverse, "carol"),
        "{traverse}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn as_of_on_a_graph_write_is_a_parse_error() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vt_writes")
        .await
        .expect("create");
    server
        .expect_error(
            "GRAPH INSERT EDGE IN 'vt_writes' FROM 'a' TO 'b' TYPE 'l' AS OF SYSTEM TIME 5",
            "does not accept AS OF",
        )
        .await;
    server
        .expect_error(
            "GRAPH DELETE EDGE IN 'vt_writes' FROM 'a' TO 'b' TYPE 'l' AS OF VALID TIME 5",
            "does not accept AS OF",
        )
        .await;
    server
        .expect_error(
            "GRAPH LABEL 'a' AS 'Person' AS OF SYSTEM TIME 5",
            "does not accept AS OF",
        )
        .await;
    server
        .expect_error(
            "GRAPH INSERT EDGE IN 'vt_writes' FROM 'a' TO 'b' TYPE 'l' VALID_TIME 10 UNTIL 10",
            "UNTIL must be after VALID_TIME",
        )
        .await;
    server
        .expect_error(
            "GRAPH INSERT EDGE IN 'vt_writes' FROM 'a' TO 'b' TYPE 'l' UNTIL 10",
            "UNTIL needs VALID_TIME",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_versions_and_valid_time_ranges_are_refused_on_graph_reads() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vt_refuse")
        .await
        .expect("create");
    server
        .exec("GRAPH INSERT EDGE IN 'vt_refuse' FROM 'a' TO 'b' TYPE 'l'")
        .await
        .expect("insert edge");
    for sql in [
        "GRAPH NEIGHBORS IN 'vt_refuse' OF 'a' AS OF SYSTEM TIME NULL",
        "GRAPH TRAVERSE FROM 'a' IN 'vt_refuse' DEPTH 2 AS OF SYSTEM TIME NULL",
        "GRAPH PATH IN 'vt_refuse' FROM 'a' TO 'b' AS OF SYSTEM TIME NULL",
        "GRAPH ALGO PAGERANK ON vt_refuse AS OF SYSTEM TIME NULL",
        "MATCH (x)-[:l]->(y) IN 'vt_refuse' AS OF SYSTEM TIME NULL RETURN y",
    ] {
        server
            .expect_error(sql, "AS OF SYSTEM TIME NULL is not supported")
            .await;
    }
    server
        .expect_error(
            "GRAPH NEIGHBORS IN 'vt_refuse' OF 'a' FOR VALID_TIME FROM 1 TO 5",
            "AS OF VALID TIME",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rag_fusion_as_of_expands_the_graph_before_the_change() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vt_rag")
        .await
        .expect("create");
    server
        .exec("CREATE VECTOR INDEX idx_vt_rag ON vt_rag METRIC cosine DIM 3")
        .await
        .expect("vector index");
    for (id, vector) in [
        ("n1", "1.0, 0.0, 0.0"),
        ("n2", "0.0, 1.0, 0.0"),
        ("n3", "0.0, 0.0, 1.0"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO vt_rag (id, embedding) VALUES ('{id}', ARRAY[{vector}])"
            ))
            .await
            .expect("insert row");
    }
    server
        .exec("GRAPH INSERT EDGE IN 'vt_rag' FROM 'n1' TO 'n2' TYPE 'rel'")
        .await
        .expect("insert edge");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;
    server
        .exec("GRAPH DELETE EDGE IN 'vt_rag' FROM 'n1' TO 'n2' TYPE 'rel'")
        .await
        .expect("delete edge");
    server
        .exec("GRAPH INSERT EDGE IN 'vt_rag' FROM 'n1' TO 'n3' TYPE 'rel'")
        .await
        .expect("insert edge");

    let fusion = |as_of: String| {
        format!(
            "GRAPH RAG FUSION ON vt_rag QUERY ARRAY[1.0, 0.0, 0.0] VECTOR_FIELD 'embedding' \
             VECTOR_TOP_K 1 EXPANSION_DEPTH 1 EDGE_LABEL 'rel' FINAL_TOP_K 10 {as_of}"
        )
    };
    let before = server
        .query_text(&fusion(format!("AS OF SYSTEM TIME {cut}")))
        .await
        .expect("historical fusion")
        .join("");
    assert!(has(&before, "n2") && !has(&before, "n3"), "{before}");
    let now = server
        .query_text(&fusion(String::new()))
        .await
        .expect("current fusion")
        .join("");
    assert!(!has(&now, "n2") && has(&now, "n3"), "{now}");
}
