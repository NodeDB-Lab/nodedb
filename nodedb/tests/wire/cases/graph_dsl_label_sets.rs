// SPDX-License-Identifier: BUSL-1.1

//! Graph DSL walks over a label set.
//!
//! `LABEL 'a', 'b'` keeps an edge whose label is any listed label. An omitted
//! `LABEL` keeps every edge. An unknown label adds nothing to the set. The
//! seeded graph gives each label its own reachable node, so a walk that drops
//! or ignores one listed label misses that node.

use crate::harness::TestServer;

/// `hub -knows-> kn_node`, `hub -works-> wk_node`, `hub -likes-> lk_node`,
/// `kn_node -works-> far_node`.
async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    for (src, dst, label) in [
        ("hub", "kn_node", "knows"),
        ("hub", "wk_node", "works"),
        ("hub", "lk_node", "likes"),
        ("kn_node", "far_node", "works"),
    ] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN '{collection}' FROM '{src}' TO '{dst}' TYPE '{label}'"
            ))
            .await
            .unwrap();
    }
}

async fn blob(server: &TestServer, sql: &str) -> String {
    server
        .query_text_joined(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_follows_every_listed_label() {
    let server = TestServer::start().await;
    seed(&server, "lset_trav").await;

    let got = blob(
        &server,
        "GRAPH TRAVERSE IN 'lset_trav' FROM 'hub' DEPTH 2 LABEL 'knows', 'works' DIRECTION out",
    )
    .await;
    for node in ["kn_node", "wk_node", "far_node"] {
        assert!(
            got.contains(node),
            "{node} is reachable over the set: {got}"
        );
    }
    assert!(!got.contains("lk_node"), "'likes' is not listed: {got}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_parenthesised_label_list_matches_bare_list() {
    let server = TestServer::start().await;
    seed(&server, "lset_paren").await;

    let bare = blob(
        &server,
        "GRAPH TRAVERSE IN 'lset_paren' FROM 'hub' DEPTH 2 LABEL 'knows', 'works'",
    )
    .await;
    let paren = blob(
        &server,
        "GRAPH TRAVERSE IN 'lset_paren' FROM 'hub' DEPTH 2 LABEL ('knows', 'works')",
    )
    .await;
    assert_eq!(bare, paren);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_without_label_follows_every_edge() {
    let server = TestServer::start().await;
    seed(&server, "lset_all").await;

    let got = blob(&server, "GRAPH TRAVERSE IN 'lset_all' FROM 'hub' DEPTH 2").await;
    for node in ["kn_node", "wk_node", "lk_node", "far_node"] {
        assert!(got.contains(node), "{node} is reachable: {got}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn neighbors_follow_every_listed_label() {
    let server = TestServer::start().await;
    seed(&server, "lset_nbr").await;

    let got = blob(
        &server,
        "GRAPH NEIGHBORS IN 'lset_nbr' OF 'hub' LABEL 'knows', 'works', 'absent' DIRECTION out",
    )
    .await;
    assert!(got.contains("kn_node"), "{got}");
    assert!(got.contains("wk_node"), "{got}");
    assert!(!got.contains("lk_node"), "'likes' is not listed: {got}");

    let none = blob(
        &server,
        "GRAPH NEIGHBORS IN 'lset_nbr' OF 'hub' LABEL 'absent' DIRECTION out",
    )
    .await;
    for node in ["kn_node", "wk_node", "lk_node"] {
        assert!(
            !none.contains(node),
            "a set of only unknown labels keeps no edge: {none}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn path_follows_every_listed_label() {
    let server = TestServer::start().await;
    seed(&server, "lset_path").await;

    let got = blob(
        &server,
        "GRAPH PATH IN 'lset_path' FROM 'hub' TO 'far_node' MAX_DEPTH 4 LABEL 'knows', 'works'",
    )
    .await;
    assert!(
        got.contains("kn_node") && got.contains("far_node"),
        "the path crosses 'knows' then 'works': {got}"
    );

    let single = blob(
        &server,
        "GRAPH PATH IN 'lset_path' FROM 'hub' TO 'far_node' MAX_DEPTH 4 LABEL 'knows'",
    )
    .await;
    assert!(
        !single.contains("far_node"),
        "'knows' alone does not reach far_node: {single}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn label_without_quoted_label_is_a_syntax_error() {
    let server = TestServer::start().await;
    seed(&server, "lset_bad").await;

    for sql in [
        "GRAPH TRAVERSE IN 'lset_bad' FROM 'hub' LABEL DIRECTION out",
        "GRAPH TRAVERSE IN 'lset_bad' FROM 'hub' LABEL knows",
        "GRAPH NEIGHBORS IN 'lset_bad' OF 'hub' LABEL",
        "GRAPH PATH IN 'lset_bad' FROM 'hub' TO 'far_node' LABEL knows",
    ] {
        server.expect_error(sql, "SQLSTATE 42601").await;
        server.expect_error(sql, "LABEL").await;
    }
}
