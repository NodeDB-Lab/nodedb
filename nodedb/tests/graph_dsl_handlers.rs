//! Integration coverage for the Graph DSL pgwire handlers
//! (`pgwire/ddl/graph_ops.rs`, `pgwire/ddl/match_ops.rs`).
//!
//! The handlers are substring-parsed and thin-dispatched; this file locks
//! in four correctness contracts that must hold for every graph DSL
//! statement reaching the pgwire layer:
//!
//! 1. `GRAPH PATH` returns an actual source→…→destination path, not the
//!    BFS discovery set.
//! 2. `MATCH` returns user-facing (unscoped) node ids, never the internal
//!    `{tenant_id}:{name}` form.
//! 3. Numeric DSL parameters (`DEPTH`, `MAX_DEPTH`, `ITERATIONS`,
//!    `VECTOR_TOP_K`) are clamped; absurd values are rejected with
//!    SQLSTATE 22023 instead of being forwarded unchanged to the engine.
//! 4. The `extract_*_after` helpers must parse statement structure, not
//!    user data: node ids, labels, and object-literal property values
//!    that contain DSL keywords must not short-circuit extraction.
//!
//! Every test asserts the correct specification. Tests for items that
//! are currently broken will fail until the fixes land — that red is the
//! intended regression surface for the handler rewrite.

mod common;

use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

// ── 1. GRAPH PATH returns a path, not a BFS frontier ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_path_returns_ordered_path_not_bfs_frontier() {
    let server = TestServer::start().await;

    server.exec("CREATE COLLECTION path_nodes").await.unwrap();

    // Chain a → b → c, plus branches off `a` that must not appear in the path.
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'b' TO 'c' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'x' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'y' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH PATH FROM 'a' TO 'c' MAX_DEPTH 5 LABEL 'l'")
        .await
        .unwrap();

    let blob = rows.join("");
    // Regression guard: the handler must not return branch nodes that
    // happen to be in the BFS closure but are not on the path.
    assert!(
        !blob.contains("\"x\"") && !blob.contains("\"y\""),
        "GRAPH PATH must not leak BFS frontier; got: {blob}"
    );
    // Spec: the path is exactly [a, b, c] in order.
    let idx_a = blob.find("\"a\"").expect("path must include src");
    let idx_b = blob
        .find("\"b\"")
        .expect("path must include intermediate hop");
    let idx_c = blob.find("\"c\"").expect("path must include dst");
    assert!(
        idx_a < idx_b && idx_b < idx_c,
        "path must be ordered src→hop→dst; got: {blob}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_path_returns_empty_when_dst_unreachable() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION path_nodes").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH PATH FROM 'a' TO 'z' MAX_DEPTH 5 LABEL 'l'")
        .await
        .unwrap();
    let blob = rows.join("");
    assert!(
        !blob.contains("\"a\"") && !blob.contains("\"b\""),
        "unreachable dst must yield empty path, not BFS closure; got: {blob}"
    );
}

// ── 2. MATCH returns unscoped node ids ────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn match_does_not_leak_tenant_scoped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION match_docs").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'alice' TO 'bob' TYPE 'l'")
        .await
        .unwrap();

    // Read all columns — `query_text` only exposes column 0.
    let msgs = server
        .client
        .simple_query("MATCH (x)-[:l]->(y) RETURN x, y")
        .await
        .expect("MATCH should succeed");

    let mut row_count = 0usize;
    for msg in msgs {
        if let SimpleQueryMessage::Row(row) = msg {
            row_count += 1;
            let x = row.get(0).unwrap_or("").to_string();
            let y = row.get(1).unwrap_or("").to_string();
            // Regression guard: no "<digits>:" tenant prefix may leak.
            assert!(
                !x.contains(':'),
                "column x must be unscoped (no tenant prefix); got {x:?}"
            );
            assert!(
                !y.contains(':'),
                "column y must be unscoped (no tenant prefix); got {y:?}"
            );
            assert_eq!(x, "alice", "expected unscoped src id");
            assert_eq!(y, "bob", "expected unscoped dst id");
        }
    }
    assert_eq!(row_count, 1, "expected exactly one matched row");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn match_preserves_digit_prefixed_user_id() {
    // The exit-boundary unscoper must strip the exact `<tid>:` prefix,
    // not any leading `\d+:` run. A user id like `'99:event'` — a
    // composite stringified key — must round-trip through MATCH
    // unchanged. A heuristic stripper silently drops the `99:`.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION mdp_docs").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM '99:event' TO 'bob' TYPE 'l'")
        .await
        .unwrap();

    let msgs = server
        .client
        .simple_query("MATCH (x)-[:l]->(y) RETURN x, y")
        .await
        .expect("MATCH should succeed");
    let mut saw = false;
    for msg in msgs {
        if let SimpleQueryMessage::Row(row) = msg {
            let x = row.get(0).unwrap_or("").to_string();
            if x == "99:event" {
                saw = true;
            }
            assert_ne!(
                x, "event",
                "MATCH must not corrupt '99:event' into 'event' via heuristic strip"
            );
        }
    }
    assert!(saw, "MATCH must return the full user id '99:event'");
}

// ── 3. Numeric DSL parameters are clamped / rejected ──────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_traverse_rejects_absurd_depth() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION tdocs").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    // Out-of-range numeric params must be rejected with SQLSTATE 22023
    // before dispatch, not forwarded to `cross_core_bfs` unchanged.
    server
        .expect_error(
            "GRAPH TRAVERSE FROM 'a' DEPTH 4294967295 LABEL 'l' DIRECTION both",
            "22023",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_path_rejects_absurd_max_depth() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION pdocs").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    server
        .expect_error(
            "GRAPH PATH FROM 'a' TO 'b' MAX_DEPTH 4294967295 LABEL 'l'",
            "22023",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_algo_rejects_absurd_iterations() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION algodocs").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    server
        .expect_error(
            "GRAPH ALGO PAGERANK ON algodocs ITERATIONS 1000000000 TOLERANCE 1e-30",
            "22023",
        )
        .await;
}

// ── Exit-boundary unscoping must be exact, not heuristic ─────────────
//
// TRAVERSE / NEIGHBORS / PATH return user-visible node ids. The
// unscoper at the handler boundary must strip the exact `<tid>:`
// prefix the Data Plane prepended, not "everything up to the first
// colon" — the latter corrupts any colon-containing user id.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_traverse_preserves_colon_containing_user_id() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION trv_docs").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'foo:bar' TO 'z' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH TRAVERSE FROM 'foo:bar' DEPTH 2 LABEL 'l'")
        .await
        .expect("TRAVERSE must succeed");
    let blob = rows.join("");
    assert!(
        blob.contains("\"foo:bar\"") || blob.contains("foo:bar"),
        "TRAVERSE must preserve 'foo:bar' exactly; got: {blob}"
    );
    assert!(
        !blob.contains("\"bar\""),
        "TRAVERSE must not corrupt 'foo:bar' into 'bar' via first-colon strip; got: {blob}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_neighbors_preserves_colon_containing_user_id() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION nbr_docs").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'src' TO 'ns:dst' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH NEIGHBORS OF 'src' LABEL 'l' DIRECTION out")
        .await
        .expect("NEIGHBORS must succeed");
    let blob = rows.join("");
    assert!(
        blob.contains("ns:dst"),
        "NEIGHBORS must preserve 'ns:dst' exactly; got: {blob}"
    );
    assert!(
        !blob.contains("\"dst\""),
        "NEIGHBORS must not corrupt 'ns:dst' into 'dst' via first-colon strip; got: {blob}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_path_preserves_colon_containing_user_id() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION pp_docs").await.unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'src' TO 'ns:mid' TYPE 'l'")
        .await
        .unwrap();
    server
        .exec("GRAPH INSERT EDGE FROM 'ns:mid' TO 'dst' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH PATH FROM 'src' TO 'dst' MAX_DEPTH 5 LABEL 'l'")
        .await
        .expect("PATH must succeed");
    let blob = rows.join("");
    assert!(
        blob.contains("ns:mid"),
        "PATH must preserve intermediate 'ns:mid' exactly; got: {blob}"
    );
    assert!(
        !blob.contains("\"mid\""),
        "PATH must not corrupt 'ns:mid' into 'mid' via first-colon strip; got: {blob}"
    );
}

// ── 4. extract_*_after must parse statement structure, not user data ─

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_insert_edge_with_keyword_shaped_node_ids() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION kwnodes").await.unwrap();

    // Node ids, label, and edge type all shadow DSL keywords. The
    // handler must parse statement structure (quoted values after
    // keywords), not match the literal substrings 'TO', 'FROM',
    // 'LABEL', 'TYPE' inside user data.
    server
        .exec("GRAPH INSERT EDGE FROM 'TO' TO 'FROM' TYPE 'LABEL'")
        .await
        .expect("keyword-shaped ids must parse");

    // The edge is addressable from its real src 'TO', not the literal
    // 'FROM' that appears later in the statement.
    let neighbors = server
        .query_text("GRAPH NEIGHBORS OF 'TO' LABEL 'LABEL' DIRECTION out")
        .await
        .unwrap();
    let blob = neighbors.join("");
    assert!(
        blob.contains("\"FROM\""),
        "neighbor lookup must return dst id 'FROM' (was the TO-arg); got: {blob}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_insert_edge_properties_with_brace_in_string_value() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION bracedocs").await.unwrap();

    // Object-literal extraction must be brace/quote-aware, not
    // "everything from `{` to end of statement minus `;`". A `}` or
    // `;` embedded in a string value must not terminate the object.
    server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'l' PROPERTIES { note: '} DEPTH 999' }")
        .await
        .expect("brace-balanced property parsing must accept `}` inside strings");

    // And the note value must not have been mis-parsed as a DEPTH
    // argument — the edge still exists with its correct TYPE.
    let rows = server
        .query_text("GRAPH NEIGHBORS OF 'a' LABEL 'l' DIRECTION out")
        .await
        .unwrap();
    let blob = rows.join("");
    assert!(
        blob.contains("\"b\""),
        "edge must exist despite `}}` inside property value; got: {blob}"
    );
}

// ── Edge-label validation at DSL ingress ──────────────────────────────
//
// `CsrIndex::ensure_label` silently truncates `id_to_label.len() as u16`
// past 65 536 distinct labels, aliasing id 1 with a later unrelated label.
// The DSL handler (`graph_ops/edge.rs`) passes the raw user string
// through with no validation. Any correct fix MUST reject empty labels,
// control characters, and labels over a length cap at ingress — so the
// interner never sees degenerate input that could hit the wrap path.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_insert_edge_rejects_empty_label() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION lblchk").await.unwrap();

    // Spec: an empty label is never a valid edge type. Current code
    // accepts it and interns it as the 0-length string.
    let res = server
        .exec("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE ''")
        .await;
    assert!(
        res.is_err(),
        "empty TYPE must be rejected at DSL ingress; got Ok"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_insert_edge_rejects_control_chars_in_label() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION lblchk2").await.unwrap();

    // Spec: labels with ASCII control characters (0x00..=0x1F) are
    // rejected at ingress. Inject a literal control byte into the TYPE
    // value to bypass any E-string unescaping differences.
    let sql = format!(
        "GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE 'bad{}label'",
        '\u{0001}'
    );
    let res = server.exec(&sql).await;
    assert!(
        res.is_err(),
        "labels with control characters must be rejected at DSL ingress; got Ok"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_insert_edge_rejects_overlong_label() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION lblchk3").await.unwrap();

    // Spec: label length is capped at a reasonable limit (the fix picks
    // the exact cap; 256 bytes is a common choice). A 4 KiB label MUST
    // be rejected regardless of where the cap lands.
    let overlong = "x".repeat(4096);
    let sql = format!("GRAPH INSERT EDGE FROM 'a' TO 'b' TYPE '{overlong}'");
    let res = server.exec(&sql).await;
    assert!(
        res.is_err(),
        "4 KiB label must be rejected at DSL ingress; got Ok"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_traverse_node_id_containing_keyword_substring() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION kwnodes2").await.unwrap();

    // A node id containing the literal substring DEPTH must not be
    // mistaken for the DEPTH keyword. The handler must tokenise, not
    // `upper.find("DEPTH")`.
    server
        .exec("GRAPH INSERT EDGE FROM 'node_with_DEPTH_in_name' TO 'b' TYPE 'l'")
        .await
        .unwrap();

    let rows = server
        .query_text("GRAPH TRAVERSE FROM 'node_with_DEPTH_in_name' DEPTH 2 LABEL 'l'")
        .await
        .expect("traversal from keyword-substring node id must succeed");
    let blob = rows.join("");
    assert!(
        blob.contains("\"b\""),
        "traversal must reach 'b'; substring 'DEPTH' in src id must not be parsed as DEPTH keyword; got: {blob}"
    );
}
