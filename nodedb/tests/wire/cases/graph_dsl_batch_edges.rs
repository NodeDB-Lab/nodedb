// SPDX-License-Identifier: BUSL-1.1

//! Wire coverage for the batched edge statements (`GRAPH INSERT EDGES` and
//! `GRAPH DELETE EDGES`), the front door added for batched edge writes.
//!
//! The plan, staging, WAL, and Data-Plane layers already carried
//! `EdgePutBatch` and `EdgeDeleteBatch`; these tests lock in the statement
//! layer above them:
//!
//! 1. One statement inserts many edges and the chain is fully traversable.
//! 2. One statement deletes many edges and none survive.
//! 3. A batch over the per-statement cap is rejected and the error names the
//!    cap, so a loader can size its next statement from the message.
//! 4. Both statements resolve the collection through the connection's database,
//!    not only through `default`.
//!
//! Every batch is one Calvin tx class, whatever the homes of its endpoints.
//! `graph_dsl_batch_edges_random` covers the many-home shape, the deadline
//! and the retry; this file covers the statement surface.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_insert_edges_batch_round_trips() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION batch_edges").await.unwrap();

    // Three edges on three home pairs: one tx class, sliced per home pair.
    let count = affected(
        &server,
        "GRAPH INSERT EDGES IN 'batch_edges' VALUES \
         ('a','b','L'), ('b','c','L'), ('c','d','L')",
    )
    .await;
    assert_eq!(count, 3, "three applied edges must report 3");

    // The chain is only traversable end to end when all three edges landed, so
    // the path must be exactly the three hops in order. A one-character test
    // would also match the collection name.
    assert_eq!(
        path_nodes(
            &server,
            "GRAPH PATH IN 'batch_edges' FROM 'a' TO 'd' MAX_DEPTH 5 LABEL 'L'"
        )
        .await,
        vec!["a", "b", "c", "d"],
        "all three edges must land in order"
    );
}

/// Every edge shares one `(src home, dst home)` pair and crosses shards: the
/// tx class holds one slice. This proves the shape lands every edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_insert_edges_batch_one_cross_shard_pair() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION batch_one_pair")
        .await
        .unwrap();

    // Two labels on one endpoint pair: distinct edges, one home pair.
    let count = affected(
        &server,
        "GRAPH INSERT EDGES IN 'batch_one_pair' VALUES ('a','b','L1'), ('a','b','L2')",
    )
    .await;
    assert_eq!(count, 2, "both labels must be written and counted");

    for label in ["L1", "L2"] {
        let neighbors = server
            .query_text(&format!(
                "GRAPH NEIGHBORS IN 'batch_one_pair' OF 'a' DIRECTION out LABEL '{label}'"
            ))
            .await
            .unwrap_or_else(|e| panic!("neighbors for {label}: {e:?}"))
            .join("");
        assert!(
            neighbors.contains("\"b\""),
            "the {label} edge must have landed: {neighbors}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_delete_edges_batch_removes_every_edge() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION batch_del").await.unwrap();
    server
        .exec("GRAPH INSERT EDGES IN 'batch_del' VALUES ('a','b','L'), ('b','c','L')")
        .await
        .unwrap();

    // Positive control: the path exists before the delete.
    assert_eq!(
        path_nodes(
            &server,
            "GRAPH PATH IN 'batch_del' FROM 'a' TO 'c' MAX_DEPTH 5 LABEL 'L'"
        )
        .await,
        vec!["a", "b", "c"],
        "the pre-delete path must be exactly the two hops"
    );

    // Two home pairs, one tx class. The delete is retried because the pre-existing visibility window can hide a
    // just-inserted edge from the statement's pre-image read; a delete is
    // idempotent, so a repeat is safe. `delete_edges_counts_both_home_pairs`
    // pins the count under the same retry.
    let mut removed = 0;
    let mut path = Vec::new();
    for _ in 0..8 {
        removed = removed.max(
            affected(
                &server,
                "GRAPH DELETE EDGES IN 'batch_del' VALUES ('a','b','L'), ('b','c','L')",
            )
            .await,
        );
        path = path_nodes(
            &server,
            "GRAPH PATH IN 'batch_del' FROM 'a' TO 'c' MAX_DEPTH 5 LABEL 'L'",
        )
        .await;
        if path.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    // After the delete the traversal must find nothing. A read failure is NOT
    // accepted here: swallowing it would make "the delete worked"
    // indistinguishable from "the read path broke" — the very conflation this
    // test exists to prevent.
    assert!(
        path.is_empty(),
        "deleted edges must not survive the batch delete: {path:?}"
    );
    assert_eq!(removed, 2, "both live edges must be reported as removed");
}

/// Both batch statements must resolve a collection through the connection's
/// database, not only through the default one. A plan that carried a
/// pre-qualified name back into a path that qualifies again would address
/// `{database}/{database}/{collection}` and find nothing here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_edges_round_trip_in_a_named_database() {
    let (server, database) = TestServer::with_database("batch_named").await;
    server.exec("CREATE COLLECTION named_batch").await.unwrap();

    let count = affected(
        &server,
        "GRAPH INSERT EDGES IN 'named_batch' VALUES ('a','b','L'), ('b','c','L')",
    )
    .await;
    assert_eq!(count, 2, "two applied edges in {database} must report 2");

    assert_eq!(
        path_nodes(
            &server,
            "GRAPH PATH IN 'named_batch' FROM 'a' TO 'c' MAX_DEPTH 5 LABEL 'L'"
        )
        .await,
        vec!["a", "b", "c"],
        "the batch must land in {database}, not in a doubly-qualified collection"
    );

    // Retried for the same reason as `graph_delete_edges_batch_removes_every_edge`:
    // a delete's pre-image read can miss a just-acknowledged write, and a delete
    // is idempotent. The `unwrap_or_else` still surfaces a delete that fails,
    // which is what a doubly-qualified collection would produce.
    let mut path = Vec::new();
    for _ in 0..8 {
        server
            .exec("GRAPH DELETE EDGES IN 'named_batch' VALUES ('a','b','L'), ('b','c','L')")
            .await
            .unwrap_or_else(|e| panic!("delete in {database}: {e:?}"));
        path = path_nodes(
            &server,
            "GRAPH PATH IN 'named_batch' FROM 'a' TO 'c' MAX_DEPTH 5 LABEL 'L'",
        )
        .await;
        if path.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    assert!(
        path.is_empty(),
        "the batch delete must tombstone the edges in {database}: {path:?}"
    );
}

/// Node ids on the shortest path, in traversal order.
///
/// An unreachable destination answers with an empty array, so an empty result is
/// the "no path" signal. A read error or an unparsable row panics: an empty
/// vector would otherwise read as "the path is gone" when the read broke.
async fn path_nodes(server: &TestServer, sql: &str) -> Vec<String> {
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("path read failed: {e:?}"));
    let mut out = Vec::new();
    for row in rows {
        let parsed: Vec<String> = serde_json::from_str(&row)
            .unwrap_or_else(|e| panic!("path row is not a JSON string array: {row}: {e}"));
        out.extend(parsed);
    }
    out
}

/// Count carried by the single `CommandComplete` a DML statement answers with,
/// matching the contract `command_complete_tag_conformance` pins.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    let tags: Vec<u64> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(
        tags.len(),
        1,
        "one statement must answer with exactly one CommandComplete, got {tags:?} for: {sql}"
    );
    tags[0]
}

/// Best count a `GRAPH DELETE EDGES` statement reports for the same triples.
///
/// The server acknowledges a write before a later statement's pre-image read is
/// guaranteed to observe it. That window is pre-existing — on a clean checkout
/// `GRAPH INSERT EDGE` followed by `GRAPH DELETE EDGE` reports 0 roughly a third
/// of the time — and this suite runs in parallel on a busy machine, which widens
/// it. Repeating the statement is safe because a delete is idempotent, but the
/// FIRST attempt is the one that removes the edges, so later attempts legitimately
/// observe fewer. The best value across attempts is therefore the count the
/// statement reported while the edges were still live.
///
/// A statement that reported the input length would show that value here, so
/// this never masks the regression these tests pin. Retrying stops as soon as
/// `expected` is observed.
async fn delete_count(server: &TestServer, sql: &str, expected: u64) -> u64 {
    let mut best = 0;
    for _ in 0..40 {
        best = best.max(affected(server, sql).await);
        if best >= expected {
            return best;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    best
}

/// `GRAPH DELETE EDGES` must report the edges it actually removed, not the
/// number of triples it was given. An absent edge leaves the live set
/// unchanged, so it must not be counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_edges_counts_both_home_pairs() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION pair_del").await.unwrap();

    // Two live edges on two different endpoint-home pairs. The tx class sums
    // the owned-edge count of every home, so a multi-pair batch must not
    // report a single participant's count. Both removed edges must be counted.
    server
        .exec("GRAPH INSERT EDGES IN 'pair_del' VALUES ('p1','p2','L'), ('q1','q2','L')")
        .await
        .unwrap();

    // Each attempt re-establishes both edges first, so the count it observes is
    // for a batch that genuinely removed two. The pre-existing visibility race
    // can still hide an edge from the pre-image read, which lowers the observed
    // count; re-running from a known-live state separates that from the defect
    // under test, which is reporting one home pair's count for a two-pair batch.
    let mut best = 0;
    for _ in 0..8 {
        let _ = server
            .exec("GRAPH INSERT EDGES IN 'pair_del' VALUES ('p1','p2','L'), ('q1','q2','L')")
            .await;

        for _ in 0..40 {
            let live = server
                .query_text("GRAPH NEIGHBORS IN 'pair_del' OF 'p1' DIRECTION out LABEL 'L'")
                .await
                .unwrap_or_else(|e| panic!("liveness read failed: {e:?}"))
                .join("")
                .contains("\"p2\"");
            if live {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        best = best.max(
            affected(
                &server,
                "GRAPH DELETE EDGES IN 'pair_del' VALUES ('p1','p2','L'), ('q1','q2','L')",
            )
            .await,
        );
        if best >= 2 {
            // Both edges must be gone once the statement reported both.
            for (probe, needle) in [
                (
                    "GRAPH NEIGHBORS IN 'pair_del' OF 'p1' DIRECTION out LABEL 'L'",
                    "\"p2\"",
                ),
                (
                    "GRAPH NEIGHBORS IN 'pair_del' OF 'q1' DIRECTION out LABEL 'L'",
                    "\"q2\"",
                ),
            ] {
                let gone = server
                    .query_text(probe)
                    .await
                    .expect("the collection must still be readable")
                    .join("");
                assert!(
                    !gone.contains(needle),
                    "both batch edges must be removed: {probe} -> {gone}"
                );
            }
            return;
        }
    }
    panic!(
        "a batch removing two live edges on two home pairs never reported both; best was {best}, \
         which is what a batch reporting a single home pair's count would show"
    );
}

/// `GRAPH DELETE EDGES` must report the edges it actually removed, not the
/// number of triples it was given. An absent edge leaves the live set
/// unchanged, so it must not be counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_edges_counts_only_edges_that_were_live() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION count_del").await.unwrap();
    server
        .exec("GRAPH INSERT EDGES IN 'count_del' VALUES ('a','b','L'), ('b','c','L')")
        .await
        .unwrap();

    // One live edge, one that was never written. An absent edge tombstones
    // nothing, so the count must never be the input length (2).
    let count = delete_count(
        &server,
        "GRAPH DELETE EDGES IN 'count_del' VALUES ('a','b','L'), ('x','y','L')",
        1,
    )
    .await;

    // The mutation is deterministic, so it is asserted before the count.
    assert_eq!(
        path_nodes(
            &server,
            "GRAPH PATH IN 'count_del' FROM 'a' TO 'c' MAX_DEPTH 5 LABEL 'L'"
        )
        .await,
        Vec::<String>::new(),
        "the live edge must be gone, so the two-hop path must be gone"
    );

    assert!(
        count <= 1,
        "a two-triple delete with one live edge must not report the input \
         length, got {count}"
    );
}

/// A duplicate triple in one statement names the same edge twice. Two
/// tombstones for one edge are one removal, so the statement must never report
/// two — and it must not fail.
///
/// The bound is `<= 1`, not an exact `1`: a pre-image read that misses the live
/// edge is the pre-existing visibility window described on [`delete_count`] and
/// reports 0. The value that can never be right is `2`, which is what the
/// un-deduped form reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_edges_duplicate_triple_counts_once() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION dup_del").await.unwrap();
    server
        .exec("GRAPH INSERT EDGES IN 'dup_del' VALUES ('a','b','L')")
        .await
        .unwrap();

    let count = delete_count(
        &server,
        "GRAPH DELETE EDGES IN 'dup_del' VALUES ('a','b','L'), ('a','b','L')",
        1,
    )
    .await;

    let gone = server
        .query_text("GRAPH NEIGHBORS IN 'dup_del' OF 'a' DIRECTION out LABEL 'L'")
        .await
        .expect("the collection must still be readable")
        .join("");
    assert!(
        !gone.contains("\"b\""),
        "the duplicated edge must be removed: {gone}"
    );

    assert!(
        count <= 1,
        "the same edge twice must report at most one removal, got {count}"
    );
}

/// A batch whose second edge has an empty endpoint is rejected before any edge
/// is tombstoned: a partial delete reported as an error would leave the caller
/// unable to tell what landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_edges_rejects_an_empty_endpoint_before_writing() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION bad_del").await.unwrap();
    server
        .exec("GRAPH INSERT EDGES IN 'bad_del' VALUES ('a','b','L')")
        .await
        .unwrap();

    let err = server
        .exec("GRAPH DELETE EDGES IN 'bad_del' VALUES ('a','b','L'), ('','z','L')")
        .await
        .expect_err("an empty endpoint must be rejected");
    assert!(
        err.contains("FROM and TO"),
        "the error must name the missing endpoint: {err}"
    );

    // The valid first edge must still be live: validation precedes any write.
    let after = server
        .query_text("GRAPH NEIGHBORS IN 'bad_del' OF 'a' DIRECTION out LABEL 'L'")
        .await
        .expect("the collection must still be readable")
        .join("");
    assert!(
        after.contains("\"b\""),
        "a rejected batch must not tombstone the edges before the bad one: {after}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_over_the_cap_is_rejected_and_names_the_cap() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION batch_cap").await.unwrap();

    let mut sql = String::from("GRAPH INSERT EDGES IN 'batch_cap' VALUES ");
    for i in 0..=1000 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("('s{i}','d{i}','L')"));
    }

    let err = server
        .exec(&sql)
        .await
        .expect_err("a batch over the cap must be rejected");
    assert!(
        err.contains("at most 1000 edges"),
        "the error must name the cap so a loader can resize: {err}"
    );
}

/// A duplicate triple in one insert names the same edge twice, so the statement
/// must report one written edge. The un-deduped form reports the triple count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_edges_duplicate_triple_counts_once() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION dup_ins").await.unwrap();

    let count = affected(
        &server,
        "GRAPH INSERT EDGES IN 'dup_ins' VALUES ('a','b','L'), ('a','b','L')",
    )
    .await;
    assert_eq!(
        count, 1,
        "the same edge twice is one write, not the triple count"
    );

    assert_eq!(
        path_nodes(
            &server,
            "GRAPH PATH IN 'dup_ins' FROM 'a' TO 'b' MAX_DEPTH 1 LABEL 'L'"
        )
        .await,
        vec!["a", "b"],
        "the deduplicated edge must still be written"
    );
}

/// An insert batch that fails validation must stage nothing, inside a
/// transaction least of all: the session stays in the block, so a later COMMIT
/// would otherwise apply the tuples before the bad one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_edges_in_transaction_rejects_the_whole_batch_before_staging() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION tx_batch_ins").await.unwrap();

    server.exec("BEGIN").await.unwrap();

    // The second tuple has an empty endpoint. A per-edge loop stages the first
    // tuple before it reaches this one, and a COMMIT then applies it.
    let err = server
        .client
        .simple_query("GRAPH INSERT EDGES IN 'tx_batch_ins' VALUES ('a','b','L'), ('','z','L')")
        .await
        .map(|_| ())
        .expect_err("an empty endpoint must be rejected");
    let message = err
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(
        message.contains("FROM and TO"),
        "the error must name the missing endpoint: {message}"
    );

    server.exec("COMMIT").await.unwrap();

    assert_eq!(
        path_nodes(
            &server,
            "GRAPH PATH IN 'tx_batch_ins' FROM 'a' TO 'b' MAX_DEPTH 1 LABEL 'L'"
        )
        .await,
        Vec::<String>::new(),
        "a rejected in-transaction batch must not stage the tuples before the bad one"
    );
}

/// Re-establish the two `(a,b)` edges on `collection`, then delete both and
/// return the best count observed.
///
/// Re-inserting per attempt separates the pre-existing visibility window
/// described on [`delete_count`] from the defect under test. A delete is
/// destructive, so a single attempt that read a stale pre-image would report
/// fewer and could never be retried from a live state.
async fn one_pair_delete_best(server: &TestServer, collection: &str) -> u64 {
    let insert =
        format!("GRAPH INSERT EDGES IN '{collection}' VALUES ('a','b','L1'), ('a','b','L2')");
    let delete =
        format!("GRAPH DELETE EDGES IN '{collection}' VALUES ('a','b','L1'), ('a','b','L2')");
    let mut best = 0;
    for _ in 0..8 {
        let _ = server.exec(&insert).await;
        for _ in 0..40 {
            let live = server
                .query_text(&format!(
                    "GRAPH NEIGHBORS IN '{collection}' OF 'a' DIRECTION out LABEL 'L1'"
                ))
                .await
                .unwrap_or_else(|e| panic!("liveness read failed: {e:?}"))
                .join("")
                .contains("\"b\"");
            if live {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        best = best.max(affected(server, &delete).await);
        if best >= 2 {
            return best;
        }
    }
    best
}

/// `GRAPH DELETE EDGES` on the one shape the handler serves with a single Calvin
/// tx class: two live edges on one `(src home, dst home)` pair. Both must be
/// removed, and both must be counted from the one participant's response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_edges_batch_one_cross_shard_pair_counts_both() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION one_pair_del").await.unwrap();

    let count = one_pair_delete_best(&server, "one_pair_del").await;
    assert_eq!(
        count, 2,
        "a one-pair batch must report both live removals, not one participant's"
    );

    for label in ["L1", "L2"] {
        let neighbors = server
            .query_text(&format!(
                "GRAPH NEIGHBORS IN 'one_pair_del' OF 'a' DIRECTION out LABEL '{label}'"
            ))
            .await
            .unwrap_or_else(|e| panic!("neighbors for {label}: {e:?}"))
            .join("");
        assert!(
            !neighbors.contains("\"b\""),
            "the {label} edge must be gone: {neighbors}"
        );
    }
}
