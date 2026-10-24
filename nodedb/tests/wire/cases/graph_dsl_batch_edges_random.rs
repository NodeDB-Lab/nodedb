// SPDX-License-Identifier: BUSL-1.1

//! `GRAPH INSERT EDGES` and `GRAPH DELETE EDGES` over batches whose endpoints
//! hash to many different vShards.
//!
//! A batch of random node ids spans many `(src home, dst home)` pairs. The
//! statement must still be one transaction: every edge lands or none does, a
//! statement that ends on its deadline leaves either all edges or none, and a
//! retry of the same statement is idempotent. These tests pin that, counting
//! the landed edges with `GRAPH NEIGHBORS` reads.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use nodedb_types::id::VShardId;

use crate::harness::TestServer;

/// Edges per batch. Far above the few-pair shape, far below the 1000 cap.
const EDGES: usize = 240;

/// Deterministic pseudo-random node id (splitmix64), so a failure reproduces.
fn node_id(seed: u64, index: usize, side: char) -> String {
    let mut z = seed
        .wrapping_add((index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(if side == 's' {
            0
        } else {
            0xD1B5_4A32_D192_ED03
        });
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    format!("{side}{z:016x}")
}

/// `(src, dst)` pairs with random ids, all distinct.
fn random_edges(seed: u64, count: usize) -> Vec<(String, String)> {
    (0..count)
        .map(|i| (node_id(seed, i, 's'), node_id(seed, i, 'd')))
        .collect()
}

fn insert_sql(collection: &str, edges: &[(String, String)], label: &str) -> String {
    let values: Vec<String> = edges
        .iter()
        .map(|(s, d)| format!("('{s}','{d}','{label}')"))
        .collect();
    format!(
        "GRAPH INSERT EDGES IN '{collection}' VALUES {}",
        values.join(", ")
    )
}

fn delete_sql(collection: &str, edges: &[(String, String)], label: &str) -> String {
    insert_sql(collection, edges, label).replace("GRAPH INSERT EDGES", "GRAPH DELETE EDGES")
}

/// Distinct `(src home, dst home)` pairs of `edges`.
fn home_pairs(edges: &[(String, String)]) -> usize {
    edges
        .iter()
        .map(|(s, d)| {
            (
                VShardId::from_key(s.as_bytes()).as_u32(),
                VShardId::from_key(d.as_bytes()).as_u32(),
            )
        })
        .collect::<BTreeSet<_>>()
        .len()
}

/// Whether `node` has a neighbor `other` in `direction` (`out` or `in`).
/// A read error panics: it must not read as "the edge is absent".
async fn has_neighbor(
    server: &TestServer,
    collection: &str,
    node: &str,
    direction: &str,
    other: &str,
) -> bool {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{node}' DIRECTION {direction}"
        ))
        .await
        .unwrap_or_else(|e| panic!("neighbors of {node} failed: {e:?}"));
    rows.join("").contains(&format!("\"{other}\""))
}

/// Count of `edges` readable from their source (forward row).
async fn landed_out(server: &TestServer, collection: &str, edges: &[(String, String)]) -> usize {
    let mut n = 0;
    for (s, d) in edges {
        if has_neighbor(server, collection, s, "out", d).await {
            n += 1;
        }
    }
    n
}

/// Count of `edges` readable from their destination (reverse row).
async fn landed_in(server: &TestServer, collection: &str, edges: &[(String, String)]) -> usize {
    let mut n = 0;
    for (s, d) in edges {
        if has_neighbor(server, collection, d, "in", s).await {
            n += 1;
        }
    }
    n
}

/// Count carried by the single `CommandComplete` of a statement.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("statement failed: {e:?}"));
    let tags: Vec<u64> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(tags.len(), 1, "one CommandComplete expected, got {tags:?}");
    tags[0]
}

/// A batch of random ids spans many home pairs and lands completely, forward
/// and reverse, in one statement that reports the whole count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_id_batch_lands_completely() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION rnd_insert").await.unwrap();
    let edges = random_edges(0x369, EDGES);
    assert!(
        home_pairs(&edges) > 50,
        "the fixture must span many home pairs, got {}",
        home_pairs(&edges)
    );

    let started = Instant::now();
    let count = affected(&server, &insert_sql("rnd_insert", &edges, "L")).await;
    let elapsed = started.elapsed();

    assert_eq!(count as usize, EDGES, "the statement must count every edge");
    assert_eq!(
        landed_out(&server, "rnd_insert", &edges).await,
        EDGES,
        "every forward row must be readable"
    );
    assert_eq!(
        landed_in(&server, "rnd_insert", &edges).await,
        EDGES,
        "every reverse row must be readable"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "one transaction must not cost one round per edge: took {elapsed:?}"
    );
}

/// Every edge on one home (both endpoints co-resident) used to need the
/// per-edge route. It is one transaction now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_home_batch_lands_completely() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION rnd_single").await.unwrap();
    assert_eq!(
        VShardId::from_key(b"a"),
        VShardId::from_key(b"xy"),
        "the endpoints must be co-resident"
    );
    let values: Vec<String> = (0..60)
        .map(|i| format!("('a','xy','L{i}'), ('xy','a','M{i}')"))
        .collect();
    let count = affected(
        &server,
        &format!(
            "GRAPH INSERT EDGES IN 'rnd_single' VALUES {}",
            values.join(", ")
        ),
    )
    .await;
    assert_eq!(count, 120, "all 120 labelled edges must be counted");
    let rows = server
        .query_text("GRAPH NEIGHBORS IN 'rnd_single' OF 'a' DIRECTION out LABEL 'L59'")
        .await
        .unwrap()
        .join("");
    assert!(rows.contains("\"xy\""), "the last forward edge: {rows}");
    let rows = server
        .query_text("GRAPH NEIGHBORS IN 'rnd_single' OF 'xy' DIRECTION out LABEL 'M59'")
        .await
        .unwrap()
        .join("");
    assert!(rows.contains("\"a\""), "the last return edge: {rows}");
}

/// A member the statement refuses leaves nothing: the whole list is checked
/// before any edge is written, wherever the bad member sits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_with_a_refused_member_lands_nothing() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION rnd_bad").await.unwrap();
    let edges = random_edges(0x370, EDGES);

    let mut values: Vec<String> = edges
        .iter()
        .map(|(s, d)| format!("('{s}','{d}','L')"))
        .collect();
    // The last member carries an empty label, which the statement refuses.
    values.push(format!(
        "('{}','{}','')",
        node_id(0x370, EDGES, 's'),
        node_id(0x370, EDGES, 'd')
    ));
    let result = server
        .exec(&format!(
            "GRAPH INSERT EDGES IN 'rnd_bad' VALUES {}",
            values.join(", ")
        ))
        .await;
    assert!(
        result.is_err(),
        "the refused member must fail the statement"
    );
    assert_eq!(
        landed_out(&server, "rnd_bad", &edges).await,
        0,
        "no edge of a refused batch may land"
    );
    assert_eq!(landed_in(&server, "rnd_bad", &edges).await, 0);
}

/// A statement that ends on its deadline leaves all edges or none, never a
/// part. The budgets run from one that cannot be met up to one that can; the
/// invariant holds for each, and the landed count is read only after it stops
/// changing, because the transaction can still apply after the client gave up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_never_leaves_a_partial_batch() {
    let server = TestServer::start().await;
    for (i, budget) in ["1us", "5ms", "30ms", "120ms", "400ms", "5s"]
        .into_iter()
        .enumerate()
    {
        let collection = format!("rnd_deadline_{i}");
        server
            .exec(&format!("CREATE COLLECTION {collection}"))
            .await
            .unwrap();
        let edges = random_edges(0x400 + i as u64, EDGES);

        server
            .exec(&format!("SET statement_timeout = '{budget}'"))
            .await
            .unwrap();
        let outcome = server.exec(&insert_sql(&collection, &edges, "L")).await;
        server.exec("SET statement_timeout = 0").await.unwrap();

        // Wait until the landed count is stable for 1.5 s.
        let mut last = usize::MAX;
        let mut stable_since = Instant::now();
        let give_up = Instant::now() + Duration::from_secs(30);
        let landed = loop {
            let now = landed_out(&server, &collection, &edges).await;
            if now != last {
                last = now;
                stable_since = Instant::now();
            } else if stable_since.elapsed() > Duration::from_millis(1500) {
                break now;
            }
            assert!(Instant::now() < give_up, "the landed count never settled");
        };

        assert!(
            landed == 0 || landed == EDGES,
            "budget {budget}: a partial batch landed, {landed} of {EDGES} (statement: {outcome:?})"
        );
        if outcome.is_ok() {
            assert_eq!(
                landed, EDGES,
                "budget {budget}: success must land every edge"
            );
        }
        // An error that still landed every edge must not read as a refusal.
        if let Err(error) = &outcome
            && landed == EDGES
        {
            let text = format!("{error:?}");
            assert!(
                text.contains("outcome unknown"),
                "budget {budget}: a deadline error after a landed batch must say the outcome is unknown: {text}"
            );
        }
        if landed == EDGES {
            assert_eq!(
                landed_in(&server, &collection, &edges).await,
                EDGES,
                "budget {budget}: forward and reverse rows must agree"
            );
        }
    }
}

/// Delete batch, then insert the same batch twice: each step is idempotent and
/// the delete reports the edges it removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_after_delete_batch_is_idempotent() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION rnd_retry").await.unwrap();
    let edges = random_edges(0x371, EDGES);

    assert_eq!(
        affected(&server, &insert_sql("rnd_retry", &edges, "L")).await as usize,
        EDGES
    );
    assert_eq!(landed_out(&server, "rnd_retry", &edges).await, EDGES);

    // A delete's pre-image read can miss a just-acknowledged write, so the
    // best count over a few attempts is the one taken while the edges were live.
    let mut removed = 0;
    for _ in 0..8 {
        removed = removed.max(affected(&server, &delete_sql("rnd_retry", &edges, "L")).await);
        if landed_out(&server, "rnd_retry", &edges).await == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        removed as usize, EDGES,
        "the delete must count every live edge"
    );
    assert_eq!(landed_out(&server, "rnd_retry", &edges).await, 0);
    assert_eq!(landed_in(&server, "rnd_retry", &edges).await, 0);

    // A second delete of absent edges removes nothing and succeeds.
    assert_eq!(
        affected(&server, &delete_sql("rnd_retry", &edges, "L")).await,
        0,
        "a repeated delete must count zero"
    );

    // Insert, then insert again: the second statement rewrites the same edges.
    for round in 0..2 {
        assert_eq!(
            affected(&server, &insert_sql("rnd_retry", &edges, "L")).await as usize,
            EDGES,
            "insert round {round}"
        );
        assert_eq!(
            landed_out(&server, "rnd_retry", &edges).await,
            EDGES,
            "insert round {round}"
        );
    }
    // The retry did not duplicate: each source still has exactly one neighbor.
    let (s, d) = &edges[0];
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN 'rnd_retry' OF '{s}' DIRECTION out"
        ))
        .await
        .unwrap()
        .join("");
    assert_eq!(
        rows.matches(d.as_str()).count(),
        1,
        "one edge, not two: {rows}"
    );
}
