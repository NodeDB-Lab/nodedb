// SPDX-License-Identifier: BUSL-1.1
//! Cross-node PK point-lookup correctness from a non-member coordinator.
//!
//! ## The bug this guards against
//!
//! A PK point-lookup (`SELECT ... WHERE id = <pk>`) resolves pk → surrogate on
//! the QUERY COORDINATOR's local catalog. The surrogate↔PK map
//! (`surrogate_pk{,_rev}_v3`) is SHARDED to the collection's data-group
//! members. `document_strict` collections are single-vShard-homed, so when the
//! coordinator is NOT a member of that group, resolution misses → the
//! coordinator ships `Surrogate::ZERO` to the owner → the owner renders
//! `StorageKey::for_surrogate(ZERO)` → the row is NOT FOUND. So cross-node PK reads
//! from a non-member coordinator silently returned EMPTY.
//!
//! Scans are unaffected: they route + scan on the owner with no surrogate
//! resolution, which is why `COUNT(*)` worked even while the point-get failed.
//!
//! ## The invariant being verified
//!
//! The owner's `exec_receiver` re-resolves a ZERO `DocumentOp::PointGet`
//! surrogate against ITS OWN local catalog (it is a group member, so it holds
//! the binding) after decoding the plan and before dispatching to the Data
//! Plane. The point-get must succeed from EVERY coordinator in
//! the cluster — member or not.
//!
//! ## Test shape
//!
//!  1. Spawn a 3-node cluster, create a `document_strict` collection with a PK,
//!     insert a few rows via one node, and converge.
//!  2. For EVERY node, `SELECT payload FROM coll WHERE id = 'row-0'` and assert
//!     it returns the expected value, including from a non-member
//!     coordinator.
//!  3. Sanity: `COUNT(*)` from every node equals the row count (scans already
//!     worked even with the bug — this proves the cluster is healthy and the
//!     point-get failure was isolated to surrogate resolution).

use crate::common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

const ROW_COUNT: u32 = 5;

/// Format a `tokio_postgres` error as `sqlstate: message` (or plain text).
fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// Is this error a transient cluster catch-up condition that warrants a retry
/// (catalog/descriptor lag), as opposed to a genuine empty/wrong result?
///
/// We retry ONLY on:
///   - "table not found" / "collection not found" (sqlstate 42601): the catalog
///     has not yet propagated to this coordinator.
///   - "schema changed during execution" / "please retry": a descriptor version
///     race that resolves on the next attempt.
///
/// We never retry a successful-but-empty result — that is the bug, and the
/// caller asserts on it directly.
fn is_transient(e: &tokio_postgres::Error) -> bool {
    if let Some(db) = e.as_db_error() {
        let code = db.code().code();
        let msg = db.message();
        code == "42601"
            || msg.contains("table not found")
            || msg.contains("collection not found")
            || msg.contains("schema changed during execution")
            || msg.contains("please retry")
    } else {
        false
    }
}

/// Run `SELECT payload FROM <coll> WHERE id = <pk>` on `client`, returning the
/// single `payload` value. Retries only transient catch-up errors until
/// `timeout`; a query that SUCCEEDS but returns no row is returned as `None`
/// (the caller asserts it is `Some`, catching the cross-node-empty bug).
async fn point_get_payload(
    client: &tokio_postgres::Client,
    coll: &str,
    pk: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT payload FROM {coll} WHERE id = '{pk}'"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                        return r.get(0).map(|s| s.to_string());
                    }
                }
                // Query succeeded with zero data rows. This is the failure mode
                // the test exists to catch — do NOT retry it.
                return None;
            }
            Err(ref e) => {
                if is_transient(e) && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                panic!("point-get `{pk}` on {coll} failed: {}", pg_detail(e));
            }
        }
    }
}

/// Run `SELECT COUNT(*) FROM <coll>`, retrying only transient catch-up errors.
async fn count_rows(client: &tokio_postgres::Client, coll: &str, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT COUNT(*) FROM {coll}"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
                        && let Some(s) = r.get(0)
                    {
                        return s.parse::<usize>().expect("COUNT(*) parse");
                    }
                }
                panic!("COUNT(*) returned no rows for {coll}");
            }
            Err(ref e) => {
                if is_transient(e) && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                panic!("COUNT(*) on {coll} failed: {}", pg_detail(e));
            }
        }
    }
}

/// Every node in the cluster — including coordinators that are NOT members of
/// the collection's single-homed data group — must resolve a PK point-lookup
/// to the owning row's value, not ship `Surrogate::ZERO` from a non-member
/// coordinator and get an empty result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_node_pk_point_lookup_resolves_from_every_coordinator() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION xn_pk \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION xn_pk");

    // Insert the rows through a single node. The collection is single-homed,
    // so exactly one vShard owner holds the surrogate↔PK binding for all keys;
    // the other two nodes are non-members for these keys.
    for i in 0..ROW_COUNT {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO xn_pk (id, payload) VALUES ('row-{i}', 'payload-{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert row-{i}: {}", pg_detail(&e)));
    }

    // Single deterministic barrier: every replica's Data Plane has applied
    // every committed entry, so every node's read below sees the same state.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // The point-lookup must resolve from EVERY coordinator, and the scan-based
    // COUNT(*) must agree on every node (scans already worked — this is a
    // health sanity check that isolates the point-get path as the regression).
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let count = count_rows(&node.client, "xn_pk", Duration::from_secs(10)).await;
        assert_eq!(
            count, ROW_COUNT as usize,
            "node {idx}: COUNT(*) = {count}, expected {ROW_COUNT}"
        );

        for i in 0..ROW_COUNT {
            let pk = format!("row-{i}");
            let got = point_get_payload(&node.client, "xn_pk", &pk, Duration::from_secs(10)).await;
            assert_eq!(
                got.as_deref(),
                Some(format!("payload-{i}").as_str()),
                "node {idx}: PK point-lookup for `{pk}` returned {got:?} \
                 (a non-member coordinator shipped Surrogate::ZERO before the fix)"
            );
        }
    }

    cluster.shutdown().await;
}
