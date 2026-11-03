// SPDX-License-Identifier: BUSL-1.1

//! A `DELETE ... RETURNING` routed through Calvin surfaces its deleted rows,
//! not a bare command tag.
//!
//! An edge-bearing schemaless collection routes a `DELETE FROM coll WHERE id =
//! 'x'` through the OLLP/Calvin dependent path (the implicit-edge routing gate
//! rewrites it as a `BulkDelete` so mirrored edges are cleaned atomically). The
//! Calvin completion path signals done via a Raft-replicated ack that carries no
//! payload, so before this fix the applied Data-Plane response — including the
//! `RETURNING` rows — was dropped and the client saw only `DELETE 1`.
//!
//! This test proves the fix: with the single-node Calvin stack on, deleting one
//! implicit-edge document with `RETURNING *` returns that document's row through
//! the completion path.
//!
//! The same path evaluates a RETURNING expression as the direct path does: a
//! shaping error keeps its own SQLSTATE and native code, and `nextval` reads
//! the session's sequences.
//!
//! File name contains "calvin" within the cluster-tests crate so nextest applies
//! the cluster test-group serialization.

use crate::common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::cluster_harness::{TestClusterNode, wait_for};

/// Number of implicit-edge documents seeded before the RETURNING delete.
const SOURCES: usize = 6;

/// Count of transactions the single-node sequencer has admitted to an epoch, or
/// `0` if the sequencer metrics handle is not installed yet.
fn sequencer_admitted(node: &TestClusterNode) -> u64 {
    node.shared
        .sequencer_metrics
        .get()
        .map(|m| m.admitted_total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Whether `coll` is flagged edge-bearing in this node's local catalog. The
/// implicit-edge mark is committed via the replicated metadata path, so the
/// DELETE must wait for it before planning — otherwise the PK delete lowers to a
/// static `PointDelete` (fast path) instead of the Calvin/OLLP `BulkDelete`.
fn collection_edge_bearing(node: &TestClusterNode, coll: &str) -> bool {
    node.shared
        .credentials
        .catalog()
        .load_collections_for_tenant(nodedb_types::DatabaseId::DEFAULT, 1)
        .map(|v| v.iter().any(|c| c.name == coll && c.has_implicit_edges))
        .unwrap_or(false)
}

/// Spawn a single-node Calvin server and seed `coll` with [`SOURCES`]
/// implicit-edge documents `edge_0..`, each carrying `n: 0`. Returns once the
/// collection is edge-bearing, so a PK delete on it routes through Calvin.
async fn calvin_edge_collection(coll: &str) -> TestClusterNode {
    // 4 Data-Plane cores so the edge endpoints land on distinct vShards — the
    // doc-delete participant carries the RETURNING rows while the edge-delete
    // participants carry none, exercising the deposit gate.
    let node = TestClusterNode::spawn_single_node_calvin(4)
        .await
        .expect("spawn standalone single-node-calvin server");

    // The lone sequencer voter self-elects; wait for it before submitting.
    wait_for(
        "single-node sequencer leader elected",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.sequencer_leader() == node.node_id,
    )
    .await;

    node.client
        .simple_query(&format!(
            "CREATE COLLECTION {coll} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("CREATE COLLECTION");
    wait_for(
        "collection visible on the node",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || node.cached_collection_count() >= 1,
    )
    .await;

    // src_0..src_(SOURCES-1) -> hub as IMPLICIT edges (plain docs carrying
    // _from/_to/_type). Inserting an edge document marks the collection
    // edge-bearing, which is what routes the later DELETE through OLLP/Calvin.
    for i in 0..SOURCES {
        node.client
            .simple_query(&format!(
                "INSERT INTO {coll} {{ id: 'edge_{i}', _from: 'src_{i}', _to: 'hub', _type: 'l', mark: 'keep', n: 0 }}"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert implicit edge src_{i} -> hub: {e}"));
    }

    // Wait for the implicit-edge mark to land so the PK delete plans as a
    // `BulkDelete` and routes through Calvin (not the fast `PointDelete` path).
    wait_for(
        "collection marked edge-bearing",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || collection_edge_bearing(&node, coll),
    )
    .await;
    node
}

/// Assert the sequencer admitted at least one transaction since `before`, so
/// the statement took the Calvin path rather than the fast `PointDelete` path.
fn assert_admitted_since(node: &TestClusterNode, before: u64, what: &str) {
    let after = sequencer_admitted(node);
    assert!(
        after > before,
        "{what} must be admitted to a Calvin epoch (before={before}, after={after})"
    );
}

/// Flag ON: a `DELETE ... RETURNING` on an edge-bearing collection routes
/// through the single-node Calvin OLLP path and returns the deleted row through
/// the completion path (previously dropped as a bare tag).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_returning_delete_surfaces_deleted_row() {
    // The collection name deliberately embeds "returning": it doubles as
    // regression coverage that a `*_returning` identifier is not mistaken for the
    // RETURNING keyword by the clause stripper.
    let coll = "sncalvin_returning";
    let node = calvin_edge_collection(coll).await;

    let admitted_before = sequencer_admitted(&node);

    // A PK-equality DELETE on the edge-bearing collection is rewritten to a
    // `BulkDelete` (id filter) and routed through the OLLP/Calvin dependent path.
    // With RETURNING it must come back carrying the deleted document's row.
    let msgs = node
        .client
        .simple_query(&format!(
            "DELETE FROM {coll} WHERE id = 'edge_3' RETURNING *"
        ))
        .await
        .expect("RETURNING delete routed through Calvin must complete");

    // Proof it traversed the sequencer→scheduler path (not the fast PointDelete
    // path that never touches Calvin): the delete was admitted to a Calvin epoch.
    assert_admitted_since(&node, admitted_before, "the RETURNING delete");

    let rows: Vec<&tokio_postgres::SimpleQueryRow> = msgs
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();

    // The core assertion: the response CONTAINS the deleted row, not just a bare
    // `DELETE 1` command tag with an empty row vec.
    assert_eq!(
        rows.len(),
        1,
        "DELETE ... RETURNING routed through Calvin must surface exactly the one \
         deleted row, not a bare command tag; got {} row(s)",
        rows.len()
    );
    let id = rows[0].get("id").expect("returned row has an id column");
    assert_eq!(
        id, "edge_3",
        "the returned row must be the deleted document (id = 'edge_3'); got id = {id:?}"
    );

    // Sanity: the deleted document is actually gone and the others remain.
    let remaining = node
        .client
        .simple_query(&format!("SELECT * FROM {coll}"))
        .await
        .expect("SELECT all rows");
    let remaining_count = remaining
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        remaining_count,
        SOURCES - 1,
        "exactly the RETURNING-deleted document must be removed"
    );

    node.shutdown().await;
}

/// A RETURNING expression that fails to shape on the Calvin path fails the
/// statement with its own class: `22012` over pgwire and
/// `ErrorCode::DIVISION_BY_ZERO` over the native protocol. It never degrades
/// to a bare command tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_returning_shaping_error_keeps_its_sqlstate() {
    let coll = "sncalvin_ret_err";
    let node = calvin_edge_collection(coll).await;

    let admitted_before = sequencer_admitted(&node);
    let error = node
        .client
        .simple_query(&format!(
            "DELETE FROM {coll} WHERE id = 'edge_0' RETURNING id, 10 / n AS q"
        ))
        .await
        .expect_err("a RETURNING division by zero must fail the statement");
    assert_admitted_since(&node, admitted_before, "the pgwire RETURNING delete");
    let db_err = error
        .as_db_error()
        .unwrap_or_else(|| panic!("expected a DbError, got: {error}"));
    assert_eq!(
        db_err.code().code(),
        "22012",
        "expected division_by_zero, got {}: {}",
        db_err.code().code(),
        db_err.message()
    );

    let admitted_before = sequencer_admitted(&node);
    let error = node
        .native_client()
        .query(&format!(
            "DELETE FROM {coll} WHERE id = 'edge_1' RETURNING id, 10 / n AS q"
        ))
        .await
        .expect_err("a native RETURNING division by zero must fail the statement");
    assert_admitted_since(&node, admitted_before, "the native RETURNING delete");
    assert_eq!(
        error.code(),
        nodedb_types::error::ErrorCode::DIVISION_BY_ZERO,
        "expected DIVISION_BY_ZERO, got {}: {}",
        error.code(),
        error.message()
    );

    node.shutdown().await;
}

/// A RETURNING `nextval` on the Calvin path reads the session's sequences, as
/// the direct path does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calvin_returning_nextval_evaluates() {
    let coll = "sncalvin_ret_seq";
    let node = calvin_edge_collection(coll).await;
    node.client
        .simple_query("CREATE SEQUENCE sncalvin_seq")
        .await
        .expect("CREATE SEQUENCE");

    let admitted_before = sequencer_admitted(&node);
    let msgs = node
        .client
        .simple_query(&format!(
            "DELETE FROM {coll} WHERE id = 'edge_2' RETURNING id, nextval('sncalvin_seq') AS n"
        ))
        .await
        .expect("a RETURNING nextval routed through Calvin must complete");
    assert_admitted_since(&node, admitted_before, "the RETURNING nextval delete");

    let rows: Vec<&tokio_postgres::SimpleQueryRow> = msgs
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1, "the delete returns its one row");
    assert_eq!(rows[0].get("id"), Some("edge_2"));
    assert_eq!(
        rows[0].get("n"),
        Some("1"),
        "the first nextval of a fresh sequence is 1"
    );

    node.shutdown().await;
}
