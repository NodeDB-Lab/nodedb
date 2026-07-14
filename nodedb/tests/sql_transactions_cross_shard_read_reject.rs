// SPDX-License-Identifier: BUSL-1.1

//! An interactive transaction that writes one vShard and reads a DIFFERENT
//! vShard is a cross-shard transaction at COMMIT time — even though the
//! buffered write batch itself is single-shard — and on a non-cluster
//! (embedded/local) node it is rejected because cross-shard commit needs the
//! Calvin sequencer, which embedded mode does not stand up.
//!
//! The interactive-COMMIT orchestrator (`run_commit`, in
//! `control/server/shared/session/commit.rs`) widens `classify_dispatch`'s
//! participant set with the session's read-set vShards
//! (`read_vshards_of(&read_set)`) before classifying the buffered write
//! batch. A transaction that only ever wrote collection A (one vShard) but
//! also read collection B (a DIFFERENT vShard) therefore classifies as
//! `DispatchClass::MultiShard { vshards: {A, B} }` at COMMIT time. On a real
//! cluster this whole batch commits atomically through the Calvin barrier;
//! but this test runs against a standalone (non-cluster) `TestServer` with no
//! sequencer wired, so the strict cross-shard COMMIT path surfaces
//! `Error::SequencerUnavailable` ("cross-shard transactions require a cluster
//! deployment with the Calvin sequencer"). Previously the buffered batch
//! alone (containing only the write to A) classified as `SingleShard` and
//! committed on the fast path without ever considering the read of B.

mod common;

use common::pgwire_harness::TestServer;
use nodedb::types::VShardId;

/// Find two collection names whose vShards differ. Deterministic within a
/// process. Mirrors `calvin_sql_routing::find_two_distinct_collections`.
fn find_two_distinct_collections() -> (String, String) {
    let mut first: Option<(String, u32)> = None;
    for i in 0u32..512 {
        let name = format!("xrd_col_{i}");
        let vshard =
            VShardId::from_collection_in_database(nodedb::types::DatabaseId::DEFAULT, &name)
                .as_u32();
        if let Some((ref fname, fv)) = first {
            if fv != vshard {
                return (fname.clone(), name);
            }
        } else {
            first = Some((name, vshard));
        }
    }
    panic!("could not find two distinct-vshard collections in 512 tries");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_one_shard_read_another_rejected_at_commit_in_explicit_txn() {
    let server = TestServer::start().await;

    let (col_a, col_b) = find_two_distinct_collections();

    server
        .exec(&format!(
            "CREATE COLLECTION {col_a} (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION col_a");
    server
        .exec(&format!(
            "CREATE COLLECTION {col_b} (id TEXT PRIMARY KEY, val INT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION col_b");

    // Seed col_b with a row before the transaction so the in-transaction
    // SELECT reads real committed data, not just an absent-key phantom.
    server
        .exec(&format!("INSERT INTO {col_b} (id, val) VALUES ('b1', 1)"))
        .await
        .expect("seed col_b");

    server.exec("BEGIN").await.unwrap();

    // WRITE to col_a: a single-shard write by itself.
    server
        .exec(&format!("INSERT INTO {col_a} (id, val) VALUES ('a1', 1)"))
        .await
        .expect("write to col_a should succeed at statement time (single-shard write)");

    // READ from col_b: a DIFFERENT vShard than the one just written.
    let read_rows = server
        .query_text(&format!("SELECT id FROM {col_b} WHERE id = 'b1'"))
        .await
        .expect("read from col_b should succeed at statement time");
    assert_eq!(read_rows, vec!["b1".to_string()]);

    // COMMIT: the write-shard (col_a) union read-shard (col_b) read set now
    // spans 2 vShards, so classify_dispatch reports MultiShard. On this
    // standalone (non-cluster) server there is no Calvin sequencer, so the
    // strict cross-shard COMMIT path is rejected with SequencerUnavailable.
    let err = server
        .exec("COMMIT")
        .await
        .expect_err("COMMIT must be rejected: write to col_a + read of col_b span 2 vShards");
    assert!(
        err.contains(
            "cross-shard transactions require a cluster deployment with the Calvin sequencer"
        ),
        "expected SequencerUnavailable (embedded/local) error text, got: {err}"
    );

    // The rejected transaction must not have persisted the buffered write.
    server.exec("ROLLBACK").await.ok();
    let rows = server
        .query_text(&format!("SELECT id FROM {col_a} WHERE id = 'a1'"))
        .await
        .expect("post-rollback read of col_a should succeed");
    assert!(
        rows.is_empty(),
        "write to col_a must not have persisted after the rejected COMMIT, got: {rows:?}"
    );
}
