// SPDX-License-Identifier: BUSL-1.1

//! Statement-level DML tag fold on the Calvin multi-shard path, from a
//! coordinator that is NOT the sequencer leader (routed submit).
//!
//! A statement whose tasks span vShards commits as ONE Calvin batch and
//! answers ONE command tag with the REAL affected count: never one tag per
//! task, never the task count, never a bare `OK`. `tokio_postgres` hides the
//! tag verb, so pgwire assertions read the wire through `RawPgConn`; the
//! native protocol reports the same fold as `(rows_affected, command)`.
//!
//! `MERGE INTO a USING b` across two vShards is NOT a Calvin batch:
//! `DocumentOp::Merge` is not a Calvin write (`calvin/write_class.rs`), so
//! `classify_dispatch` sees zero write vShards and the autocommit statement
//! reaches `merge_orchestrator::run_authorized_merge`, which scans the source
//! on its own core, resolves the arms, and proposes the resolved apply through
//! Raft to the target's owner and every replica. The MERGE test pins the tag
//! that path answers and that the rows land on every node.

use super::calvin_multishard_fixture::{
    Fixture, edge_batch_sql, edge_doc_sql, keyed_ddl, native_outcome, schemaless_ddl, tags,
};
use super::vshard_names::distinct_vshard_collections;

/// An autocommit implicit-edge INSERT fans out to a document task and an
/// `EdgePut` task on another vShard. The statement answers ONE `INSERT 0 1`;
/// a three-row `VALUES` list answers ONE `INSERT 0 3`.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_shard_single_statement_reports_one_folded_insert_tag_pgwire() {
    let coll = "tagfold_edges_pg";
    let fx = Fixture::spawn(&[schemaless_ddl(coll)]).await;
    let mut conn = fx.raw().await;

    assert_eq!(
        tags(&mut conn, &edge_doc_sql(coll, "e1", "keep")).await,
        vec!["INSERT 0 1"],
        "document + cross-vShard edge task fold into one INSERT tag"
    );
    assert_eq!(
        tags(&mut conn, &edge_batch_sql(coll, ["e2", "e3", "e4"])).await,
        vec!["INSERT 0 3"],
        "three-row VALUES with cross-vShard edge tasks folds into one INSERT tag"
    );

    fx.converge().await;
    assert_eq!(
        fx.count_on_coordinator(&format!("SELECT id FROM {coll}"))
            .await,
        4
    );

    fx.cluster.shutdown().await;
}

/// Native-protocol mirror of the pgwire fold: `(1, INSERT)` then `(3, INSERT)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_shard_single_statement_reports_one_folded_insert_tag_native() {
    let coll = "tagfold_edges_nat";
    let fx = Fixture::spawn(&[schemaless_ddl(coll)]).await;
    let node = fx.coordinator();

    assert_eq!(
        native_outcome(node, &edge_doc_sql(coll, "e1", "keep")).await,
        (1, Some("INSERT".to_owned())),
        "native single-row implicit-edge insert reports the document count"
    );
    assert_eq!(
        native_outcome(node, &edge_batch_sql(coll, ["e2", "e3", "e4"])).await,
        (3, Some("INSERT".to_owned())),
        "native three-row implicit-edge insert reports the row count"
    );

    fx.converge().await;
    assert_eq!(
        fx.count_on_coordinator(&format!("SELECT id FROM {coll}"))
            .await,
        4
    );

    fx.cluster.shutdown().await;
}

/// A predicate DELETE over implicit-edge documents runs through OLLP/Calvin
/// with one edge-delete task per matched row on other vShards. The tag counts
/// the two deleted documents, not the tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_shard_delete_reports_real_count_never_task_count() {
    let pg_coll = "tagfold_del_pg";
    let nat_coll = "tagfold_del_nat";
    let fx = Fixture::spawn(&[schemaless_ddl(pg_coll), schemaless_ddl(nat_coll)]).await;
    let mut conn = fx.raw().await;
    let node = fx.coordinator();

    assert_eq!(
        tags(&mut conn, &edge_batch_sql(pg_coll, ["d1", "d2", "d3"])).await,
        vec!["INSERT 0 3"]
    );
    assert_eq!(
        native_outcome(node, &edge_batch_sql(nat_coll, ["d1", "d2", "d3"])).await,
        (3, Some("INSERT".to_owned()))
    );
    fx.converge().await;

    assert_eq!(
        tags(
            &mut conn,
            &format!("DELETE FROM {pg_coll} WHERE mark = 'del'")
        )
        .await,
        vec!["DELETE 2"],
        "pgwire predicate delete counts deleted documents, not edge tasks"
    );
    assert_eq!(
        native_outcome(node, &format!("DELETE FROM {nat_coll} WHERE mark = 'del'")).await,
        (2, Some("DELETE".to_owned())),
        "native predicate delete counts deleted documents, not edge tasks"
    );

    fx.converge().await;
    assert_eq!(
        fx.count_on_coordinator(&format!("SELECT id FROM {pg_coll}"))
            .await,
        1
    );
    assert_eq!(
        fx.count_on_coordinator(&format!("SELECT id FROM {nat_coll}"))
            .await,
        1
    );

    fx.cluster.shutdown().await;
}

/// `MERGE INTO target USING source` with the two collections on distinct
/// vShards answers `MERGE <n>` with `n` = matched updates + unmatched inserts,
/// on pgwire and native alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_shard_merge_reports_merge_tag() {
    let (target, source) = distinct_vshard_collections("tagfold_merge_tgt", "tagfold_merge_src");
    let fx = Fixture::spawn(&[keyed_ddl(&target), keyed_ddl(&source)]).await;
    let mut conn = fx.raw().await;
    let node = fx.coordinator();

    assert_eq!(
        tags(
            &mut conn,
            &format!("INSERT INTO {target} (id, v) VALUES ('k1', 'old')")
        )
        .await,
        vec!["INSERT 0 1"]
    );
    assert_eq!(
        tags(
            &mut conn,
            &format!("INSERT INTO {source} (id, v) VALUES ('k1', 'new'), ('k2', 'two')")
        )
        .await,
        vec!["INSERT 0 2"]
    );
    fx.converge().await;

    let merge = format!(
        "MERGE INTO {target} t USING {source} s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET v = s.v \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    );
    assert_eq!(
        tags(&mut conn, &merge).await,
        vec!["MERGE 2"],
        "pgwire MERGE across vShards: one update + one insert"
    );
    fx.converge().await;
    assert_eq!(
        fx.count_on_coordinator(&format!("SELECT id FROM {target} WHERE v = 'new'"))
            .await,
        1
    );
    assert_eq!(
        fx.count_on_coordinator(&format!("SELECT id FROM {target} WHERE id = 'k2'"))
            .await,
        1
    );
    fx.wait_rows_on_every_node(&format!("SELECT id FROM {target} WHERE v = 'new'"), 1)
        .await;
    fx.wait_rows_on_every_node(&format!("SELECT id FROM {target} WHERE id = 'k2'"), 1)
        .await;

    assert_eq!(
        native_outcome(
            node,
            &format!("INSERT INTO {source} (id, v) VALUES ('k3', 'three')")
        )
        .await,
        (1, Some("INSERT".to_owned()))
    );
    fx.converge().await;
    assert_eq!(
        native_outcome(node, &merge).await,
        (3, Some("MERGE".to_owned())),
        "native MERGE across vShards: two updates + one insert"
    );
    fx.converge().await;
    assert_eq!(
        fx.count_on_coordinator(&format!("SELECT id FROM {target}"))
            .await,
        3
    );
    fx.wait_rows_on_every_node(&format!("SELECT id FROM {target}"), 3)
        .await;
    fx.wait_rows_on_every_node(&format!("SELECT id FROM {target} WHERE id = 'k3'"), 1)
        .await;

    fx.cluster.shutdown().await;
}
