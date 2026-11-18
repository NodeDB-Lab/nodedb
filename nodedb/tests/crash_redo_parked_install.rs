// SPDX-License-Identifier: BUSL-1.1

//! A batched install parked between its batches survives a process abort.
//!
//! The commit is the one `crash_harness::redo_batched` describes: its
//! install spans several batches and parks between them. Each test aborts
//! the process at a park and restarts it on the same data directory:
//!
//! - at the park after the third batch;
//! - at a later park, after a write of another database ran and answered
//!   while the install was held parked.
//!
//! Boot completes the record forward. Every source row reads back once,
//! the sum holds one fold of every row, and the other write stays.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::Duration;

use crash_harness::diagnostics;
use crash_harness::log_fields::log_field;
use crash_harness::redo_batched::{
    CRASH_TIMEOUT, abort_installs_forward_once, assert_installed_once, await_abort_at,
    commit_large_transaction, create, harness_with, names,
};
use crash_harness::vshards::vshard_of;

#[tokio::test(flavor = "multi_thread")]
async fn abort_while_parked_folds_once() {
    abort_installs_forward_once("bi_parked", "redo_apply::batched::parked::Document::2", &[]).await;
}

/// The install is held parked after its second batch while a write of
/// another database, on another vShard, runs and answers. The install then
/// resumes, and the process aborts at a later park. A restart holds the
/// write and one fold of every row.
#[tokio::test(flavor = "multi_thread")]
async fn abort_while_parked_with_a_concurrent_write_keeps_both() {
    const HOLD_AT: &str = "redo_apply::batched::hold_parked::Document::1";
    const ABORT_AT: &str = "redo_apply::batched::parked::Document::3";
    const OTHER_DB: &str = "bi_parked_db";
    let names = names("bi_parked_write");
    let notes = name_off_vshard("bi_parked_notes", vshard_of(&names.source));
    // One core, so the write runs on the core the install is parked on.
    let mut h = harness_with("[server]\ndata_plane_cores = 1\n");
    h.spawn();
    h.wait_ready();
    create(&h, &names).await;
    h.exec(&format!("CREATE DATABASE {OTHER_DB}")).await;
    let ddl = format!(
        "CREATE COLLECTION {notes} (id TEXT PRIMARY KEY, body TEXT) WITH (engine='document_strict')"
    );
    h.exec_in(OTHER_DB, &ddl).await;
    h.kill_9();
    let gate = h.data_dir().join("parked_gate");
    let spec = format!("{HOLD_AT}=wait_file({}),{ABORT_AT}=abort", gate.display());
    h.set_env("NODEDB_FAILPOINTS", &spec);
    h.reopen();

    // The install holds at its park after batch 1 until the gate opens.
    let write = async {
        let parked = async {
            while !log_field(&h.server_log(), "batched install parked", "batch").contains(&1) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        let held = tokio::time::timeout(CRASH_TIMEOUT, parked).await;
        let conn = h.pgwire_conn_str_for(OTHER_DB);
        let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
            .await
            .expect("connect to the other database");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let insert = format!("INSERT INTO {notes} (id, body) VALUES ('n1', 'kept')");
        let inserted = tokio::time::timeout(CRASH_TIMEOUT, client.simple_query(&insert)).await;
        std::fs::write(&gate, b"").expect("open the gate");
        (held.is_ok(), inserted.map(|result| result.map(|_| ())))
    };
    let (committed, (held, inserted)) =
        tokio::join!(commit_large_transaction(&h, &names.source), write);
    assert!(held, "the install never parked after batch 1");
    assert!(
        matches!(inserted, Ok(Ok(()))),
        "the write did not answer while the install was parked: {inserted:?}"
    );
    assert!(
        !committed,
        "COMMIT answered success, but the server was armed to abort"
    );
    await_abort_at(&mut h, ABORT_AT);

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();
    assert_installed_once(&h, &names, "after the restart").await;
    let read = format!("SELECT body FROM {notes} WHERE id = 'n1'");
    let kept = h.query_col_idx_in(OTHER_DB, &read, 0);
    assert_eq!(
        kept.await,
        vec!["kept".to_string()],
        "the write that ran while the install was parked is lost.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
}

/// A collection name whose vShard is not `vshard` in any database up to
/// id 64, so its writes never wait on the install's event vShard.
fn name_off_vshard(prefix: &str, vshard: u32) -> String {
    (0u32..)
        .map(|i| format!("{prefix}_{i}"))
        .find(|name| {
            (1..=64).all(|db| {
                nodedb_types::CollectionKey::from_bare(nodedb_types::id::DatabaseId::new(db), name)
                    .vshard()
                    .as_u32()
                    != vshard
            })
        })
        .unwrap_or_else(|| panic!("no {prefix} name off vShard {vshard}"))
}
