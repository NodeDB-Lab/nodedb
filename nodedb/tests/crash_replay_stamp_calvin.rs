// SPDX-License-Identifier: BUSL-1.1

//! A Calvin transaction's redo record still in flight when a checkpoint is
//! written survives a crash.
//!
//! The server runs the default single-node Calvin stack. A transaction that
//! writes two collections on different vShards commits through the Calvin
//! scheduler. Each vShard's scheduler appends the transaction's redo record,
//! then sends a flush that installs it on the core. The sequence:
//!
//! 1. Transaction A writes the held collection and a peer collection on
//!    another vShard. The held vShard's scheduler appends A's redo record and
//!    holds its flush at a fail point. No core installed the record.
//! 2. Writes B, autocommit `INSERT`s into a third collection, apply with
//!    higher LSNs until a KV checkpoint's replay stamp names one of them above
//!    its prefix. That proves the checkpoint was written while A's record was
//!    appended and not applied.
//! 3. The test releases the flush. The core installs A's record, and the
//!    process aborts before the flush response leaves.
//! 4. After restart, replay must apply A's record. The Calvin scheduler reads
//!    the same record as the transaction's applied marker and never runs the
//!    transaction again, so replay is the only way A's row returns.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::log_fields::{boot_section, count_lines, log_field};
use crash_harness::{CrashHarness, diagnostics};
use nodedb_types::id::{DatabaseId, VShardId};

/// How long the test waits for the held flush, and for a checkpoint whose
/// stamp names a B: thirty checkpoint cycles at one per second.
const CHECKPOINT_DEADLINE: Duration = Duration::from_secs(30);

/// How long the process may take to abort once the flush is released. The
/// scheduler sends a held flush again on its next pass.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the Calvin sequencer may take to elect its leader after a boot.
const CALVIN_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The scheduler's log line for a flush held at the fail point.
const FLUSH_HELD: &str = "calvin: flush held at a fail point";

const LOG_DIRECTIVES: &str = "warn,nodedb::data::executor::kv_checkpoint=info,\
                              nodedb::control::cluster::calvin::scheduler::driver::core=info";

/// Three KV collection names on three different vShards: held, peer and
/// applied.
fn collections_on_distinct_vshards() -> [String; 3] {
    let mut taken: Vec<u32> = Vec::with_capacity(3);
    ["calvin_held", "calvin_peer", "calvin_applied"].map(|prefix| {
        let (name, vshard) = (0..512u32)
            .map(|i| {
                let name = format!("{prefix}_{i}");
                let vshard =
                    VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
                (name, vshard)
            })
            .find(|(_, vshard)| !taken.contains(vshard))
            .unwrap_or_else(|| panic!("no {prefix} name on a free vShard in 512 tries"));
        taken.push(vshard);
        name
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_calvin_redo_in_flight_at_a_checkpoint_survives_kill_9() {
    let [held, peer, applied] = collections_on_distinct_vshards();
    let h = CrashHarness::new();
    let release = h.data_dir().join("release-held-flush");
    let mut h = h
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "1")
        .with_env("RUST_LOG", LOG_DIRECTIVES)
        .with_env(
            "NODEDB_FAILPOINTS",
            &format!(
                "calvin::before_flush::{held}=wait_file({}),core::after_apply::{held}=abort",
                release.display()
            ),
        );
    h.spawn();
    h.wait_ready();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    for name in [&held, &peer, &applied] {
        h.exec(&format!(
            "CREATE COLLECTION {name} (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')"
        ))
        .await;
    }

    // Transaction A. Its COMMIT waits for the held flush, so it runs on its
    // own connection.
    let conn_str = h.pgwire_conn_str();
    let statements = [
        "BEGIN".to_string(),
        format!("INSERT INTO {held} (k, v) VALUES ('held', 'a')"),
        format!("INSERT INTO {peer} (k, v) VALUES ('peer', 'p')"),
        "COMMIT".to_string(),
    ];
    let txn_task = tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        for statement in &statements {
            client
                .simple_query(statement)
                .await
                .map_err(|e| format!("{statement}: {e}"))?;
        }
        Ok::<(), String>(())
    });

    // A's redo record is appended before its flush is sent, so a held flush
    // proves the record is in the WAL and not installed.
    let deadline = Instant::now() + CHECKPOINT_DEADLINE;
    while count_lines(&boot_section(&h.server_log(), 1), &[FLUSH_HELD]) == 0 {
        assert!(
            Instant::now() < deadline && !txn_task.is_finished(),
            "the flush of transaction A was never held: A did not commit through the \
             Calvin scheduler.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Apply B writes until a checkpoint names one of them above its prefix.
    let deadline = Instant::now() + CHECKPOINT_DEADLINE;
    let mut written = 0usize;
    loop {
        h.exec(&format!(
            "INSERT INTO {applied} (k, v) VALUES ('k{written:03}', 'v{written}')"
        ))
        .await;
        written += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let log = boot_section(&h.server_log(), 1);
        if log_field(&log, "KV checkpoint published", "applied_ranges")
            .iter()
            .any(|n| *n > 0)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no KV checkpoint named an applied LSN above its prefix within \
             {CHECKPOINT_DEADLINE:?} while A's flush was held.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
    assert!(
        !txn_task.is_finished(),
        "transaction A finished before its flush was released"
    );
    let read_applied = format!("SELECT v FROM {applied}");
    let mut live = h.query_col_idx(&read_applied, 0).await;
    live.sort();

    // Release the flush. The core installs A's record, and the process aborts
    // before the flush response leaves.
    std::fs::write(&release, b"release").expect("create the release file");
    h.await_self_crash(CRASH_TIMEOUT);
    let marker = format!("fail_point aborting process: core::after_apply::{held}");
    assert!(
        h.server_log().contains(&marker),
        "the process exited, but not after A's flush installed its record.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
    // A's client lost its connection with the process; its result says nothing.
    let _ = txn_task.await;

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;

    let proof = log_field(
        &boot_section(&h.server_log(), 2),
        "KV checkpoint restored",
        "applied_ranges",
    );
    assert!(
        proof.iter().any(|n| *n > 0),
        "no KV checkpoint restored with applied_ranges above zero, so this run did not \
         reproduce the in-flight record (values: {proof:?}).{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );

    for (name, key, value) in [(&held, "held", "a"), (&peer, "peer", "p")] {
        let read = h
            .query_col_idx(&format!("SELECT v FROM {name} WHERE k = '{key}'"), 0)
            .await;
        assert_eq!(
            read,
            vec![value.to_string()],
            "transaction A's row in {name} is missing: its redo record applied after the \
             checkpoint and before the crash, and replay must apply it"
        );
    }
    let mut replayed = h.query_col_idx(&read_applied, 0).await;
    replayed.sort();
    assert_eq!(
        replayed, live,
        "the replayed state of {applied} must equal the live state: every B write once"
    );
}
