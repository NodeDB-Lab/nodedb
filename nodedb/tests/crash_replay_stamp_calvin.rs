// SPDX-License-Identifier: BUSL-1.1

//! A Calvin slice's stamped redo still in flight when a checkpoint is written
//! survives a crash.
//!
//! The server runs the default single-node Calvin stack. A transaction that
//! writes two collections in different data groups commits through the Calvin
//! scheduler. On the COMMIT verdict each slice's leader proposes the slice's
//! stamped redo to its data group, and the group's apply installs it. The
//! sequence:
//!
//! 1. Transaction A writes the held collection and a peer collection. The
//!    held group's apply appends A's stamped redo record and parks it at
//!    `funnel::before_dispatch::<held>`. No core installed the record.
//! 2. Writes B, autocommit `INSERT`s into a third collection in another data
//!    group, apply with higher LSNs until a KV checkpoint's replay stamp names
//!    one of them above its prefix. That proves the checkpoint was written
//!    while A's record was appended and not applied.
//!    A's COMMIT stays unacknowledged for the whole hold. It either still
//!    waits, or it gave up at the server's completion deadline, which is no
//!    acknowledgement.
//! 3. The test releases the gate. The core installs A's record, and the
//!    process aborts before the install's response leaves.
//! 4. After restart, replay must apply A's record. Its stamp marks A's
//!    position applied, so the scheduler never runs A again and the data
//!    group's re-delivered entry installs nothing.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::log_fields::{boot_section, log_field};
use crash_harness::vshards::names_in_distinct_data_groups;
use crash_harness::{CrashHarness, diagnostics};

/// How long the test waits for the held install, and for a checkpoint whose
/// stamp names a B: thirty checkpoint cycles at one per second.
const CHECKPOINT_DEADLINE: Duration = Duration::from_secs(30);

/// How long the process may take to abort once the gate is released.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the Calvin sequencer may take to elect its leader after a boot.
const CALVIN_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The error a COMMIT returns once its completion deadline passes with no
/// acknowledgement. The redo record stays appended and the install stays held.
const COMMIT_DEADLINE: &str = "timed out waiting for Calvin transaction completion";

const LOG_DIRECTIVES: &str = "warn,nodedb::data::executor::kv_checkpoint=info";

/// The boot that holds A's install.
const HOLD_BOOT: u32 = 2;

/// The boot after the crash.
const RESTORE_BOOT: u32 = 3;

#[tokio::test(flavor = "multi_thread")]
async fn a_calvin_redo_in_flight_at_a_checkpoint_survives_kill_9() {
    let [held, peer, applied] =
        names_in_distinct_data_groups(["calvin_held", "calvin_peer", "calvin_applied"]);
    let mut h = CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "1")
        .with_env("RUST_LOG", LOG_DIRECTIVES);
    h.spawn();
    h.wait_ready();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    for name in [&held, &peer, &applied] {
        h.exec(&format!(
            "CREATE COLLECTION {name} (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')"
        ))
        .await;
    }

    // The next boot arms the gate and the abort, keyed to the held
    // collection. Both match only a request carrying a WAL LSN, so boot
    // itself passes them.
    h.kill_9();
    let release = h.data_dir().join("release-held-install");
    let parked = h.data_dir().join("release-held-install.parked");
    h.set_env(
        "NODEDB_FAILPOINTS",
        &format!(
            "funnel::before_dispatch::{held}=wait_file({}),core::after_apply::{held}=abort",
            release.display()
        ),
    );
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;

    // Transaction A. Its COMMIT waits for the held install, so it runs on its
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
            client.simple_query(statement).await.map_err(|e| {
                let detail = e
                    .as_db_error()
                    .map(|db| format!("{}: {}", db.code().code(), db.message()))
                    .unwrap_or_else(|| e.to_string());
                format!("{statement}: {detail}")
            })?;
        }
        Ok::<(), String>(())
    });

    // The gate parks a write after its record is appended, so a parked
    // install proves A's stamped record is in the WAL and not installed.
    let deadline = Instant::now() + CHECKPOINT_DEADLINE;
    while !parked.exists() {
        assert!(
            Instant::now() < deadline && !txn_task.is_finished(),
            "the install of transaction A was never held: A did not commit through the \
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
        let log = boot_section(&h.server_log(), HOLD_BOOT);
        if log_field(&log, "KV checkpoint published", "applied_ranges")
            .iter()
            .any(|n| *n > 0)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no KV checkpoint named an applied LSN above its prefix within \
             {CHECKPOINT_DEADLINE:?} while A's install was held.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
    // A COMMIT that gave up at its deadline is unacknowledged, as the hold
    // requires. Only a COMMIT that returned success, or failed for another
    // reason, breaks the hold.
    let txn_task = if txn_task.is_finished() {
        let outcome = txn_task.await.expect("transaction A's task panicked");
        match outcome {
            Err(error) if error.contains(COMMIT_DEADLINE) => None,
            other => panic!(
                "transaction A finished before its install was released: {other:?}.{}\n{}",
                h.keep_data_dir_note(),
                diagnostics::log_tail_section(&h.server_log())
            ),
        }
    } else {
        Some(txn_task)
    };
    let read_applied = format!("SELECT v FROM {applied}");
    let mut live = h.query_col_idx(&read_applied, 0).await;
    live.sort();

    // Release the gate. The core installs A's record, and the process aborts
    // before the install's response leaves.
    std::fs::write(&release, b"release").expect("create the release file");
    h.await_self_crash(CRASH_TIMEOUT);
    let marker = format!("fail_point aborting process: core::after_apply::{held}");
    assert!(
        h.server_log().contains(&marker),
        "the process exited, but not after A's install applied its record.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
    // A's client lost its connection with the process; its result says nothing.
    if let Some(txn_task) = txn_task {
        let _ = txn_task.await;
    }

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;

    let proof = log_field(
        &boot_section(&h.server_log(), RESTORE_BOOT),
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
