// SPDX-License-Identifier: BUSL-1.1

//! A KV write still in flight when a checkpoint is written survives a crash.
//!
//! LSNs are node-global and a write reaches its core out of mint order. Client
//! row writes apply through the one Raft apply loop, which finishes each entry
//! before it starts the next, so two of them never overtake each other. A
//! write outside that loop can: the engine step of an index DDL committed in
//! a transaction runs on its own session. The test uses that step as write A:
//!
//! 1. `COMMIT` of a block holding `CREATE SORTED INDEX` lands the catalog
//!    record, then appends A, the sorted-index registration, and parks it at
//!    the funnel gate. Its LSN is minted and no core holds it.
//! 2. Client writes B apply through the Raft loop with higher LSNs, until a
//!    KV checkpoint's replay stamp names one of them above its prefix. That
//!    proves the checkpoint was written while A was minted and not applied.
//! 3. The test releases A. A applies, and the process aborts before A's
//!    response leaves, so no later checkpoint holds A.
//! 4. After restart, replay must apply A: the stamp does not name it. A stamp
//!    holding only the highest applied LSN would skip A, and the index would
//!    have a catalog record and no tree.
//!
//! The columnar engine has no write outside the Raft loop, so its in-flight
//! case runs in-process in `wal_replay_all.rs`.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::{CrashHarness, diagnostics};

/// One checkpoint per second, so several are written while A is parked.
const CHECKPOINT_INTERVAL_SECS: &str = "1";

/// How long the test waits for a checkpoint whose stamp names a B: thirty
/// checkpoint cycles at one per second. A is parked until the test releases
/// it, so this bounds only the wait for the checkpoint manager.
const STAMP_DEADLINE: Duration = Duration::from_secs(30);

/// How long the process may take to abort once A is released.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

const HELD: &str = "stamp_kv_lo";
const INDEX: &str = "stamp_kv_idx";
const SEEDED_ROWS: u64 = 3;

#[tokio::test(flavor = "multi_thread")]
async fn a_kv_write_in_flight_at_a_checkpoint_survives_kill_9() {
    let mut h = CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", CHECKPOINT_INTERVAL_SECS)
        .with_env(
            "RUST_LOG",
            "warn,nodedb::data::executor::kv_checkpoint=info",
        );
    h.spawn();
    h.wait_ready();
    h.exec(&format!(
        "CREATE COLLECTION {HELD} (k STRING PRIMARY KEY, score INT) WITH (engine='kv')"
    ))
    .await;
    for i in 0..SEEDED_ROWS {
        h.exec(&format!(
            "INSERT INTO {HELD} (k, score) VALUES ('p{i}', {})",
            i * 10
        ))
        .await;
    }
    h.exec("CREATE COLLECTION stamp_kv_hi (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await;

    // Boot 2 arms the gate and the abort, keyed to the held collection. Both
    // match only a request carrying a WAL LSN, so boot itself passes them.
    h.kill_9();
    let release = h.data_dir().join("release-held-write");
    h.set_env(
        "NODEDB_FAILPOINTS",
        &format!(
            "funnel::before_dispatch::{HELD}=wait_file({}),core::after_apply::{HELD}=abort",
            release.display()
        ),
    );
    h.reopen();

    let conn_str = h.pgwire_conn_str();
    let held_task = tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        for sql in [
            "BEGIN".to_string(),
            format!("CREATE SORTED INDEX {INDEX} ON {HELD} (score DESC) KEY k"),
            "COMMIT".to_string(),
        ] {
            client
                .simple_query(&sql)
                .await
                .map_err(|e| format!("{sql}: {e}"))?;
        }
        Ok::<(), String>(())
    });

    // Apply B writes until a checkpoint names one of them above its prefix.
    let deadline = Instant::now() + STAMP_DEADLINE;
    let mut applied = 0usize;
    loop {
        h.exec(&format!(
            "INSERT INTO stamp_kv_hi (k, v) VALUES ('k{applied:03}', 'v{applied}')"
        ))
        .await;
        applied += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let log = boot_section(&h.server_log(), 2);
        if applied_ranges(&log, "KV checkpoint published")
            .iter()
            .any(|n| *n > 0)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no KV checkpoint named an applied LSN above its prefix within \
             {STAMP_DEADLINE:?}: write A never parked, or no checkpoint ran while it was.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
    assert!(
        !held_task.is_finished(),
        "write A finished before its release: the gate never parked it"
    );
    let mut live = h.query_col_idx("SELECT v FROM stamp_kv_hi", 0).await;
    live.sort();

    // Release A. It applies, and the process aborts before its response
    // leaves, so no checkpoint written after it can hold it.
    std::fs::write(&release, b"release").expect("create the release file");
    h.await_self_crash(CRASH_TIMEOUT);
    let marker = format!("fail_point aborting process: core::after_apply::{HELD}");
    assert!(
        h.server_log().contains(&marker),
        "the process exited, but not after write A applied.{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );
    // A's client lost its connection with the process; its result says nothing.
    let _ = held_task.await;

    h.clear_env("NODEDB_FAILPOINTS");
    h.reopen();

    let ranges = applied_ranges(&boot_section(&h.server_log(), 3), "KV checkpoint restored");
    assert!(
        ranges.iter().any(|n| *n > 0),
        "the restored generation must name an applied LSN above its prefix, or this run did \
         not reproduce the in-flight write (restored stamps: {ranges:?}).{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );

    let count = h
        .query_col_idx(&format!("SELECT SORTED_COUNT({INDEX})"), 0)
        .await;
    assert_eq!(
        count.first().and_then(|text| json_field(text, "count")),
        Some(SEEDED_ROWS),
        "write A applied after the checkpoint and before the crash; replay must rebuild \
         the index tree from it, never skip it as covered by a higher applied LSN \
         (got {count:?})"
    );
    let mut replayed = h.query_col_idx("SELECT v FROM stamp_kv_hi", 0).await;
    replayed.sort();
    assert_eq!(
        replayed, live,
        "the replayed state must equal the live state: every B write once"
    );
}

/// The server output of boot `n`, from its harness marker to the next one.
fn boot_section(log: &str, n: u32) -> String {
    let marker = format!("=== crash harness boot {n} (pid");
    let Some(start) = log.find(&marker) else {
        return String::new();
    };
    let rest = &log[start..];
    let next = format!("=== crash harness boot {} (pid", n + 1);
    match rest.find(&next) {
        Some(end) => rest[..end].to_string(),
        None => rest.to_string(),
    }
}

/// The `applied_ranges` field of every log line carrying `message`.
fn applied_ranges(log: &str, message: &str) -> Vec<u64> {
    strip_ansi(log)
        .lines()
        .filter(|line| line.contains(message))
        .filter_map(|line| {
            let rest = line.split_once("applied_ranges=")?.1;
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse().ok()
        })
        .collect()
}

/// The unsigned integer a single-cell JSON reply carries under `field`.
fn json_field(text: &str, field: &str) -> Option<u64> {
    let rest = text.split_once(&format!("\"{field}\":"))?.1;
    let digits: String = rest
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// `text` without terminal colour escape sequences.
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for next in chars.by_ref() {
                if next == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[test]
fn log_fields_are_read_through_colour_codes() {
    let log = "INFO KV checkpoint published \u{1b}[3mapplied_ranges\u{1b}[0m\u{1b}[2m=\u{1b}[0m2\n\
               INFO KV checkpoint published applied_ranges=0\n";
    assert_eq!(applied_ranges(log, "KV checkpoint published"), vec![2, 0]);
    let booted = "=== crash harness boot 1 (pid 1) ===\nfirst-line\n\
                  === crash harness boot 2 (pid 2) ===\nsecond-line\n";
    assert!(boot_section(booted, 2).contains("second-line"));
    assert!(!boot_section(booted, 2).contains("first-line"));
    assert!(boot_section(booted, 1).contains("first-line"));
    assert!(!boot_section(booted, 1).contains("second-line"));
    assert_eq!(json_field("{\"count\":3}", "count"), Some(3));
}
