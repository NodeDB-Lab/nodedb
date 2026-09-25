// SPDX-License-Identifier: BUSL-1.1

//! A write still in flight when a checkpoint is written survives a crash.
//!
//! LSNs are node-global, and a write reaches its core out of mint order. The
//! server runs standalone: with no Raft proposer, every autocommit write takes
//! the local funnel route, so two writes to different keys apply in any order.
//! Each engine case runs the same sequence:
//!
//! 1. Write A, an `INSERT` into the held collection, mints its LSN and parks at
//!    the funnel gate. No core holds it.
//! 2. Writes B, `INSERT`s into a second collection, apply with higher LSNs
//!    until a checkpoint's replay stamp names one of them above its prefix.
//!    That proves the checkpoint was written while A was minted and not
//!    applied.
//! 3. The test releases A. A applies, and the process aborts before A's
//!    response leaves, so no later checkpoint holds A.
//! 4. After restart, replay must apply A: the stamp does not name it. A stamp
//!    that holds only the highest applied LSN skips A, and A's row is lost.
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

/// One engine's run of the in-flight sequence.
struct Case {
    /// The collection write A goes to.
    held: &'static str,
    /// The collection the B writes go to.
    applied: &'static str,
    create_held: &'static str,
    create_applied: &'static str,
    /// Write A.
    insert_held: &'static str,
    /// Reads A's row back as one column.
    read_held: &'static str,
    /// The value `read_held` returns once A applied.
    held_value: &'static str,
    /// Write B number `n`.
    insert_applied: fn(usize) -> String,
    /// Reads every B row as one column.
    read_applied: &'static str,
    /// The engine's checkpoint module, as a `RUST_LOG` target.
    log_target: &'static str,
    /// The log message of a published checkpoint.
    published: &'static str,
    /// The log message of a checkpoint restored at boot.
    restored: &'static str,
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kv_write_in_flight_at_a_checkpoint_survives_kill_9() {
    run(Case {
        held: "stamp_kv_lo",
        applied: "stamp_kv_hi",
        create_held: "CREATE COLLECTION stamp_kv_lo (k STRING PRIMARY KEY, v STRING) \
                      WITH (engine='kv')",
        create_applied: "CREATE COLLECTION stamp_kv_hi (k STRING PRIMARY KEY, v STRING) \
                         WITH (engine='kv')",
        insert_held: "INSERT INTO stamp_kv_lo (k, v) VALUES ('held', 'a')",
        read_held: "SELECT v FROM stamp_kv_lo WHERE k = 'held'",
        held_value: "a",
        insert_applied: |n| format!("INSERT INTO stamp_kv_hi (k, v) VALUES ('k{n:03}', 'v{n}')"),
        read_applied: "SELECT v FROM stamp_kv_hi",
        log_target: "nodedb::data::executor::kv_checkpoint",
        published: "KV checkpoint published",
        restored: "KV checkpoint restored",
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_columnar_write_in_flight_at_a_checkpoint_survives_kill_9() {
    run(Case {
        held: "stamp_col_lo",
        applied: "stamp_col_hi",
        create_held: "CREATE COLLECTION stamp_col_lo COLUMNS (id TEXT, v TEXT) \
                      WITH (engine='columnar')",
        create_applied: "CREATE COLLECTION stamp_col_hi COLUMNS (id TEXT, v TEXT) \
                         WITH (engine='columnar')",
        insert_held: "INSERT INTO stamp_col_lo (id, v) VALUES ('held', 'a')",
        read_held: "SELECT v FROM stamp_col_lo WHERE id = 'held'",
        held_value: "a",
        insert_applied: |n| format!("INSERT INTO stamp_col_hi (id, v) VALUES ('r{n:03}', 'v{n}')"),
        read_applied: "SELECT v FROM stamp_col_hi",
        log_target: "nodedb::data::executor::columnar_checkpoint",
        published: "columnar checkpoint published",
        restored: "columnar checkpoint restored",
    })
    .await;
}

async fn run(case: Case) {
    let mut h = CrashHarness::new()
        .standalone()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", CHECKPOINT_INTERVAL_SECS)
        .with_env("RUST_LOG", &format!("warn,{}=info", case.log_target));
    h.spawn();
    h.wait_ready();
    h.exec(case.create_held).await;
    h.exec(case.create_applied).await;

    // Boot 2 arms the gate and the abort, keyed to the held collection. Both
    // match only a request carrying a WAL LSN, so boot itself passes them.
    h.kill_9();
    let release = h.data_dir().join("release-held-write");
    h.set_env(
        "NODEDB_FAILPOINTS",
        &format!(
            "funnel::before_dispatch::{held}=wait_file({}),core::after_apply::{held}=abort",
            release.display(),
            held = case.held,
        ),
    );
    h.reopen();

    let conn_str = h.pgwire_conn_str();
    let insert_held = case.insert_held;
    let held_task = tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        client
            .simple_query(insert_held)
            .await
            .map_err(|e| format!("{insert_held}: {e}"))?;
        Ok::<(), String>(())
    });

    // Apply B writes until a checkpoint names one of them above its prefix.
    let deadline = Instant::now() + STAMP_DEADLINE;
    let mut applied = 0usize;
    loop {
        h.exec(&(case.insert_applied)(applied)).await;
        applied += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let log = boot_section(&h.server_log(), 2);
        if applied_ranges(&log, case.published).iter().any(|n| *n > 0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no {} named an applied LSN above its prefix within {STAMP_DEADLINE:?}: write A \
             never parked, or no checkpoint ran while it was.{}\n{}",
            case.published,
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
    assert!(
        !held_task.is_finished(),
        "write A finished before its release: the gate never parked it"
    );
    let mut live = h.query_col_idx(case.read_applied, 0).await;
    live.sort();

    // Release A. It applies, and the process aborts before its response
    // leaves, so no checkpoint written after it can hold it.
    std::fs::write(&release, b"release").expect("create the release file");
    h.await_self_crash(CRASH_TIMEOUT);
    let marker = format!(
        "fail_point aborting process: core::after_apply::{}",
        case.held
    );
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

    let ranges = applied_ranges(&boot_section(&h.server_log(), 3), case.restored);
    assert!(
        ranges.iter().any(|n| *n > 0),
        "the restored generation must name an applied LSN above its prefix, or this run did \
         not reproduce the in-flight write (restored stamps: {ranges:?}).{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );

    let held = h.query_col_idx(case.read_held, 0).await;
    assert_eq!(
        held,
        vec![case.held_value.to_string()],
        "write A to {} applied after the checkpoint and before the crash; replay must \
         apply it, never skip it as covered by a higher applied LSN",
        case.held
    );
    let mut replayed = h.query_col_idx(case.read_applied, 0).await;
    replayed.sort();
    assert_eq!(
        replayed, live,
        "the replayed state of {} must equal the live state: every B write once",
        case.applied
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
}
