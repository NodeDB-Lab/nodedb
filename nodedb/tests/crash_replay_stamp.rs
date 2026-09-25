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
//! An array or timeseries checkpoint writes only a collection that holds
//! unwritten state. Those cases seed the held collection in boot 1 and start
//! boot 2's checkpoints after A is minted, so the checkpoint that names B also
//! writes the held collection.
//!
//! The WAL-truncation case seals segments below A in boot 1, and the segment
//! that holds A's record while A is parked. Truncation must remove segments
//! below A and keep A's segment, and must remove it once A settled after the
//! restart.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::log_fields::{boot_section, log_field, same_value};
use crash_harness::wal_truncation::{WAL_TRUNCATED, segment_first_lsn, truncation_finished_from};
use crash_harness::{CrashHarness, diagnostics};

/// Boot 1 writes no checkpoint, so a seeded write stays in memory until the
/// kill.
const QUIET_CHECKPOINT_INTERVAL_SECS: &str = "3600";

/// How long the test waits for a checkpoint whose stamp names a B, for
/// truncation runs, or for a segment to go: thirty checkpoint cycles at one
/// per second. A is parked until the test releases it, so this bounds only the
/// wait for the checkpoint manager.
const CHECKPOINT_DEADLINE: Duration = Duration::from_secs(30);

/// How long the process may take to abort once A is released.
const CRASH_TIMEOUT: Duration = Duration::from_secs(60);

/// The smallest WAL segment target the config accepts, in whole MiB.
const WAL_SEGMENT_TARGET_MB: &str = "1";

/// One filler value. Five of them hold 2.5 MiB, which seals the segment that
/// holds A's record under a 1 MiB target.
const FILLER_VALUE_BYTES: usize = 512 * 1024;
const FILLER_ROWS: usize = 5;

/// The collection the filler goes to. The test never reads it back.
const FILLER: &str = "stamp_trunc_fill";

/// One engine's run of the in-flight sequence.
struct Case {
    /// The collection write A goes to.
    held: &'static str,
    /// The collection the B writes go to.
    applied: &'static str,
    create_held: &'static str,
    create_applied: &'static str,
    /// A write to the held collection in boot 1, still in memory at the kill.
    /// Boot 2 replays it, so the held collection has state for boot 2's
    /// checkpoint to write while A is parked.
    seed_held: Option<&'static str>,
    /// Write A.
    insert_held: &'static str,
    /// Reads A's row back as one column.
    read_held: &'static str,
    /// The value `read_held` returns once A applied. Two numbers compare by
    /// value, so `8` equals `8.0`.
    held_value: &'static str,
    /// Write B number `n`.
    insert_applied: fn(usize) -> String,
    /// Reads every B row as one column.
    read_applied: &'static str,
    /// Boot 2's checkpoint interval. It must pass after A is minted when the
    /// engine writes a collection only while it holds unwritten state.
    checkpoint_interval_secs: &'static str,
    /// `RUST_LOG` directives that enable the `published` and `restored` lines.
    log_directives: &'static str,
    /// The log message of a published checkpoint. Its `applied_ranges` field
    /// counts the ranges its stamp names above the prefix.
    published: &'static str,
    /// The boot-3 log message that proves this run reproduced the in-flight
    /// write, and the numeric field that must be above zero on it.
    restored: (&'static str, &'static str),
    /// Seal A's segment and wait for truncation runs while A is parked.
    wal_truncation: bool,
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
        seed_held: None,
        insert_held: "INSERT INTO stamp_kv_lo (k, v) VALUES ('held', 'a')",
        read_held: "SELECT v FROM stamp_kv_lo WHERE k = 'held'",
        held_value: "a",
        insert_applied: |n| format!("INSERT INTO stamp_kv_hi (k, v) VALUES ('k{n:03}', 'v{n}')"),
        read_applied: "SELECT v FROM stamp_kv_hi",
        checkpoint_interval_secs: "1",
        log_directives: "nodedb::data::executor::kv_checkpoint=info",
        published: "KV checkpoint published",
        restored: ("KV checkpoint restored", "applied_ranges"),
        wal_truncation: false,
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
        seed_held: None,
        insert_held: "INSERT INTO stamp_col_lo (id, v) VALUES ('held', 'a')",
        read_held: "SELECT v FROM stamp_col_lo WHERE id = 'held'",
        held_value: "a",
        insert_applied: |n| format!("INSERT INTO stamp_col_hi (id, v) VALUES ('r{n:03}', 'v{n}')"),
        read_applied: "SELECT v FROM stamp_col_hi",
        checkpoint_interval_secs: "1",
        log_directives: "nodedb::data::executor::columnar_checkpoint=info",
        published: "columnar checkpoint published",
        restored: ("columnar checkpoint restored", "applied_ranges"),
        wal_truncation: false,
    })
    .await;
}

/// An array flush writes only an array whose memtable holds cells, and it
/// stamps that array's manifest. So the held array carries a boot-1 cell into
/// boot 2's memtable, and boot 2's first checkpoint runs after A is minted.
/// That checkpoint flushes the held array with a stamp naming B writes above
/// A. Boot 3 replays A below that stamp's highest LSN (`in_flight`).
#[tokio::test(flavor = "multi_thread")]
async fn an_array_write_in_flight_at_a_checkpoint_survives_kill_9() {
    run(Case {
        held: "stamp_arr_lo",
        applied: "stamp_arr_hi",
        create_held: "CREATE ARRAY stamp_arr_lo DIMS (k INT64 [0..15]) ATTRS (v FLOAT64) \
                      TILE_EXTENTS (16) CELL_ORDER ROW_MAJOR",
        create_applied: "CREATE ARRAY stamp_arr_hi DIMS (k INT64 [0..1023]) ATTRS (v FLOAT64) \
                         TILE_EXTENTS (64) CELL_ORDER ROW_MAJOR",
        seed_held: Some("INSERT INTO ARRAY stamp_arr_lo COORDS (0) VALUES (1.0)"),
        insert_held: "INSERT INTO ARRAY stamp_arr_lo COORDS (1) VALUES (7.0)",
        read_held: "SELECT * FROM ARRAY_AGG('stamp_arr_lo', 'v', 'sum')",
        held_value: "8",
        insert_applied: |n| format!("INSERT INTO ARRAY stamp_arr_hi COORDS ({n}) VALUES ({n}.0)"),
        read_applied: "SELECT * FROM ARRAY_AGG('stamp_arr_hi', 'v', 'sum')",
        checkpoint_interval_secs: "10",
        log_directives: "nodedb::data::executor::array_checkpoint=info,\
                         nodedb::data::executor::wal_replay::array=info",
        published: "array checkpoint flushed",
        restored: ("WAL array replay complete", "in_flight"),
        wal_truncation: false,
    })
    .await;
}

/// A timeseries checkpoint flushes only a collection whose memtable holds
/// rows, and stamps that collection's partition. So the held collection
/// carries a boot-1 row into boot 2's memtable, and boot 2's first checkpoint
/// runs after A is minted. That checkpoint flushes the held collection with a
/// stamp naming B writes above A. Boot 3 replays A below that stamp's highest
/// LSN (`in_flight`).
#[tokio::test(flavor = "multi_thread")]
async fn a_timeseries_write_in_flight_at_a_checkpoint_survives_kill_9() {
    run(Case {
        held: "stamp_ts_lo",
        applied: "stamp_ts_hi",
        create_held: "CREATE COLLECTION stamp_ts_lo \
                      COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
                      WITH (engine='timeseries')",
        create_applied: "CREATE COLLECTION stamp_ts_hi \
                         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
                         WITH (engine='timeseries')",
        seed_held: Some("INSERT INTO stamp_ts_lo (id, ts, value) VALUES ('seed', 1000, 1.0)"),
        insert_held: "INSERT INTO stamp_ts_lo (id, ts, value) VALUES ('held', 2000, 7.0)",
        read_held: "SELECT value FROM stamp_ts_lo WHERE id = 'held'",
        held_value: "7",
        insert_applied: |n| {
            format!(
                "INSERT INTO stamp_ts_hi (id, ts, value) VALUES ('r{n:03}', {}, {n}.0)",
                1_000 + n
            )
        },
        read_applied: "SELECT id FROM stamp_ts_hi",
        checkpoint_interval_secs: "10",
        log_directives: "nodedb::data::executor::handlers::timeseries::flush=info,\
                         nodedb::data::executor::handlers::timeseries_wal=info",
        published: "timeseries columnar flush complete",
        restored: ("WAL timeseries replay complete", "in_flight"),
        wal_truncation: false,
    })
    .await;
}

/// A checkpoint that runs while A is parked reports a floor below A. So a
/// truncation run keeps the segment that holds A's record, even after filler
/// writes above A seal it. A floor taken from the highest applied LSN lets the
/// run remove that segment, and A's row is lost with it.
#[tokio::test(flavor = "multi_thread")]
async fn a_kv_write_in_flight_at_a_wal_truncation_survives_kill_9() {
    run(Case {
        held: "stamp_trunc_lo",
        applied: "stamp_trunc_hi",
        create_held: "CREATE COLLECTION stamp_trunc_lo (k STRING PRIMARY KEY, v STRING) \
                      WITH (engine='kv')",
        create_applied: "CREATE COLLECTION stamp_trunc_hi (k STRING PRIMARY KEY, v STRING) \
                         WITH (engine='kv')",
        seed_held: None,
        insert_held: "INSERT INTO stamp_trunc_lo (k, v) VALUES ('held', 'a')",
        read_held: "SELECT v FROM stamp_trunc_lo WHERE k = 'held'",
        held_value: "a",
        insert_applied: |n| format!("INSERT INTO stamp_trunc_hi (k, v) VALUES ('k{n:03}', 'v{n}')"),
        read_applied: "SELECT v FROM stamp_trunc_hi",
        checkpoint_interval_secs: "1",
        log_directives: "nodedb::data::executor::kv_checkpoint=info,\
                         nodedb::control::checkpoint_manager=debug",
        published: "KV checkpoint published",
        restored: ("KV checkpoint restored", "applied_ranges"),
        wal_truncation: true,
    })
    .await;
}

async fn run(case: Case) {
    let mut h = CrashHarness::new()
        .standalone()
        .with_env(
            "NODEDB_CHECKPOINT_INTERVAL_SECS",
            QUIET_CHECKPOINT_INTERVAL_SECS,
        )
        .with_env("RUST_LOG", &format!("warn,{}", case.log_directives));
    if case.wal_truncation {
        h.set_env("NODEDB_WAL_SEGMENT_TARGET_MB", WAL_SEGMENT_TARGET_MB);
    }
    h.spawn();
    h.wait_ready();
    h.exec(case.create_held).await;
    h.exec(case.create_applied).await;
    if case.wal_truncation {
        h.exec(&format!(
            "CREATE COLLECTION {FILLER} (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')"
        ))
        .await;
        // Sealed segments below A, so the floor A holds lets truncation run.
        write_filler(&h, "below").await;
    }
    if let Some(seed) = case.seed_held {
        h.exec(seed).await;
    }

    // Boot 2 arms the gate and the abort, keyed to the held collection. Both
    // match only a request carrying a WAL LSN, so boot itself passes them.
    h.kill_9();
    let release = h.data_dir().join("release-held-write");
    h.set_env(
        "NODEDB_CHECKPOINT_INTERVAL_SECS",
        case.checkpoint_interval_secs,
    );
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
    let deadline = Instant::now() + CHECKPOINT_DEADLINE;
    let mut applied = 0usize;
    loop {
        h.exec(&(case.insert_applied)(applied)).await;
        applied += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let log = boot_section(&h.server_log(), 2);
        if log_field(&log, case.published, "applied_ranges")
            .iter()
            .any(|n| *n > 0)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no {} named an applied LSN above its prefix within {CHECKPOINT_DEADLINE:?}: \
             write A never parked, or no checkpoint ran while it was.{}\n{}",
            case.published,
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
    }
    let held_segment = if case.wal_truncation {
        Some(truncate_while_held(&h).await)
    } else {
        None
    };
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

    let (restored, field) = case.restored;
    let proof = log_field(&boot_section(&h.server_log(), 3), restored, field);
    assert!(
        proof.iter().any(|n| *n > 0),
        "no {restored} line has {field} above zero, so this run did not reproduce the \
         in-flight write (values: {proof:?}).{}\n{}",
        h.keep_data_dir_note(),
        diagnostics::log_tail_section(&h.server_log())
    );

    assert_restored(&h, &case, &live).await;

    if let Some(segment) = held_segment {
        truncation_advances_once_settled(&mut h, &segment).await;
        h.kill_9();
        h.reopen();
        assert_restored(&h, &case, &live).await;
    }
}

/// A's row is back, and every B write is present once.
async fn assert_restored(h: &CrashHarness, case: &Case, live: &[String]) {
    let held = h.query_col_idx(case.read_held, 0).await;
    assert!(
        held.len() == 1 && same_value(&held[0], case.held_value),
        "read {held:?}, expected [{}]: write A to {} applied after the checkpoint and \
         before the crash; replay must apply it, never skip it as covered by a higher \
         applied LSN",
        case.held_value,
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

/// Filler rows that seal the active WAL segment.
async fn write_filler(h: &CrashHarness, tag: &str) {
    let filler = "x".repeat(FILLER_VALUE_BYTES);
    for i in 0..FILLER_ROWS {
        h.exec(&format!(
            "INSERT INTO {FILLER} (k, v) VALUES ('{tag}{i}', '{filler}')"
        ))
        .await;
    }
}

/// Seal the segment that holds A's record, then wait while A is parked for a
/// checkpoint that ran after the seal and a truncation that removed segments.
/// Returns the name of A's segment.
///
/// A parked after its append and before any B write, and the B writes are too
/// small to fill a segment. So the active segment now holds A's record.
async fn truncate_while_held(h: &CrashHarness) -> String {
    let held_segment = h.active_wal_segment();
    write_filler(h, "above").await;
    let active = h.active_wal_segment();
    assert_ne!(
        active, held_segment,
        "the filler did not seal A's segment. Truncation never removes the active \
         segment, so this run proves nothing"
    );

    // Every record in the active segment is above A. A marker at or above its
    // first LSN comes from a checkpoint that ran after the seal.
    let sealed_at = segment_first_lsn(&active);
    let deadline = Instant::now() + CHECKPOINT_DEADLINE;
    loop {
        let log = boot_section(&h.server_log(), 2);
        let ran_after_seal = truncation_finished_from(&log, sealed_at);
        let truncated = !log_field(&log, WAL_TRUNCATED, "segments_deleted").is_empty();
        if ran_after_seal && truncated {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "within {CHECKPOINT_DEADLINE:?} no checkpoint finished truncation after the filler \
             sealed A's segment ({ran_after_seal}), or no truncation removed a segment below A \
             ({truncated}).{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let segments = h.wal_segments();
    assert!(
        segments.contains(&held_segment),
        "a truncation removed {held_segment} while write A in it was in flight: \
         truncation must stay below the lowest checkpoint floor. Segments: {segments:?}"
    );
    held_segment
}

/// After the restart A is applied, so truncation must remove A's segment.
/// A new write lets the Event Plane persist a watermark above it too.
async fn truncation_advances_once_settled(h: &mut CrashHarness, segment: &str) {
    h.exec(&format!(
        "INSERT INTO {FILLER} (k, v) VALUES ('settled', 's')"
    ))
    .await;
    let deadline = Instant::now() + CHECKPOINT_DEADLINE;
    while h.wal_segments().iter().any(|name| name == segment) {
        assert!(
            Instant::now() < deadline,
            "truncation never removed {segment} after write A settled: the floor held \
             below a record that has its outcome.{}\n{}",
            h.keep_data_dir_note(),
            diagnostics::log_tail_section(&h.server_log())
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
