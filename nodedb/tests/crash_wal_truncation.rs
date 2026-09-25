// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill regressions for WAL truncation against memory-only
//! engines: a write must survive after the checkpoint deletes the WAL
//! segment holding its record, the only durable copy. Requires a short
//! checkpoint interval, forced WAL segment rotation (`truncate_segments`
//! skips the active segment), and confirming the sealed segment was unlinked.

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::{Duration, Instant};

/// Checkpoint cycle short enough to fire several times inside one test.
const CHECKPOINT_INTERVAL_SECS: &str = "2";

/// Smallest WAL segment target the config accepts (`wal_segment_target_mb` is
/// whole MiB), so the filler below only has to write a little over 1 MiB per
/// rotation.
const WAL_SEGMENT_TARGET_MB: &str = "1";

/// Filler payload per row — large so a handful of writes seal the segment,
/// not dozens (each is its own WAL fsync round-trip). Stays under the 1 MiB
/// segment target and far under the 64 MiB WAL record cap.
const FILLER_VALUE_BYTES: usize = 512 * 1024;

/// ~2.5 MiB of filler over a 1 MiB segment target: enough to seal at least two
/// segments, so the canary's segment is sealed and strictly below the active
/// one no matter where the boot records happened to land.
const FILLER_ROWS: usize = 5;

/// How long to wait for the checkpoint to unlink the canary's segment. Several
/// times the checkpoint interval, but bounded: a timeout here means truncation
/// never ran, and that must FAIL the test rather than let it pass vacuously.
const TRUNCATION_TIMEOUT: Duration = Duration::from_secs(45);

fn tuned_harness() -> CrashHarness {
    CrashHarness::new()
        .with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", CHECKPOINT_INTERVAL_SECS)
        .with_env("NODEDB_WAL_SEGMENT_TARGET_MB", WAL_SEGMENT_TARGET_MB)
}

fn filler_value() -> String {
    "x".repeat(FILLER_VALUE_BYTES)
}

/// Block until `segment` has been unlinked, panicking if it never is. The
/// only thing distinguishing "checkpoint restored the row" from "WAL record
/// was still there all along".
fn await_segment_deleted(h: &CrashHarness, segment: &str) {
    let deadline = Instant::now() + TRUNCATION_TIMEOUT;
    loop {
        let live = h.wal_segments();
        if !live.iter().any(|s| s == segment) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "WAL segment {segment} still exists after {TRUNCATION_TIMEOUT:?} — the checkpoint \
             never truncated it, so this test proves NOTHING about surviving truncation. \
             Segments on disk: {live:?}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// KV is the flagship case: `KvEngine` is a plain in-memory `HashMap`, so
/// before it had a checkpoint the WAL held the only copy of every row —
/// and KV writes advanced the watermark that authorized deleting it.
#[tokio::test(flavor = "multi_thread")]
async fn kv_row_survives_wal_segment_truncation() {
    let mut h = tuned_harness();
    h.spawn();
    h.wait_ready_extended();

    h.exec("CREATE COLLECTION trunc_kv (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await;
    h.exec("INSERT INTO trunc_kv (k, v) VALUES ('canary', 'survives')")
        .await;

    // The segment holding the canary's WAL record, captured while it is still
    // the active one and therefore provably not yet truncated.
    let canary_segment = h.active_wal_segment();

    // Live sanity BEFORE anything else: the row reads back now, so a failure
    // after the restart is attributable to recovery and not to test setup.
    let live = h
        .query_col("SELECT v FROM trunc_kv WHERE k = 'canary'", "v")
        .await;
    assert_eq!(
        live,
        vec!["survives".to_string()],
        "KV row must read back BEFORE the crash (test-setup sanity): {live:?}"
    );

    // Force rotation: seal the canary's segment, or truncation skips it
    // unconditionally while it's still active.
    let filler = filler_value();
    for i in 0..FILLER_ROWS {
        h.exec(&format!(
            "INSERT INTO trunc_kv (k, v) VALUES ('filler{i}', '{filler}')"
        ))
        .await;
    }
    assert_ne!(
        h.active_wal_segment(),
        canary_segment,
        "filler writes did not rotate the WAL — the canary's segment is still active and \
         truncation would skip it, making this test vacuous"
    );

    // Wait for a checkpoint cycle to actually delete it.
    await_segment_deleted(&h, &canary_segment);

    h.kill_9();
    h.reopen();

    // The canary's WAL record is gone from disk. If the row comes back, it came
    // from the KV checkpoint — the only other copy that can exist.
    let recovered = h
        .query_col("SELECT v FROM trunc_kv WHERE k = 'canary'", "v")
        .await;
    assert_eq!(
        recovered,
        vec!["survives".to_string()],
        "KV row was LOST: its WAL segment was truncated by the checkpoint and the KV engine \
         had no durable copy of its own (got {recovered:?})"
    );
}

/// Columnar is memory-only on both halves — the live memtable and the
/// encoded bytes of flushed segments — so it faces the same truncation.
#[tokio::test(flavor = "multi_thread")]
async fn columnar_row_survives_wal_segment_truncation() {
    let mut h = tuned_harness();
    h.spawn();
    h.wait_ready_extended();

    h.exec(
        "CREATE COLLECTION trunc_columnar \
         COLUMNS (id TEXT, region TEXT, payload TEXT) \
         WITH (engine='columnar')",
    )
    .await;
    h.exec("INSERT INTO trunc_columnar (id, region, payload) VALUES ('canary', 'us', 'small')")
        .await;

    let canary_segment = h.active_wal_segment();

    let live = h
        .query_col(
            "SELECT region FROM trunc_columnar WHERE id = 'canary'",
            "region",
        )
        .await;
    assert_eq!(
        live,
        vec!["us".to_string()],
        "columnar row must read back BEFORE the crash (test-setup sanity): {live:?}"
    );

    let filler = filler_value();
    for i in 0..FILLER_ROWS {
        h.exec(&format!(
            "INSERT INTO trunc_columnar (id, region, payload) VALUES ('filler{i}', 'eu', '{filler}')"
        ))
        .await;
    }
    assert_ne!(
        h.active_wal_segment(),
        canary_segment,
        "filler writes did not rotate the WAL — the canary's segment is still active and \
         truncation would skip it, making this test vacuous"
    );

    await_segment_deleted(&h, &canary_segment);

    h.kill_9();
    h.reopen();

    let recovered = h
        .query_col(
            "SELECT region FROM trunc_columnar WHERE id = 'canary'",
            "region",
        )
        .await;
    assert_eq!(
        recovered,
        vec!["us".to_string()],
        "columnar row was LOST: its WAL segment was truncated by the checkpoint and the \
         columnar engine had no durable copy of its own (got {recovered:?})"
    );
}
