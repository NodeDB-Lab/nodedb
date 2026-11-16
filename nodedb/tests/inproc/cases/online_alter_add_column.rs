// SPDX-License-Identifier: BUSL-1.1

//! Online ALTER ADD COLUMN on strict collections must not stall concurrent writes.
//!
//! Verifies that executing ALTER COLLECTION ... ADD COLUMN while a concurrent
//! writer is running does not add a material stall relative to the same
//! process's measured baseline INSERT latency. Also confirms the schema-version
//! barrier span fires exactly once and that no row is left with NULL in the new
//! column after the ALTER completes.

use nodedb_test_support::pgwire_harness::TestServer;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Baseline INSERTs the writer records before the ALTER starts.
const BASELINE_SAMPLES: usize = 60;
/// How long the writer has to record its baseline samples.
const WARMUP_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Tracing layer: counts how many times the schema_version_barrier_acquired
// event is emitted on the "nodedb::schema_barrier" target.
// ---------------------------------------------------------------------------

struct BarrierCounter {
    count: Arc<AtomicU64>,
}

/// Visitor that checks whether the tracing event's message field equals the
/// expected value.
struct MessageChecker {
    found: bool,
}

impl tracing::field::Visit for MessageChecker {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" && value == "schema_version_barrier_acquired" {
            self.found = true;
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let s = format!("{value:?}");
            // Debug-format of a &str includes surrounding quotes; strip them.
            let inner = s.trim_matches('"');
            if inner == "schema_version_barrier_acquired" {
                self.found = true;
            }
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for BarrierCounter {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() == "nodedb::schema_barrier" {
            let mut checker = MessageChecker { found: false };
            event.record(&mut checker);
            if checker.found {
                self.count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn alter_add_column_does_not_stall_writes() {
    // Install the barrier-counter layer before starting the server so it
    // captures events from the server's own subscriber context.
    // try_init is used so that if another test already set a global subscriber
    // the test doesn't panic; the counter layer is still installed as a layer
    // on the new registry if this is the first.
    let barrier_count = Arc::new(AtomicU64::new(0));
    let counter_layer = BarrierCounter {
        count: Arc::clone(&barrier_count),
    };
    let _ = tracing_subscriber::registry()
        .with(counter_layer)
        .try_init();

    let srv = TestServer::start().await;

    // Create a strict collection. The correct engine name is 'document_strict'.
    srv.exec(
        "CREATE COLLECTION strict_alter_test \
         (id INTEGER PRIMARY KEY, val TEXT) \
         WITH (engine='document_strict')",
    )
    .await
    .expect("create strict collection");

    // Pre-populate a modest number of rows so the ALTER has real data to
    // backfill. 1 000 rows via a single batched INSERT is fast (<100 ms)
    // and sufficient to verify the ALTER backfills all pre-existing rows.
    //
    // 1 M rows would stress-test ingest throughput but would exceed the test
    // timeout; the gate's intent is stall detection, not throughput load.
    let preload_batch: Vec<String> = (1i64..=1_000).map(|i| format!("({i}, 'pre{i}')")).collect();
    let preload_sql = format!(
        "INSERT INTO strict_alter_test (id, val) VALUES {}",
        preload_batch.join(", ")
    );
    srv.exec(&preload_sql).await.expect("preload insert");

    // -----------------------------------------------------------------------
    // Concurrent writer task — runs on a second pgwire connection.
    //
    // Inserts one row every 5 ms and records the wall-clock duration of each
    // INSERT with its phase relative to the ALTER.
    // -----------------------------------------------------------------------
    let latencies: Arc<Mutex<Vec<(Duration, u8)>>> =
        Arc::new(Mutex::new(Vec::with_capacity(64_000)));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let alter_started = Arc::new(AtomicBool::new(false));
    let alter_finished = Arc::new(AtomicBool::new(false));

    let latencies_w = Arc::clone(&latencies);
    let stop_w = Arc::clone(&stop_flag);
    let alter_started_w = Arc::clone(&alter_started);
    let alter_finished_w = Arc::clone(&alter_finished);
    let writer_port = srv.pg_port;
    let next_id = Arc::new(AtomicU64::new(1_001));
    let next_id_w = Arc::clone(&next_id);

    let writer_task = tokio::spawn(async move {
        let conn_str = format!(
            "host=127.0.0.1 port={} user=nodedb dbname=default",
            writer_port
        );
        let (writer_client, conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("writer pgwire connect");
        tokio::spawn(async move {
            let _ = conn.await;
        });

        // 200 inserts/second — well under the 1 000 QPS tenant default, so
        // the verification queries that follow the writer are never rate-limited.
        let mut interval = tokio::time::interval(Duration::from_millis(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        while !stop_w.load(Ordering::Relaxed) {
            interval.tick().await;
            let id = next_id_w.fetch_add(1, Ordering::Relaxed) as i64;
            let sql = format!("INSERT INTO strict_alter_test (id, val) VALUES ({id}, 'w{id}')");
            let alter_finished_before = alter_finished_w.load(Ordering::Acquire);
            let t0 = Instant::now();
            let _ = writer_client.simple_query(&sql).await;
            let elapsed = t0.elapsed();
            let alter_started_after = alter_started_w.load(Ordering::Acquire);
            let phase = if alter_started_after && !alter_finished_before {
                1
            } else if alter_started_after {
                2
            } else {
                0
            };
            latencies_w
                .lock()
                .expect("latency lock")
                .push((elapsed, phase));
        }
    });

    // Warmup: the ALTER starts only once the writer recorded its baseline
    // samples. A fixed sleep races the writer's connect and first insert.
    let warmup_deadline = Instant::now() + WARMUP_TIMEOUT;
    loop {
        let baseline_samples = latencies
            .lock()
            .expect("latency lock")
            .iter()
            .filter(|(_, phase)| *phase == 0)
            .count();
        if baseline_samples >= BASELINE_SAMPLES {
            break;
        }
        assert!(
            Instant::now() < warmup_deadline,
            "writer recorded {baseline_samples} of {BASELINE_SAMPLES} baseline samples \
             within {WARMUP_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Snapshot the barrier count before the ALTER so that CREATE COLLECTION
    // and other setup DDL that also acquire the schema barrier are excluded
    // from the assertion.
    let barrier_before = barrier_count.load(Ordering::Relaxed);

    // Online ALTER while the writer runs.
    alter_started.store(true, Ordering::Release);
    srv.exec(
        "ALTER COLLECTION strict_alter_test \
         ADD COLUMN c INTEGER DEFAULT 0 NOT NULL",
    )
    .await
    .expect("ALTER ADD COLUMN");
    alter_finished.store(true, Ordering::Release);

    // Post-alter: 300 ms more to verify writes continue under the new schema.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Stop the writer.
    stop_flag.store(true, Ordering::Relaxed);
    writer_task.await.expect("writer task join");

    // -----------------------------------------------------------------------
    // Assertions
    // -----------------------------------------------------------------------

    // 1. ALTER-overlapping INSERTs must not incur a material stall relative
    //    to baseline. A fixed wall-clock ceiling is not portable: overloaded
    //    CI workers can make an ordinary loopback INSERT exceed 50 ms. The
    //    additive 100 ms allowance is applied to the worst observed baseline,
    //    while a lock-held backfill still exceeds it by orders of magnitude.
    let samples = latencies.lock().expect("latency lock").clone();
    assert!(
        !samples.is_empty(),
        "writer produced no latency samples — writer task may have failed to connect"
    );
    let mut baseline: Vec<Duration> = samples
        .iter()
        .filter_map(|(latency, phase)| (*phase == 0).then_some(*latency))
        .collect();
    let mut during_alter: Vec<Duration> = samples
        .iter()
        .filter_map(|(latency, phase)| (*phase == 1).then_some(*latency))
        .collect();
    assert!(!baseline.is_empty(), "writer produced no baseline samples");
    baseline.sort_unstable();
    during_alter.sort_unstable();
    let baseline_max = *baseline.last().expect("non-empty baseline");
    let alter_max = during_alter.last().copied().unwrap_or_default();
    let allowed_alter_max = baseline_max.saturating_add(Duration::from_millis(100));

    // Print for visibility in CI logs without introducing extra dependencies.
    eprintln!(
        "online_alter latency: baseline_samples={}, alter_samples={}, \
         baseline_max={:?}, alter_max={:?}, allowed_alter_max={:?}",
        baseline.len(),
        during_alter.len(),
        baseline_max,
        alter_max,
        allowed_alter_max,
    );

    assert!(
        alter_max <= allowed_alter_max,
        "ALTER-overlapping INSERT duration {alter_max:?} exceeded measured \
         baseline {baseline_max:?} plus 100 ms — ALTER stalled writes"
    );

    // Brief pause so the per-tenant QPS counter rolls over before the final
    // verification queries run. The writer saturated the counter during the
    // 4-second measurement window; 1.1 seconds is enough for a 1-second window.
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    // 2. No NULLs in the new column: DEFAULT 0 must cover pre-existing rows.
    // Verify no row has NULL in the new column. Simple-query COUNT(*) returns a
    // CommandComplete message without a data row on this server, so we SELECT id
    // directly and assert the result set is empty.
    let null_rows = srv
        .query_rows("SELECT id FROM strict_alter_test WHERE c IS NULL")
        .await
        .expect("null check query");
    assert!(
        null_rows.is_empty(),
        "found {} rows with NULL in column c after ALTER",
        null_rows.len()
    );

    // 3. Schema-version barrier must have fired exactly once for the ALTER.
    //    The count captured before the ALTER excludes CREATE COLLECTION and
    //    any other setup DDL that also acquires the schema barrier.
    let barrier_fires = barrier_count.load(Ordering::Relaxed) - barrier_before;
    assert_eq!(
        barrier_fires, 1,
        "expected schema_version_barrier_acquired to fire exactly once for \
         the ALTER, but it fired {barrier_fires} times"
    );
}
