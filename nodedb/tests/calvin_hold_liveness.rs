// SPDX-License-Identifier: BUSL-1.1

//! A held Calvin flush never stalls replicated writes to other collections.
//!
//! Transaction A writes two collections on two vShards, so it commits
//! through the Calvin scheduler. Its flush on one vShard is held at a fail
//! point, so A stays staged: its rows are owned until the flush. A replicated
//! autocommit write to a third collection shares no row and no collection
//! with A. It must apply and be acknowledged while A's flush is held.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::CrashHarness;
use crash_harness::log_fields::{boot_section, count_lines};
use crash_harness::vshards::names_on_distinct_vshards;

/// How long the Calvin sequencer may take to elect its leader after a boot.
const CALVIN_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the test waits for A's flush to be held.
const HOLD_DEADLINE: Duration = Duration::from_secs(30);

/// How long the unrelated write may take. Well under the 30s request
/// deadline a write parked behind A would wait out.
const UNRELATED_WRITE_BUDGET: Duration = Duration::from_secs(10);

/// The scheduler's log line for a flush held at the fail point.
const FLUSH_HELD: &str = "calvin: flush held at a fail point";

const LOG_DIRECTIVES: &str = "warn,nodedb::control::cluster::calvin::scheduler::driver::core=info";

#[tokio::test(flavor = "multi_thread")]
async fn a_replicated_write_to_an_unrelated_collection_applies_while_a_calvin_flush_is_held() {
    let [held, peer, other] = names_on_distinct_vshards(["hold_held", "hold_peer", "hold_other"]);
    let h = CrashHarness::new();
    let release = h.data_dir().join("release-held-flush");
    let mut h = h.with_env("RUST_LOG", LOG_DIRECTIVES).with_env(
        "NODEDB_FAILPOINTS",
        &format!(
            "calvin::before_flush::{held}=wait_file({})",
            release.display()
        ),
    );
    h.spawn();
    h.wait_ready();
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    for name in [&held, &peer, &other] {
        h.exec(&format!(
            "CREATE COLLECTION {name} (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')"
        ))
        .await;
    }

    // Transaction A, on its own connection: its COMMIT waits for the held
    // flush.
    let conn_str = h.pgwire_conn_str();
    let statements = [
        "BEGIN".to_string(),
        format!("INSERT INTO {held} (k, v) VALUES ('held', 'a')"),
        format!("INSERT INTO {peer} (k, v) VALUES ('peer', 'p')"),
        "COMMIT".to_string(),
    ];
    let txn = tokio::spawn(async move {
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

    let deadline = Instant::now() + HOLD_DEADLINE;
    while count_lines(&boot_section(&h.server_log(), 1), &[FLUSH_HELD]) == 0 {
        assert!(
            Instant::now() < deadline && !txn.is_finished(),
            "transaction A's flush was never held"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::timeout(
        UNRELATED_WRITE_BUDGET,
        h.exec(&format!("INSERT INTO {other} (k, v) VALUES ('o', 'x')")),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "a replicated write to {other} stalled behind transaction A's held flush, \
             though it shares no row and no collection with A"
        )
    });
    assert_eq!(
        h.query_col_idx(&format!("SELECT v FROM {other} WHERE k = 'o'"), 0)
            .await,
        vec!["x".to_string()]
    );

    std::fs::write(&release, b"release").expect("create the release file");
    txn.await
        .expect("transaction A's task")
        .unwrap_or_else(|e| panic!("transaction A: {e}"));
}
