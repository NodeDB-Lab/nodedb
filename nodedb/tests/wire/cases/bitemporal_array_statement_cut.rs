// SPDX-License-Identifier: BUSL-1.1

//! An array aggregate reads one system-time cut.
//!
//! The Control Plane pins an `ARRAY_AGG ... AS OF SYSTEM TIME` to one cut,
//! as it pins a scan's. Every core's partial reads at that cut, so a
//! grouped aggregate run while a writer appends cells sees a prefix of the
//! writer's history. A past cut reads the same groups on every run.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::harness::TestServer;

/// Cells the writer appends, one per `x`. Tiles of extent 4 on `x` spread
/// them over 64 tiles and every core.
const CELLS: i64 = 256;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

/// A cut one day past the wall clock.
fn future_ms() -> i64 {
    now_ms() + 86_400_000
}

async fn create_array(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE ARRAY {name} DIMS (x INT64 [0..{}], y INT64 [0..0]) ATTRS (v INT64) \
             TILE_EXTENTS (4, 1)",
            CELLS - 1
        ))
        .await
        .expect("create array");
}

async fn write_cell(server: &TestServer, name: &str, x: i64) {
    server
        .exec(&format!(
            "INSERT INTO ARRAY {name} COORDS ({x}, 0) VALUES ({x})"
        ))
        .await
        .expect("insert cell");
}

/// The `x` groups `ARRAY_AGG(<name>, 'v', 'count', 'x')` reads at `cut`.
/// Every group counts exactly one cell.
async fn groups_at(server: &TestServer, name: &str, cut: i64) -> BTreeSet<i64> {
    let sql =
        format!("SELECT * FROM ARRAY_AGG('{name}', 'v', 'count', 'x') AS OF SYSTEM TIME {cut}");
    let rows = server
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    // The temporal summary row carries no group.
    rows.iter()
        .filter_map(|row| {
            let group = row.get("group").filter(|g| !g.is_empty())?;
            let count: f64 = row["result"].parse().expect("count result");
            assert_eq!(count, 1.0, "{sql}: group {group} counts one cell");
            Some(group.parse().expect("group"))
        })
        .collect()
}

/// Check that `groups` is a prefix `0..n` of the writer's history. The
/// writer finishes cell `x` before it starts `x + 1`, so one cut holds a
/// prefix. A core that read an older cut leaves a hole.
fn assert_prefix(groups: &BTreeSet<i64>) {
    let expected: BTreeSet<i64> = (0..groups.len() as i64).collect();
    assert_eq!(
        groups, &expected,
        "an aggregate read a hole in the writer's history: its cores read different cuts"
    );
}

/// A grouped aggregate `AS OF` a future time, run while a writer appends
/// cells across every core, reads one cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_future_cut_aggregate_reads_one_cut_under_concurrent_writes() {
    let server = TestServer::start_multicores(4).await;
    create_array(&server, "sac_cc").await;

    let (writer, _writer_task) = server.connect_as("nodedb", "nodedb").await.unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let finished = Arc::clone(&done);
    let inserter = tokio::spawn(async move {
        for x in 0..CELLS {
            writer
                .simple_query(&format!(
                    "INSERT INTO ARRAY sac_cc COORDS ({x}, 0) VALUES ({x})"
                ))
                .await
                .expect("cell insert");
        }
        finished.store(true, Ordering::SeqCst);
    });

    let mut reads = 0usize;
    while !done.load(Ordering::SeqCst) {
        assert_prefix(&groups_at(&server, "sac_cc", future_ms()).await);
        reads += 1;
    }
    inserter.await.expect("inserter");
    assert!(reads > 0, "the aggregate ran while the writer wrote");

    let all = groups_at(&server, "sac_cc", future_ms()).await;
    assert_eq!(
        all.len() as i64,
        CELLS,
        "every cell is under a cut after the writer ends"
    );
    assert_prefix(&all);
}

/// A past cut reads the same groups on every run, however many cells land
/// after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_past_cut_aggregate_reads_the_same_groups_every_run() {
    let server = TestServer::start_multicores(4).await;
    create_array(&server, "sac_past").await;
    for x in 0..16 {
        write_cell(&server, "sac_past", x).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let cut = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let first = groups_at(&server, "sac_past", cut).await;
    assert_eq!(first, (0..16).collect::<BTreeSet<i64>>());
    for x in 16..64 {
        write_cell(&server, "sac_past", x).await;
    }
    assert_eq!(
        groups_at(&server, "sac_past", cut).await,
        first,
        "a cell written after the cut stays out of every run"
    );
}
