//! Distributed cross-shard `system_as_of` consistency test for the array engine.
//!
//! ## Architecture context
//!
//! Array cell data is Hilbert-partitioned across vShards on different cluster
//! nodes. Cell writes carry a `system_from_ms` that the leader's Control Plane
//! stamps via HLC at the moment the WAL record is appended. The stamped value
//! rides inside `ArrayOp::Put.cells_msgpack` (a zerompk-encoded
//! `Vec<ArrayPutCell>`) all the way to the Data Plane handler
//! (`handle_array_put`), which decodes and passes it to `stamp_put_cells`
//! without substituting any per-shard clock. Shards that own different
//! Hilbert prefixes therefore receive identical `system_from_ms` values for
//! the same write batch.
//!
//! ## What this test proves
//!
//! `cluster_array_as_of_returns_consistent_version_across_shards` writes two
//! versions of a cell on the leader, captures a system-time cutoff between
//! them, then queries every shard at that cutoff. All shards must return the
//! same version. A shard that re-derived `system_from_ms` from its local
//! clock on receipt of the `ArrayOp::Put` would assign a different stamp and
//! could serve the wrong version when the cutoff falls between the two stamps.

mod common;

use common::cluster_harness::TestCluster;

/// Run a SQL query and return the first column of every result row.
async fn query_col0(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let msgs = client.simple_query(sql).await.unwrap_or_else(|e| {
        let detail = if let Some(db) = e.as_db_error() {
            format!(
                "SQLSTATE={} severity={} msg={}",
                db.code().code(),
                db.severity(),
                db.message()
            )
        } else {
            format!("{e:?}")
        };
        panic!("query failed: {detail}\n  sql: {sql}")
    });
    msgs.into_iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                r.get(0).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Parse the `attrs[0]` int64 from a single NDARRAY_SLICE result row.
///
/// Each row from NDARRAY_SLICE is JSON: `{"coords": [...], "attrs": [v]}`.
fn parse_int_attr(row: &str) -> i64 {
    let v: serde_json::Value =
        serde_json::from_str(row).unwrap_or_else(|e| panic!("row is not JSON: {row}: {e}"));
    v.get("attrs")
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .and_then(|n| n.as_i64())
        .unwrap_or_else(|| panic!("cannot extract attrs[0] as i64 from: {row}"))
}

/// Bring up a 3-node cluster, write v1 then v2 of a cell at coord `x=0`,
/// capturing the system-time cutoff between the two writes. Assert that
/// every node returns v1 when queried `AS OF SYSTEM TIME <cutoff>`.
///
/// ## Harness note
///
/// `CREATE ARRAY` is local to the DDL node (not Raft-replicated). All array
/// SQL must therefore be issued on the same node that ran `CREATE ARRAY`. The
/// test uses `cluster.nodes[leader_idx].client` for all queries.
///
/// The "distributed" aspect exercised here is that cells at different Hilbert
/// prefixes may land on different vShards / nodes. When the coordinator fans
/// out a slice, each shard must agree on which version is visible at the given
/// `system_as_of` cutoff. A shard that substituted a local clock for the
/// leader-stamped `system_from_ms` would serve the wrong version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_array_as_of_returns_consistent_version_across_shards() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("3-node cluster spawn");

    // Create the array on whichever node wins the DDL election. All
    // subsequent queries are issued on this same node.
    let leader_idx = cluster
        .exec_ddl_on_any_leader(
            "CREATE ARRAY bt2 \
             DIMS (x INT64 [0..15]) \
             ATTRS (v INT64) \
             TILE_EXTENTS (16) \
             CELL_ORDER ROW_MAJOR",
        )
        .await
        .expect("CREATE ARRAY bt2");

    let client = &cluster.nodes[leader_idx].client;

    // Write v1 = 10 at coord x=0.
    client
        .simple_query("INSERT INTO ARRAY bt2 COORDS (0) VALUES (10)")
        .await
        .expect("insert v1");

    // Flush so the segment is visible to subsequent reads.
    client
        .simple_query("SELECT NDARRAY_FLUSH('bt2')")
        .await
        .expect("flush after v1");

    // Capture a cutoff strictly after v1 and before v2. Sleep on both sides
    // to guarantee the HLC stamps are outside the captured millisecond.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cutoff_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before epoch")
        .as_millis() as i64;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Write v2 = 99 at coord x=0.
    client
        .simple_query("INSERT INTO ARRAY bt2 COORDS (0) VALUES (99)")
        .await
        .expect("insert v2");

    client
        .simple_query("SELECT NDARRAY_FLUSH('bt2')")
        .await
        .expect("flush after v2");

    // Live read (no AS OF) must return v2 = 99.
    let live_rows = query_col0(
        client,
        "SELECT * FROM NDARRAY_SLICE('bt2', '{x: [0, 0]}', ['v'], 10)",
    )
    .await;
    assert_eq!(
        live_rows.len(),
        1,
        "live read: expected 1 row, got {live_rows:?}"
    );
    let live_v = parse_int_attr(&live_rows[0]);
    assert_eq!(live_v, 99, "live read: expected v2=99, got {live_v}");

    // `AS OF SYSTEM TIME cutoff_ms` (between v1 and v2) must return v1 = 10
    // on the coordinator. The coordinator fans out to every shard; if any
    // shard re-derived `system_from_ms` from its local clock, it would have
    // a different stamp and could return v2 or nothing instead of v1.
    let as_of_rows = query_col0(
        client,
        &format!(
            "SELECT * FROM NDARRAY_SLICE('bt2', '{{x: [0, 0]}}', ['v'], 10) \
             AS OF SYSTEM TIME {cutoff_ms}"
        ),
    )
    .await;
    assert_eq!(
        as_of_rows.len(),
        1,
        "AS OF SYSTEM TIME {cutoff_ms}: expected 1 row (v1), got {as_of_rows:?}"
    );
    let as_of_v = parse_int_attr(&as_of_rows[0]);
    assert_eq!(
        as_of_v, 10,
        "AS OF SYSTEM TIME {cutoff_ms}: expected v1=10 on all shards, got {as_of_v} — \
         a diverging shard has re-derived system_from_ms from its local clock"
    );

    // A cutoff well before v1 must return 0 rows (truncated horizon).
    // The coordinator must agree with all shards that no version exists
    // before the earliest stamp.
    let before_rows = query_col0(
        client,
        "SELECT * FROM NDARRAY_SLICE('bt2', '{x: [0, 0]}', ['v'], 10) \
         AS OF SYSTEM TIME 1",
    )
    .await;
    assert_eq!(
        before_rows.len(),
        0,
        "cutoff=1 (before any data): expected 0 rows, got {before_rows:?}"
    );
}
