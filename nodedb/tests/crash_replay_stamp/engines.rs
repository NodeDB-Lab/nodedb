// SPDX-License-Identifier: BUSL-1.1

//! One in-flight-write case per engine, and one for WAL truncation.

use crate::crash_harness::vshards::ArrayCellRoute;

use super::case::{Case, LiveApplied};
use super::run::run;

#[tokio::test(flavor = "multi_thread")]
async fn a_kv_write_in_flight_at_a_checkpoint_survives_kill_9() {
    run(Case {
        held: "stamp_kv_lo",
        applied: "stamp_kv_hi",
        create_held: "CREATE COLLECTION {held} (k STRING PRIMARY KEY, v STRING) \
                      WITH (engine='kv')",
        create_applied: "CREATE COLLECTION {applied} (k STRING PRIMARY KEY, v STRING) \
                         WITH (engine='kv')",
        seed_held: None,
        insert_held: "INSERT INTO {held} (k, v) VALUES ('held', 'a')",
        read_held: "SELECT v FROM {held} WHERE k = 'held'",
        held_value: "a",
        insert_applied: |t, n| format!("INSERT INTO {t} (k, v) VALUES ('k{n:03}', 'v{n}')"),
        read_applied: "SELECT v FROM {applied}",
        live_applied: LiveApplied::Read,
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
        create_held: "CREATE COLLECTION {held} COLUMNS (id TEXT, v TEXT) \
                      WITH (engine='columnar')",
        create_applied: "CREATE COLLECTION {applied} COLUMNS (id TEXT, v TEXT) \
                         WITH (engine='columnar')",
        seed_held: None,
        insert_held: "INSERT INTO {held} (id, v) VALUES ('held', 'a')",
        read_held: "SELECT v FROM {held} WHERE id = 'held'",
        held_value: "a",
        insert_applied: |t, n| format!("INSERT INTO {t} (id, v) VALUES ('r{n:03}', 'v{n}')"),
        read_applied: "SELECT v FROM {applied}",
        live_applied: LiveApplied::Read,
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
///
/// An array cell write routes by its tile, not by the array's name (see
/// [`ArrayCellRoute`]). Both arrays take `prefix_bits = 10`, so the tiles of
/// the applied array spread over every data group. Each B cell sits at a coordinate whose group
/// is not the group of A's cell.
#[tokio::test(flavor = "multi_thread")]
async fn an_array_write_in_flight_at_a_checkpoint_survives_kill_9() {
    run(Case {
        held: "stamp_arr_lo",
        applied: "stamp_arr_hi",
        create_held: "CREATE ARRAY {held} DIMS (k INT64 [0..15]) ATTRS (v FLOAT64) \
                      TILE_EXTENTS (16) CELL_ORDER ROW_MAJOR WITH (prefix_bits = 10)",
        create_applied: "CREATE ARRAY {applied} DIMS (k INT64 [0..1023]) ATTRS (v FLOAT64) \
                         TILE_EXTENTS (64) CELL_ORDER ROW_MAJOR WITH (prefix_bits = 10)",
        seed_held: Some("INSERT INTO ARRAY {held} COORDS (0) VALUES (1.0)"),
        insert_held: "INSERT INTO ARRAY {held} COORDS (1) VALUES (7.0)",
        read_held: "SELECT * FROM ARRAY_AGG('{held}', 'v', 'sum')",
        held_value: "8",
        insert_applied: |t, n| {
            let c = applied_array_coord(n);
            format!("INSERT INTO ARRAY {t} COORDS ({c}) VALUES ({c}.0)")
        },
        read_applied: "SELECT * FROM ARRAY_AGG('{applied}', 'v', 'sum')",
        // B number n stores the value of its coordinate, so the sum is the
        // sum of the coordinates written.
        live_applied: LiveApplied::Computed(|count| {
            let sum: i64 = (0..count).map(applied_array_coord).sum();
            vec![sum.to_string()]
        }),
        checkpoint_interval_secs: "10",
        log_directives: "nodedb::data::executor::array_checkpoint=info,\
                         nodedb::data::executor::wal_replay::array=info",
        published: "array checkpoint flushed",
        restored: ("WAL array replay complete", "in_flight"),
        wal_truncation: false,
    })
    .await;
}

/// The `prefix_bits` both arrays of the array case take. At 10 bits every
/// bucket owns its own vShard, so cells spread over every data group.
const ARRAY_PREFIX_BITS: u8 = 10;

/// The coordinate of B number `n` in the array case: the `n`th coordinate of
/// `[0..1023]` whose cell applies in another data group than A's cell, coord 1
/// of `[0..15]`. The domains and tile extents match the case's `CREATE
/// ARRAY` statements.
fn applied_array_coord(n: usize) -> i64 {
    let held_group = ArrayCellRoute::new(0, 15, 16, ARRAY_PREFIX_BITS).group(1);
    let applied = ArrayCellRoute::new(0, 1023, 64, ARRAY_PREFIX_BITS);
    (0..=1023)
        .filter(|coord| applied.group(*coord) != held_group)
        .nth(n)
        .unwrap_or_else(|| panic!("no B coordinate number {n} outside data group {held_group}"))
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
        create_held: "CREATE COLLECTION {held} \
                      COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
                      WITH (engine='timeseries')",
        create_applied: "CREATE COLLECTION {applied} \
                         COLUMNS (id TEXT, ts BIGINT TIME_KEY, value FLOAT) \
                         WITH (engine='timeseries')",
        seed_held: Some("INSERT INTO {held} (id, ts, value) VALUES ('seed', 1000, 1.0)"),
        insert_held: "INSERT INTO {held} (id, ts, value) VALUES ('held', 2000, 7.0)",
        read_held: "SELECT value FROM {held} WHERE id = 'held'",
        held_value: "7",
        insert_applied: |t, n| {
            format!(
                "INSERT INTO {t} (id, ts, value) VALUES ('r{n:03}', {}, {n}.0)",
                1_000 + n
            )
        },
        read_applied: "SELECT id FROM {applied}",
        live_applied: LiveApplied::Read,
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
        create_held: "CREATE COLLECTION {held} (k STRING PRIMARY KEY, v STRING) \
                      WITH (engine='kv')",
        create_applied: "CREATE COLLECTION {applied} (k STRING PRIMARY KEY, v STRING) \
                         WITH (engine='kv')",
        seed_held: None,
        insert_held: "INSERT INTO {held} (k, v) VALUES ('held', 'a')",
        read_held: "SELECT v FROM {held} WHERE k = 'held'",
        held_value: "a",
        insert_applied: |t, n| format!("INSERT INTO {t} (k, v) VALUES ('k{n:03}', 'v{n}')"),
        read_applied: "SELECT v FROM {applied}",
        live_applied: LiveApplied::Read,
        checkpoint_interval_secs: "1",
        log_directives: "nodedb::data::executor::kv_checkpoint=info,\
                         nodedb::control::checkpoint_manager=debug",
        published: "KV checkpoint published",
        restored: ("KV checkpoint restored", "applied_ranges"),
        wal_truncation: true,
    })
    .await;
}
