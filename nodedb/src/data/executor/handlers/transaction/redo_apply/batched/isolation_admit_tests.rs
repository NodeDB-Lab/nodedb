// SPDX-License-Identifier: BUSL-1.1

//! A second stream marker that arrives while another install is parked.
//!
//! The parked install writes [`FILL`] in the default database, with its
//! events on vShard 0. The second marker writes [`OTHER`] on another
//! vShard. One install parks at a time, so the second marker always waits.
//! While it waits it holds the keys it declares and its vShard alone. Once
//! the first install ends, it parks under its own fence and completes.

use crate::wal::{RedoStreamKeys, RedoSubRecord};

use super::super::stream_validate::tests::{LSN, hold, plain_core, record};
use super::equivalence_tests::install_whole;
use super::isolation_marker_tests::{
    KNOWS, SECOND, UNRELATED, UNRELATED_COLLECTION, edge_of, edge_put_op, on_third_vshard,
    record_reports, second_rows, second_rows_landed, undeclared_reports,
};
use super::isolation_tests::{OTHER, point_put};
use super::yield_tests::{
    INSTALL, MarkerSpec, OTHER_VSHARD, ROWS, Ring, assert_ok, install_request, installed_rows,
    tick_until_answered_while_waiting,
};

/// Queue the second marker of `ops` into `collections`, whose declaration
/// `declare` leaves of the keys the proposer's walk declares.
fn push_second(
    ring: &mut Ring,
    ops: Vec<RedoSubRecord>,
    collections: &[&str],
    declare: impl FnOnce(&mut RedoStreamKeys),
) {
    let collections = collections.iter().map(|name| name.to_string()).collect();
    let plan =
        ring.marker_plan_declaring(&record(ops), collections, MarkerSpec::default(), declare);
    let mut second = install_request(SECOND, plan, LSN + 1);
    second.vshard_id = OTHER_VSHARD;
    ring.push(second, None);
}

/// Tick until the first install answers while the second marker waits.
/// The second marker then parks under its own fence, and completes.
fn second_parks_after_the_first(ring: &mut Ring) {
    tick_until_answered_while_waiting(ring, INSTALL, &[SECOND]);
    assert_ok(ring.answer(INSTALL).expect("the first install answered"));
    assert!(
        ring.answer(SECOND).is_none() && ring.core.has_parked_install(),
        "the second marker parked under its own fence"
    );
    assert_ok(ring.tick_until_answered(SECOND));
    assert!(!ring.core.has_parked_install());
    assert_eq!(installed_rows(&ring.core), ROWS as usize);
    assert_eq!(second_rows_landed(&ring.core), ROWS as usize);
}

/// A second marker that touches nothing the parked install holds waits all
/// the same. A keyed write unrelated to both runs past it.
#[test]
fn a_disjoint_marker_waits_and_an_unrelated_write_passes_it() {
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    push_second(&mut ring, second_rows(), &[OTHER], |_| {});
    ring.push(
        on_third_vshard(UNRELATED, point_put(UNRELATED_COLLECTION, Vec::new())),
        None,
    );
    ring.tick();
    assert!(ring.core.has_parked_install(), "the first install parked");
    assert!(ring.answer(SECOND).is_none(), "the second marker waits");
    assert_ok(
        ring.answer(UNRELATED)
            .expect("an unrelated write runs past the waiting marker"),
    );
    second_parks_after_the_first(&mut ring);
}

/// The second marker writes an edge, and its declaration omits the graph
/// state. While it waits, nothing holds the graph state for it. Once it
/// runs, its walk files the report and arms the key. It keeps parking, and
/// leaves what an unyielded install of its ops leaves.
///
/// The recorder is process-wide: nextest runs this test in its own
/// process.
#[test]
fn an_undeclared_key_reports_when_the_marker_runs() {
    let reports = record_reports();
    let mut ops = vec![edge_put_op()];
    ops.extend(second_rows());
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    push_second(&mut ring, ops.clone(), &[OTHER, KNOWS], |keys| {
        let RedoStreamKeys::Declared(declared) = keys else {
            panic!("the walk declares its keys");
        };
        assert!(declared.graph, "the walk declares the edge's graph state");
        declared.graph = false;
    });
    ring.tick();
    assert!(ring.answer(SECOND).is_none(), "the second marker waits");
    assert_eq!(undeclared_reports(&reports), 0, "the marker has not run");

    second_parks_after_the_first(&mut ring);
    assert_eq!(undeclared_reports(&reports), 1);

    let collections = vec![OTHER.to_string(), KNOWS.to_string()];
    let mut whole = hold(plain_core);
    assert_ok(&install_whole(
        &mut whole.core,
        &record(ops),
        &[],
        &collections,
    ));
    assert_eq!(
        second_rows_landed(&ring.core),
        second_rows_landed(&whole.core)
    );
    assert!(
        edge_of(&whole.core).is_some(),
        "the unyielded install wrote the edge"
    );
    assert_eq!(edge_of(&ring.core), edge_of(&whole.core));
}

/// A marker whose keys passed the final entry's room declares none. It
/// waits as a core task, so an unrelated write behind it waits too.
#[test]
fn an_unbounded_marker_waits_as_a_core_task() {
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    push_second(&mut ring, second_rows(), &[OTHER], |keys| {
        *keys = RedoStreamKeys::Unbounded;
    });
    ring.push(
        on_third_vshard(UNRELATED, point_put(UNRELATED_COLLECTION, Vec::new())),
        None,
    );
    ring.tick();
    assert!(ring.core.has_parked_install(), "the first install parked");
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[SECOND, UNRELATED]);
    assert_ok(ring.tick_until_answered(SECOND));
    assert_ok(ring.tick_until_answered(UNRELATED));
    assert_eq!(second_rows_landed(&ring.core), ROWS as usize);
}
