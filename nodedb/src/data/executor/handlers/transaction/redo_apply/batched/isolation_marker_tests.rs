// SPDX-License-Identifier: BUSL-1.1

//! A stream marker keys what it declares for its content.
//!
//! The parked install writes [`FILL`] in the default database, with its
//! events on vShard 0. A second marker waits, and holds only the keys it
//! declares. A marker whose declaration misses a key its walk finds files
//! one report, arms the key, and keeps parking.

use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};
use nodedb_types::{StorageKey, Surrogate, TenantId};
use nodedb_wal::record::RecordType;

use crate::bridge::envelope::Request;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::VShardId;
use crate::wal::{RedoStreamKeys, RedoSubRecord};

use super::super::stream_validate::tests::{LSN, TID, hold, plain_core, record};
use super::super::test_commit::doc_put_sub_record;
use super::equivalence_tests::{FILL, FILL_FROM, fill, install_whole};
use super::install::job_stage::ParkPoint;
use super::isolation_tests::{OTHER, point_put};
use super::yield_tests::{
    INSTALL, MarkerSpec, OTHER_VSHARD, ROWS, Ring, assert_ok, install_request, installed_rows,
    request, tick_until_answered_while_waiting,
};

/// The second stream marker.
pub(super) const SECOND: u64 = 40;
/// A write of the second marker's collection, queued behind it.
const SAME_KEY: u64 = 41;
/// A write that touches nothing either marker holds.
pub(super) const UNRELATED: u64 = 42;
/// The vShard the writes behind the second marker carry.
const THIRD_VSHARD: VShardId = VShardId::new(2);
/// The vShard of a write behind no waiting write: a waiting write holds its
/// own vShard.
const FOURTH_VSHARD: VShardId = VShardId::new(3);
/// The first surrogate the second marker's rows take.
const SECOND_FROM: u32 = 40_000;
/// The collection the unrelated write stores to.
pub(super) const UNRELATED_COLLECTION: &str = "c";
/// The edge collection of the marker whose declaration misses its graph
/// state.
pub(super) const KNOWS: &str = "knows";

/// Document puts into [`OTHER`], one row each, from [`SECOND_FROM`].
pub(super) fn second_rows() -> Vec<RedoSubRecord> {
    (SECOND_FROM..SECOND_FROM + ROWS)
        .map(|surrogate| {
            let body = nodedb_types::json_to_msgpack(&serde_json::json!({"n": surrogate}))
                .expect("encode body");
            doc_put_sub_record(OTHER, &format!("s{surrogate}"), &body, surrogate)
        })
        .collect()
}

/// How many of the second marker's rows `core` holds.
pub(super) fn second_rows_landed(core: &CoreLoop) -> usize {
    (SECOND_FROM..SECOND_FROM + ROWS)
        .filter(|surrogate| {
            core.sparse
                .get(
                    0,
                    TID,
                    OTHER,
                    &StorageKey::for_surrogate(Surrogate::new(*surrogate)),
                )
                .expect("read row")
                .is_some()
        })
        .count()
}

pub(super) fn read_of(collection: &str) -> VersionedReadEntry {
    VersionedReadEntry {
        engine: EngineTag::Document,
        collection: collection.into(),
        key: ReadKeyIdent::Predicate,
        home_vshard: None,
    }
}

/// A request `id` of `plan` whose events carry [`THIRD_VSHARD`].
pub(super) fn on_third_vshard(
    id: u64,
    plan: nodedb_physical::physical_plan::PhysicalPlan,
) -> Request {
    let mut request = request(id, plan);
    request.vshard_id = THIRD_VSHARD;
    request
}

/// A second marker writes [`OTHER`] on another vShard, and its content
/// reads [`FILL`], which the parked install holds. Only the declared read
/// names the overlap. The marker waits. A write of its collection waits
/// behind it, and an unrelated write runs past it.
#[test]
fn an_overlapping_marker_waits_and_holds_only_its_keys() {
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    let plan = ring.marker_plan_of(
        &record(second_rows()),
        vec![OTHER.to_string()],
        MarkerSpec {
            reads: vec![read_of(FILL)],
            ..MarkerSpec::default()
        },
    );
    let mut second = install_request(SECOND, plan, LSN + 1);
    second.vshard_id = OTHER_VSHARD;
    ring.push(second, None);
    ring.push(
        on_third_vshard(SAME_KEY, point_put(OTHER, Vec::new())),
        None,
    );
    let mut unrelated = request(UNRELATED, point_put(UNRELATED_COLLECTION, Vec::new()));
    unrelated.vshard_id = FOURTH_VSHARD;
    ring.push(unrelated, None);
    ring.tick();
    assert!(ring.core.has_parked_install(), "the first install parked");
    assert!(
        ring.answer(SECOND).is_none(),
        "the marker waits on the collection its content reads"
    );
    assert!(
        ring.answer(SAME_KEY).is_none(),
        "a write of the marker's collection waits behind it"
    );
    assert_ok(
        ring.answer(UNRELATED)
            .expect("an unrelated write runs past the waiting marker"),
    );

    tick_until_answered_while_waiting(&mut ring, INSTALL, &[SECOND, SAME_KEY]);
    assert_ok(ring.answer(INSTALL).expect("the first install answered"));
    assert_ok(ring.tick_until_answered(SECOND));
    assert_ok(ring.tick_until_answered(SAME_KEY));
    assert_eq!(installed_rows(&ring.core), ROWS as usize);
    assert_eq!(second_rows_landed(&ring.core), ROWS as usize);
    assert!(!ring.core.has_parked_install());
}

/// An edge put of [`KNOWS`], then the filler rows, so the install parks.
fn edge_then_fill() -> Vec<RedoSubRecord> {
    let mut ops = vec![edge_put_op()];
    ops.extend(fill(FILL_FROM, ROWS));
    ops
}

/// One edge put of [`KNOWS`], which writes the tenant's graph state.
pub(super) fn edge_put_op() -> RedoSubRecord {
    RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload: zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
            collection: KNOWS.into(),
            src_id: "carol".into(),
            label: "KNOWS".into(),
            dst_id: "dave".into(),
            properties: Vec::new(),
            src_surrogate: 31_101,
            dst_surrogate: 31_102,
            system_from: Some(500),
            applied: None,
            valid_from_ms: None,
            valid_until_ms: None,
        })
        .expect("encode edge put"),
    }
}

/// The graph state of the test tenant.
fn graph_key() -> crate::data::executor::core_loop::install_fence::FenceKey {
    crate::data::executor::core_loop::install_fence::FenceKey::Graph {
        db: crate::types::DatabaseId::DEFAULT,
        tenant: TenantId::new(TID),
    }
}

/// The edge [`edge_put_op`] writes, as `core` holds it.
pub(super) fn edge_of(core: &CoreLoop) -> Option<Vec<u8>> {
    core.edge_store
        .get_edge(0, TenantId::new(TID), KNOWS, "carol", "KNOWS", "dave")
        .expect("read edge")
}

/// A marker whose declaration misses the graph state its edge writes. The
/// walk finds the key, files one report, and arms it. The install keeps
/// parking, and leaves what an unyielded install leaves.
///
/// The recorder is process-wide: nextest runs this test in its own
/// process.
#[test]
fn a_marker_that_misses_a_walked_key_reports_and_keeps_parking() {
    let reports = record_reports();
    let collections = vec![FILL.to_string(), KNOWS.to_string()];
    let mut whole = hold(plain_core);
    assert_ok(&install_whole(
        &mut whole.core,
        &record(edge_then_fill()),
        &[],
        &collections,
    ));

    let mut ring = Ring::open(plain_core);
    let plan = ring.marker_plan_declaring(
        &record(edge_then_fill()),
        collections,
        MarkerSpec::default(),
        |keys| {
            let RedoStreamKeys::Declared(declared) = keys else {
                panic!("the walk declares its keys");
            };
            assert!(declared.graph, "the walk declares the edge's graph state");
            declared.graph = false;
        },
    );
    ring.push(install_request(INSTALL, plan, LSN), None);
    ring.tick();
    assert!(ring.core.has_parked_install(), "the install parked");
    assert!(
        ring.core
            .install_fence
            .as_ref()
            .is_some_and(|fence| fence.keys.contains(&graph_key())),
        "the slice that walked the edge armed its graph state"
    );
    assert_ok(ring.tick_until_answered(INSTALL));
    assert!(!ring.core.has_install_fence(), "the fence released");
    assert_eq!(installed_rows(&ring.core), installed_rows(&whole.core));
    assert_eq!(installed_rows(&ring.core), ROWS as usize);
    assert!(edge_of(&ring.core).is_some(), "the edge installed");
    assert_eq!(edge_of(&ring.core), edge_of(&whole.core));
    assert_eq!(
        undeclared_reports(&reports),
        1,
        "one report for the undeclared key"
    );
}

/// Start the process-wide recorder, filing its reports in the returned
/// directory. A test that calls it runs in its own process under nextest.
pub(super) fn record_reports() -> tempfile::TempDir {
    let reports = tempfile::tempdir().expect("reports dir");
    assert!(
        faultbox::init(
            faultbox::Config::new("nodedb-test", "0.0.0", reports.path()).install_panic_hook(false),
        ),
        "this test must be the first initializer of the recorder"
    );
    reports
}

/// How many undeclared-key reports `reports` holds.
pub(super) fn undeclared_reports(reports: &tempfile::TempDir) -> u64 {
    faultbox::reader::list(reports.path())
        .expect("list reports")
        .iter()
        .filter(|group| group.first.domain_kind.as_deref() == Some("nodedb.stream_key_undeclared"))
        .map(faultbox::reader::Group::occurrences)
        .sum()
}

/// A snapshot marker whose walk keys a checked read and the graph state of
/// an edge. The proposer's walk declared both, so the job files no report
/// and parks through its validate and into its install batches.
#[test]
fn a_walk_of_reads_and_graph_state_finds_only_declared_keys() {
    let reports = record_reports();
    let ledger_read = VersionedReadEntry {
        home_vshard: Some(0),
        ..read_of("ledger")
    };
    let mut ring = Ring::open(plain_core);
    let plan = ring.marker_plan_of(
        &record(edge_then_fill()),
        vec![FILL.to_string(), KNOWS.to_string()],
        MarkerSpec {
            reads: vec![ledger_read; 200],
            snapshot: Some(i64::MAX / 2),
            ..MarkerSpec::default()
        },
    );
    ring.push(install_request(INSTALL, plan, LSN), None);
    let mut reached_install = false;
    for _ in 0..10_000 {
        ring.tick();
        if ring.answer(INSTALL).is_some() {
            break;
        }
        if matches!(ring.core.parked_point(), Some(ParkPoint::Batch { .. })) {
            reached_install = true;
        }
    }
    assert!(
        reached_install,
        "the job parked between its install batches"
    );
    assert_ok(ring.answer(INSTALL).expect("the install answered"));
    assert_eq!(installed_rows(&ring.core), ROWS as usize);
    assert!(edge_of(&ring.core).is_some(), "the edge installed");
    assert_eq!(
        undeclared_reports(&reports),
        0,
        "the walk met no undeclared key"
    );
}

/// A marker that declares every key its walk finds files no report, and
/// still parks between its batches.
#[test]
fn a_marker_that_declares_every_walked_key_parks() {
    let mut ring = Ring::open(plain_core);
    let plan = ring.marker_plan_of(
        &record(edge_then_fill()),
        vec![FILL.to_string(), KNOWS.to_string()],
        MarkerSpec::default(),
    );
    ring.push(install_request(INSTALL, plan, LSN), None);
    ring.tick();
    assert!(ring.core.has_parked_install(), "the install parked");
    assert!(
        ring.core
            .install_fence
            .as_ref()
            .is_some_and(|fence| fence.keys.contains(&graph_key())),
        "the fence holds the declared graph state"
    );
    assert_ok(ring.tick_until_answered(INSTALL));
    assert!(edge_of(&ring.core).is_some(), "the edge installed");
}
