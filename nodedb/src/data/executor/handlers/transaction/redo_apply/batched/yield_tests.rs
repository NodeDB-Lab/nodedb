// SPDX-License-Identifier: BUSL-1.1

//! A stream install that `run_task` runs gives the core back between the
//! batches its validate walks, between the batches it installs, and
//! between the batches its rollback reverses.
//!
//! Each case queues requests on a core's request ring and drives `tick()`.
//! The core cuts a stream into batches of [`YIELD_BATCH_BYTES`], and each
//! marker's content spans several of them.

use std::collections::HashMap;

use nodedb_bridge::buffer::{Consumer, Producer};
use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinReplySpec, DocumentOp, MetaOp, PhysicalPlan, RedoOrigin, RedoSumTargets,
};
use nodedb_types::{QualifiedCollection, StorageKey, Surrogate};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse, JournalGroup, WriteSetJournal};
use crate::bridge::envelope::{ErrorCode, Request, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::core_loop::tests::make_default_task;
use crate::engine::sparse::btree::SparseEngine;
use crate::types::{DatabaseId, Lsn, RequestId, TenantId, VShardId};
#[cfg(feature = "failpoints")]
use crate::wal::redo::install_cursor::{CursorState, InstallCursor};
use crate::wal::{RedoRecord, RedoSubRecord, WriteSetCapture};

use super::super::calvin_fold_tests::{
    ACCOUNT, SOURCE, TARGET, balance, core_with_sum, sum_targets,
};
use super::super::install_refusal_tests as refusal;
use super::super::replay_fold::tests::crash_image;
use super::super::stream_validate::tests::{LSN, StreamWal, TID, framed, plain_core, record};
use super::super::test_commit::doc_put_sub_record;
use super::equivalence_tests::{FILL, FILL_EACH, FILL_FROM, digest, fill, install_whole, spread};
use super::install::job_stage::ParkPoint;

/// The bytes of one batch the core cuts a stream into.
const YIELD_BATCH_BYTES: usize = 4096;
/// Filler rows a record writes: several batches of them.
const ROWS: u32 = 300;
/// Filler rows a record writes when a case needs many validate slices.
const MANY_ROWS: u32 = 1200;
/// The database of the foreign writes, which the fence never holds.
const OTHER_DB: DatabaseId = DatabaseId::new(7);
const OTHER: &str = "other";
const OTHER_SURROGATE: u32 = 77;
/// The vShard the foreign writes' events carry. The install's carry 0.
const OTHER_VSHARD: VShardId = VShardId::new(1);

const INSTALL: u64 = 1;
const READ_B: u64 = 2;
const READ_A: u64 = 3;
const FOREIGN: u64 = 4;
const FENCED_WRITE: u64 = 7;

/// How long a test request may wait behind the install's fence.
const WAITER_DEADLINE: std::time::Duration = std::time::Duration::from_secs(600);

/// The ticks a case drives at most before it gives up on an answer.
const MAX_TICKS: usize = 10_000;

type Ends = (Producer<BridgeRequest>, Consumer<BridgeResponse>);

/// A core driven through its request ring, with its WAL and the answers
/// it sent.
struct Ring {
    core: CoreLoop,
    wal: StreamWal,
    requests: Producer<BridgeRequest>,
    responses: Consumer<BridgeResponse>,
    answers: HashMap<u64, Response>,
    dir: tempfile::TempDir,
}

impl Ring {
    fn open(setup: impl FnOnce(&std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>)) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, ends) = setup(dir.path());
        let (requests, responses) = *ends.downcast::<Ends>().expect("ring ends");
        core.redo_apply.stream_batch_bytes = YIELD_BATCH_BYTES;
        let wal = StreamWal::open(&core);
        Self {
            core,
            wal,
            requests,
            responses,
            answers: HashMap::new(),
            dir,
        }
    }

    /// Queue `request`, journalled into `group` when it names one.
    fn push(&mut self, request: Request, group: Option<JournalGroup>) {
        self.requests
            .try_push(BridgeRequest {
                inner: request,
                outcome_floor: Lsn::ZERO,
                journal: WriteSetJournal {
                    group,
                    settled: Vec::new(),
                },
            })
            .expect("queue request");
    }

    /// One tick, and every answer it sent.
    fn tick(&mut self) {
        self.core.tick();
        while let Ok(response) = self.responses.try_pop() {
            let id = response.inner.request_id.as_u64();
            assert!(
                self.answers.insert(id, response.inner).is_none(),
                "request {id} answered twice"
            );
        }
    }

    fn answer(&self, id: u64) -> Option<&Response> {
        self.answers.get(&id)
    }

    /// Tick until request `id` answered.
    fn tick_until_answered(&mut self, id: u64) -> &Response {
        for _ in 0..MAX_TICKS {
            if self.answers.contains_key(&id) {
                break;
            }
            self.tick();
        }
        self.answer(id)
            .unwrap_or_else(|| panic!("request {id} never answered"))
    }

    /// The install of `redo` as a stream marker in this core's WAL.
    fn marker_plan(
        &mut self,
        redo: &RedoRecord,
        collections: Vec<String>,
        sum_targets: Vec<RedoSumTargets>,
        calvin: Option<CalvinInstall>,
    ) -> PhysicalPlan {
        let reference = self.wal.write(&framed(redo, &sum_targets, &[]));
        let marker = reference.marker(None, None, Vec::new()).expect("marker");
        PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo: marker.to_bytes().expect("encode marker"),
            collections,
            sum_targets,
            origin: RedoOrigin::Commit,
            calvin,
            reads: Vec::new(),
            snapshot_ordinal: None,
            pins: Default::default(),
        })
    }

    /// The install of `redo` as a stream marker whose plan and content name
    /// what `spec` names.
    fn marker_plan_of(
        &mut self,
        redo: &RedoRecord,
        collections: Vec<String>,
        spec: MarkerSpec,
    ) -> PhysicalPlan {
        let reference = self
            .wal
            .write(&framed(redo, &spec.header_targets, &spec.reads));
        let marker = reference.marker(None, None, Vec::new()).expect("marker");
        PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo: marker.to_bytes().expect("encode marker"),
            collections,
            sum_targets: spec.plan_targets,
            origin: RedoOrigin::Commit,
            calvin: None,
            reads: Vec::new(),
            snapshot_ordinal: spec.snapshot,
            pins: Default::default(),
        })
    }

    /// Queue the install of `ROWS` filler rows as request [`INSTALL`].
    fn push_fill_install(&mut self) {
        self.push_install_of(fill(FILL_FROM, ROWS));
    }

    /// Queue the install of `ops`, which write [`FILL`], as request
    /// [`INSTALL`].
    fn push_install_of(&mut self, ops: Vec<RedoSubRecord>) {
        let plan = self.marker_plan(&record(ops), vec![FILL.to_string()], Vec::new(), None);
        self.push(install_request(INSTALL, plan, LSN), None);
    }

    /// Whether the parked install is in its validate.
    fn validating(&self) -> bool {
        matches!(self.core.parked_point(), Some(ParkPoint::Validating { .. }))
    }

    /// Tick until the parked install's validate ended. Returns how many
    /// ticks it stayed in its validate.
    fn tick_past_validate(&mut self) -> usize {
        for ticks in 0..MAX_TICKS {
            if !self.validating() {
                return ticks;
            }
            self.tick();
        }
        panic!("the validate never ended");
    }
}

/// What a marker's plan and content name beside its ops.
#[derive(Default)]
struct MarkerSpec {
    /// The sum targets the plan names, which the fence keys before the
    /// validate.
    plan_targets: Vec<RedoSumTargets>,
    /// The sum targets the content header names.
    header_targets: Vec<RedoSumTargets>,
    /// The read set the content holds.
    reads: Vec<nodedb_types::calvin::VersionedReadEntry>,
    /// The snapshot the read set validates at.
    snapshot: Option<i64>,
}

/// A request of `plan`. A waiter behind the fence answers that it expired
/// once its deadline passes, so the deadline outlasts any install a loaded
/// test machine runs.
fn request(id: u64, plan: PhysicalPlan) -> Request {
    let mut request = make_default_task().request;
    request.request_id = RequestId::new(id);
    request.tenant_id = TenantId::new(TID);
    request.plan = plan;
    // no-determinism: a test request's deadline; no Calvin state reads it.
    request.deadline = std::time::Instant::now() + WAITER_DEADLINE;
    request
}

fn install_request(id: u64, plan: PhysicalPlan, lsn: u64) -> Request {
    let mut request = request(id, plan);
    request.wal_lsn = Some(Lsn::new(lsn));
    request
}

fn point_get(collection: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointGet {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        document_id: "x".into(),
        surrogate: None,
        pk_bytes: Vec::new(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    })
}

fn scan(collection: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::Scan {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        limit: 10_000,
        offset: 0,
        sort_keys: Vec::new(),
        filters: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
    })
}

/// A put of one row into a collection of [`OTHER_DB`].
fn foreign_put(id: u64) -> Request {
    let mut request = request(
        id,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(OTHER_DB, OTHER),
            document_id: "o1".into(),
            value: crate::data::executor::doc_format::encode_to_msgpack(
                &serde_json::json!({"value": id}),
            ),
            surrogate: Surrogate::new(OTHER_SURROGATE),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }),
    );
    request.database_id = OTHER_DB;
    // Its events carry another vShard than the install's.
    request.vshard_id = OTHER_VSHARD;
    request
}

/// A put of one row into [`FILL`], which the install fences. Its events
/// carry another vShard than the install's, so only the collection's fence
/// holds it.
fn fenced_put(id: u64) -> Request {
    let mut request = request(
        id,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, FILL),
            document_id: "w1".into(),
            value: crate::data::executor::doc_format::encode_to_msgpack(
                &serde_json::json!({"n": id}),
            ),
            surrogate: Surrogate::new(FENCED_SURROGATE),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }),
    );
    request.vshard_id = OTHER_VSHARD;
    request
}

/// The surrogate of the row [`fenced_put`] writes: no filler row's.
const FENCED_SURROGATE: u32 = FILL_FROM + MANY_ROWS + 1;

/// The stored name of the foreign collection.
fn other_stored() -> String {
    QualifiedCollection::new(OTHER_DB, OTHER)
        .as_str()
        .to_owned()
}

/// Whether `sparse` holds the foreign row.
fn holds_foreign(sparse: &SparseEngine) -> bool {
    sparse
        .get(
            OTHER_DB.as_u64(),
            TID,
            &other_stored(),
            &StorageKey::for_surrogate(Surrogate::new(OTHER_SURROGATE)),
        )
        .expect("read foreign row")
        .is_some()
}

/// The filler rows `core` holds.
fn installed_rows(core: &CoreLoop) -> usize {
    (FILL_FROM..FILL_FROM + ROWS)
        .filter(|surrogate| {
            core.sparse
                .get(
                    0,
                    TID,
                    FILL,
                    &StorageKey::for_surrogate(Surrogate::new(*surrogate)),
                )
                .expect("read row")
                .is_some()
        })
        .count()
}

/// The rows a scan answered.
fn scanned_rows(response: &Response) -> usize {
    let payload = response.payload.to_vec();
    nodedb_query::msgpack_scan::array_header(&payload, 0)
        .expect("a scan answers an array")
        .0
}

fn assert_ok(response: &Response) {
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
}

/// The source rows of a sum record: `amount` 1 each into the account.
fn charges(count: u32) -> Vec<RedoSubRecord> {
    (0..count)
        .map(|index| {
            let body = crate::data::executor::doc_format::encode_to_msgpack(
                &serde_json::json!({"account_id": ACCOUNT, "amount": 1}),
            );
            doc_put_sub_record(SOURCE, &format!("c{index}"), &body, 100 + index)
        })
        .collect()
}

fn group(origin: u64, collection: &str) -> JournalGroup {
    JournalGroup {
        origin: Lsn::new(origin),
        collection: collection.into(),
        apply_key: 0,
        commit_hlc: None,
        change_position: None,
    }
}

/// A read of an untouched collection and a write of another database run
/// between the install's batches. A read of the installing collection waits
/// and answers every row once the install ends.
#[test]
fn another_task_runs_between_batches_and_sees_no_partial_state() {
    let mut ring = Ring::open(plain_core);
    let (mut producers, mut consumers) = crate::event::bus::create_event_bus_with_capacity(1, 1024);
    ring.core
        .set_event_producer(producers.pop().expect("producer"));
    let mut events = consumers.pop().expect("consumer");
    ring.push_fill_install();
    ring.push(request(READ_B, point_get("b")), None);
    ring.push(request(READ_A, scan(FILL)), None);
    ring.push(foreign_put(FOREIGN), None);

    ring.tick();
    assert!(ring.core.has_parked_install(), "the install parked");
    assert!(ring.answer(INSTALL).is_none());
    assert!(ring.answer(READ_B).is_some(), "the untouched read answered");
    assert!(ring.answer(READ_A).is_none(), "the fenced read waits");
    assert_ok(ring.answer(FOREIGN).expect("the foreign write answered"));
    ring.tick_past_validate();
    assert!(ring.core.has_parked_install(), "the install parked");
    assert!(ring.answer(READ_A).is_none(), "the fenced read waits");
    let installed = installed_rows(&ring.core);
    assert!(
        installed > 0 && installed < ROWS as usize,
        "the install holds part of its rows: {installed}"
    );
    assert!(
        !ring.core.sparse.db().is_deferred(),
        "no store defers between batches"
    );
    let mut foreign_events = 0;
    while let Some(event) = events.try_recv() {
        if event.collection.contains(OTHER) {
            foreign_events += 1;
        }
    }
    assert!(
        foreign_events > 0,
        "the foreign write's event left while parked"
    );
    let image = crash_image(ring.dir.path());
    let sparse =
        SparseEngine::open(&image.path().join("sparse/core-0.redb")).expect("open crash image");
    assert!(holds_foreign(&sparse), "the foreign write is durable");
    drop(sparse);

    assert_ok(ring.tick_until_answered(INSTALL));
    let read = ring.tick_until_answered(READ_A).clone();
    assert_ok(&read);
    assert_eq!(
        scanned_rows(&read),
        ROWS as usize,
        "the read sees every row"
    );
    assert!(!ring.core.has_install_fence());
}

/// A journalled write between the batches journals into its own window.
/// Both write sets are stored, and the fold lands once.
#[test]
fn a_journalled_write_between_batches_keeps_both_captures() {
    const SOURCES: u32 = 200;
    let mut ring = Ring::open(core_with_sum);
    let plan = ring.marker_plan(
        &record(charges(SOURCES)),
        vec![SOURCE.to_string()],
        sum_targets(),
        None,
    );
    ring.push(
        install_request(INSTALL, plan, LSN),
        Some(group(LSN, SOURCE)),
    );
    let mut foreign = foreign_put(FOREIGN);
    foreign.wal_lsn = Some(Lsn::new(LSN + 1));
    ring.push(foreign, Some(group(LSN + 1, &other_stored())));

    ring.tick();
    assert!(ring.core.has_parked_install());
    assert_ok(ring.answer(FOREIGN).expect("the journalled write answered"));
    assert_ok(ring.tick_until_answered(INSTALL));

    let mut origins: Vec<u64> = ring
        .core
        .sparse
        .load_write_set_captures()
        .expect("load captures")
        .into_iter()
        .map(|(_, bytes)| WriteSetCapture::from_bytes(&bytes).expect("decode").origin)
        .collect();
    origins.sort_unstable();
    assert_eq!(
        origins,
        vec![LSN, LSN + 1],
        "each task stored its own write set"
    );
    assert_eq!(
        balance(&ring.core),
        Some((100 + SOURCES).to_string()),
        "one fold of every row"
    );
    assert!(holds_foreign(&ring.core.sparse));
}

/// A parked Calvin slice answers its reply once the install ends, and its
/// fixed-ordinal KV scope closes.
#[test]
fn a_parked_calvin_install_answers_its_reply() {
    let mut ring = Ring::open(plain_core);
    let reply = vec![0xc1, 0x05];
    let plan = ring.marker_plan(
        &record(fill(FILL_FROM, ROWS)),
        vec![FILL.to_string()],
        Vec::new(),
        Some(CalvinInstall {
            epoch: 3,
            position: 0,
            epoch_system_ms: 0,
            reply: CalvinReplySpec::Count(reply.clone()),
            user_write: true,
            mirror: false,
        }),
    );
    ring.push(install_request(INSTALL, plan, LSN), None);

    ring.tick();
    assert!(ring.core.has_parked_install(), "the slice parked");
    assert_eq!(
        ring.core.kv_engine.current_write_scope(),
        crate::engine::kv::engine::KvWriteScope::Closed,
        "the slice's scope is out of the core while parked"
    );
    let answer = ring.tick_until_answered(INSTALL).clone();
    assert_ok(&answer);
    assert_eq!(answer.payload.to_vec(), reply, "the slice's reply answers");
    assert_eq!(
        ring.core.kv_engine.current_write_scope(),
        crate::engine::kv::engine::KvWriteScope::Closed
    );
    assert_eq!(installed_rows(&ring.core), ROWS as usize);
}

/// The install parked across its batches leaves the state the whole-record
/// install leaves.
#[test]
fn a_parked_install_matches_an_unyielded_one() {
    let (redo, collections) = kv_and_fill_record();

    let mut whole = super::super::stream_validate::tests::hold(plain_core);
    refusal::seed_kv(&mut whole.core);
    assert_ok(&install_whole(&mut whole.core, &redo, &[], &collections));

    let mut ring = Ring::open(plain_core);
    refusal::seed_kv(&mut ring.core);
    let plan = ring.marker_plan(&redo, collections, Vec::new(), None);
    ring.push(install_request(INSTALL, plan, LSN), None);
    ring.tick();
    assert!(ring.core.has_parked_install(), "the install parked");
    assert_ok(ring.tick_until_answered(INSTALL));

    assert_eq!(digest(&ring.core), digest(&whole.core));
    let rows = |core: &CoreLoop| {
        (FILL_FROM..FILL_FROM + 2 * FILL_EACH + ROWS)
            .filter(|surrogate| {
                core.sparse
                    .get(
                        0,
                        TID,
                        FILL,
                        &StorageKey::for_surrogate(Surrogate::new(*surrogate)),
                    )
                    .expect("read row")
                    .is_some()
            })
            .count()
    };
    assert_eq!(rows(&ring.core), rows(&whole.core));
}

/// A panic at `point`, outside the install's arms, stops the core. The
/// install's task answers once, and no fence or parked job stays.
#[cfg(feature = "failpoints")]
fn assert_a_panic_at_stops_and_answers(point: &str) {
    use nodedb_types::fail_point::{FailAction, FailGuard};

    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    ring.push(request(READ_A, scan(FILL)), None);
    let _panic = FailGuard::install(point, FailAction::Panic);
    let answer = ring.tick_until_answered(INSTALL).clone();
    assert_eq!(answer.status, Status::Error, "{point}");
    assert!(
        matches!(
            answer.error_code.as_deref(),
            Some(ErrorCode::Internal { detail }) if detail.contains("panicked")
        ),
        "{point}: {:?}",
        answer.error_code
    );
    assert!(ring.core.is_fail_stopped(), "{point}");
    assert!(!ring.core.has_parked_install(), "{point}");
    assert!(!ring.core.has_install_fence(), "{point}");
    assert!(ring.core.redo_apply.parking.is_none(), "{point}");
    let read = ring.tick_until_answered(READ_A);
    assert!(
        matches!(
            read.error_code.as_deref(),
            Some(ErrorCode::CoreFailStopped { .. })
        ),
        "{point}: the waiter is refused: {:?}",
        read.error_code
    );
    for _ in 0..8 {
        ring.tick();
    }
}

/// The first slice's validate, under the armed fence.
#[cfg(feature = "failpoints")]
#[test]
fn a_panic_in_the_first_slice_validate_stops_and_answers() {
    assert_a_panic_at_stops_and_answers("redo_apply::batched::before_validate");
}

/// The first park, inside the task's run, after the validate's first
/// batch.
#[cfg(feature = "failpoints")]
#[test]
fn a_panic_at_the_first_park_stops_and_answers() {
    assert_a_panic_at_stops_and_answers("redo_apply::batched::validate_parked::0");
}

/// The park after a driven validate slice.
#[cfg(feature = "failpoints")]
#[test]
fn a_panic_at_a_later_validate_park_stops_and_answers() {
    assert_a_panic_at_stops_and_answers("redo_apply::batched::validate_parked::1");
}

/// The first park of the arms, after the slice that ended the validate.
#[cfg(feature = "failpoints")]
#[test]
fn a_panic_at_the_first_arm_park_stops_and_answers() {
    assert_a_panic_at_stops_and_answers("redo_apply::batched::parked::Document::0");
}

/// The return of the install's state to the core.
#[cfg(feature = "failpoints")]
#[test]
fn a_panic_at_enter_stops_and_answers() {
    assert_a_panic_at_stops_and_answers("redo_apply::batched::before_enter");
}

/// A batch's durable commit inside a driven slice.
#[cfg(feature = "failpoints")]
#[test]
fn a_panic_in_a_slice_stops_and_answers() {
    assert_a_panic_at_stops_and_answers("redo_apply::batched::after_batch_durable::Document::2");
}

/// The park after a driven slice.
#[cfg(feature = "failpoints")]
#[test]
fn a_panic_at_a_later_park_stops_and_answers() {
    assert_a_panic_at_stops_and_answers("redo_apply::batched::parked::Document::1");
}

/// The task's finish once the install answered.
#[cfg(feature = "failpoints")]
#[test]
fn a_panic_in_the_completion_stops_and_answers() {
    assert_a_panic_at_stops_and_answers(&format!("core::after_apply::{FILL}"));
}

/// A core that stops while the install is parked answers its task and its
/// waiters. The install's cursor stays for the next boot.
#[test]
fn a_fail_stop_while_parked_answers_and_keeps_the_cursor() {
    let mut ring = Ring::open(plain_core);
    let (mut producers, mut consumers) = crate::event::bus::create_event_bus_with_capacity(1, 1024);
    ring.core
        .set_event_producer(producers.pop().expect("producer"));
    let mut events = consumers.pop().expect("consumer");
    ring.push_fill_install();
    ring.push(request(READ_A, scan(FILL)), None);
    ring.tick();
    ring.tick_past_validate();
    assert!(ring.core.has_parked_install());
    assert!(ring.answer(READ_A).is_none(), "the read waits");

    ring.core
        .fail_stop_core(FailStopCause::RollbackFailed, "a test stops the core");
    let answer = ring.tick_until_answered(INSTALL).clone();
    assert!(
        matches!(
            answer.error_code.as_deref(),
            Some(ErrorCode::Internal { detail }) if detail.contains("stopped part way")
        ),
        "{:?}",
        answer.error_code
    );
    let read = ring.tick_until_answered(READ_A);
    assert!(
        matches!(
            read.error_code.as_deref(),
            Some(ErrorCode::CoreFailStopped { .. })
        ),
        "{:?}",
        read.error_code
    );
    assert!(!ring.core.has_parked_install());
    assert_eq!(
        ring.core
            .sparse
            .load_install_cursors()
            .expect("load cursors")
            .len(),
        1,
        "the cursor stays for boot"
    );
    // The install's events never leave, so no heartbeat passes its LSN.
    ring.core.advance_watermark(Lsn::new(LSN + 100));
    ring.core.emit_heartbeat();
    while let Some(event) = events.try_recv() {
        assert!(
            event.op != crate::event::WriteOp::Heartbeat,
            "a stopped core publishes no heartbeat: {:?}",
            event.lsn
        );
    }
}

/// While the install is parked, a heartbeat publishes below its LSN. Once
/// it ends, the heartbeat publishes the watermark.
#[test]
fn the_heartbeat_stays_below_the_parked_lsn() {
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    ring.tick();
    assert!(ring.core.has_parked_install());
    ring.core.advance_watermark(Lsn::new(LSN + 100));
    assert_eq!(ring.core.heartbeat_lsn(), Lsn::new(LSN - 1));

    assert_ok(ring.tick_until_answered(INSTALL));
    assert_eq!(ring.core.heartbeat_lsn(), ring.core.watermark);
    assert!(ring.core.watermark.as_u64() >= LSN + 100);
}

/// Maintenance that comes due while the install is parked runs once the
/// install ends.
#[test]
fn maintenance_owed_while_parked_runs_after_the_install() {
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    ring.tick();
    assert!(ring.core.has_parked_install());
    assert!(
        !ring.core.maybe_run_maintenance(),
        "a parked install skips it"
    );
    assert!(ring.core.maintenance.last_maintenance.is_none());

    assert_ok(ring.tick_until_answered(INSTALL));
    assert!(
        ring.core.maintenance.last_maintenance.is_some(),
        "the release ran the owed pass"
    );
}

/// The governor holds the RAM a parked job keeps between slices, and gets
/// it back when the install ends.
#[test]
fn the_governor_holds_a_parked_jobs_ram() {
    let mut ring = Ring::open(plain_core);
    let query = |core: &CoreLoop| {
        core.governor
            .budget(nodedb_mem::EngineId::Query)
            .allocated()
    };
    let before = query(&ring.core);
    ring.push_fill_install();
    ring.tick();
    ring.tick_past_validate();
    assert!(ring.core.has_parked_install());
    assert!(query(&ring.core) > before, "the parked job is charged");

    assert_ok(ring.tick_until_answered(INSTALL));
    assert_eq!(query(&ring.core), before, "the charge is released");
}

/// A write of another database whose events carry the install's vShard
/// waits while the install is parked, and runs once it ends.
#[test]
fn a_write_of_the_installs_vshard_waits_while_parked() {
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    let mut same = foreign_put(FOREIGN);
    same.vshard_id = VShardId::new(0);
    ring.push(same, None);

    ring.tick();
    assert!(ring.core.has_parked_install());
    assert!(
        ring.answer(FOREIGN).is_none(),
        "the write of the install's vShard waits"
    );
    assert_ok(ring.tick_until_answered(INSTALL));
    assert_ok(ring.tick_until_answered(FOREIGN));
}

/// A sampler that reads across the core waits for the install and holds no
/// later task: a write of another database still runs between batches.
#[test]
fn a_waiting_sampler_holds_no_later_write() {
    const SAMPLER: u64 = 5;
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    ring.push(
        request(
            SAMPLER,
            PhysicalPlan::Meta(MetaOp::KvDueExpiries {
                limit: 16,
                skip_databases: Vec::new(),
            }),
        ),
        None,
    );
    ring.push(foreign_put(FOREIGN), None);

    ring.tick();
    assert!(ring.core.has_parked_install());
    assert!(ring.answer(SAMPLER).is_none(), "the sampler waits");
    assert_ok(
        ring.answer(FOREIGN)
            .expect("the write runs past the sampler"),
    );
    assert_ok(ring.tick_until_answered(INSTALL));
    assert_ok(ring.tick_until_answered(SAMPLER));
}

/// A checkpoint that arrives while the install is parked waits for the
/// install and holds no later write. It runs once the install ends.
#[test]
fn a_checkpoint_while_parked_holds_no_later_write() {
    const CHECKPOINT: u64 = 6;
    let mut ring = Ring::open(plain_core);
    ring.push_fill_install();
    ring.push(
        request(CHECKPOINT, PhysicalPlan::Meta(MetaOp::Checkpoint)),
        None,
    );
    ring.push(foreign_put(FOREIGN), None);

    ring.tick();
    assert!(ring.core.has_parked_install());
    assert!(ring.answer(CHECKPOINT).is_none(), "the checkpoint waits");
    assert_ok(
        ring.answer(FOREIGN)
            .expect("the write runs past the checkpoint"),
    );
    assert_ok(ring.tick_until_answered(INSTALL));
    // The checkpoint answers once the release puts it back in the queue.
    ring.tick_until_answered(CHECKPOINT);
}

/// The fail point that fails the document arm's pass once it wrote every
/// batch: the install rolls back.
#[cfg(feature = "failpoints")]
const ARM_FAILS: &str = "redo_apply::batched::install_batch_fails::Document";

/// The state of the install cursor `core` holds, `None` when it holds none.
#[cfg(feature = "failpoints")]
fn cursor_state(core: &CoreLoop) -> Option<CursorState> {
    core.sparse
        .load_install_cursors()
        .expect("load cursors")
        .into_iter()
        .next()
        .map(|(lsn, bytes)| {
            InstallCursor::from_bytes(lsn, &bytes)
                .expect("decode cursor")
                .state
        })
}

/// Whether `core` holds the rollback of its install parked.
#[cfg(feature = "failpoints")]
fn rollback_parked(core: &CoreLoop) -> bool {
    core.has_parked_install() && matches!(cursor_state(core), Some(CursorState::RollingBack { .. }))
}

/// Whether `response` reports an install that rolled back.
#[cfg(feature = "failpoints")]
fn rolled_back(response: &Response) -> bool {
    matches!(
        response.error_code.as_deref(),
        Some(ErrorCode::RetryableRefusal { .. })
    )
}

/// The install fails once it wrote every batch, and its rollback parks
/// after each batch it reverses. A read of the installing collection waits
/// through every slice, forward and back, and answers what it answered
/// before the install. The state ends as it stood before the install.
#[cfg(feature = "failpoints")]
#[test]
fn a_rollback_yields_and_a_fenced_read_sees_none_of_the_install() {
    use nodedb_types::fail_point::FailGuard;

    let mut ring = Ring::open(plain_core);
    let before = digest(&ring.core);
    let _failing = FailGuard::fail(ARM_FAILS, "a late batch fails");
    // The read before the install answers what the collection held.
    ring.push(request(READ_B, scan(FILL)), None);
    ring.push_fill_install();
    ring.push(request(READ_A, scan(FILL)), None);

    let mut rollback_parks = 0_usize;
    for _ in 0..MAX_TICKS {
        ring.tick();
        if ring.answer(INSTALL).is_some() {
            break;
        }
        assert!(
            ring.answer(READ_A).is_none(),
            "the fenced read waits while the install runs or rolls back"
        );
        if rollback_parked(&ring.core) {
            rollback_parks += 1;
        }
    }
    assert!(
        rollback_parks > 1,
        "the rollback gave the core back between its batches: {rollback_parks} parks"
    );
    let answer = ring.answer(INSTALL).expect("the install answered");
    assert!(rolled_back(answer), "{:?}", answer.error_code);
    let baseline = ring
        .answer(READ_B)
        .expect("the read before answered")
        .clone();
    let read = ring.tick_until_answered(READ_A).clone();
    assert_eq!(read.status, baseline.status, "{:?}", read.error_code);
    assert_eq!(
        read.payload.to_vec(),
        baseline.payload.to_vec(),
        "the read sees none of the install"
    );
    assert_eq!(installed_rows(&ring.core), 0);
    assert_eq!(
        digest(&ring.core),
        before,
        "the state is the pre-install state"
    );
    assert!(
        cursor_state(&ring.core).is_none(),
        "the rollback end removed the cursor"
    );
    assert!(!ring.core.has_install_fence());
    assert!(!ring.core.is_fail_stopped());
}

/// A write of another database queued while the rollback is parked runs
/// before the rollback's next slice, and the rollback then ends.
#[cfg(feature = "failpoints")]
#[test]
fn an_unfenced_task_runs_between_rollback_slices() {
    use nodedb_types::fail_point::FailGuard;

    let mut ring = Ring::open(plain_core);
    let _failing = FailGuard::fail(ARM_FAILS, "a late batch fails");
    ring.push_fill_install();
    for _ in 0..MAX_TICKS {
        if rollback_parked(&ring.core) {
            break;
        }
        assert!(ring.answer(INSTALL).is_none(), "the install rolls back");
        ring.tick();
    }
    let Some(CursorState::RollingBack { batch: kept, .. }) = cursor_state(&ring.core) else {
        panic!("the rollback never parked");
    };
    assert!(kept > 0, "batches before batch {kept} are left to reverse");

    ring.push(foreign_put(FOREIGN), None);
    ring.tick();
    assert_ok(
        ring.answer(FOREIGN)
            .expect("the foreign write ran between rollback slices"),
    );
    assert!(
        rollback_parked(&ring.core),
        "the rollback is still parked: the write ran between its slices"
    );
    assert!(ring.answer(INSTALL).is_none());

    let answer = ring.tick_until_answered(INSTALL).clone();
    assert!(rolled_back(&answer), "{:?}", answer.error_code);
    assert!(holds_foreign(&ring.core.sparse), "the foreign write stays");
    assert_eq!(installed_rows(&ring.core), 0);
    assert!(cursor_state(&ring.core).is_none());
}

/// At a spill threshold of 0 the undo log spills after every batch, so the
/// rollback reads its spill lane back from disk across its parked slices.
/// A read of the installing collection waits through every slice, and the
/// state ends as it stood before the install.
#[cfg(feature = "failpoints")]
#[test]
fn a_parked_rollback_reads_its_spilled_undo_log_across_slices() {
    use nodedb_types::fail_point::FailGuard;

    let mut ring = Ring::open(plain_core);
    ring.core.query_tuning.max_txn_overlay_bytes = 0;
    let before = digest(&ring.core);
    let _failing = FailGuard::fail(ARM_FAILS, "a late batch fails");
    // The read before the install answers what the collection held.
    ring.push(request(READ_B, scan(FILL)), None);
    ring.push_fill_install();
    ring.push(request(READ_A, scan(FILL)), None);

    let mut rollback_parks = 0_usize;
    for _ in 0..MAX_TICKS {
        ring.tick();
        if ring.answer(INSTALL).is_some() {
            break;
        }
        assert!(
            ring.answer(READ_A).is_none(),
            "the fenced read waits while the install runs or rolls back"
        );
        if rollback_parked(&ring.core) {
            rollback_parks += 1;
        }
    }
    assert!(
        rollback_parks > 1,
        "the rollback gave the core back between its batches: {rollback_parks} parks"
    );
    let report = ring.core.redo_apply.last_rollback;
    assert!(
        report.spill_lane > 0 && report.spill_lane_hot_bytes == 0,
        "the rollback began with its spill lane on disk: {report:?}"
    );
    let answer = ring.answer(INSTALL).expect("the install answered");
    assert!(rolled_back(answer), "{:?}", answer.error_code);
    assert_eq!(installed_rows(&ring.core), 0);
    assert_eq!(
        digest(&ring.core),
        before,
        "the state is the pre-install state"
    );
    assert!(cursor_state(&ring.core).is_none());
    let baseline = ring
        .answer(READ_B)
        .expect("the read before answered")
        .clone();
    let read = ring.tick_until_answered(READ_A).clone();
    assert_eq!(read.status, baseline.status, "{:?}", read.error_code);
    assert_eq!(
        read.payload.to_vec(),
        baseline.payload.to_vec(),
        "the read sees none of the install"
    );
}

/// The record of [`refusal::kv_predicate_delete`] between filler rows, with
/// `ROWS` filler rows after it, and every collection it writes.
fn kv_and_fill_record() -> (RedoRecord, Vec<String>) {
    let plans = [refusal::kv_predicate_delete()];
    let mut redo = super::equivalence_tests::resolved(&plans, refusal::seed_kv);
    let mut ops = spread(std::mem::take(&mut redo.ops));
    ops.extend(fill(FILL_FROM + 2 * FILL_EACH, ROWS));
    redo.ops = ops;
    let mut collections = nodedb_physical::physical_plan::written_collections(&plans);
    collections.push(FILL.to_string());
    (redo, collections)
}

/// The validate walks its batches across ticks, at a spill threshold of 0
/// and of `usize::MAX`. What it hands the install is what the one-call
/// validate hands it: the install leaves the state the one-call validate
/// and install leave.
#[test]
fn a_validate_across_ticks_matches_the_one_call_validate() {
    for spill in [0, usize::MAX] {
        let (redo, collections) = kv_and_fill_record();
        let mut whole = super::super::stream_validate::tests::hold(plain_core);
        refusal::seed_kv(&mut whole.core);
        assert_ok(&super::equivalence_tests::install_batched_at(
            &mut whole,
            &redo,
            &[],
            &collections,
            spill,
        ));

        let mut ring = Ring::open(plain_core);
        ring.core.query_tuning.max_txn_overlay_bytes = spill;
        refusal::seed_kv(&mut ring.core);
        let plan = ring.marker_plan(&redo, collections, Vec::new(), None);
        ring.push(install_request(INSTALL, plan, LSN), None);
        ring.tick();
        assert!(ring.validating(), "spill {spill}: the validate parked");
        let ticks = ring.tick_past_validate();
        assert!(
            ticks > 1,
            "spill {spill}: the validate spanned several ticks: {ticks}"
        );
        assert_ok(ring.tick_until_answered(INSTALL));
        assert_eq!(digest(&ring.core), digest(&whole.core), "spill {spill}");
        assert_eq!(
            installed_rows(&ring.core),
            installed_rows(&whole.core),
            "spill {spill}"
        );
    }
}

/// A write to a collection the install writes waits through every validate
/// slice, and runs once the install ends.
#[test]
fn a_write_to_a_fenced_collection_waits_during_the_validate() {
    let mut ring = Ring::open(plain_core);
    ring.push_install_of(fill(FILL_FROM, MANY_ROWS));
    ring.push(fenced_put(FENCED_WRITE), None);
    ring.tick();
    let mut validate_ticks = 0_usize;
    while ring.validating() {
        assert!(
            ring.answer(FENCED_WRITE).is_none(),
            "the write waits while the validate runs"
        );
        validate_ticks += 1;
        assert!(validate_ticks < MAX_TICKS, "the validate never ended");
        ring.tick();
    }
    assert!(
        validate_ticks > 1,
        "the validate spanned several ticks: {validate_ticks}"
    );
    for _ in 0..MAX_TICKS {
        if ring.answer(INSTALL).is_some() {
            break;
        }
        assert!(
            ring.answer(FENCED_WRITE).is_none(),
            "the write waits while the install runs"
        );
        ring.tick();
    }
    assert_ok(ring.answer(INSTALL).expect("the install answered"));
    assert_ok(ring.tick_until_answered(FENCED_WRITE));
    assert!(
        ring.core
            .sparse
            .get(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                QualifiedCollection::new(DatabaseId::DEFAULT, FILL).as_str(),
                &StorageKey::for_surrogate(Surrogate::new(FENCED_SURROGATE)),
            )
            .expect("read the written row")
            .is_some(),
        "the write landed after the install"
    );
}

/// A write of another database queued while the validate is parked runs
/// before the validate's next slice.
#[test]
fn an_unfenced_task_runs_between_validate_slices() {
    let mut ring = Ring::open(plain_core);
    ring.push_install_of(fill(FILL_FROM, MANY_ROWS));
    ring.tick();
    assert!(ring.validating(), "the validate parked");

    ring.push(foreign_put(FOREIGN), None);
    ring.tick();
    assert_ok(
        ring.answer(FOREIGN)
            .expect("the foreign write ran between validate slices"),
    );
    assert!(
        ring.validating(),
        "the validate is still parked: the write ran between its slices"
    );
    assert!(ring.answer(INSTALL).is_none());

    assert_ok(ring.tick_until_answered(INSTALL));
    assert!(holds_foreign(&ring.core.sparse), "the foreign write stays");
}

/// The refusal the one-call validate reaches on `redo` as a stream at
/// [`YIELD_BATCH_BYTES`], on a fresh core.
fn one_call_refusal(redo: &RedoRecord) -> ErrorCode {
    use super::super::stream_validate::tests::default_task;
    use super::super::stream_validate::{StreamLimits, StreamValidateInput};

    let mut held = super::super::stream_validate::tests::hold(plain_core);
    let reference = held.wal.write(&framed(redo, &[], &[]));
    let marker = reference.marker(None, None, Vec::new()).expect("marker");
    let task = default_task();
    let verdict = held.core.validate_redo_stream(&StreamValidateInput {
        task: &task,
        tid: TID,
        marker: &marker,
        reference: &reference,
        origin: RedoOrigin::Commit,
        snapshot: None,
        judged_at_ms: crate::data::executor::enforcement::retention::judged_instant_ms(
            None,
            task.request.system_ordinal,
        ),
        lsn: Lsn::new(LSN),
        limits: StreamLimits {
            batch_bytes: YIELD_BATCH_BYTES,
            spill_threshold: usize::MAX,
        },
    });
    match verdict {
        Err(code) => code,
        Ok(_) => panic!("the one-call validate refuses"),
    }
}

/// Content that refuses at the end of its validate answers the refusal the
/// one-call validate answers, once, after several parked validate slices. It
/// writes no cursor and nothing else, and the fence releases.
#[test]
fn a_validate_refusal_after_parked_slices_answers_once_and_leaves_no_cursor() {
    let mut ops = fill(FILL_FROM, MANY_ROWS);
    ops.push(refusal::raw_crdt_delta_sub_record());
    let expected = one_call_refusal(&record(ops.clone()));

    let mut ring = Ring::open(plain_core);
    let before = digest(&ring.core);
    ring.push_install_of(ops);
    ring.push(request(READ_A, scan(FILL)), None);
    let mut parked_slices = 0_usize;
    for _ in 0..MAX_TICKS {
        ring.tick();
        if ring.answer(INSTALL).is_some() {
            break;
        }
        assert!(ring.validating(), "the install refuses in its validate");
        assert!(ring.answer(READ_A).is_none(), "the fenced read waits");
        parked_slices += 1;
    }
    assert!(
        parked_slices > 1,
        "the validate parked across several slices: {parked_slices}"
    );
    let answer = ring.answer(INSTALL).expect("the install answered").clone();
    assert_eq!(answer.status, Status::Error);
    assert_eq!(
        answer.error_code.as_deref(),
        Some(&expected),
        "the refusal the one-call validate answers"
    );
    assert!(
        ring.core
            .sparse
            .load_install_cursors()
            .expect("load cursors")
            .is_empty(),
        "a refused validate writes no cursor"
    );
    assert!(!ring.core.has_parked_install());
    assert!(!ring.core.has_install_fence());
    assert!(!ring.core.is_fail_stopped());
    assert_eq!(digest(&ring.core), before, "nothing was written");
    assert_ok(ring.tick_until_answered(READ_A));
    // A second answer to the install panics in `tick`.
    for _ in 0..8 {
        ring.tick();
    }
}

/// Tick until request `id` answers, asserting each tick that every request
/// of `waiting` stays unanswered until then.
fn tick_until_answered_while_waiting(ring: &mut Ring, id: u64, waiting: &[u64]) {
    for _ in 0..MAX_TICKS {
        if ring.answer(id).is_some() {
            return;
        }
        for waiter in waiting {
            assert!(
                ring.answer(*waiter).is_none(),
                "request {waiter} answered before request {id}"
            );
        }
        ring.tick();
    }
    panic!("request {id} never answered");
}

/// The content header alone names the sum targets: the plan names none. A
/// read of the target collection waits from the slice that walks the
/// header through every later validate slice, and the install folds once.
#[test]
fn a_read_of_a_header_sum_target_waits_during_the_validate() {
    const SOURCES: u32 = 400;
    let mut ring = Ring::open(core_with_sum);
    let plan = ring.marker_plan_of(
        &record(charges(SOURCES)),
        vec![SOURCE.to_string()],
        MarkerSpec {
            header_targets: sum_targets(),
            ..MarkerSpec::default()
        },
    );
    ring.push(
        install_request(INSTALL, plan, LSN),
        Some(group(LSN, SOURCE)),
    );
    ring.push(request(READ_A, scan(TARGET)), None);
    ring.tick();
    assert!(ring.validating(), "the validate parked");
    assert!(
        ring.tick_past_validate() > 0,
        "the validate spanned several ticks"
    );
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[READ_A]);
    assert_ok(ring.answer(INSTALL).expect("the install answered"));
    assert_ok(ring.tick_until_answered(READ_A));
    assert_eq!(
        balance(&ring.core),
        Some((100 + SOURCES).to_string()),
        "one fold of every row"
    );
}

/// A header sum target the fence cannot key: the job runs to its end in
/// the task's one slice, as an install the fence cannot key does.
#[test]
fn a_header_target_that_cannot_be_keyed_runs_in_one_slice() {
    let mut ring = Ring::open(plain_core);
    let plan = ring.marker_plan_of(
        &record(fill(FILL_FROM, MANY_ROWS)),
        vec![FILL.to_string()],
        MarkerSpec {
            // A name of another database than the task's.
            header_targets: vec![RedoSumTargets {
                collection: "7/acct".to_string(),
                resolved: Vec::new(),
            }],
            ..MarkerSpec::default()
        },
    );
    ring.push(install_request(INSTALL, plan, LSN), Some(group(LSN, FILL)));
    ring.tick();
    assert!(
        ring.answer(INSTALL).is_some(),
        "the install answered in its task's slice"
    );
    assert!(!ring.core.has_parked_install(), "the install never parked");
    assert!(!ring.core.has_install_fence(), "the fence released");
}

/// The snapshot every read of [`snapshot_spec`] validates at: above every
/// version a fresh core holds.
const SNAPSHOT: i64 = i64::MAX / 2;
/// The read-only collection the read set names.
const LEDGER: &str = "ledger";
/// Reads of [`LEDGER`] in the read set: several validate batches of them.
const LEDGER_READS: usize = 400;

/// A read set that spans several validate slices: one read of the whole of
/// the task's vShard, then reads of [`LEDGER`], all homed on that vShard.
fn snapshot_spec() -> MarkerSpec {
    use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};

    let read = |collection: &str| VersionedReadEntry {
        engine: EngineTag::Document,
        collection: collection.to_string(),
        key: ReadKeyIdent::Predicate,
        home_vshard: Some(0),
    };
    let mut reads = vec![read("")];
    reads.extend(std::iter::repeat_with(|| read(LEDGER)).take(LEDGER_READS));
    MarkerSpec {
        reads,
        snapshot: Some(SNAPSHOT),
        ..MarkerSpec::default()
    }
}

/// Queue the snapshot install of `ROWS` filler rows with
/// [`snapshot_spec`]'s read set as request [`INSTALL`].
fn push_snapshot_install(ring: &mut Ring) {
    let plan = ring.marker_plan_of(
        &record(fill(FILL_FROM, ROWS)),
        vec![FILL.to_string()],
        snapshot_spec(),
    );
    ring.push(install_request(INSTALL, plan, LSN), None);
}

/// A write of another database whose events carry the install's vShard
/// waits while the validate checks a read set that spans several slices:
/// the whole-vShard read is checked once, so the write would go unseen.
#[test]
fn a_same_vshard_write_waits_while_a_read_set_spans_validate_slices() {
    let mut ring = Ring::open(plain_core);
    push_snapshot_install(&mut ring);
    let mut same = foreign_put(FOREIGN);
    same.vshard_id = VShardId::new(0);
    ring.push(same, None);
    ring.tick();
    assert!(ring.validating(), "the validate parked");
    assert!(
        ring.tick_past_validate() > 0,
        "the read set spans several validate slices"
    );
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[FOREIGN]);
    assert_ok(ring.answer(INSTALL).expect("the install answered"));
    assert_ok(ring.tick_until_answered(FOREIGN));
    assert!(holds_foreign(&ring.core.sparse));
}

/// The slice that checks a chunk of the read set fences the chunk's
/// collection. A write to it and a read of it wait from then until the
/// install ends.
#[test]
fn a_write_to_a_read_set_collection_waits_after_its_chunk_was_checked() {
    use crate::data::executor::core_loop::install_fence::FenceKey;

    let mut ring = Ring::open(plain_core);
    push_snapshot_install(&mut ring);
    ring.tick();
    assert!(ring.validating(), "the validate parked");
    let ledger = FenceKey::Collection {
        db: DatabaseId::DEFAULT,
        name: QualifiedCollection::new(DatabaseId::DEFAULT, LEDGER)
            .as_str()
            .to_owned(),
    };
    assert!(
        ring.core
            .install_fence
            .as_ref()
            .is_some_and(|fence| fence.keys.contains(&ledger)),
        "the first slice fenced the collection of the reads it checked"
    );
    let mut write = foreign_put(FENCED_WRITE);
    write.database_id = DatabaseId::DEFAULT;
    write.plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, LEDGER),
        document_id: "l1".into(),
        value: crate::data::executor::doc_format::encode_to_msgpack(&serde_json::json!({"n": 1})),
        surrogate: Surrogate::new(FENCED_SURROGATE),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
    });
    ring.push(write, None);
    ring.push(request(READ_A, scan(LEDGER)), None);
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[FENCED_WRITE, READ_A]);
    assert_ok(ring.answer(INSTALL).expect("the install answered"));
    assert_ok(ring.tick_until_answered(FENCED_WRITE));
    assert_ok(ring.tick_until_answered(READ_A));
}

/// While the validate is parked, the governor holds exactly what the job
/// measures: the reader's buffer, the post-state, the spools, the arms'
/// validate scope and the rest of the walk. The charge follows the job past
/// the verdict, and returns once the install ends.
#[test]
fn the_governor_charge_follows_the_validate() {
    let mut ring = Ring::open(plain_core);
    let query = |core: &CoreLoop| {
        core.governor
            .budget(nodedb_mem::EngineId::Query)
            .allocated()
    };
    refusal::seed_kv(&mut ring.core);
    let before = query(&ring.core);
    let (redo, collections) = kv_and_fill_record();
    let plan = ring.marker_plan(&redo, collections, Vec::new(), None);
    ring.push(install_request(INSTALL, plan, LSN), None);
    ring.tick();
    let mut validate_parks = 0_usize;
    while ring.validating() {
        let resident = ring.core.parked_resident().expect("a parked job");
        assert!(resident > 0, "the parked validate holds RAM");
        assert_eq!(
            query(&ring.core) - before,
            resident,
            "the charge is the validate's measure"
        );
        validate_parks += 1;
        assert!(validate_parks < MAX_TICKS, "the validate never ended");
        ring.tick();
    }
    assert!(validate_parks > 1, "the validate parked several times");
    if ring.core.has_parked_install() {
        assert!(
            query(&ring.core) > before,
            "the install past the verdict is charged"
        );
    }
    assert_ok(ring.tick_until_answered(INSTALL));
    assert_eq!(query(&ring.core), before, "the charge is released");
}

// ── judged view across slices ────────────────────────────────────────────

/// The wire form of [`FILL`], with a UNIQUE index on `n` when `unique`.
fn fill_wire(unique: bool) -> nodedb_physical::physical_plan::DocConfigWire {
    use nodedb_physical::physical_plan::{
        DocConfigWire, EnforcementOptions, RegisteredIndex, RegisteredIndexState, StorageMode,
    };
    let index_paths = if unique {
        vec![RegisteredIndex {
            name: "n".into(),
            path: "n".into(),
            unique: true,
            case_insensitive: false,
            state: RegisteredIndexState::Ready,
            predicate: None,
        }]
    } else {
        Vec::new()
    };
    DocConfigWire {
        bitemporal: false,
        crdt_enabled: false,
        enforcement: EnforcementOptions::default(),
        storage_mode: StorageMode::Schemaless,
        index_paths,
        declared_key: None,
        vector_primary: None,
        declared_columns: Vec::new(),
        vector_fields: Vec::new(),
    }
}

/// A core with no config of [`FILL`] that maintains `n` as a UNIQUE path,
/// as a core does once the pinned index's build applied.
fn fill_unique_core(dir: &std::path::Path) -> (CoreLoop, Box<dyn std::any::Any>) {
    use crate::engine::document::store::IndexPath;
    let (mut core, ends) = plain_core(dir);
    core.maintained_indexes.insert_for_test(
        (DatabaseId::DEFAULT, TenantId::new(TID), FILL.to_string()),
        vec![IndexPath {
            unique: true,
            ..IndexPath::new("n")
        }],
    );
    (core, ends)
}

/// Pins as a proposer sets them for a record that writes [`FILL`], whose
/// proposer saw it with or without UNIQUE.
fn fill_pins(unique: bool) -> nodedb_physical::physical_plan::RedoPins {
    use nodedb_physical::physical_plan::{ConfigPin, ConfigPinKey, CrsPin, RedoPins};
    use std::sync::Arc;
    let (wire, digest) = fill_wire(unique).pin_parts().expect("pin parts");
    let set = zerompk::to_msgpack_vec(&nodedb_crs::CustomCrsSet::new()).expect("encode set");
    RedoPins {
        configs: vec![Arc::new(ConfigPin {
            key: ConfigPinKey {
                database_id: DatabaseId::DEFAULT,
                tenant_id: TenantId::new(TID),
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, FILL)
                    .as_str()
                    .to_owned(),
            },
            digest,
            wire: wire.into(),
        })],
        crs: Some(Arc::new(CrsPin {
            digest: nodedb_physical::physical_plan::pin_digest(&set),
            bytes: set.into(),
        })),
    }
}

/// Filler rows across many batches, then one more row whose `n` repeats
/// the first row's. Only a UNIQUE index on `n` refuses it.
fn fill_with_late_duplicate() -> Vec<RedoSubRecord> {
    let mut ops = fill(FILL_FROM, MANY_ROWS);
    let body =
        nodedb_types::json_to_msgpack(&serde_json::json!({"n": FILL_FROM})).expect("encode body");
    let late = FILL_FROM + MANY_ROWS;
    ops.push(doc_put_sub_record(FILL, &format!("f{late}"), &body, late));
    ops
}

impl Ring {
    /// Queue the install of `ops`, which write [`FILL`], pinned with
    /// `pins`, as request [`INSTALL`].
    fn push_pinned_install_of(
        &mut self,
        ops: Vec<RedoSubRecord>,
        pins: &nodedb_physical::physical_plan::RedoPins,
    ) {
        let content =
            super::super::stream_validate::tests::framed_pinned(&record(ops), &[], &[], pins);
        let reference = self.wal.write(&content);
        let marker = reference.marker(None, None, Vec::new()).expect("marker");
        let plan = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
            redo: marker.to_bytes().expect("encode marker"),
            collections: vec![FILL.to_string()],
            sum_targets: Vec::new(),
            origin: RedoOrigin::Commit,
            calvin: None,
            reads: Vec::new(),
            snapshot_ordinal: None,
            pins: Default::default(),
        });
        self.push(install_request(INSTALL, plan, LSN), None);
    }

    /// Whether the core holds a judged view now, as a task would see it.
    fn sees_judged_view(&self) -> bool {
        self.core.apply_scope.judged.is_some() || self.core.apply_scope.judging
    }
}

fn is_unique_violation(response: &Response) -> bool {
    matches!(
        response.error_code.as_deref(),
        Some(ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique"
    )
}

/// The judged view leaves the core with each park and returns with each
/// slice: the duplicate in the validate's last batch is judged under the
/// pinned UNIQUE index. A task between slices sees no view.
#[test]
fn the_judged_view_survives_a_yield_and_no_task_between_slices_sees_it() {
    let mut ring = Ring::open(fill_unique_core);
    ring.push_pinned_install_of(fill_with_late_duplicate(), &fill_pins(true));
    ring.tick();
    assert!(ring.validating(), "the validate parked");
    assert!(
        !ring.sees_judged_view(),
        "the parked view is out of the core"
    );

    ring.push(foreign_put(FOREIGN), None);
    ring.tick();
    assert_ok(
        ring.answer(FOREIGN)
            .expect("the foreign write ran between validate slices"),
    );
    assert!(ring.validating(), "the write ran between validate slices");
    assert!(!ring.sees_judged_view());

    let answer = ring.tick_until_answered(INSTALL).clone();
    assert!(is_unique_violation(&answer), "{:?}", answer.error_code);
    assert!(!ring.sees_judged_view(), "the view drops with the record");
}

/// A `Register` of [`FILL`] that lands between validate slices changes no
/// verdict: the record is judged under its pin either way.
#[test]
fn a_register_between_validate_slices_changes_nothing() {
    for pinned_unique in [true, false] {
        let mut ring = Ring::open(fill_unique_core);
        ring.push_pinned_install_of(fill_with_late_duplicate(), &fill_pins(pinned_unique));
        ring.tick();
        assert!(ring.validating(), "the validate parked");

        // The register lands the config the proposer did not see.
        let registered = fill_wire(!pinned_unique);
        let digest = registered.digest().expect("digest");
        let collection = QualifiedCollection::new(DatabaseId::DEFAULT, FILL);
        let response = ring.core.execute_register_document_collection(
            &make_default_task(),
            crate::data::executor::handlers::document::write::RegisterDocumentCollectionParams {
                tid: TID,
                collection: collection.as_str(),
                indexes: &registered.index_paths,
                crdt_enabled: false,
                storage_mode: &registered.storage_mode,
                enforcement: &registered.enforcement,
                bitemporal: false,
                conflict_policy: None,
                timeseries: None,
                vector_primary: None,
                vector_fields: &[],
                declared_columns: &[],
                declared_key: None,
                text_fields: &nodedb_physical::physical_plan::TextFieldSet::default(),
                incarnation: nodedb_types::Hlc::ZERO,
                digest,
            },
        );
        assert_ok(&response);
        assert!(ring.validating(), "the register landed between slices");

        let answer = ring.tick_until_answered(INSTALL).clone();
        assert_eq!(
            is_unique_violation(&answer),
            pinned_unique,
            "pinned unique {pinned_unique}: {:?}",
            answer.error_code
        );
    }
}
