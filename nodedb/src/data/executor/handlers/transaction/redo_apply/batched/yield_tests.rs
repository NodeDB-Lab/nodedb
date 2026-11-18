// SPDX-License-Identifier: BUSL-1.1

//! A stream install that `run_task` runs gives the core back between its
//! batches, and its rollback between the batches it reverses.
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

use super::super::calvin_fold_tests::{ACCOUNT, SOURCE, balance, core_with_sum, sum_targets};
use super::super::install_refusal_tests as refusal;
use super::super::replay_fold::tests::crash_image;
use super::super::stream_validate::tests::{LSN, StreamWal, TID, framed, plain_core, record};
use super::super::test_commit::doc_put_sub_record;
use super::equivalence_tests::{FILL, FILL_EACH, FILL_FROM, digest, fill, install_whole, spread};

/// The bytes of one batch the core cuts a stream into.
const YIELD_BATCH_BYTES: usize = 4096;
/// Filler rows a record writes: several batches of them.
const ROWS: u32 = 300;
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
        })
    }

    /// Queue the install of `ROWS` filler rows as request [`INSTALL`].
    fn push_fill_install(&mut self) {
        let plan = self.marker_plan(
            &record(fill(FILL_FROM, ROWS)),
            vec![FILL.to_string()],
            Vec::new(),
            None,
        );
        self.push(install_request(INSTALL, plan, LSN), None);
    }
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
    let plans = [refusal::kv_predicate_delete()];
    let mut redo = super::equivalence_tests::resolved(&plans, refusal::seed_kv);
    let mut ops = spread(std::mem::take(&mut redo.ops));
    ops.extend(fill(FILL_FROM + 2 * FILL_EACH, ROWS));
    redo.ops = ops;
    let mut collections = nodedb_physical::physical_plan::written_collections(&plans);
    collections.push(FILL.to_string());

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

/// The first park, inside the task's run.
#[cfg(feature = "failpoints")]
#[test]
fn a_panic_at_the_first_park_stops_and_answers() {
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
