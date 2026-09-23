// SPDX-License-Identifier: BUSL-1.1

//! Shared test fixtures for the Calvin scheduler driver's `core` unit tests.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nodedb_cluster::MultiRaft;
use nodedb_cluster::RoutingTable;
use nodedb_cluster::calvin::types::{
    EngineKeySet, EngineTag, ReadKeyIdent, ReadWriteSet, SchedulerInput, SequencedTxn, SortedVec,
    TxClass, VersionedReadEntry, VersionedReadSet,
};
use nodedb_cluster::calvin::{CalvinCompletionRegistry, SequencerStateMachine};
use nodedb_physical::physical_plan::wire as plan_wire;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_types::{KeyRepr, QualifiedCollection, TenantId};
use tokio::sync::mpsc;

use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide, Dispatcher};
use crate::bridge::envelope::{
    Admission, ErrorCode, ExemptReason, Payload, Priority, Request, Response, Status,
};
use crate::control::cluster::calvin::scheduler::driver::barrier::ReadResultEvent;
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::{
    Scheduler, SchedulerParams,
};
use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::CapturingProposer;
use crate::control::cluster::calvin::scheduler::driver::types::{CommitState, PendingTxn};
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
use crate::control::cluster::calvin::scheduler::{NOT_YET_APPLIED_EPOCH, SchedulerConfig};
use crate::control::shutdown::ShutdownWatch;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, VShardId};
use crate::wal::WalManager;

/// Build a minimally-wired `Scheduler` for driver-level unit tests. The Data
/// Plane is NOT started — tests exercise Control-Plane routing, guards, and
/// request dispatch only, so no core loop is needed. The returned `TempDir`
/// must be kept alive for the scheduler's lifetime (backs the WAL and Raft
/// storage).
pub(super) fn build_test_scheduler(vshard_id: u32) -> (Scheduler, tempfile::TempDir) {
    let registry = CalvinCompletionRegistry::new_detached();
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
    let (dispatcher, mut data_sides) = Dispatcher::new(1, 64);
    let _data_side = data_sides
        .pop()
        .expect("one configured core has one data side");
    let shared = SharedState::new(dispatcher, wal).unwrap();

    let rt = RoutingTable::uniform(1, &[1], 1);
    let multi_raft = Arc::new(Mutex::new(MultiRaft::new(1, rt, dir.path().to_path_buf())));

    let sequencer_state_machine = Arc::new(Mutex::new(SequencerStateMachine::new(
        HashMap::new(),
        Arc::clone(&registry),
    )));

    let (_tx, receiver) = tokio::sync::mpsc::channel(16);
    let (_rr_tx, read_result_rx) = tokio::sync::mpsc::channel(16);
    let (_prom_tx, promotion_rx) = tokio::sync::mpsc::unbounded_channel();
    let (verdict_tx, verdict_rx) = tokio::sync::mpsc::channel(16);
    registry.register_verdict_signal_sender(vshard_id, verdict_tx);

    let lock_manager = Arc::new(Mutex::new(LockManager::new()));

    let scheduler = Scheduler::new(SchedulerParams {
        vshard_id,
        receiver,
        shared,
        multi_raft,
        sequencer_proposer: CapturingProposer::accepting(),
        sequencer_state_machine,
        // A freshly-built scheduler has applied nothing, so its watermark is the
        // not-yet-applied sentinel (matching `read_applied_recovery` for a clean
        // node). Hardcoding `0` here would instead claim epoch 0 is fully applied,
        // making the exactly-once gate (`AppliedGate::is_applied`) short-circuit
        // every epoch-0 replay before it reaches the lock table — silently
        // defeating the end-to-end drain tests below.
        fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
        applied_tail: BTreeSet::new(),
        rebuild_target_epoch: 0,
        config: SchedulerConfig::default(),
        metrics: SchedulerMetrics::new(),
        read_result_rx,
        lock_manager,
        promotion_rx,
        registry,
        verdict_rx,
    });
    (scheduler, dir)
}

/// Same minimal scheduler fixture as [`build_test_scheduler`], sharing a
/// caller-supplied completion `registry` (so several schedulers can register
/// against it) and retaining its Data-Plane request receiver for tests that
/// must observe scheduler dispatches.
pub(super) fn build_test_scheduler_with_data_side(
    vshard_id: u32,
    registry: Arc<CalvinCompletionRegistry>,
) -> (Scheduler, tempfile::TempDir, CoreChannelDataSide) {
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("test.wal")).unwrap());
    let (dispatcher, mut data_sides) = Dispatcher::new(1, 64);
    let data_side = data_sides
        .pop()
        .expect("one configured core has one data side");
    let shared = SharedState::new(dispatcher, wal).unwrap();

    let rt = RoutingTable::uniform(1, &[1], 1);
    let multi_raft = Arc::new(Mutex::new(MultiRaft::new(1, rt, dir.path().to_path_buf())));

    let sequencer_state_machine = Arc::new(Mutex::new(SequencerStateMachine::new(
        HashMap::new(),
        Arc::clone(&registry),
    )));

    let (_tx, receiver) = tokio::sync::mpsc::channel(16);
    let (_rr_tx, read_result_rx) = tokio::sync::mpsc::channel(16);
    let (_prom_tx, promotion_rx) = tokio::sync::mpsc::unbounded_channel();
    let (verdict_tx, verdict_rx) = tokio::sync::mpsc::channel(16);
    registry.register_verdict_signal_sender(vshard_id, verdict_tx);

    let lock_manager = Arc::new(Mutex::new(LockManager::new()));

    let scheduler = Scheduler::new(SchedulerParams {
        vshard_id,
        receiver,
        shared,
        multi_raft,
        sequencer_proposer: CapturingProposer::accepting(),
        sequencer_state_machine,
        fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
        applied_tail: BTreeSet::new(),
        rebuild_target_epoch: 0,
        config: SchedulerConfig::default(),
        metrics: SchedulerMetrics::new(),
        read_result_rx,
        lock_manager,
        promotion_rx,
        registry,
        verdict_rx,
    });
    (scheduler, dir, data_side)
}

/// Build a static-write `SequencedTxn` at `(epoch, position)`.
pub(super) fn make_sequenced_txn(epoch: u64, position: u32) -> SequencedTxn {
    let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
        collection: "test_coll".to_string(),
        surrogates: SortedVec::new(vec![1]),
    }]);
    let tx_class = TxClass::new_single_vshard(
        ReadWriteSet::new(vec![]),
        write_set,
        vec![],
        TenantId::new(1),
        None,
        VersionedReadSet::default(),
    )
    .expect("valid TxClass");
    SequencedTxn {
        epoch,
        position,
        tx_class,
        epoch_system_ms: 1_700_000_000_000,
        epoch_vshard_txn_count: 1,
        lock_owner: None,
    }
}

/// The vShard that `"test_coll"` homes to in the default database. A
/// scheduler built on this vShard owns the reads of [`make_validate_only_txn`].
pub(super) fn test_coll_vshard() -> u32 {
    VShardId::from_collection_in_database(DatabaseId::DEFAULT, "test_coll").as_u32()
}

/// Build a static `SequencedTxn` at `(epoch, position)` that reaches the
/// `CalvinExecuteStatic` stage dispatch on the [`test_coll_vshard`] scheduler.
///
/// It carries an encoded empty plan batch and one versioned read on
/// `"test_coll"`, so that scheduler stages it as a validate-only read
/// participant. Its write set locks `"test_coll"` surrogate 1, the same key
/// as [`make_sequenced_txn`].
pub(super) fn make_validate_only_txn(epoch: u64, position: u32) -> SequencedTxn {
    let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
        collection: "test_coll".to_string(),
        surrogates: SortedVec::new(vec![1]),
    }]);
    let plans = plan_wire::encode_batch(&Vec::new()).expect("encode empty plan batch");
    let versioned_reads = VersionedReadSet::new(vec![VersionedReadEntry {
        engine: EngineTag::Document,
        collection: "test_coll".to_string(),
        key: ReadKeyIdent::Point(KeyRepr::Surrogate(1)),
        read_lsn: Lsn::ZERO,
    }]);
    let tx_class = TxClass::new_single_vshard(
        ReadWriteSet::new(vec![]),
        write_set,
        plans,
        TenantId::new(1),
        None,
        versioned_reads,
    )
    .expect("valid TxClass");
    SequencedTxn {
        epoch,
        position,
        tx_class,
        epoch_system_ms: 1_700_000_000_000,
        epoch_vshard_txn_count: 1,
        lock_owner: None,
    }
}

/// Build a `SequencedTxn` at `(epoch, position)` whose one write plan, a
/// truncate of `"test_coll"`, homes to [`test_coll_vshard`].
///
/// The plan carries no identity to bind, so it reaches the stage dispatch of
/// either path unchanged. Its write set locks the same key as
/// [`make_sequenced_txn`].
pub(super) fn make_local_write_txn(epoch: u64, position: u32) -> SequencedTxn {
    let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
        collection: "test_coll".to_string(),
        surrogates: SortedVec::new(vec![1]),
    }]);
    let batch = vec![PhysicalPlan::Document(DocumentOp::Truncate {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "test_coll"),
        restart_identity: false,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
    })];
    let plans = plan_wire::encode_batch(&batch).expect("encode one truncate plan");
    let tx_class = TxClass::new_single_vshard(
        ReadWriteSet::new(vec![]),
        write_set,
        plans,
        TenantId::new(1),
        None,
        VersionedReadSet::default(),
    )
    .expect("valid TxClass");
    SequencedTxn {
        epoch,
        position,
        tx_class,
        epoch_system_ms: 1_700_000_000_000,
        epoch_vshard_txn_count: 1,
        lock_owner: None,
    }
}

/// Upper bound on filler dispatches. The fixture dispatcher caps a tenant at
/// 64 in-flight requests, so the cap is hit long before this bound.
const MAX_FILLERS: usize = 4096;

/// How long a test waits for a request to reach the Data Plane side.
const DATA_PLANE_WAIT: Duration = Duration::from_secs(5);

/// A read request for `tenant_id` that holds one in-flight slot until the
/// Data Plane answers it.
fn filler_request(request_id: RequestId, tenant_id: TenantId) -> Request {
    Request {
        request_id,
        tenant_id,
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "filler"),
            document_id: "d".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }),
        // no-determinism: test-only filler deadline, never Calvin WAL data.
        deadline: Instant::now() + Duration::from_secs(60),
        priority: Priority::Normal,
        trace_id: nodedb_types::TraceId([0u8; 16]),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: Admission::Exempt(ExemptReason::Read),
    }
}

/// Dispatch filler reads for `tenant_id` until the dispatcher refuses the
/// tenant at its in-flight cap. Returns the filler request ids.
///
/// Each accepted filler is popped off the request ring at once, so the ring
/// and the weighted-fair queue stay empty. The only refusal left is the
/// per-tenant in-flight cap, and this function panics on any other refusal.
pub(super) fn fill_tenant_inflight(
    shared: &SharedState,
    data_side: &mut CoreChannelDataSide,
    tenant_id: TenantId,
) -> Vec<RequestId> {
    let mut fillers = Vec::new();
    let mut dispatcher = shared.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
    for _ in 0..MAX_FILLERS {
        let request_id = shared.next_request_id();
        match dispatcher.dispatch(filler_request(request_id, tenant_id)) {
            Ok(()) => {
                fillers.push(request_id);
                while data_side.request_rx.try_pop().is_ok() {}
            }
            Err(crate::Error::DispatchCapacity {
                scope: crate::DispatchCapacityScope::TenantInflight { .. },
            }) => {
                assert!(
                    !fillers.is_empty(),
                    "the cap must admit at least one filler"
                );
                return fillers;
            }
            Err(other) => panic!("unexpected filler dispatch error: {other}"),
        }
    }
    panic!("tenant in-flight cap not reached after {MAX_FILLERS} fillers");
}

/// Answer one filler request on the Data Plane side and poll it back, which
/// frees one in-flight slot for its tenant.
pub(super) fn release_filler(
    shared: &SharedState,
    data_side: &mut CoreChannelDataSide,
    request_id: RequestId,
) {
    let mut response = staged_response(Status::Ok, None);
    response.request_id = request_id;
    data_side
        .response_tx
        .try_push(BridgeResponse { inner: response })
        .expect("response ring has room for one filler response");
    let polled = shared.poll_and_route_responses();
    assert!(polled >= 1, "the filler response must be polled");
}

/// Wait until a request whose plan satisfies `wanted` reaches the Data Plane
/// side. Returns `false` if none arrives within [`DATA_PLANE_WAIT`].
pub(super) async fn await_data_plane_request(
    data_side: &mut CoreChannelDataSide,
    wanted: impl Fn(&PhysicalPlan) -> bool,
) -> bool {
    let wait = async {
        loop {
            while let Ok(request) = data_side.request_rx.try_pop() {
                if wanted(&request.inner.plan) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(DATA_PLANE_WAIT, wait).await.is_ok()
}

/// A scheduler run loop spawned on the test runtime.
///
/// Holds every input sender so no loop channel reports closed.
pub(super) struct RunningScheduler {
    shutdown: ShutdownWatch,
    handle: tokio::task::JoinHandle<()>,
    input_tx: mpsc::Sender<SchedulerInput>,
    _read_result_tx: mpsc::Sender<ReadResultEvent>,
    _promotion_tx: mpsc::UnboundedSender<Vec<TxnId>>,
}

impl RunningScheduler {
    /// The sender feeding the loop's sequenced-input receiver.
    pub(super) fn input_tx(&self) -> &mpsc::Sender<SchedulerInput> {
        &self.input_tx
    }

    /// Signal shutdown and wait for the loop to exit.
    pub(super) async fn stop(self) {
        self.shutdown.signal();
        tokio::time::timeout(DATA_PLANE_WAIT, self.handle)
            .await
            .expect("scheduler loop exits after shutdown")
            .expect("scheduler loop does not panic");
    }
}

/// Spawn `scheduler`'s run loop with open input channels and a short
/// liveness tick.
pub(super) fn spawn_scheduler_loop(mut scheduler: Scheduler) -> RunningScheduler {
    let (input_tx, input_rx) = mpsc::channel(16);
    let (read_result_tx, read_result_rx) = mpsc::channel(16);
    let (promotion_tx, promotion_rx) = mpsc::unbounded_channel();
    scheduler.receiver = input_rx;
    scheduler.read_result_rx = read_result_rx;
    scheduler.promotion_rx = promotion_rx;
    // The loop's liveness tick fires every quarter of this interval.
    scheduler.config.verdict_stall_warn_ms = 200;
    let shutdown = ShutdownWatch::new();
    let receiver = shutdown.subscribe();
    let handle = tokio::spawn(scheduler.run(receiver));
    RunningScheduler {
        shutdown,
        handle,
        input_tx,
        _read_result_tx: read_result_tx,
        _promotion_tx: promotion_tx,
    }
}

/// A `PendingTxn` staged and parked awaiting the cross-shard commit verdict.
pub(super) fn staged_pending(txn: SequencedTxn, txn_id: TxnId) -> PendingTxn {
    PendingTxn {
        txn,
        lock_owner: txn_id,
        // no-determinism: test-only dispatch timestamp for a fabricated PendingTxn fixture.
        dispatch_time: Instant::now(),
        has_primary_write: true,
        has_returning: false,
        change_sets: Vec::new(),
        commit_state: Some(CommitState::Staged),
        verdict_deadline: None,
        stage_error: None,
    }
}

/// A staged executor `Response` carrying the given status and read-set vote.
pub(super) fn staged_response(status: Status, read_set_valid: Option<bool>) -> Response {
    Response {
        request_id: RequestId::new(1),
        status,
        attempt: 1,
        partial: false,
        payload: Payload::empty(),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        read_set_valid,
        read_version_lsn: Lsn::ZERO,
        write_set: Vec::new(),
    }
}

/// An executor `Response` with `Status::Error` carrying `code`.
pub(super) fn error_response(code: ErrorCode) -> Response {
    let mut response = staged_response(Status::Error, None);
    response.error_code = Some(Box::new(code));
    response
}

/// Close the dispatcher's Data Plane enqueue gate, as a node shutdown does.
/// Every later dispatch is refused terminally.
pub(super) fn begin_data_plane_drain(shared: &SharedState) {
    shared
        .dispatcher
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .begin_data_plane_drain();
}

/// A scheduler on vShard 7 with `txn_id` pending in `state`.
pub(super) fn scheduler_with_pending(
    txn_id: TxnId,
    state: CommitState,
) -> (Scheduler, tempfile::TempDir) {
    let (mut scheduler, dir) = build_test_scheduler(7);
    let mut pending = staged_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id);
    pending.commit_state = Some(state);
    scheduler.pending.insert(txn_id, pending);
    (scheduler, dir)
}
