// SPDX-License-Identifier: BUSL-1.1

//! Shared test fixtures for the Calvin scheduler driver's `core` unit tests.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use nodedb_cluster::MultiRaft;
use nodedb_cluster::RoutingTable;
use nodedb_cluster::calvin::types::{
    EngineKeySet, ReadWriteSet, SequencedTxn, SortedVec, TxClass, VersionedReadSet,
};
use nodedb_cluster::calvin::{CalvinCompletionRegistry, SequencerStateMachine};
use nodedb_types::TenantId;

use crate::bridge::dispatch::{CoreChannelDataSide, Dispatcher};
use crate::bridge::envelope::{Payload, Response, Status};
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::{
    Scheduler, SchedulerParams,
};
use crate::control::cluster::calvin::scheduler::driver::types::{CommitState, PendingTxn};
use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
use crate::control::cluster::calvin::scheduler::metrics::SchedulerMetrics;
use crate::control::cluster::calvin::scheduler::{NOT_YET_APPLIED_EPOCH, SchedulerConfig};
use crate::control::state::SharedState;
use crate::types::{Lsn, RequestId};
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

/// A `PendingTxn` staged and parked awaiting the cross-shard commit verdict.
pub(super) fn staged_pending(txn: SequencedTxn, txn_id: TxnId) -> PendingTxn {
    PendingTxn {
        txn,
        lock_owner: txn_id,
        dispatch_time: Instant::now(),
        has_primary_write: true,
        has_returning: false,
        change_sets: Vec::new(),
        commit_state: Some(CommitState::Staged),
        verdict_deadline: None,
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
