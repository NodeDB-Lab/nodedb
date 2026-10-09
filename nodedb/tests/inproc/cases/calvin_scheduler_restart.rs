// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler restart idempotency tests.
//!
//! Covers the WAL-recovery layer: after N epochs, a freshly-opened
//! `WalManager` reads back the correct applied `(epoch, position)` pairs from
//! the stamped `TransactionRedo` records, including out-of-order writes,
//! vshard isolation, the greenfield sentinel, and — critically — a
//! MULTI-POSITION epoch where only some positions committed before the crash
//! (a torn transaction must be re-applied, not lost).
//!
//! End-to-end scheduler rebuild via `MultiRaft::read_committed_entries`
//! is covered by `nodedb-cluster/tests/calvin_3node_shard_failover.rs::
//! scheduler_catchup_via_raft_log_replay`.

use tempfile::TempDir;

use nodedb::control::cluster::calvin::scheduler::{
    AppliedRecovery, NOT_YET_APPLIED_EPOCH, recover_all_applied,
};
use nodedb::control::security::catalog::SystemCatalog;
use nodedb::event::EventSource;
use nodedb::types::{DatabaseId, TenantId, VShardId};
use nodedb::wal::manager::{NO_APPLY_KEY, WalManager};
use nodedb::wal::{CalvinStamp, RedoRecord};

// ── Helper ────────────────────────────────────────────────────────────────────

fn open_wal(dir: &TempDir) -> WalManager {
    WalManager::open_for_testing(dir.path()).expect("open wal")
}

/// Append the stamped redo record of the Calvin slice at `(epoch, position)`
/// on `vshard_id`: the WAL record a committed slice's install writes.
fn append_stamped(wal: &WalManager, vshard_id: u32, epoch: u64, position: u32) {
    let record = RedoRecord {
        version: 1,
        ops: Vec::new(),
        calvin_stamp: Some(CalvinStamp {
            epoch,
            position,
            vshard_id,
        }),
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
    };
    wal.appender(NO_APPLY_KEY)
        .with_event_source(EventSource::User)
        .append_transaction_redo(
            TenantId::new(0),
            VShardId::new(vshard_id),
            DatabaseId::DEFAULT,
            &record,
        )
        .expect("append stamped redo");
}

/// The recovered state of `vshard_id` from a catalog with nothing saved. A
/// vShard with no stamped redo record recovers as the greenfield state: the
/// `NOT_YET_APPLIED_EPOCH` sentinel and an empty tail.
fn recover_vshard(wal: &WalManager, vshard_id: u32) -> AppliedRecovery {
    let catalog = SystemCatalog::open_in_memory().expect("in-memory catalog");
    recover_all_applied(wal, &catalog, &|_| None)
        .expect("recovery scan should succeed")
        .remove(&vshard_id)
        .unwrap_or(AppliedRecovery {
            fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
            applied_tail: Default::default(),
            max_applied_epoch: NOT_YET_APPLIED_EPOCH,
        })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Simulating 5 epochs (each with a single position 0): append stamped redo
/// records, sync, then verify that a freshly-opened WalManager reads back all
/// five positions and a max-applied epoch of 5.
#[test]
fn scheduler_restart_reads_applied_markers_after_five_epochs() {
    let dir = TempDir::new().unwrap();
    let vshard_id = 3u32;

    {
        let wal = open_wal(&dir);
        for epoch in 1u64..=5 {
            append_stamped(&wal, vshard_id, epoch, 0);
        }
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let rec = recover_vshard(&wal, vshard_id);
        assert_eq!(rec.max_applied_epoch, 5, "max applied epoch should be 5");
        for epoch in 1u64..=5 {
            assert!(
                rec.applied_tail.contains(&(epoch, 0)),
                "epoch {epoch} position 0 should be recorded as applied"
            );
        }
    }
}

/// Epochs applied out-of-order in the WAL are still reported with the correct
/// max and complete tail. The recovery scanner must not depend on write order.
#[test]
fn scheduler_restart_reports_max_and_all_positions_regardless_of_order() {
    let dir = TempDir::new().unwrap();
    let vshard_id = 7u32;

    {
        let wal = open_wal(&dir);
        for epoch in [3u64, 1, 5, 2] {
            append_stamped(&wal, vshard_id, epoch, 0);
        }
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let rec = recover_vshard(&wal, vshard_id);
        assert_eq!(
            rec.max_applied_epoch, 5,
            "max should be 5, not last-written 2"
        );
        for epoch in [1u64, 2, 3, 5] {
            assert!(rec.applied_tail.contains(&(epoch, 0)));
        }
        assert!(
            !rec.applied_tail.contains(&(4, 0)),
            "epoch 4 was never applied"
        );
    }
}

/// A MULTI-POSITION epoch where only position 0 committed before the crash.
/// The recovery scan MUST report `(E,1)` as NOT applied so that on restart it is
/// re-delivered and re-applied — position 1 is an independent transaction that
/// would otherwise be silently lost (a torn transaction). Position 0 must remain
/// recorded so it is NOT re-applied (exactly-once).
#[test]
fn scheduler_restart_multi_position_epoch_does_not_lose_uncommitted_position() {
    let dir = TempDir::new().unwrap();
    let vshard_id = 4u32;
    let torn_epoch = 9u64;

    {
        let wal = open_wal(&dir);
        // Prior epoch fully applied.
        append_stamped(&wal, vshard_id, 8, 0);
        // Torn epoch: position 0 committed, position 1 did NOT (crash between).
        append_stamped(&wal, vshard_id, torn_epoch, 0);
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let rec = recover_vshard(&wal, vshard_id);
        assert!(
            rec.applied_tail.contains(&(torn_epoch, 0)),
            "committed position 0 must stay applied (not re-applied on restart)"
        );
        assert!(
            !rec.applied_tail.contains(&(torn_epoch, 1)),
            "uncommitted position 1 must be reported NOT applied so it is \
             re-delivered and applied on restart — else a lost/torn transaction"
        );
        // The fully-applied watermark stays below the torn epoch.
        assert!(
            rec.fully_applied_epoch == NOT_YET_APPLIED_EPOCH
                || rec.fully_applied_epoch < torn_epoch,
            "watermark must not cover a torn epoch"
        );
    }
}

/// Greenfield: a WAL with no stamped redo record returns the
/// `NOT_YET_APPLIED_EPOCH` sentinel and an empty tail. Epoch 0 is a valid real
/// epoch, so a distinct sentinel distinguishes "never applied" from "applied
/// epoch 0".
#[test]
fn scheduler_restart_greenfield_returns_sentinel() {
    let dir = TempDir::new().unwrap();
    let wal = open_wal(&dir);
    let rec = recover_vshard(&wal, 1);
    assert_eq!(rec.fully_applied_epoch, NOT_YET_APPLIED_EPOCH);
    assert_eq!(rec.max_applied_epoch, NOT_YET_APPLIED_EPOCH);
    assert!(rec.applied_tail.is_empty());
}

/// Multiple vshards: recovery for one vshard must not see another's records.
#[test]
fn scheduler_restart_vshard_isolation() {
    let dir = TempDir::new().unwrap();

    {
        let wal = open_wal(&dir);
        append_stamped(&wal, 1, 10, 0);
        append_stamped(&wal, 2, 99, 0);
        append_stamped(&wal, 1, 20, 0);
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let r1 = recover_vshard(&wal, 1);
        let r2 = recover_vshard(&wal, 2);
        assert_eq!(r1.max_applied_epoch, 20, "vshard 1 max is 20");
        assert!(r1.applied_tail.contains(&(10, 0)) && r1.applied_tail.contains(&(20, 0)));
        assert!(!r1.applied_tail.contains(&(99, 0)), "must not see vshard 2");
        assert_eq!(r2.max_applied_epoch, 99, "vshard 2 max is 99");
        assert!(r2.applied_tail.contains(&(99, 0)));
    }
}
