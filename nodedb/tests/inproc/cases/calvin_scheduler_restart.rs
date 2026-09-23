// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler restart idempotency tests.
//!
//! Covers the WAL-recovery layer: after N epochs, a freshly-opened
//! `WalManager` reads back the correct applied `(epoch, position)` markers,
//! including out-of-order writes, vshard isolation, the greenfield sentinel,
//! and — critically — a MULTI-POSITION epoch where only some positions
//! committed before the crash (a torn transaction must be re-applied, not lost).
//!
//! End-to-end scheduler rebuild via `MultiRaft::read_committed_entries`
//! is covered by `nodedb-cluster/tests/calvin_3node_shard_failover.rs::
//! scheduler_catchup_via_raft_log_replay`.

use tempfile::TempDir;

use nodedb::control::cluster::calvin::scheduler::{NOT_YET_APPLIED_EPOCH, read_applied_recovery};
use nodedb::types::VShardId;
use nodedb::wal::manager::{NO_APPLY_KEY, WalManager};

// ── Helper ────────────────────────────────────────────────────────────────────

fn open_wal(dir: &TempDir) -> WalManager {
    WalManager::open_for_testing(dir.path()).expect("open wal")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Simulating 5 epochs (each with a single position 0): append CalvinApplied
/// records, sync, then verify that a freshly-opened WalManager reads back all
/// five markers and a max-applied epoch of 5.
#[test]
fn scheduler_restart_reads_applied_markers_after_five_epochs() {
    let dir = TempDir::new().unwrap();
    let vshard_id = 3u32;

    {
        let wal = open_wal(&dir);
        for epoch in 1u64..=5 {
            wal.appender(NO_APPLY_KEY)
                .append_calvin_applied(VShardId::new(vshard_id), epoch, 0)
                .unwrap();
        }
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let rec = read_applied_recovery(&wal, vshard_id).expect("recovery scan should succeed");
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
        wal.appender(NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(vshard_id), 3, 0)
            .unwrap();
        wal.appender(NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(vshard_id), 1, 0)
            .unwrap();
        wal.appender(NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(vshard_id), 5, 0)
            .unwrap();
        wal.appender(NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(vshard_id), 2, 0)
            .unwrap();
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let rec = read_applied_recovery(&wal, vshard_id).unwrap();
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
        wal.appender(NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(vshard_id), 8, 0)
            .unwrap();
        // Torn epoch: position 0 committed, position 1 did NOT (crash between).
        wal.appender(NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(vshard_id), torn_epoch, 0)
            .unwrap();
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let rec = read_applied_recovery(&wal, vshard_id).unwrap();
        assert!(
            rec.applied_tail.contains(&(torn_epoch, 0)),
            "committed position 0 must stay applied (not re-applied on restart)"
        );
        assert!(
            !rec.applied_tail.contains(&(torn_epoch, 1)),
            "uncommitted position 1 must be reported NOT applied so it is \
             re-delivered and applied on restart — else a lost/torn transaction"
        );
        // The fully-applied watermark stays below the torn epoch: the per-epoch
        // collapse that caused the bug is gone.
        assert!(
            rec.fully_applied_epoch == NOT_YET_APPLIED_EPOCH
                || rec.fully_applied_epoch < torn_epoch,
            "watermark must not cover a torn epoch"
        );
    }
}

/// Greenfield: a WAL with no CalvinApplied records returns the
/// `NOT_YET_APPLIED_EPOCH` sentinel and an empty tail. Epoch 0 is a valid real
/// epoch, so a distinct sentinel distinguishes "never applied" from "applied
/// epoch 0".
#[test]
fn scheduler_restart_greenfield_returns_sentinel() {
    let dir = TempDir::new().unwrap();
    let wal = open_wal(&dir);
    let rec = read_applied_recovery(&wal, 1).unwrap();
    assert_eq!(rec.fully_applied_epoch, NOT_YET_APPLIED_EPOCH);
    assert_eq!(rec.max_applied_epoch, NOT_YET_APPLIED_EPOCH);
    assert!(rec.applied_tail.is_empty());
}

/// Multiple vshards: recovery for one vshard must not see another's markers.
#[test]
fn scheduler_restart_vshard_isolation() {
    let dir = TempDir::new().unwrap();

    {
        let wal = open_wal(&dir);
        wal.appender(NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(1), 10, 0)
            .unwrap();
        wal.appender(NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(2), 99, 0)
            .unwrap();
        wal.appender(NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(1), 20, 0)
            .unwrap();
        wal.sync().unwrap();
    }

    {
        let wal = open_wal(&dir);
        let r1 = read_applied_recovery(&wal, 1).unwrap();
        let r2 = read_applied_recovery(&wal, 2).unwrap();
        assert_eq!(r1.max_applied_epoch, 20, "vshard 1 max is 20");
        assert!(r1.applied_tail.contains(&(10, 0)) && r1.applied_tail.contains(&(20, 0)));
        assert!(!r1.applied_tail.contains(&(99, 0)), "must not see vshard 2");
        assert_eq!(r2.max_applied_epoch, 99, "vshard 2 max is 99");
        assert!(r2.applied_tail.contains(&(99, 0)));
    }
}
