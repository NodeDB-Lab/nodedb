// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler WAL recovery.
//!
//! Provides [`read_applied_recovery`] which scans the WAL for
//! `RecordType::CalvinApplied` records AND `RecordType::TransactionRedo`
//! records carrying a `calvin_stamp` (write-bearing Calvin transactions
//! journal the redo record as their applied-marker instead of a standalone
//! `CalvinApplied` marker), returning for a given vShard the union of applied
//! `(epoch, position)` pairs together with a fully-applied watermark. The
//! scheduler uses this on startup to seed its exactly-once applied gate
//! (see [`super::applied_gate::AppliedGate`]).
//!
//! Each `CalvinApplied` marker is per `(epoch, position, vShard)` — one per
//! independent transaction position — so the scan preserves `position` rather
//! than collapsing an epoch to a single "applied" bit. Collapsing to the max
//! epoch would mark a whole epoch applied on the strength of its first committed
//! position and, on restart, skip every other position of that epoch: a lost /
//! torn transaction.
//!
//! The returned watermark is deliberately conservative: at recovery the
//! per-epoch expected position counts are not yet known (they arrive with the
//! sequencer's re-fan-out), so nothing can be *proven* fully applied and the
//! watermark stays at [`NOT_YET_APPLIED_EPOCH`]. Every marker rides in the tail,
//! where the applied-gate skip is exact; the watermark then advances as the
//! re-fan-out supplies the counts. `max_applied_epoch` is the highest epoch with
//! any marker — the rebuild-target cursor, independent of the exact gate.

use std::collections::BTreeSet;

use nodedb_wal::record::RecordType;
use nodedb_wal::{CalvinAppliedPayload, WalRecord};
use tracing::warn;

use crate::wal::RedoRecord;
use crate::wal::manager::WalManager;

/// Sentinel used when no Calvin epoch has ever been fully applied / no marker
/// exists for this vShard.
pub const NOT_YET_APPLIED_EPOCH: u64 = u64::MAX;

/// Result of scanning the WAL for a vShard's `CalvinApplied` markers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedRecovery {
    /// Fully-applied watermark `W` to seed the applied gate. Conservative at
    /// recovery: [`NOT_YET_APPLIED_EPOCH`] unless proven otherwise (it isn't,
    /// without the per-epoch expected counts), so the tail carries every marker.
    pub fully_applied_epoch: u64,
    /// Applied `(epoch, position)` pairs found for this vShard.
    pub applied_tail: BTreeSet<(u64, u32)>,
    /// Highest epoch with any marker for this vShard, or
    /// [`NOT_YET_APPLIED_EPOCH`] if none. Used as the rebuild-target cursor.
    pub max_applied_epoch: u64,
}

/// Scan the WAL and collect this vShard's applied `(epoch, position)` markers.
///
/// Returns an empty tail and the [`NOT_YET_APPLIED_EPOCH`] sentinel for a
/// greenfield node (no `CalvinApplied` records exist).
///
/// Records that fail to decode are logged and skipped — a corrupt record does
/// not abort the scan.
pub fn read_applied_recovery(wal: &WalManager, vshard_id: u32) -> crate::Result<AppliedRecovery> {
    let records = wal.replay()?;
    let mut applied_tail = BTreeSet::new();
    let mut max_applied_epoch = NOT_YET_APPLIED_EPOCH;

    for record in &records {
        match record_type_of(record) {
            Some(RecordType::CalvinApplied) => {
                match CalvinAppliedPayload::from_bytes(&record.payload) {
                    Ok(p) if p.vshard_id == vshard_id => {
                        accumulate(
                            &mut applied_tail,
                            &mut max_applied_epoch,
                            p.epoch,
                            p.position,
                        );
                    }
                    Ok(_) => {
                        // Different vshard — skip.
                    }
                    Err(e) => {
                        warn!(
                            lsn = record.header.lsn,
                            error = %e,
                            "calvin recovery: failed to decode CalvinApplied payload; skipping"
                        );
                    }
                }
            }
            Some(RecordType::TransactionRedo) => match RedoRecord::from_bytes(&record.payload) {
                Ok(redo) => {
                    if let Some(stamp) = redo.calvin_stamp
                        && stamp.vshard_id == vshard_id
                    {
                        accumulate(
                            &mut applied_tail,
                            &mut max_applied_epoch,
                            stamp.epoch,
                            stamp.position,
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        lsn = record.header.lsn,
                        error = %e,
                        "calvin recovery: failed to decode TransactionRedo payload; skipping"
                    );
                }
            },
            _ => continue,
        }
    }

    Ok(AppliedRecovery {
        // Conservative: the applied gate advances the watermark once the
        // re-fan-out supplies the per-epoch expected counts.
        fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
        applied_tail,
        max_applied_epoch,
    })
}

/// This vShard's applied state after a restart: the state the last
/// checkpoint saved in `catalog`, together with the markers the WAL still
/// holds.
///
/// A checkpoint deletes WAL segments that hold applied markers, while the
/// sequencer log keeps delivering their entries after a restart. Without the
/// saved state the scheduler would take an applied transaction for a new
/// one: its local stage refuses the rows it already wrote, the scheduler
/// halts, and the transaction's completion ack never settles.
pub fn recover_applied(
    wal: &WalManager,
    catalog: &crate::control::security::catalog::SystemCatalog,
    vshard_id: u32,
) -> crate::Result<AppliedRecovery> {
    let from_wal = read_applied_recovery(wal, vshard_id)?;
    let Some(saved) = catalog.load_calvin_applied(vshard_id)? else {
        return Ok(from_wal);
    };
    Ok(merge_saved(from_wal, saved.fully_applied_epoch, saved.tail))
}

/// Fold a saved `(fully_applied_epoch, tail)` into a WAL scan's result.
fn merge_saved(
    from_wal: AppliedRecovery,
    fully_applied_epoch: u64,
    saved_tail: BTreeSet<(u64, u32)>,
) -> AppliedRecovery {
    let above_watermark =
        |epoch: u64| fully_applied_epoch == NOT_YET_APPLIED_EPOCH || epoch > fully_applied_epoch;
    let applied_tail: BTreeSet<(u64, u32)> = from_wal
        .applied_tail
        .into_iter()
        .chain(saved_tail)
        .filter(|(epoch, _)| above_watermark(*epoch))
        .collect();
    let mut max_applied_epoch = from_wal.max_applied_epoch;
    let candidates = applied_tail
        .iter()
        .map(|(epoch, _)| *epoch)
        .chain((fully_applied_epoch != NOT_YET_APPLIED_EPOCH).then_some(fully_applied_epoch));
    for epoch in candidates {
        if max_applied_epoch == NOT_YET_APPLIED_EPOCH || epoch > max_applied_epoch {
            max_applied_epoch = epoch;
        }
    }
    AppliedRecovery {
        fully_applied_epoch,
        applied_tail,
        max_applied_epoch,
    }
}

/// Decode a WAL record's logical [`RecordType`], stripping the encryption
/// flag (bit 31) before comparing.
fn record_type_of(record: &WalRecord) -> Option<RecordType> {
    let raw_type = record.header.record_type & !nodedb_wal::record::ENCRYPTED_FLAG;
    RecordType::from_raw(raw_type)
}

/// Feed one applied `(epoch, position)` pair into the shared tail / watermark
/// accumulation, used by both the `CalvinApplied` and `TransactionRedo`
/// (`calvin_stamp`) marker sources so the two unify into one applied set.
fn accumulate(
    applied_tail: &mut BTreeSet<(u64, u32)>,
    max_applied_epoch: &mut u64,
    epoch: u64,
    position: u32,
) {
    applied_tail.insert((epoch, position));
    if *max_applied_epoch == NOT_YET_APPLIED_EPOCH || epoch > *max_applied_epoch {
        *max_applied_epoch = epoch;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::wal::manager::WalManager;

    fn open_wal(dir: &TempDir) -> WalManager {
        WalManager::open(dir.path(), false).expect("open wal")
    }

    #[test]
    fn greenfield_returns_sentinel_and_empty_tail() {
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        let rec = read_applied_recovery(&wal, 1).unwrap();
        assert_eq!(rec.fully_applied_epoch, NOT_YET_APPLIED_EPOCH);
        assert_eq!(rec.max_applied_epoch, NOT_YET_APPLIED_EPOCH);
        assert!(rec.applied_tail.is_empty());
    }

    #[test]
    fn tail_records_exact_positions_and_max_epoch() {
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);

        use crate::types::VShardId;
        // Epoch 5: position 0 applied, position 1 NOT applied. Epoch 2: pos 0.
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(1), 2, 0)
            .unwrap();
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(1), 5, 0)
            .unwrap();
        // A different vshard (must be ignored).
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(2), 99, 0)
            .unwrap();
        wal.sync().unwrap();

        let rec = read_applied_recovery(&wal, 1).unwrap();
        // The recovery API reports (5,0) applied and (5,1) NOT applied — the
        // exact per-position distinction the old max-epoch collapse destroyed.
        assert!(rec.applied_tail.contains(&(5, 0)), "(5,0) is applied");
        assert!(!rec.applied_tail.contains(&(5, 1)), "(5,1) is NOT applied");
        assert!(rec.applied_tail.contains(&(2, 0)));
        assert_eq!(rec.max_applied_epoch, 5);
        // The watermark stays below E: nothing is proven fully applied yet, so
        // the exact skip lives entirely in the tail.
        assert_eq!(rec.fully_applied_epoch, NOT_YET_APPLIED_EPOCH);
        assert!(rec.fully_applied_epoch == NOT_YET_APPLIED_EPOCH || rec.fully_applied_epoch < 5);

        let rec2 = read_applied_recovery(&wal, 2).unwrap();
        assert!(rec2.applied_tail.contains(&(99, 0)));
        assert_eq!(rec2.max_applied_epoch, 99);
    }

    #[test]
    fn multi_position_epoch_is_not_collapsed() {
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        use crate::types::VShardId;
        let vshard = 3u32;

        // Epoch 7 carries two independent positions on this vShard; only
        // position 0 committed before the crash.
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(vshard), 7, 0)
            .unwrap();
        wal.sync().unwrap();

        let rec = read_applied_recovery(&wal, vshard).unwrap();
        assert!(rec.applied_tail.contains(&(7, 0)));
        assert!(
            !rec.applied_tail.contains(&(7, 1)),
            "position 1 of epoch 7 must be reported as NOT applied so it is \
             re-applied on restart rather than lost"
        );
    }

    #[test]
    fn transaction_redo_calvin_stamp_unions_with_calvin_applied() {
        use crate::types::{DatabaseId, TenantId, VShardId};
        use crate::wal::{CalvinStamp, RedoRecord, RedoSubRecord};

        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        let vshard = 4u32;

        // A pure-read/empty-ops txn still writes a standalone CalvinApplied
        // marker at (epoch 1, position 0).
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(vshard), 1, 0)
            .unwrap();

        // A write-bearing Calvin txn journals its applied-marker as a
        // TransactionRedo record carrying a calvin_stamp at (epoch 1, position 1).
        let write_bearing = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: nodedb_wal::record::RecordType::Put as u32,
                payload: vec![1, 2, 3],
            }],
            calvin_stamp: Some(CalvinStamp {
                epoch: 1,
                position: 1,
                vshard_id: vshard,
                collections: Vec::new(),
                sum_targets: Vec::new(),
            }),
        };
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .append_transaction_redo(
                TenantId::new(0),
                VShardId::new(vshard),
                DatabaseId::DEFAULT,
                &write_bearing,
            )
            .unwrap();

        // A single-shard TransactionRedo (calvin_stamp: None) must be ignored
        // by Calvin recovery.
        let single_shard = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: nodedb_wal::record::RecordType::Put as u32,
                payload: vec![9, 9, 9],
            }],
            calvin_stamp: None,
        };
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .append_transaction_redo(
                TenantId::new(0),
                VShardId::new(vshard),
                DatabaseId::DEFAULT,
                &single_shard,
            )
            .unwrap();

        wal.sync().unwrap();

        let rec = read_applied_recovery(&wal, vshard).unwrap();
        assert!(
            rec.applied_tail.contains(&(1, 0)),
            "CalvinApplied marker still contributes"
        );
        assert!(
            rec.applied_tail.contains(&(1, 1)),
            "TransactionRedo calvin_stamp contributes its (epoch, position) too"
        );
        assert_eq!(rec.applied_tail.len(), 2);
        assert_eq!(rec.max_applied_epoch, 1);
    }

    #[test]
    fn saved_state_restores_what_a_truncated_wal_lost() {
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        use crate::types::VShardId;
        // The WAL still holds only the marker written after the checkpoint.
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .append_calvin_applied(VShardId::new(1), 6, 0)
            .unwrap();
        wal.sync().unwrap();
        let catalog_dir = TempDir::new().unwrap();
        let catalog = crate::control::security::catalog::SystemCatalog::open(
            &catalog_dir.path().join("system.redb"),
        )
        .unwrap();
        catalog
            .save_calvin_applied(vec![
                crate::control::security::catalog::calvin_applied::StoredCalvinApplied {
                    vshard_id: 1,
                    fully_applied_epoch: 2,
                    tail: [(4, 1)].into_iter().collect(),
                },
            ])
            .unwrap();

        let rec = recover_applied(&wal, &catalog, 1).unwrap();
        assert_eq!(rec.fully_applied_epoch, 2);
        assert!(rec.applied_tail.contains(&(4, 1)));
        assert!(rec.applied_tail.contains(&(6, 0)));
        assert_eq!(rec.max_applied_epoch, 6);

        // A vShard with nothing saved keeps the plain WAL scan.
        let other = recover_applied(&wal, &catalog, 9).unwrap();
        assert_eq!(other, read_applied_recovery(&wal, 9).unwrap());
    }
}
