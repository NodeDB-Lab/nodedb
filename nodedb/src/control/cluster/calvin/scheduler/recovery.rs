// SPDX-License-Identifier: BUSL-1.1

//! Calvin scheduler WAL recovery.
//!
//! Provides [`read_applied_recovery`] which scans the WAL for
//! `RecordType::CalvinApplied` records AND `RecordType::TransactionRedo`
//! records carrying a `calvin_stamp` (write-bearing Calvin transactions
//! journal the redo record as their applied-marker instead of a standalone
//! `CalvinApplied` marker), returning for a given vShard the union of applied
//! `(epoch, position)` pairs together with a fully-applied watermark.
//!
//! Boot runs [`recover_all_applied`] once, before the data-group apply loop
//! starts: one WAL pass yields every vShard's applied state, merged with the
//! state the last checkpoint saved. It fills the node's applied ledgers (see
//! [`super::applied_ledger::CalvinAppliedLedgers`]), and each scheduler seeds
//! its exactly-once applied gate (see [`super::applied_gate::AppliedGate`])
//! from its vShard's ledger.
//!
//! Each `CalvinApplied` marker is per `(epoch, position, vShard)` — one per
//! independent transaction position — so the scan preserves `position` rather
//! than collapsing an epoch to a single "applied" bit. Collapsing to the max
//! epoch will mark a whole epoch applied on the strength of its first committed
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

use std::collections::{BTreeMap, BTreeSet};

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

impl AppliedRecovery {
    /// The state of a vShard with no applied marker. The watermark stays
    /// conservative: the applied gate advances it once the re-fan-out
    /// supplies the per-epoch expected counts.
    fn empty() -> Self {
        Self {
            fully_applied_epoch: NOT_YET_APPLIED_EPOCH,
            applied_tail: BTreeSet::new(),
            max_applied_epoch: NOT_YET_APPLIED_EPOCH,
        }
    }

    /// Record one applied `(epoch, position)`.
    fn note(&mut self, epoch: u64, position: u32) {
        self.applied_tail.insert((epoch, position));
        if self.max_applied_epoch == NOT_YET_APPLIED_EPOCH || epoch > self.max_applied_epoch {
            self.max_applied_epoch = epoch;
        }
    }
}

/// Scan the WAL and collect this vShard's applied `(epoch, position)` markers.
///
/// Returns an empty tail and the [`NOT_YET_APPLIED_EPOCH`] sentinel for a
/// greenfield node (no `CalvinApplied` records exist).
///
/// Records that fail to decode are logged and skipped — a corrupt record does
/// not abort the scan.
pub fn read_applied_recovery(wal: &WalManager, vshard_id: u32) -> crate::Result<AppliedRecovery> {
    scan_applied(wal, vshard_id, None)
}

/// [`read_applied_recovery`] for a vShard of data group `group_id`: a
/// snapshot install of the group drops every marker before it. The install
/// replaced the vShard's storage, and the applied state it brought replaced
/// this node's in the catalog.
fn scan_applied(
    wal: &WalManager,
    vshard_id: u32,
    group_id: Option<u64>,
) -> crate::Result<AppliedRecovery> {
    let group_of = |vshard: u32| if vshard == vshard_id { group_id } else { None };
    let mut scans = scan_all(wal, &group_of, Some(vshard_id))?;
    Ok(scans
        .remove(&vshard_id)
        .unwrap_or_else(AppliedRecovery::empty))
}

/// Every vShard's applied markers in one pass over the WAL. `only` limits
/// the pass to one vShard. `group_of` names a vShard's data group: a
/// snapshot install of the group drops the vShard's markers before it.
fn scan_all(
    wal: &WalManager,
    group_of: &dyn Fn(u32) -> Option<u64>,
    only: Option<u32>,
) -> crate::Result<BTreeMap<u32, AppliedRecovery>> {
    let records = wal.replay()?;
    let mut scans: BTreeMap<u32, AppliedRecovery> = BTreeMap::new();
    let note =
        |scans: &mut BTreeMap<u32, AppliedRecovery>, vshard_id: u32, epoch: u64, position: u32| {
            if only.is_none_or(|wanted| wanted == vshard_id) {
                scans
                    .entry(vshard_id)
                    .or_insert_with(AppliedRecovery::empty)
                    .note(epoch, position);
            }
        };

    for record in &records {
        match record_type_of(record) {
            Some(RecordType::SnapshotInstalled) => {
                let Some(installed) = <[u8; 8]>::try_from(record.payload.as_slice())
                    .ok()
                    .map(u64::from_le_bytes)
                else {
                    continue;
                };
                for (vshard_id, scan) in &mut scans {
                    if group_of(*vshard_id) == Some(installed) {
                        *scan = AppliedRecovery::empty();
                    }
                }
            }
            Some(RecordType::CalvinApplied) => {
                match CalvinAppliedPayload::from_bytes(&record.payload) {
                    Ok(p) => note(&mut scans, p.vshard_id, p.epoch, p.position),
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
                    if let Some(stamp) = redo.calvin_stamp {
                        note(&mut scans, stamp.vshard_id, stamp.epoch, stamp.position);
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
    Ok(scans)
}

/// Every vShard's applied state after a restart, in one pass over the WAL:
/// the state the last checkpoint saved in `catalog`, together with the
/// markers the WAL still holds. A vShard absent from the result has no
/// Calvin history on this node.
///
/// A checkpoint deletes WAL segments that hold applied markers, while the
/// sequencer log keeps delivering their entries after a restart. Without the
/// saved state the scheduler will take an applied transaction for a new
/// one: its local stage refuses the rows it already wrote, the scheduler
/// halts, and the transaction's completion ack never settles.
///
/// `group_of` names the data group of a vShard, when known: the markers
/// before the group's last snapshot install no longer hold.
pub fn recover_all_applied(
    wal: &WalManager,
    catalog: &crate::control::security::catalog::SystemCatalog,
    group_of: &dyn Fn(u32) -> Option<u64>,
) -> crate::Result<BTreeMap<u32, AppliedRecovery>> {
    let mut recovered = scan_all(wal, group_of, None)?;
    for saved in catalog.load_all_calvin_applied()? {
        let from_wal = recovered
            .remove(&saved.vshard_id)
            .unwrap_or_else(AppliedRecovery::empty);
        recovered.insert(
            saved.vshard_id,
            merge_saved(from_wal, saved.fully_applied_epoch, saved.tail),
        );
    }
    Ok(recovered)
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
            }),
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
        };
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
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
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
        };
        wal.appender(crate::wal::manager::NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
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

        let recovered = recover_all_applied(&wal, &catalog, &|_| None).unwrap();
        let rec = &recovered[&1];
        assert_eq!(rec.fully_applied_epoch, 2);
        assert!(rec.applied_tail.contains(&(4, 1)));
        assert!(rec.applied_tail.contains(&(6, 0)));
        assert_eq!(rec.max_applied_epoch, 6);

        // A vShard with nothing saved and no marker has no history.
        assert!(!recovered.contains_key(&9));
    }

    fn empty_catalog(dir: &TempDir) -> crate::control::security::catalog::SystemCatalog {
        crate::control::security::catalog::SystemCatalog::open(&dir.path().join("system.redb"))
            .unwrap()
    }

    /// The one boot pass yields, for every vShard, what a scan of that vShard
    /// alone yields.
    #[test]
    fn one_pass_recovery_matches_per_vshard_scans() {
        use crate::types::VShardId;
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        let appender = || wal.appender(crate::wal::manager::NO_APPLY_KEY);
        appender()
            .append_calvin_applied(VShardId::new(1), 1, 0)
            .unwrap();
        appender()
            .append_calvin_applied(VShardId::new(2), 1, 1)
            .unwrap();
        appender().append_snapshot_installed(10).unwrap();
        appender()
            .append_calvin_applied(VShardId::new(3), 4, 0)
            .unwrap();
        appender()
            .append_calvin_applied(VShardId::new(1), 2, 3)
            .unwrap();
        appender().append_snapshot_installed(20).unwrap();
        appender()
            .append_calvin_applied(VShardId::new(2), 6, 0)
            .unwrap();
        wal.sync().unwrap();
        let group_of = |vshard: u32| match vshard {
            1 => Some(10),
            2 => Some(20),
            _ => None,
        };
        let catalog_dir = TempDir::new().unwrap();
        let catalog = empty_catalog(&catalog_dir);

        let all = recover_all_applied(&wal, &catalog, &group_of).unwrap();

        assert_eq!(all.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
        for (vshard, recovered) in &all {
            let alone = scan_applied(&wal, *vshard, group_of(*vshard)).unwrap();
            assert_eq!(recovered, &alone, "vShard {vshard}");
        }
        assert_eq!(all[&1].applied_tail, [(2, 3)].into_iter().collect());
        assert_eq!(all[&2].applied_tail, [(6, 0)].into_iter().collect());
        assert_eq!(all[&3].applied_tail, [(4, 0)].into_iter().collect());
    }

    /// A snapshot install record drops the markers of its own group's
    /// vShards only.
    #[test]
    fn a_snapshot_install_record_clears_only_its_group() {
        use crate::types::VShardId;
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        let appender = || wal.appender(crate::wal::manager::NO_APPLY_KEY);
        appender()
            .append_calvin_applied(VShardId::new(1), 3, 0)
            .unwrap();
        appender()
            .append_calvin_applied(VShardId::new(2), 3, 1)
            .unwrap();
        appender().append_snapshot_installed(8).unwrap();
        appender()
            .append_calvin_applied(VShardId::new(1), 5, 2)
            .unwrap();
        wal.sync().unwrap();
        let group_of = |vshard: u32| if vshard == 1 { Some(8) } else { Some(9) };
        let catalog_dir = TempDir::new().unwrap();
        let catalog = empty_catalog(&catalog_dir);

        let all = recover_all_applied(&wal, &catalog, &group_of).unwrap();

        assert_eq!(all[&1].applied_tail, [(5, 2)].into_iter().collect());
        assert_eq!(all[&1].max_applied_epoch, 5);
        assert_eq!(all[&2].applied_tail, [(3, 1)].into_iter().collect());
    }

    /// A snapshot install of the vShard's group drops every marker before
    /// it: the install replaced the storage those positions wrote.
    #[test]
    fn a_group_snapshot_install_drops_the_markers_before_it() {
        use crate::types::VShardId;
        let dir = TempDir::new().unwrap();
        let wal = open_wal(&dir);
        let appender = || wal.appender(crate::wal::manager::NO_APPLY_KEY);
        appender()
            .append_calvin_applied(VShardId::new(1), 3, 0)
            .unwrap();
        appender().append_snapshot_installed(8).unwrap();
        appender()
            .append_calvin_applied(VShardId::new(1), 5, 2)
            .unwrap();
        appender().append_snapshot_installed(9).unwrap();
        wal.sync().unwrap();

        let in_group = scan_applied(&wal, 1, Some(8)).unwrap();
        assert_eq!(in_group.applied_tail, [(5, 2)].into_iter().collect());
        assert_eq!(in_group.max_applied_epoch, 5);

        let other_group = scan_applied(&wal, 1, Some(4)).unwrap();
        assert_eq!(other_group, read_applied_recovery(&wal, 1).unwrap());
        assert!(other_group.applied_tail.contains(&(3, 0)));
    }
}
