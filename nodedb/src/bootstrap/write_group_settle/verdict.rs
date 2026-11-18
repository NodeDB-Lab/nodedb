// SPDX-License-Identifier: BUSL-1.1

//! The boot verdict on each committed redo record that folds sums.
//!
//! A committed record's install folds materialized sums into target rows in
//! the source row's own store transaction, with the store's durability
//! deferred. The fold becomes durable only with the install's stored write
//! set. The record's WAL copy carries the sum targets the install used, and
//! it can be durable while the write set is not: a concurrent WAL sync makes
//! it so. Boot judges each record that carries sum targets:
//!
//! - [`RedoVerdict::Complete`]: a stored write set or a part of the record's
//!   group is present. The fold rows are durable, or the parts carry them.
//!   Replay applies the record plainly.
//! - [`RedoVerdict::NeverDurable`]: neither is present. The crash reverted
//!   the fold, or a batched install made it durable through its fold
//!   ledger. Replay folds every document write of the record at or above
//!   the ledger into its targets, as the install did.

use std::collections::HashSet;

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::wal::redo::StreamContents;
use crate::wal::{NeverDurableRedos, RedoRecord, RedoStreamRef};

use super::scan::{GroupScan, WalGroups};

/// What boot holds of one committed record's install.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedoVerdict {
    /// A stored write set or a part of the group is present.
    Complete,
    /// No write set and no part is stored: the install never became
    /// durable.
    NeverDurable,
}

/// The verdict on a committed record. `captured` is whether a stored write
/// set names it, `group` its group as the WAL shows it. A record whole at
/// append journals no part, so its group holds everything it ever will.
pub fn redo_verdict(captured: bool, group: Option<&GroupScan>) -> RedoVerdict {
    if captured {
        return RedoVerdict::Complete;
    }
    match group {
        Some(scan) if scan.closed.is_some() || !scan.seen.is_empty() => RedoVerdict::Complete,
        _ => RedoVerdict::NeverDurable,
    }
}

/// The committed records of `records` that carry sum targets and whose
/// verdict is [`RedoVerdict::NeverDurable`]. `captured` holds the origins
/// of the stored write sets.
pub fn never_durable_redos(
    records: &[WalRecord],
    scanned: &WalGroups,
    captured: &HashSet<u64>,
) -> crate::Result<NeverDurableRedos> {
    // A marker of the WAL tail holds its chunks in the tail. A marker whose
    // chunks lie below the retained segments was checkpointed, so its group
    // is long whole.
    let mut fetch = |_: &RedoStreamRef| -> crate::Result<Option<Vec<u8>>> { Ok(None) };
    let mut contents = StreamContents::new(records, &mut fetch);
    let mut never_durable = NeverDurableRedos::default();
    for record in records {
        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::TransactionRedo) {
            continue;
        }
        let lsn = record.header.lsn;
        let verdict = redo_verdict(captured.contains(&lsn), scanned.groups.get(&lsn));
        if verdict == RedoVerdict::Complete {
            continue;
        }
        let redo = RedoRecord::from_bytes(&record.payload)?;
        let folds = match RedoStreamRef::of_marker(&redo)? {
            Some(reference) => contents
                .installed(&redo, &reference)?
                .is_some_and(|installed| !installed.sum_targets.is_empty()),
            None => !redo.sum_targets.is_empty(),
        };
        if folds {
            never_durable.insert(lsn);
        }
    }
    Ok(never_durable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::{NO_APPLY_KEY, WalManager};
    use crate::wal::{WriteGroup, WriteGroupRecord};
    use nodedb_physical::physical_plan::{RedoSumTargets, ResolvedSumTarget};

    fn scan_with(seen: bool, closed: bool) -> GroupScan {
        let mut scan = GroupScan::default();
        if seen {
            scan.parts = Some(2);
            scan.seen.insert(
                1,
                (
                    9,
                    super::super::scan::RecordHome {
                        tenant_id: TenantId::new(1),
                        vshard_id: VShardId::new(0),
                        database_id: DatabaseId::DEFAULT,
                        apply_key: NO_APPLY_KEY,
                        event_source: 0,
                        commit_hlc: 0,
                    },
                ),
            );
        }
        if closed {
            scan.closed = Some((1, false));
        }
        scan
    }

    #[test]
    fn a_stored_write_set_or_a_part_makes_the_install_complete() {
        assert_eq!(redo_verdict(true, None), RedoVerdict::Complete);
        assert_eq!(
            redo_verdict(true, Some(&scan_with(false, false))),
            RedoVerdict::Complete
        );
        assert_eq!(
            redo_verdict(false, Some(&scan_with(true, false))),
            RedoVerdict::Complete
        );
        assert_eq!(
            redo_verdict(false, Some(&scan_with(false, true))),
            RedoVerdict::Complete
        );
    }

    #[test]
    fn no_write_set_and_no_part_is_never_durable() {
        assert_eq!(redo_verdict(false, None), RedoVerdict::NeverDurable);
        assert_eq!(
            redo_verdict(false, Some(&scan_with(false, false))),
            RedoVerdict::NeverDurable
        );
    }

    fn redo(targets: Vec<RedoSumTargets>) -> RedoRecord {
        RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: targets,
        }
    }

    fn targets() -> Vec<RedoSumTargets> {
        vec![RedoSumTargets {
            collection: "charges".into(),
            resolved: vec![ResolvedSumTarget::new(
                "balances",
                "a1",
                nodedb_types::Surrogate::new(5),
            )],
        }]
    }

    /// Only a record that folds sums and whose group holds no part gets the
    /// never-durable verdict. A record with no targets, a record with a
    /// part and a record with a stored write set do not.
    #[test]
    fn only_a_folding_record_with_nothing_stored_is_never_durable() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let appender = wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User);
        let (t, v, d) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        let group = |group: WriteGroup| WriteGroupRecord {
            schemas: Vec::new(),
            group,
            ops: Vec::new(),
            redo: None,
        };
        let origin = |record: &RedoRecord| {
            let lsn = appender
                .append_transaction_redo(t, v, d, record)
                .expect("redo");
            appender
                .append_write_group(t, v, d, &group(WriteGroup::announcing(lsn.as_u64())))
                .expect("announce");
            lsn.as_u64()
        };
        let cut = origin(&redo(targets()));
        let plain = origin(&redo(Vec::new()));
        let parted = origin(&redo(targets()));
        let captured = origin(&redo(targets()));
        appender
            .append_write_group(t, v, d, &group(WriteGroup::part_of(parted, 1, 1)))
            .expect("part");
        wal.sync().expect("sync");

        let records = wal.replay().expect("replay");
        let scanned = WalGroups::scan(&records).expect("scan");
        let verdicts =
            never_durable_redos(&records, &scanned, &HashSet::from([captured])).expect("verdicts");
        assert_eq!(verdicts.iter().collect::<Vec<_>>(), vec![cut]);
        assert!(!verdicts.contains(plain));
    }
}
