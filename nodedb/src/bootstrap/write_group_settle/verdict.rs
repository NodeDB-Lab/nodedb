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
//!
//! A never-durable record has no outcome, whether it folds or not: the live
//! install stores its write set before it answers. Each one whose commit
//! boundary the WAL holds is a `JudgedRedo`, which replay validates at its
//! place in the stream.

use std::collections::HashSet;

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::wal::redo::StreamContents;
use crate::wal::{
    JudgedRedo, NeverDurableRedos, RedoJudgeError, RedoRecord, RedoStreamRef, StoredBoundary,
    boundaries_in, boundary_of,
};

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

/// The committed records of `records` whose verdict is
/// [`RedoVerdict::NeverDurable`]: each that carries sum targets replay
/// folds, and each that carries a commit boundary replay judges.
/// `captured` holds the origins of the stored write sets.
///
/// A never-durable record's install never answered: the live install
/// stores its write set before it answers. So the record has no outcome,
/// and replay judges it as the install would have. A Calvin slice carries
/// no boundary and keeps the plain replay: its stage vote validated it.
/// Any other record without a complete boundary is
/// [`RedoJudgeError::BoundaryMissing`]: the funnel appends the boundary
/// below the record, and the WAL is durable in LSN order.
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
    let boundaries = boundaries_in(records)?;
    let mut never_durable = NeverDurableRedos::default();
    for record in records {
        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::TransactionRedo) {
            continue;
        }
        let lsn = record.header.lsn;
        let group = scanned.groups.get(&lsn);
        let verdict = redo_verdict(captured.contains(&lsn), group);
        if verdict == RedoVerdict::Complete {
            continue;
        }
        let redo = RedoRecord::from_bytes(&record.payload)?;
        // A Calvin slice ignores any boundary: pieces of a failed append can
        // name the LSN it took.
        if redo.calvin_stamp.is_none() {
            let Some(stored) = boundary_of(&boundaries, record) else {
                crate::diag::redo_boundary_missing(lsn, record.header.vshard_id);
                return Err(RedoJudgeError::BoundaryMissing { lsn }.into());
            };
            never_durable.insert_judged(judged_redo(record, group, stored)?);
        }
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

/// The committed record `record` as replay judges it, with the records a
/// refusal cancels beside it: its group's announcement and continuations,
/// and the records of its boundary `stored`.
///
/// The record's event source is checked here, once: an unknown code is
/// [`RedoJudgeError::EventSourceUnknown`].
fn judged_redo(
    record: &WalRecord,
    group: Option<&GroupScan>,
    stored: &StoredBoundary,
) -> crate::Result<JudgedRedo> {
    let header = &record.header;
    let Some(event_source) = crate::event::EventSource::from_wal_code(header.event_source) else {
        crate::diag::redo_event_source_unknown(header.lsn, header.event_source);
        return Err(RedoJudgeError::EventSourceUnknown {
            lsn: header.lsn,
            code: header.event_source,
        }
        .into());
    };
    let mut members: Vec<u64> = group
        .map(|scan| scan.members.iter().map(|(lsn, _)| *lsn).collect())
        .unwrap_or_default();
    members.extend(stored.lsns.iter().copied());
    Ok(JudgedRedo {
        lsn: header.lsn,
        tenant_id: header.tenant_id,
        vshard_id: header.vshard_id,
        database_id: header.database_id,
        apply_key: header.apply_key,
        event_source,
        commit_hlc: header.commit_hlc,
        boundary: stored.boundary.clone(),
        members,
        stored: None,
    })
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

    /// A Calvin slice's record: it carries no boundary, and boot replays it
    /// with no validate.
    fn redo(targets: Vec<RedoSumTargets>) -> RedoRecord {
        RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: Some(crate::wal::CalvinStamp {
                epoch: 1,
                position: 0,
                vshard_id: 0,
                mirror_database: None,
            }),
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: targets,
            pins: Default::default(),
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

    /// A record with no outcome that is no Calvin slice and has no
    /// boundary fails the boot: the WAL lost records below it.
    #[test]
    fn a_record_with_no_outcome_and_no_boundary_fails_the_boot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let appender = wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User);
        let mut record = redo(Vec::new());
        record.calvin_stamp = None;
        let lsn = appender
            .append_transaction_redo(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                &record,
            )
            .expect("redo");
        wal.sync().expect("sync");
        let records = wal.replay().expect("replay");
        let scanned = WalGroups::scan(&records).expect("scan");
        let error =
            never_durable_redos(&records, &scanned, &HashSet::new()).expect_err("no boundary");
        assert!(error.to_string().contains(&format!("lsn {}", lsn.as_u64())));
    }
}
