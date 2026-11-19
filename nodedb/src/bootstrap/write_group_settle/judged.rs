// SPDX-License-Identifier: BUSL-1.1

//! Settle the committed records restart replay judged, before the node
//! serves.
//!
//! - A refused record is cancelled as the live refusal cancels it: one
//!   `WriteAborted` marker for the record and one for each record of its
//!   group's start and its boundary. A final refusal's markers carry the
//!   record's apply key, so the proposal ledger counts the refusal as the
//!   entry's outcome.
//! - A passed record's group closes with one part with no rows, as a live
//!   install's group does. A record whose fold rows replay stored closes
//!   through those rows instead (see [`super::close_replayed_groups`]).
//!
//! Every core stores each verdict durably before replay goes past the
//! record, so a crash before these records are durable leaves the next boot
//! the same verdicts.

use std::collections::HashSet;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use crate::wal::{BootVerdict, BootVerdictOutcome, WriteGroup, WriteGroupRecord, WriteSetCapture};

/// Cancel each refused record of `verdicts` and close the group of each
/// passed one, then make the records durable. `captures` are the write sets
/// replay stored: a passed record among them closes through its own parts.
/// Returns whether it appended a record.
pub fn close_judged_redos(
    wal: &WalManager,
    verdicts: &[BootVerdict],
    captures: &[WriteSetCapture],
) -> crate::Result<bool> {
    let captured: HashSet<u64> = captures.iter().map(|capture| capture.origin).collect();
    let mut appended = false;
    for verdict in verdicts {
        let judged = &verdict.judged;
        let (tenant_id, vshard_id, database_id) = (
            TenantId::new(judged.tenant_id),
            VShardId::new(judged.vshard_id),
            DatabaseId::new(judged.database_id),
        );
        match verdict.outcome {
            BootVerdictOutcome::Refused { final_refusal } => {
                let marker_key = if final_refusal {
                    judged.apply_key
                } else {
                    NO_APPLY_KEY
                };
                let appender = wal.appender(marker_key);
                for lsn in std::iter::once(judged.lsn).chain(judged.members.iter().copied()) {
                    appender.append_write_aborted(
                        tenant_id,
                        vshard_id,
                        database_id,
                        Lsn::new(lsn),
                    )?;
                }
                appended = true;
            }
            BootVerdictOutcome::Passed => {
                if captured.contains(&judged.lsn) {
                    continue;
                }
                // The part carries the apply key, event source and commit
                // instant of the record it closes, as every part of a write
                // does.
                wal.appender(judged.apply_key)
                    .with_event_source(judged.event_source)
                    .with_commit_hlc(judged.commit_hlc)
                    .append_write_group(
                        tenant_id,
                        vshard_id,
                        database_id,
                        &WriteGroupRecord {
                            schemas: Vec::new(),
                            group: WriteGroup::part_of(judged.lsn, 1, 1),
                            ops: Vec::new(),
                            redo: None,
                        },
                    )?;
                appended = true;
            }
        }
    }
    if appended {
        wal.sync()?;
    }
    Ok(appended)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventSource;
    use crate::wal::{CommitBoundary, JudgedRedo, RedoRecord};
    use nodedb_physical::physical_plan::RedoOrigin;

    fn record() -> RedoRecord {
        RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        }
    }

    /// A committed record, its boundary and its announcement, as the funnel
    /// appends them, judged.
    fn appended(wal: &WalManager, apply_key: u64) -> JudgedRedo {
        let appender = wal.appender(apply_key).with_event_source(EventSource::User);
        let (t, v, d) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        let boundary = CommitBoundary {
            origin: RedoOrigin::Commit,
            snapshot_ordinal: None,
            reads: Vec::new(),
            judged_at_ms: 1,
        };
        let payload = record().to_bytes().expect("encode");
        let lsn = appender
            .append_bounded_transaction_redo_bytes(t, v, d, &payload, boundary.clone())
            .expect("redo")
            .as_u64();
        // A boundary this small takes one piece, right below its record.
        let boundary_lsn = lsn - 1;
        let announce = appender
            .append_write_group(
                t,
                v,
                d,
                &WriteGroupRecord {
                    schemas: Vec::new(),
                    group: WriteGroup::announcing(lsn),
                    ops: Vec::new(),
                    redo: None,
                },
            )
            .expect("announce")
            .as_u64();
        JudgedRedo {
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: DatabaseId::DEFAULT.as_u64(),
            apply_key,
            event_source: EventSource::User,
            commit_hlc: 0,
            boundary: boundary.with_crc(crc32c::crc32c(&payload)),
            members: vec![announce, boundary_lsn],
            stored: None,
        }
    }

    /// A refused record and every record of its start leave the replayed
    /// stream. A final refusal's markers carry the apply key, which the
    /// proposal ledger counts as the entry's outcome.
    #[test]
    fn a_refused_record_is_cancelled_with_its_start() {
        const KEY: u64 = 0x5EED;
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let judged = appended(&wal, KEY);
        let cancelled: Vec<u64> = std::iter::once(judged.lsn)
            .chain(judged.members.iter().copied())
            .collect();
        let verdict = BootVerdict {
            judged,
            outcome: BootVerdictOutcome::Refused {
                final_refusal: true,
            },
        };
        assert!(close_judged_redos(&wal, &[verdict], &[]).expect("close"));
        let records = wal.replay().expect("replay");
        assert!(
            !records
                .iter()
                .any(|record| cancelled.contains(&record.header.lsn)),
            "no record of the refused write replays"
        );
        let ledger = crate::control::distributed_applier::ProposalLedger::from_records(&records, 8);
        assert!(
            ledger.prior(KEY).is_some(),
            "the refusal is the entry's outcome"
        );
    }

    /// A passed record's group closes with one part, so the next boot finds
    /// it whole and judges it no more.
    #[test]
    fn a_passed_record_closes_its_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let judged = appended(&wal, NO_APPLY_KEY);
        let lsn = judged.lsn;
        let verdict = BootVerdict {
            judged,
            outcome: BootVerdictOutcome::Passed,
        };
        assert!(close_judged_redos(&wal, &[verdict], &[]).expect("close"));
        let records = wal.replay().expect("replay");
        let scanned = super::super::WalGroups::scan(&records).expect("scan");
        assert!(scanned.groups[&lsn].is_whole());
        let never_durable = super::super::never_durable_redos(&records, &scanned, &HashSet::new())
            .expect("verdicts");
        assert!(never_durable.judged(lsn).is_none());
    }
}
