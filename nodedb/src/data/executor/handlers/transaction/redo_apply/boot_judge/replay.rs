// SPDX-License-Identifier: BUSL-1.1

//! Restart replay that judges each committed record with no outcome at its
//! place in the stream.
//!
//! Boot settle names the records whose install never answered (see
//! `wal::redo::never_durable`). This core judges each one its vShard routes
//! here, in one forward walk of the stream in applying order:
//!
//! 1. Replay applies every record that applies below the judged record's
//!    LSN, and no record at or above it. A record of the judged record's
//!    vShard after it was queued behind its install and never ran, so the
//!    stores hold that vShard as the live install found it. The sync marks
//!    below the record are raised too.
//! 2. The judge runs the live install's validate on that state.
//! 3. Every verdict is durable before replay writes past its record: a
//!    second validate on stores that hold later writes can reach another
//!    verdict. Verdicts with no write between them share one commit.
//! 4. A pass replays the record with the records after it. A refusal
//!    leaves the stream with every record that applies at its LSN.
//!
//! A stored verdict decides its record without a validate, whichever core
//! stored it. A record a batched install left a cursor for passed its
//! validate live: replay completes it forward. Boot settle then writes each
//! verdict to the WAL.

use std::collections::{BTreeSet, HashMap};

use nodedb_wal::record::RecordType;
use nodedb_wal::{TombstoneSet, WalRecord};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::wal::redo::StoredVerdict;
use crate::wal::{BootVerdict, BootVerdictOutcome, JudgedRedo};

use super::segments::{ApplyingOrder, writes_nothing};

impl CoreLoop {
    /// [`Self::replay_engines_in_lsn_order`] over `records`, judging each
    /// committed record with no outcome this core owns at its place.
    pub(crate) fn replay_engines_judging(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &TombstoneSet,
    ) -> crate::Result<()> {
        let judged: Vec<JudgedRedo> = self
            .redo_apply
            .replay_folds
            .never_durable()
            .judged_records()
            .cloned()
            .collect();
        let redos = redo_records(records, &judged);
        let owned: Vec<(JudgedRedo, &WalRecord)> = judged
            .into_iter()
            .filter(|judged| owner_core(judged.vshard_id, num_cores) == self.core_id)
            .filter_map(|judged| redos.get(&judged.lsn).map(|record| (judged, *record)))
            .collect();
        if owned.is_empty() {
            return self.replay_engines_in_lsn_order(records, num_cores, tombstones);
        }
        let mut order = ApplyingOrder::new(records)?;
        let mut refused: BTreeSet<u64> = owned
            .iter()
            .filter(|(judged, _)| {
                judged
                    .stored
                    .is_some_and(|outcome| outcome != BootVerdictOutcome::Passed)
            })
            .map(|(judged, _)| judged.lsn)
            .collect();
        // Verdicts not yet durable.
        let mut pending: Vec<(u64, BootVerdictOutcome)> = Vec::new();
        for (judged, record) in owned {
            let lsn = judged.lsn;
            let outcome = match judged.stored {
                Some(outcome) => outcome,
                // A batched install passed its validate live and left a
                // cursor. Its verdict is stored like any other, before replay
                // completes it and boot drops the cursor: a crash after the
                // drop leaves the record unsettled, and the stored verdict
                // keeps the next boot from judging an installed record.
                None if self.boot_install_cursors().contains(lsn) => {
                    pending.push((lsn, BootVerdictOutcome::Passed));
                    BootVerdictOutcome::Passed
                }
                None => {
                    let below = order.take_below(records, lsn, &refused);
                    if !writes_nothing(&below)? {
                        self.store_boot_verdicts(&mut pending)?;
                    }
                    self.replay_engines_within(&below, records, num_cores, tombstones)?;
                    drop(below);
                    // An arm that halted stopped replay: the caller reports
                    // it.
                    if self.replay_halt_error().is_some() {
                        return Ok(());
                    }
                    self.install_sync_hwm_maps(order.marks_below(lsn));
                    let outcome = self.judge_boot_redo(record, &judged)?;
                    pending.push((lsn, outcome));
                    outcome
                }
            };
            if outcome != BootVerdictOutcome::Passed {
                refused.insert(lsn);
            }
            self.redo_apply
                .boot_verdicts
                .push(BootVerdict { judged, outcome });
        }
        self.store_boot_verdicts(&mut pending)?;
        let rest = order.take_below(records, u64::MAX, &refused);
        self.replay_engines_within(&rest, records, num_cores, tombstones)
    }

    /// The verdicts replay reached on the records it judged. Boot takes
    /// them once replay ended.
    pub(crate) fn take_boot_verdicts(&mut self) -> Vec<BootVerdict> {
        std::mem::take(&mut self.redo_apply.boot_verdicts)
    }

    /// Store every verdict of `pending` in one durable commit, then empty
    /// it.
    fn store_boot_verdicts(
        &mut self,
        pending: &mut Vec<(u64, BootVerdictOutcome)>,
    ) -> crate::Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let stored = pending
            .iter()
            .map(|&(lsn, outcome)| {
                StoredVerdict { lsn, outcome }
                    .to_bytes()
                    .map(|bytes| (lsn, bytes))
            })
            .collect::<crate::Result<Vec<_>>>()
            .and_then(|verdicts| self.sparse.put_boot_verdicts(&verdicts));
        if let Err(error) = stored {
            self.fail_stop_core(
                FailStopCause::InstallProgressUnpersisted,
                &format!(
                    "the boot verdicts on {} redo records could not be stored: {error}",
                    pending.len()
                ),
            );
            return Err(error);
        }
        #[cfg(test)]
        {
            self.redo_apply.boot_verdict_commits += 1;
        }
        pending.clear();
        Ok(())
    }
}

/// The core the replay arms route vShard `vshard_id` to.
fn owner_core(vshard_id: u32, num_cores: usize) -> usize {
    if num_cores > 0 {
        vshard_id as usize % num_cores
    } else {
        0
    }
}

/// The `TransactionRedo` record of each of `judged`, by LSN.
fn redo_records<'a>(
    records: &'a [WalRecord],
    judged: &[JudgedRedo],
) -> HashMap<u64, &'a WalRecord> {
    let wanted: BTreeSet<u64> = judged.iter().map(|judged| judged.lsn).collect();
    records
        .iter()
        .filter(|record| {
            wanted.contains(&record.header.lsn)
                && RecordType::from_raw(record.logical_record_type())
                    == Some(RecordType::TransactionRedo)
        })
        .map(|record| (record.header.lsn, record))
        .collect()
}
