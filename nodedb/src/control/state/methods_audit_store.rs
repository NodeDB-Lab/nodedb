// SPDX-License-Identifier: BUSL-1.1

//! Audit buffer flush, retention pruning, and merged reads for `SharedState`.
//!
//! The in-memory buffer and the catalog together hold the whole audit log.
//! A flush commits buffered entries to the catalog and only then evicts them.
//! Readers copy the buffer first and read the catalog after.
//! So a reader sees every entry recorded before it started.
//!
//! An entry recorded while the buffer is full lives only in the audit WAL.
//! A flush commits those entries from the WAL, and until then a reader reads them there.

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use tracing::{debug, info, warn};

use super::SharedState;
use crate::control::security::audit::guard::{AuditLogGuard, lock_audit_log};
use crate::control::security::audit::log::WalOnlyRange;
use crate::control::security::audit::{
    AuditEntry, AuditEvent, ChainReport, ChainVerifier, checkpoint_detail,
};
use crate::control::security::catalog::audit::AuditPrunePlan;

/// Microseconds in one day, for the age-based retention cutoff.
const MICROS_PER_DAY: u64 = 86_400 * 1_000_000;

/// Feeds a verifier in ascending seq order, with each seq once, through the tail seq.
///
/// A seq can sit in two sources while a flush moves it, and a WAL scan can repeat one.
struct ChainFeed<'a> {
    verifier: ChainVerifier<'a>,
    last_fed: u64,
    last_seq: u64,
}

impl ChainFeed<'_> {
    fn push(&mut self, entry: &AuditEntry) {
        if entry.seq > self.last_fed && entry.seq <= self.last_seq {
            self.verifier.push(entry);
            self.last_fed = entry.seq;
        }
    }
}

/// Merge persisted and buffered entries by seq and keep the newest `limit`.
///
/// A seq present in both sources appears once. The result is in ascending seq order.
fn merge_newest(
    persisted: Vec<AuditEntry>,
    buffered: Vec<AuditEntry>,
    limit: usize,
) -> Vec<AuditEntry> {
    let mut by_seq: BTreeMap<u64, AuditEntry> = persisted.into_iter().map(|e| (e.seq, e)).collect();
    by_seq.extend(buffered.into_iter().map(|e| (e.seq, e)));
    let skip = by_seq.len().saturating_sub(limit);
    by_seq.into_values().skip(skip).collect()
}

impl SharedState {
    fn lock_audit(&self) -> AuditLogGuard<'_> {
        lock_audit_log(&self.audit)
    }

    /// Visit the entries of `range` from the audit WAL, one at a time, in ascending seq order.
    ///
    /// A record outside `range` is never kept. A malformed record is skipped with a warning.
    /// The visitor can see an entry twice, when a truncation restarts the scan.
    fn visit_wal_only(
        &self,
        range: WalOnlyRange,
        mut visit: impl FnMut(AuditEntry) -> crate::Result<()>,
    ) -> crate::Result<()> {
        self.wal.scan_audit_entries(|_data_lsn, bytes| {
            match zerompk::from_msgpack::<AuditEntry>(bytes) {
                Ok(entry) if entry.seq > range.through_seq => return Ok(ControlFlow::Break(())),
                Ok(entry) if entry.seq >= range.first_seq => visit(entry)?,
                Ok(_) => {}
                Err(e) => warn!(error = %e, "skipping malformed audit WAL record"),
            }
            Ok(ControlFlow::Continue(()))
        })
    }

    /// The newest `limit` entries that live only in the audit WAL now and pass `filter`.
    ///
    /// The scan holds at most `limit` entries, plus the one it is reading.
    fn wal_only_entries(
        &self,
        limit: usize,
        filter: impl Fn(&AuditEntry) -> bool,
    ) -> crate::Result<Vec<AuditEntry>> {
        let Some(range) = self.lock_audit().wal_only() else {
            return Ok(Vec::new());
        };
        let mut newest: BTreeMap<u64, AuditEntry> = BTreeMap::new();
        self.visit_wal_only(range, |entry| {
            if filter(&entry) {
                newest.insert(entry.seq, entry);
                if newest.len() > limit {
                    newest.pop_first();
                }
            }
            Ok(())
        })?;
        Ok(newest.into_values().collect())
    }

    /// Newest `limit` audit entries that pass `filter`, in ascending seq order.
    ///
    /// The result holds every entry recorded before this call started.
    pub fn recent_audit_entries(
        &self,
        limit: usize,
        filter: impl Fn(&AuditEntry) -> bool,
    ) -> crate::Result<Vec<AuditEntry>> {
        let mut buffered: Vec<AuditEntry> = self
            .lock_audit()
            .all()
            .iter()
            .rev()
            .filter(|entry| filter(entry))
            .take(limit)
            .cloned()
            .collect();
        buffered.extend(self.wal_only_entries(limit, &filter)?);
        let persisted = self
            .credentials
            .catalog()
            .load_recent_audit_entries(limit, &filter)?;
        Ok(merge_newest(persisted, buffered, limit))
    }

    /// DML audit entries whose event LSN lies above its core's floor, in ascending seq order.
    ///
    /// `floors[c]` is core `c`'s floor. A core at or past `floors.len()` has floor 0.
    /// The buffer is copied first and the catalog index is read after, as in [`Self::recent_audit_entries`].
    pub fn dml_audits_above(&self, floors: &[u64]) -> crate::Result<Vec<AuditEntry>> {
        let above = |entry: &AuditEntry| {
            entry.event == AuditEvent::DmlAudit
                && crate::event::sink_ledger::audit::dml_event_position(&entry.detail).is_some_and(
                    |(core, lsn)| lsn > floors.get(core as usize).copied().unwrap_or(0),
                )
        };
        let mut buffered: Vec<AuditEntry> = self
            .lock_audit()
            .all()
            .iter()
            .filter(|entry| above(entry))
            .cloned()
            .collect();
        buffered.extend(self.wal_only_entries(usize::MAX, above)?);
        let persisted = self.credentials.catalog().load_dml_audits_above(floors)?;
        Ok(merge_newest(persisted, buffered, usize::MAX))
    }

    /// Verify the hash chain and report on the seqs in `from_seq..=to_seq`.
    ///
    /// Returns the report and the highest seq the log holds in that range.
    /// The scan holds one catalog or WAL entry at a time, plus a copy of the bounded buffer.
    /// It ends at the seq the tail hash belongs to, so a later record changes nothing.
    /// While entries live only in the WAL, the flush gate stays held, so no truncation moves them.
    /// That is only during a catalog outage, when flushes fail anyway.
    pub fn verify_audit_chain(
        &self,
        from_seq: u64,
        to_seq: u64,
    ) -> crate::Result<(ChainReport, Option<u64>)> {
        crate::control::security::audit::log_commit::hand_off_worker(|| {
            let control = self.lock_audit().flush_control();
            let gate = control.serialize();
            let (buffered, wal_only, last_seq, tail_hash) = {
                let log = self.lock_audit();
                (
                    log.all().iter().cloned().collect::<Vec<_>>(),
                    log.wal_only(),
                    log.last_seq(),
                    log.last_hash().to_string(),
                )
            };
            let _gate = wal_only.is_some().then_some(gate);

            let mut feed = ChainFeed {
                verifier: ChainVerifier::new(from_seq, to_seq, &tail_hash),
                last_fed: 0,
                last_seq,
            };
            self.credentials
                .catalog()
                .scan_audit_entries(last_seq, |entry| {
                    feed.push(&entry);
                    Ok(())
                })?;
            let mut buffered = buffered.iter().peekable();
            if let Some(range) = wal_only {
                self.visit_wal_only(range, |entry| {
                    // The two sources interleave by seq.
                    while let Some(earlier) = buffered.next_if(|e| e.seq <= entry.seq) {
                        feed.push(earlier);
                    }
                    feed.push(&entry);
                    Ok(())
                })?;
            }
            for entry in buffered {
                feed.push(entry);
            }
            let last_in_range = feed.verifier.last_in_range();
            Ok((feed.verifier.finish(), last_in_range))
        })
    }

    /// Flush buffered audit entries to the catalog, then apply retention pruning.
    ///
    /// Entries stay in the buffer until the catalog commit succeeds.
    /// On a catalog error they stay buffered for the next flush.
    /// Flushes are serialized, so two callers never interleave.
    /// The flush executor and explicit maintenance calls run this. A recorder never does.
    /// A flush that committed wakes every recorder waiting for backpressure.
    /// A flush that failed wakes none, so those recorders wait out their time bound.
    /// Returns whether the buffer reached the catalog.
    pub fn flush_audit_log(&self) -> bool {
        let control = self.lock_audit().flush_control();
        let committed = {
            let _serialized = control.serialize();
            let committed = self.persist_audit_buffer();
            if committed {
                self.prune_audit_catalog();
            }
            committed
        };
        if committed {
            control.note_flush_completed();
        }
        committed
    }

    /// Commit `entries` to the catalog.
    fn commit_audit_entries(&self, entries: &[AuditEntry]) -> crate::Result<()> {
        #[cfg(test)]
        if self.lock_audit().flush_control().catalog_fault_injected() {
            return Err(crate::Error::Internal {
                detail: "injected audit catalog error".into(),
            });
        }
        self.credentials.catalog().append_audit_entries(entries)
    }

    /// Commit the buffer and the WAL-only entries to the catalog, and evict what was committed.
    ///
    /// Returns `false` when a catalog commit failed.
    fn persist_audit_buffer(&self) -> bool {
        let (entries, wal_only) = {
            let log = self.lock_audit();
            (log.snapshot_for_persistence(), log.wal_only())
        };
        let mut committed_position = None;
        if let Some(through_seq) = entries.last().map(|e| e.seq) {
            if let Err(e) = self.commit_audit_entries(&entries) {
                warn!(
                    error = %e,
                    count = entries.len(),
                    "failed to persist audit entries, kept for the next flush"
                );
                crate::diag::audit_flush_not_persisted(&e, entries.len(), through_seq);
                return false;
            }
            committed_position = self.lock_audit().evict_persisted(through_seq);
            debug!(count = entries.len(), "flushed audit entries to catalog");
        }
        if let Some(range) = wal_only {
            if !self.commit_wal_only(range) {
                return false;
            }
            committed_position = committed_position.max(Some(range.position));
        }
        // The catalog holds every entry through `committed_position`, so the WAL drops them.
        // A truncation error leaves the records in place, and the next flush covers them.
        let wal_sink = self.lock_audit().wal_sink();
        if let (Some(position), Some(sink)) = (committed_position, wal_sink)
            && let Err(e) = sink.truncate_through(position)
        {
            warn!(error = %e, position, "failed to truncate the audit WAL");
            crate::diag::audit_wal_not_truncated(&e, position);
        }
        true
    }

    /// Commit the entries of `range` from the audit WAL to the catalog, in bounded batches.
    ///
    /// Returns `false` when a read or a commit failed. The range stays for the next flush.
    fn commit_wal_only(&self, range: WalOnlyRange) -> bool {
        let chunk_len = crate::control::security::audit::DEFAULT_FLUSH_BOUND;
        let mut chunk = Vec::with_capacity(chunk_len);
        let mut committed = 0usize;
        let scanned = self.visit_wal_only(range, |entry| {
            chunk.push(entry);
            if chunk.len() >= chunk_len {
                self.commit_audit_entries(&chunk)?;
                committed += chunk.len();
                chunk.clear();
            }
            Ok(())
        });
        let last_chunk = scanned.and_then(|()| self.commit_audit_entries(&chunk));
        if let Err(e) = last_chunk {
            warn!(
                error = %e,
                committed,
                "failed to persist WAL-only audit entries, kept for the next flush"
            );
            crate::diag::audit_flush_not_persisted(&e, committed, range.through_seq);
            return false;
        }
        debug!(
            count = committed + chunk.len(),
            "flushed WAL-only audit entries to catalog"
        );
        self.lock_audit().note_wal_only_committed(range);
        true
    }

    /// Apply the age and count retention limits to the persisted audit log.
    fn prune_audit_catalog(&self) {
        if self.audit_retention_days > 0 {
            self.prune_audit_by_age(self.audit_retention_days);
        }
        if self.audit_max_entries > 0 {
            self.prune_audit_by_count(self.audit_max_entries);
        }
    }

    /// Prune persisted entries older than `retention_days`.
    fn prune_audit_by_age(&self, retention_days: u32) {
        let retention_us = u64::from(retention_days).saturating_mul(MICROS_PER_DAY);
        let now_us = crate::control::security::audit::entry::now_us();
        let cutoff = now_us.saturating_sub(retention_us);
        let plan = self.credentials.catalog().plan_audit_prune_before(cutoff);
        self.apply_audit_prune("retention_age", plan);
    }

    /// Prune the oldest persisted entries until at most `max_entries` remain.
    fn prune_audit_by_count(&self, max_entries: u64) {
        let plan = self
            .credentials
            .catalog()
            .plan_audit_prune_to_count(max_entries);
        self.apply_audit_prune("retention_count", plan);
    }

    /// Record the prune's checkpoint, persist it, then delete the planned rows.
    ///
    /// The checkpoint names the hash the first surviving entry links to.
    /// It reaches the catalog before any row is deleted.
    /// So a crash cannot leave a surviving head that the chain verifier cannot anchor.
    fn apply_audit_prune(&self, source: &'static str, plan: crate::Result<Option<AuditPrunePlan>>) {
        let plan = match plan {
            Ok(Some(plan)) => plan,
            Ok(None) => return,
            Err(e) => {
                warn!(error = %e, source, "failed to plan audit pruning");
                crate::diag::audit_prune_not_applied(&e, source);
                return;
            }
        };
        let detail = checkpoint_detail(
            plan.pruned_count,
            plan.through_seq.saturating_add(1),
            &plan.anchor_hash,
        );
        let (durable, seq, _) =
            self.audit_record_buffered(AuditEvent::AuditCheckpoint, None, None, source, &detail);
        if let Err(e) = durable {
            warn!(error = %e, source, "audit checkpoint WAL write failed");
            crate::diag::audit_wal_append_failed(&e, seq);
        }
        if !self.persist_audit_buffer() {
            return;
        }
        match self
            .credentials
            .catalog()
            .delete_audit_through(plan.through_seq)
        {
            Ok(pruned) => info!(
                pruned,
                through_seq = plan.through_seq,
                source,
                "pruned audit entries"
            ),
            Err(e) => {
                warn!(error = %e, source, "failed to prune audit entries");
                crate::diag::audit_prune_not_applied(&e, source);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create audit store test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("audit-store.wal"))
                .expect("open audit store test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct audit store state");
        (state, directory)
    }

    fn record(state: &SharedState, prefix: &str, count: usize) {
        for i in 0..count {
            state.audit_record(
                AuditEvent::AdminAction,
                None,
                "test",
                &format!("{prefix}-{i}"),
            );
        }
    }

    fn chain_verdict(state: &SharedState) -> Option<u64> {
        state
            .verify_audit_chain(1, u64::MAX)
            .expect("read audit chain")
            .0
            .broken_at
    }

    /// A reader with a limit keeps only that many entries of a long WAL-only range.
    #[tokio::test]
    async fn a_limit_bounds_the_wal_only_read() {
        use crate::control::security::audit::HARD_BOUND_MULTIPLE;

        let (state, _dir) = test_state();
        assert!(state.flush_audit_log(), "the buffer starts empty");
        state.lock_audit().set_flush_bound(1);
        state
            .lock_audit()
            .flush_control()
            .inject_catalog_fault(true);
        record(&state, "row", HARD_BOUND_MULTIPLE + 40);

        let newest = state
            .wal_only_entries(3, |entry| entry.detail.starts_with("row-"))
            .expect("read the WAL-only range");
        let last_seq = state.lock_audit().last_seq();
        assert_eq!(
            newest.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![last_seq - 2, last_seq - 1, last_seq],
            "only the newest entries stay"
        );

        let only_even = state
            .wal_only_entries(usize::MAX, |entry| entry.seq % 2 == 0)
            .expect("read the WAL-only range");
        assert!(only_even.iter().all(|entry| entry.seq % 2 == 0));
        assert_eq!(only_even.len(), 20, "the filter runs during the scan");

        let all = state
            .recent_audit_entries(5, |_| true)
            .expect("read the log");
        assert_eq!(all.len(), 5);
        assert_eq!(all.last().map(|e| e.seq), Some(last_seq));
    }

    /// The streaming verifier reads the WAL-only range and reports a range correctly.
    #[tokio::test]
    async fn chain_verification_covers_the_wal_only_range() {
        use crate::control::security::audit::HARD_BOUND_MULTIPLE;

        let (state, _dir) = test_state();
        assert!(state.flush_audit_log(), "the buffer starts empty");
        record(&state, "persisted", 3);
        assert!(state.flush_audit_log());
        state.lock_audit().set_flush_bound(1);
        let control = state.lock_audit().flush_control();
        control.inject_catalog_fault(true);
        record(&state, "row", HARD_BOUND_MULTIPLE + 5);
        assert!(state.lock_audit().wal_only().is_some());

        let last_seq = state.lock_audit().last_seq();
        let (report, last_in_range) = state.verify_audit_chain(1, u64::MAX).expect("verify");
        assert_eq!(report.broken_at, None);
        assert_eq!(report.checked, last_seq);
        assert_eq!(last_in_range, Some(last_seq));

        let (partial, last_in_range) = state.verify_audit_chain(2, 4).expect("verify");
        assert_eq!(partial.broken_at, None);
        assert_eq!(partial.checked, 3);
        assert_eq!(last_in_range, Some(4));

        // The scan released the gate, so a flush can run.
        control.inject_catalog_fault(false);
        assert!(state.flush_audit_log());
        assert_eq!(chain_verdict(&state), None);
    }

    #[tokio::test]
    async fn chain_stays_valid_across_flushes() {
        let (state, _dir) = test_state();
        record(&state, "before", 5);
        assert_eq!(chain_verdict(&state), None);
        state.flush_audit_log();
        assert_eq!(
            chain_verdict(&state),
            None,
            "a flush must not break the chain"
        );
        record(&state, "after", 3);
        assert_eq!(
            chain_verdict(&state),
            None,
            "the buffer must link to the catalog"
        );
        state.flush_audit_log();
        assert_eq!(chain_verdict(&state), None);
    }

    #[tokio::test]
    async fn count_prune_keeps_the_chain_anchored() {
        let (state, _dir) = test_state();
        record(&state, "old", 10);
        state.flush_audit_log();
        state.prune_audit_by_count(4);
        let catalog = state.credentials.catalog();
        assert_eq!(catalog.audit_entry_count().expect("count"), 4);
        assert_eq!(
            chain_verdict(&state),
            None,
            "a pruned head must stay anchored"
        );

        // The table is within the cap, so a second pass records no new checkpoint.
        state.prune_audit_by_count(4);
        assert_eq!(catalog.audit_entry_count().expect("count"), 4);

        record(&state, "new", 3);
        state.flush_audit_log();
        state.prune_audit_by_count(4);
        assert_eq!(chain_verdict(&state), None);
    }

    #[tokio::test]
    async fn age_prune_keeps_the_chain_anchored() {
        let (state, _dir) = test_state();
        record(&state, "old", 6);
        state.flush_audit_log();
        // A zero-day window makes every persisted entry older than the cutoff.
        state.prune_audit_by_age(0);
        assert_eq!(chain_verdict(&state), None);
        record(&state, "new", 2);
        assert_eq!(chain_verdict(&state), None);
    }

    #[tokio::test]
    async fn tampered_persisted_row_is_reported() {
        let (state, _dir) = test_state();
        record(&state, "row", 5);
        state.flush_audit_log();
        let catalog = state.credentials.catalog();
        let mut rows = catalog
            .load_recent_audit_entries(usize::MAX, |_| true)
            .expect("read rows");
        let target = rows.len() / 2;
        let tampered_seq = rows[target].seq;
        rows[target].detail = "tampered".to_string();
        catalog
            .append_audit_entries(&rows[target..=target])
            .expect("overwrite row");
        assert_eq!(
            chain_verdict(&state),
            Some(tampered_seq + 1),
            "the link after the altered row must fail"
        );
    }

    /// Run a flush executor on its own thread until `stop` is set, as the flush task does.
    fn spawn_executor(
        state: &Arc<SharedState>,
        stop: &Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        let state = Arc::clone(state);
        let stop = Arc::clone(stop);
        let control = state.lock_audit().flush_control();
        control.attach_executor();
        std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Acquire) {
                if control.take_flush_request() {
                    state.flush_audit_log();
                } else {
                    std::thread::yield_now();
                }
            }
            control.detach_executor();
        })
    }

    #[tokio::test]
    async fn burst_past_the_bound_loses_nothing() {
        let (state, _dir) = test_state();
        state.lock_audit().set_flush_bound(4);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executor = spawn_executor(&state, &stop);

        let first_seq = state.lock_audit().last_seq() + 1;
        record(&state, "central", 50);
        for i in 0..50 {
            state.lock_audit().record(
                AuditEvent::AdminAction,
                None,
                "test",
                &format!("direct-{i}"),
            );
        }
        let last_seq = state.lock_audit().last_seq();
        assert!(
            state
                .credentials
                .catalog()
                .audit_entry_count()
                .expect("count")
                > 0,
            "backpressure must have the executor flush during the burst"
        );

        stop.store(true, std::sync::atomic::Ordering::Release);
        executor.join().expect("executor thread");

        let in_burst = |e: &AuditEntry| (first_seq..=last_seq).contains(&e.seq);
        let seqs: Vec<u64> = state
            .recent_audit_entries(usize::MAX, in_burst)
            .expect("read burst")
            .iter()
            .map(|e| e.seq)
            .collect();
        assert_eq!(seqs, (first_seq..=last_seq).collect::<Vec<_>>());

        state.flush_audit_log();
        let persisted = state
            .credentials
            .catalog()
            .load_recent_audit_entries(usize::MAX, in_burst)
            .expect("read persisted burst");
        assert_eq!(
            persisted.len(),
            seqs.len(),
            "every burst entry must reach the catalog"
        );
        assert_eq!(chain_verdict(&state), None);
    }

    fn event_key(lsn: u64) -> crate::event::sink_ledger::SinkEventKey {
        crate::event::sink_ledger::SinkEventKey {
            core: 0,
            lsn,
            occurrence: 0,
            delete: false,
            collection: "orders".into(),
            row_kind: 0,
            row: format!("o-{lsn}"),
        }
    }

    fn record_dml(state: &SharedState, lsn: u64) {
        let detail = format!(
            "INSERT orders:o-{lsn} lsn={lsn}{}",
            crate::event::sink_ledger::audit::detail_suffix(&event_key(lsn))
        );
        state.audit_record(AuditEvent::DmlAudit, None, "alice", &detail);
    }

    #[tokio::test]
    async fn redelivery_above_the_floor_stays_deduplicated() {
        use crate::event::sink_ledger::AuditedKeys;

        let (state, _dir) = test_state();
        // Old rows and some rows above the floor reach the catalog.
        for lsn in 1..=500 {
            record_dml(&state, lsn);
        }
        state.flush_audit_log();
        // Newer rows stay in the buffer.
        for lsn in 501..=510 {
            record_dml(&state, lsn);
        }

        let floor = 495;
        let above = state.dml_audits_above(&[floor]).expect("read DML audits");
        assert_eq!(above.len(), 15, "only rows above the floor are read");

        let audited = AuditedKeys::from_entries(above.iter(), |_| floor);
        assert!(
            audited.contains(&event_key(498)),
            "a persisted row above the floor dedups"
        );
        assert!(
            audited.contains(&event_key(505)),
            "a buffered row above the floor dedups"
        );
        assert!(
            !audited.contains(&event_key(400)),
            "a row at or below the floor is not loaded"
        );
    }

    fn wal_seqs(state: &SharedState) -> Vec<u64> {
        state
            .wal
            .recover_audit_entries()
            .expect("recover audit WAL")
            .iter()
            .map(|(_, bytes)| {
                zerompk::from_msgpack::<AuditEntry>(bytes)
                    .expect("decode audit WAL record")
                    .seq
            })
            .collect()
    }

    #[tokio::test]
    async fn flush_truncates_the_wal_and_a_crash_still_restores_everything() {
        let (state, _dir) = test_state();
        record(&state, "old", 5);
        state.flush_audit_log();
        assert!(
            wal_seqs(&state).is_empty(),
            "the WAL must drop every record the catalog holds"
        );

        record(&state, "new", 3);
        let buffered: Vec<u64> = state.lock_audit().all().iter().map(|e| e.seq).collect();
        assert_eq!(
            wal_seqs(&state),
            buffered,
            "the WAL must hold only unflushed records"
        );

        // A crash loses the buffer. The catalog and the WAL survive it.
        let all_seqs: Vec<u64> = state
            .recent_audit_entries(usize::MAX, |_| true)
            .expect("read audit")
            .iter()
            .map(|e| e.seq)
            .collect();
        let catalog = state.credentials.catalog();
        crate::control::security::audit::restore_audit_log(catalog, &state.wal)
            .expect("boot restore");
        let restored: Vec<u64> = catalog
            .load_recent_audit_entries(usize::MAX, |_| true)
            .expect("read catalog")
            .iter()
            .map(|e| e.seq)
            .collect();
        assert_eq!(
            restored, all_seqs,
            "every entry must survive a crash after truncation"
        );
        assert!(
            wal_seqs(&state).is_empty(),
            "boot restore drops what it committed"
        );
    }

    /// A flush that cannot start, as when it waits on a transaction the recorder holds.
    /// The recorder must return after the bounded wait, and every entry must stay buffered.
    #[tokio::test]
    async fn stuck_executor_never_blocks_a_recorder_past_the_wait() {
        let (state, _dir) = test_state();
        let control = {
            let mut log = state.lock_audit();
            log.set_flush_bound(1);
            log.flush_control()
        };
        control.attach_executor();
        control.set_backpressure_wait(std::time::Duration::from_millis(20));

        let first_seq = state.lock_audit().last_seq() + 1;
        let started = std::time::Instant::now();
        record(&state, "stuck", 5);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "each wait must end at its bound"
        );
        let last_seq = state.lock_audit().last_seq();
        let buffered = state
            .lock_audit()
            .all()
            .iter()
            .filter(|e| (first_seq..=last_seq).contains(&e.seq))
            .count();
        assert_eq!(
            buffered, 5,
            "a timed-out wait must keep every entry buffered"
        );
        control.detach_executor();
    }

    /// A flush that failed releases no waiter, and it reports the failure to the executor.
    #[tokio::test]
    async fn a_failed_flush_releases_no_waiter() {
        let (state, _dir) = test_state();
        let control = state.lock_audit().flush_control();
        control.attach_executor();
        control.set_backpressure_wait(std::time::Duration::from_millis(300));
        record(&state, "row", 1);
        control.inject_catalog_fault(true);

        let waiter = {
            let control = Arc::clone(&control);
            std::thread::spawn(move || control.await_flush())
        };
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            !state.flush_audit_log(),
            "the flush must report the failure"
        );
        assert!(
            !waiter.join().expect("waiter thread"),
            "a failed flush must leave the waiter to its time bound"
        );
        assert_eq!(state.lock_audit().len(), 1, "the entry stays buffered");

        control.inject_catalog_fault(false);
        control.set_backpressure_wait(std::time::Duration::from_secs(30));
        let waiter = {
            let control = Arc::clone(&control);
            std::thread::spawn(move || control.await_flush())
        };
        // A commit made before the waiter starts its wait does not count, so commit until it ends.
        while !waiter.is_finished() {
            assert!(state.flush_audit_log(), "the catalog is back");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            waiter.join().expect("waiter thread"),
            "a committed flush releases the waiter"
        );
        control.detach_executor();
    }

    /// A catalog that stays down bounds the buffer. Later entries stay in the WAL, and readers
    /// and the chain still see them. The catalog takes them all once it is back.
    #[tokio::test]
    async fn a_full_buffer_spills_to_the_wal_and_loses_nothing() {
        use crate::control::security::audit::HARD_BOUND_MULTIPLE;

        let (state, _dir) = test_state();
        assert!(state.flush_audit_log(), "the buffer starts empty");
        state.lock_audit().set_flush_bound(2);
        let control = state.lock_audit().flush_control();
        control.inject_catalog_fault(true);
        let total = 2 * HARD_BOUND_MULTIPLE + 5;
        let first_seq = state.lock_audit().last_seq() + 1;

        record(&state, "row", total);
        assert!(!state.flush_audit_log(), "the catalog refuses the flush");

        assert_eq!(
            state.lock_audit().len(),
            2 * HARD_BOUND_MULTIPLE,
            "the buffer stays at its hard bound"
        );
        let last_seq = state.lock_audit().last_seq();
        let in_run = |e: &AuditEntry| (first_seq..=last_seq).contains(&e.seq);
        let seen = state
            .recent_audit_entries(usize::MAX, in_run)
            .expect("read audit");
        assert_eq!(seen.len(), total, "a reader sees the WAL-only entries");
        assert_eq!(chain_verdict(&state), None);

        control.inject_catalog_fault(false);
        assert!(state.flush_audit_log(), "the catalog is back");
        assert_eq!(state.lock_audit().wal_only(), None);
        assert!(state.lock_audit().is_empty());
        let persisted = state
            .credentials
            .catalog()
            .load_recent_audit_entries(usize::MAX, in_run)
            .expect("read catalog");
        assert_eq!(persisted.len(), total, "the catalog holds every entry");
        assert_eq!(chain_verdict(&state), None);

        record(&state, "after", 3);
        assert_eq!(
            state.lock_audit().len(),
            3,
            "the buffer takes entries again"
        );
        assert_eq!(chain_verdict(&state), None);
    }

    /// The flush path records its checkpoint without waiting, so a prune past the bound cannot stall.
    #[tokio::test]
    async fn prune_past_the_bound_does_not_wait_on_itself() {
        let (state, _dir) = test_state();
        record(&state, "old", 6);
        state.flush_audit_log();
        let control = {
            let mut log = state.lock_audit();
            log.set_flush_bound(1);
            log.flush_control()
        };
        control.attach_executor();
        control.set_backpressure_wait(std::time::Duration::from_secs(30));
        let started = std::time::Instant::now();
        state.prune_audit_by_count(2);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "the checkpoint record must not wait for a flush"
        );
        control.detach_executor();
        assert_eq!(chain_verdict(&state), None);
    }
}
