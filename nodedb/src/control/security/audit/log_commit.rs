// SPDX-License-Identifier: BUSL-1.1

//! Record path of the audit log: stage under the audit lock, sync outside it, then publish.
//!
//! Staging allocates seqs, chains hashes, and appends to the WAL sink under the audit lock.
//! So WAL order equals seq order.
//! The sync wait runs with the audit lock released, so concurrent recorders share one fsync.
//! Publishing moves staged entries into the buffer in seq order, once a sync covers them.
//! A covering sync makes every earlier WAL record durable too.
//! So a caller can publish every staged entry up to its own last seq.
//!
//! An entry whose WAL append or sync failed is still published, so the hash chain stays whole.
//! Its record call returns the error, and the error is reported.

use std::sync::{Arc, Mutex};

use super::entry::{AuditEntry, hash_entry, now_us};
use super::guard::lock_audit_log;
use super::log::{AuditLog, AuditRecordInput};
use super::wal_sink::AuditWalSink;

/// A batch appended to the WAL sink and staged, waiting for its sync.
pub struct StagedBatch {
    /// Seq of each entry, in input order.
    seqs: Vec<u64>,
    /// Highest WAL position of the batch, when the sink took it.
    sync_position: Option<u64>,
    sink: Option<Arc<dyn AuditWalSink>>,
    append: crate::Result<()>,
}

impl StagedBatch {
    /// Wait until the batch is durable, then return its seqs with the outcome.
    ///
    /// The caller must not hold the audit lock, so other recorders can stage meanwhile.
    pub fn wait_durable(self) -> (Vec<u64>, crate::Result<()>) {
        let synced = match (self.append, self.sync_position, self.sink) {
            (Err(e), _, _) => Err(e),
            (Ok(()), Some(position), Some(sink)) => sink.sync_through(position),
            (Ok(()), _, _) => Ok(()),
        };
        (self.seqs, synced)
    }

    /// The last seq of the batch, or `None` for an empty batch.
    pub fn last_seq(&self) -> Option<u64> {
        self.seqs.last().copied()
    }
}

impl AuditLog {
    /// Allocate, chain and append `inputs` to the WAL sink, and stage them. No sync runs here.
    ///
    /// The log changes only after the sink returns.
    /// So a panic in the sink leaves the log as it was, with none of the batch staged.
    pub fn stage_batch<'a>(
        &mut self,
        inputs: impl IntoIterator<Item = AuditRecordInput<'a>>,
    ) -> StagedBatch {
        let mut next_seq = self.next_seq;
        let mut last_hash = self.staged_hash.clone();
        let mut entries = Vec::new();
        for input in inputs {
            let entry = AuditEntry {
                seq: next_seq,
                timestamp_us: now_us(),
                event: input.event,
                tenant_id: input.tenant_id,
                database_id: input.database_id,
                auth_user_id: input.auth.user_id.clone(),
                auth_user_name: input.auth.user_name.clone(),
                session_id: input.auth.session_id.clone(),
                source: input.source.to_string(),
                detail: input.detail.to_string(),
                prev_hash: last_hash,
            };
            last_hash = hash_entry(&entry);
            next_seq += 1;
            entries.push(entry);
        }
        if entries.is_empty() {
            return StagedBatch {
                seqs: Vec::new(),
                sync_position: None,
                sink: None,
                append: Ok(()),
            };
        }

        let appended = match &self.wal_sink {
            Some(sink) => sink.append_batch(&entries).map(Some),
            None => Ok(None),
        };

        self.next_seq = next_seq;
        self.staged_hash = last_hash;
        let seqs: Vec<u64> = entries.iter().map(|entry| entry.seq).collect();
        let mut sync_position = None;
        let append = match appended {
            Ok(Some(positions)) => {
                sync_position = positions.last().copied();
                self.wal_positions
                    .extend(seqs.iter().copied().zip(positions.iter().copied()));
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => Err(e),
        };
        self.staged.extend(entries);
        StagedBatch {
            seqs,
            sync_position,
            sink: sync_position.and(self.wal_sink.clone()),
            append,
        }
    }

    /// Publish every staged entry with `seq <= through_seq`, in seq order.
    ///
    /// The caller must call this only after the sync for `through_seq` returned.
    pub fn publish_through(&mut self, through_seq: u64) {
        let export = self
            .export_sink
            .as_ref()
            .is_some_and(|sink| sink.is_active());
        while self
            .staged
            .front()
            .is_some_and(|entry| entry.seq <= through_seq)
        {
            let Some(entry) = self.staged.pop_front() else {
                break;
            };
            // Route auth events to the separate auth_events stream.
            if entry.event.is_auth_event() {
                if self.auth_events.len() >= 10_000 {
                    self.auth_events.pop_front();
                }
                self.auth_events.push_back(entry.clone());
            }
            if export {
                self.export_pending.push(entry.clone());
            }
            self.published_seq = entry.seq;
            self.published_hash = hash_entry(&entry);
            self.entries.push_back(entry);
            self.total_entries += 1;
        }
        if self.entries.len() >= self.flush_bound {
            self.flush_control.request_flush();
        }
    }

    /// Record every input in order with one WAL sync, while the caller keeps the audit lock.
    ///
    /// Use this only where the lock spans more work, such as a sync session's delta push.
    /// [`record_batch_concurrent`] lets concurrent recorders share the sync instead.
    /// Returns the seq of each entry, in input order.
    pub fn record_batch_durable<'a>(
        &mut self,
        inputs: impl IntoIterator<Item = AuditRecordInput<'a>>,
    ) -> crate::Result<Vec<u64>> {
        let staged = self.stage_batch(inputs);
        let last = staged.last_seq();
        let (seqs, durable) = staged.wait_durable();
        if let Some(last) = last {
            self.publish_through(last);
        }
        durable.map(|()| seqs)
    }
}

/// Record every input in order, with the sync wait outside the audit lock.
///
/// Concurrent callers share one fsync. Each entry is durable before this returns `Ok`.
/// Returns the seq of each entry, in input order.
pub fn record_batch_concurrent<'a>(
    audit: &Mutex<AuditLog>,
    inputs: impl IntoIterator<Item = AuditRecordInput<'a>>,
) -> crate::Result<Vec<u64>> {
    let staged = lock_audit_log(audit).stage_batch(inputs);
    let last = staged.last_seq();
    let (seqs, durable) = staged.wait_durable();
    if let Some(last) = last {
        lock_audit_log(audit).publish_through(last);
    }
    durable.map(|()| seqs)
}

/// Record one entry with [`record_batch_concurrent`], best-effort.
///
/// A WAL error is logged and reported, and the entry stays published. Returns its seq.
pub fn record_concurrent(audit: &Mutex<AuditLog>, input: AuditRecordInput<'_>) -> u64 {
    record_batch_best_effort(audit, [input])
}

/// Record every input in order with one sync, best-effort.
///
/// A WAL error is logged and reported, and the entries stay published.
/// Returns the last seq, `0` for an empty batch.
pub fn record_batch_best_effort<'a>(
    audit: &Mutex<AuditLog>,
    inputs: impl IntoIterator<Item = AuditRecordInput<'a>>,
) -> u64 {
    let staged = lock_audit_log(audit).stage_batch(inputs);
    let seq = staged.last_seq().unwrap_or(0);
    let (_, durable) = staged.wait_durable();
    lock_audit_log(audit).publish_through(seq);
    if let Err(e) = durable {
        tracing::error!(error = %e, seq, "audit WAL write failed, entry is in memory only");
        crate::diag::audit_wal_append_failed(&e, seq);
    }
    seq
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Barrier, Condvar};
    use std::time::Duration;

    use super::*;
    use crate::control::security::audit::{AuditAuth, AuditEvent};
    use crate::wal::WalManager;

    fn input<'a>(detail: &'a str, auth: &'a AuditAuth) -> AuditRecordInput<'a> {
        AuditRecordInput {
            event: AuditEvent::AdminAction,
            tenant_id: None,
            database_id: None,
            source: "test",
            detail,
            auth,
        }
    }

    /// Recorders in the group-commit test.
    const RECORDERS: usize = 32;

    /// The real audit WAL, with every sync held until `expected` entries are appended.
    ///
    /// Each recorder appends under the audit lock and syncs after releasing it.
    /// So the gate opens only when every recorder staged before any sync ran.
    struct AppendGatedSink {
        wal: Arc<WalManager>,
        expected: usize,
        appended: Mutex<usize>,
        all_appended: Condvar,
    }

    impl AuditWalSink for AppendGatedSink {
        fn append_batch(&self, entries: &[AuditEntry]) -> crate::Result<Vec<u64>> {
            let positions = AuditWalSink::append_batch(&*self.wal, entries)?;
            let mut appended = self.appended.lock().unwrap_or_else(|p| p.into_inner());
            *appended += entries.len();
            self.all_appended.notify_all();
            Ok(positions)
        }

        fn sync_through(&self, position: u64) -> crate::Result<()> {
            let appended = self.appended.lock().unwrap_or_else(|p| p.into_inner());
            let (appended, _) = self
                .all_appended
                .wait_timeout_while(appended, Duration::from_secs(30), |appended| {
                    *appended < self.expected
                })
                .unwrap_or_else(|p| p.into_inner());
            if *appended < self.expected {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "only {} of {} recorders appended within 30s; a sync wait holds the audit lock",
                        *appended, self.expected
                    ),
                });
            }
            drop(appended);
            AuditWalSink::sync_through(&*self.wal, position)
        }

        fn truncate_through(&self, position: u64) -> crate::Result<()> {
            AuditWalSink::truncate_through(&*self.wal, position)
        }
    }

    #[test]
    fn concurrent_recorders_share_fsyncs_and_keep_seq_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("group.wal")).expect("WAL"));
        let sink = Arc::new(AppendGatedSink {
            wal: Arc::clone(&wal),
            expected: RECORDERS,
            appended: Mutex::new(0),
            all_appended: Condvar::new(),
        });
        let audit = Arc::new(Mutex::new(AuditLog::new(1_000)));
        lock_audit_log(&audit).set_wal_sink(sink as Arc<dyn AuditWalSink>);
        let syncs_before = wal.audit_sync_count();
        let start = Arc::new(Barrier::new(RECORDERS));

        let threads: Vec<_> = (0..RECORDERS)
            .map(|i| {
                let (audit, start) = (Arc::clone(&audit), Arc::clone(&start));
                std::thread::spawn(move || {
                    let auth = AuditAuth::default();
                    let detail = format!("thread-{i}");
                    start.wait();
                    record_batch_concurrent(&audit, [input(&detail, &auth)])
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("recorder thread").expect("durable");
        }

        // The first leader syncs every record appended so far, which is all of them.
        // Every other recorder finds its LSN covered and issues no sync.
        let syncs = wal.audit_sync_count() - syncs_before;
        assert_eq!(
            syncs, 1,
            "{RECORDERS} recorders staged before any sync must share one fsync, saw {syncs}"
        );
        let log = lock_audit_log(&audit);
        let published: Vec<u64> = log.all().iter().map(|e| e.seq).collect();
        assert_eq!(
            published,
            (1..=RECORDERS as u64).collect::<Vec<u64>>(),
            "every entry is published once"
        );
        drop(log);
        let wal_seqs: Vec<u64> = wal
            .recover_audit_entries()
            .expect("recover")
            .iter()
            .map(|(_, bytes)| {
                zerompk::from_msgpack::<AuditEntry>(bytes)
                    .expect("decode")
                    .seq
            })
            .collect();
        assert_eq!(wal_seqs, published, "WAL order equals seq order");
    }

    /// Fails every sync.
    struct FailingSink;

    impl AuditWalSink for FailingSink {
        fn append_batch(&self, entries: &[AuditEntry]) -> crate::Result<Vec<u64>> {
            Ok(entries.iter().map(|entry| entry.seq).collect())
        }

        fn sync_through(&self, _position: u64) -> crate::Result<()> {
            Err(crate::Error::Internal {
                detail: "injected fsync error".into(),
            })
        }

        fn truncate_through(&self, _position: u64) -> crate::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_sync_error_reaches_every_recorder() {
        let audit = Arc::new(Mutex::new(AuditLog::new(1_000)));
        lock_audit_log(&audit).set_wal_sink(Arc::new(FailingSink) as Arc<dyn AuditWalSink>);
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let audit = Arc::clone(&audit);
                std::thread::spawn(move || {
                    let auth = AuditAuth::default();
                    let detail = format!("fail-{i}");
                    record_batch_concurrent(&audit, [input(&detail, &auth)])
                })
            })
            .collect();
        for thread in threads {
            assert!(
                thread.join().expect("recorder thread").is_err(),
                "no recorder may report success after a failed sync"
            );
        }
    }

    /// Holds every sync until the gate opens, so a test can look at the log meanwhile.
    struct GatedSink {
        open: Mutex<bool>,
        opened: Condvar,
        waiting: AtomicBool,
    }

    impl AuditWalSink for GatedSink {
        fn append_batch(&self, entries: &[AuditEntry]) -> crate::Result<Vec<u64>> {
            Ok(entries.iter().map(|entry| entry.seq).collect())
        }

        fn sync_through(&self, _position: u64) -> crate::Result<()> {
            self.waiting.store(true, Ordering::Release);
            let mut open = self.open.lock().unwrap_or_else(|p| p.into_inner());
            while !*open {
                open = self.opened.wait(open).unwrap_or_else(|p| p.into_inner());
            }
            Ok(())
        }

        fn truncate_through(&self, _position: u64) -> crate::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn readers_never_see_an_entry_before_its_sync() {
        let sink = Arc::new(GatedSink {
            open: Mutex::new(false),
            opened: Condvar::new(),
            waiting: AtomicBool::new(false),
        });
        let audit = Arc::new(Mutex::new(AuditLog::new(1_000)));
        lock_audit_log(&audit).set_wal_sink(Arc::clone(&sink) as Arc<dyn AuditWalSink>);

        let recorder = {
            let audit = Arc::clone(&audit);
            std::thread::spawn(move || {
                let auth = AuditAuth::default();
                record_batch_concurrent(&audit, [input("gated", &auth)])
            })
        };
        while !sink.waiting.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }

        // The sync is in flight, and the audit lock is free: the recorder released it.
        {
            let log = audit
                .try_lock()
                .expect("the audit lock must be free during the sync");
            assert!(log.all().is_empty(), "a staged entry is not visible");
            assert_eq!(
                log.last_seq(),
                0,
                "the published tail does not move before the sync"
            );
        }

        *sink.open.lock().expect("gate") = true;
        sink.opened.notify_all();
        assert_eq!(
            recorder.join().expect("recorder thread").expect("durable"),
            vec![1]
        );
        let log = lock_audit_log(&audit);
        assert_eq!(log.all().len(), 1, "the entry is published once durable");
        assert_eq!(log.last_seq(), 1);
    }
}
