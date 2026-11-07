// SPDX-License-Identifier: BUSL-1.1

//! Append-only in-memory `AuditLog`.

use std::collections::VecDeque;
use std::sync::Arc;

use nodedb_types::DatabaseId;

use crate::types::TenantId;

use super::auth::AuditAuth;
use super::entry::AuditEntry;
use super::event::AuditEvent;
use super::export_sink::AuditExportSink;
use super::flush_control::{AuditFlushControl, BACKPRESSURE_MULTIPLE, HARD_BOUND_MULTIPLE};
use super::level::AuditLevel;
use super::wal_sink::AuditWalSink;

/// One audit event to record, borrowed from the caller.
pub struct AuditRecordInput<'a> {
    pub event: AuditEvent,
    pub tenant_id: Option<TenantId>,
    pub database_id: Option<DatabaseId>,
    pub source: &'a str,
    pub detail: &'a str,
    pub auth: &'a AuditAuth,
}

/// The published entries that live only in the audit WAL, because the buffer was full.
///
/// They hold every seq from `first_seq` through `through_seq`, except a seq whose WAL append failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalOnlyRange {
    pub first_seq: u64,
    pub through_seq: u64,
    /// WAL position of the entry at `through_seq`.
    pub position: u64,
}

/// Append-only audit log.
///
/// Entries are immutable once appended.
/// A record call allocates seqs, chains, and appends to the WAL sink under the lock.
/// So WAL order equals seq order.
/// Its entries wait in `staged` until the WAL sync covers them, and only then are published.
/// Readers, flushes and export see published entries only, so they never see a non-durable entry.
/// The buffer holds published entries until a flush commits them to the catalog.
/// At `flush_bound` entries the log requests an early flush.
/// At `BACKPRESSURE_MULTIPLE * flush_bound` a recorder that can wait waits for a flush.
/// At `HARD_BOUND_MULTIPLE * flush_bound` the buffer is full, and published entries stay in the WAL only.
/// A flush commits those entries from the WAL, and until then readers read them from the WAL.
///
/// Lives on the Control Plane (Send + Sync).
pub struct AuditLog {
    pub(super) entries: VecDeque<AuditEntry>,
    /// Entries appended to the WAL sink and not yet published, in seq order.
    pub(super) staged: VecDeque<AuditEntry>,
    /// Separate auth event stream for login/logout/MFA/deactivation.
    pub(super) auth_events: VecDeque<AuditEntry>,
    /// Buffer length that requests an early flush.
    pub(super) flush_bound: usize,
    /// Next sequence number to allocate.
    pub(super) next_seq: u64,
    /// Total entries ever published.
    pub(super) total_entries: u64,
    /// Hash of the last allocated entry, staged or published (for chain continuity).
    pub(super) staged_hash: String,
    /// Seq of the last published entry.
    pub(super) published_seq: u64,
    /// Hash of the last published entry.
    pub(super) published_hash: String,
    /// Current audit level (controls which events are recorded).
    level: AuditLevel,
    /// Early-flush signal and flush serialization, shared with the flush task.
    pub(super) flush_control: Arc<AuditFlushControl>,
    /// Durable append target. `None` only in a log built with no WAL.
    pub(super) wal_sink: Option<Arc<dyn AuditWalSink>>,
    /// `(seq, WAL position)` of each staged entry the WAL sink took, in seq order.
    pub(super) staged_positions: VecDeque<(u64, u64)>,
    /// `(seq, WAL position)` of each buffered entry the WAL sink took, in seq order.
    pub(super) wal_positions: VecDeque<(u64, u64)>,
    /// The entries that live only in the WAL, when the buffer filled up.
    pub(super) wal_only: Option<WalOnlyRange>,
    /// Every entry at or below this seq, outside `wal_only`, is committed to the catalog.
    pub(super) persisted_through: u64,
    /// Export target, such as the SIEM exporter.
    pub(super) export_sink: Option<Arc<dyn AuditExportSink>>,
    /// Entries published for export since the last [`Self::take_export_pending`].
    pub(super) export_pending: Vec<AuditEntry>,
}

impl AuditLog {
    pub fn new(flush_bound: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(flush_bound.min(10_000)),
            staged: VecDeque::new(),
            auth_events: VecDeque::with_capacity(1_000),
            flush_bound: flush_bound.max(1),
            next_seq: 1,
            total_entries: 0,
            staged_hash: String::new(),
            published_seq: 0,
            published_hash: String::new(),
            level: AuditLevel::default(),
            flush_control: Arc::new(AuditFlushControl::default()),
            wal_sink: None,
            staged_positions: VecDeque::new(),
            wal_positions: VecDeque::new(),
            wal_only: None,
            persisted_through: 0,
            export_sink: None,
            export_pending: Vec::new(),
        }
    }

    /// Append every entry recorded from now on to `sink` before the record call returns.
    pub fn set_wal_sink(&mut self, sink: Arc<dyn AuditWalSink>) {
        self.wal_sink = Some(sink);
    }

    /// The durable append target, when one is set.
    pub fn wal_sink(&self) -> Option<Arc<dyn AuditWalSink>> {
        self.wal_sink.clone()
    }

    /// Hand every entry recorded from now on to `sink`, while the sink is active.
    pub fn set_export_sink(&mut self, sink: Arc<dyn AuditExportSink>) {
        self.export_sink = Some(sink);
    }

    /// Move out the entries waiting for export, with the sink they go to.
    ///
    /// [`AuditLogGuard`](super::guard::AuditLogGuard) calls this on drop and exports after it unlocks.
    pub fn take_export_pending(&mut self) -> (Vec<AuditEntry>, Option<Arc<dyn AuditExportSink>>) {
        if self.export_pending.is_empty() {
            return (Vec::new(), None);
        }
        (
            std::mem::take(&mut self.export_pending),
            self.export_sink.clone(),
        )
    }

    /// The early-flush signal and flush serialization for this log.
    pub fn flush_control(&self) -> Arc<AuditFlushControl> {
        Arc::clone(&self.flush_control)
    }

    /// Set the buffer length that requests an early flush. A value of 0 counts as 1.
    pub fn set_flush_bound(&mut self, flush_bound: usize) {
        self.flush_bound = flush_bound.max(1);
    }

    /// Whether the buffer is past the point where a recorder that can wait waits for a flush.
    pub fn needs_backpressure(&self) -> bool {
        self.wal_only.is_some()
            || self.entries.len() >= self.flush_bound.saturating_mul(BACKPRESSURE_MULTIPLE)
    }

    /// Whether the buffer takes no more entries.
    ///
    /// A recorder with an error path refuses here. A best-effort entry stays in the WAL only.
    pub fn is_full(&self) -> bool {
        self.wal_only.is_some()
            || self.entries.len() >= self.flush_bound.saturating_mul(HARD_BOUND_MULTIPLE)
    }

    /// The entries that live only in the WAL, if any.
    pub fn wal_only(&self) -> Option<WalOnlyRange> {
        self.wal_only
    }

    /// Record that the catalog committed every entry of `committed`.
    ///
    /// An entry published into that range meanwhile stays in the range.
    pub fn note_wal_only_committed(&mut self, committed: WalOnlyRange) {
        self.persisted_through = self.persisted_through.max(committed.through_seq);
        self.wal_only = match self.wal_only {
            Some(current) if current.through_seq > committed.through_seq => Some(WalOnlyRange {
                first_seq: committed.through_seq.saturating_add(1),
                ..current
            }),
            Some(_) | None => None,
        };
    }

    /// Whether the catalog committed the entry at `seq`.
    pub fn is_persisted(&self, seq: u64) -> bool {
        self.persisted_through >= seq
            && self
                .wal_only
                .is_none_or(|range| !(range.first_seq..=range.through_seq).contains(&seq))
    }

    /// The seq of the last published entry, or 0 when nothing is published.
    pub fn last_seq(&self) -> u64 {
        self.published_seq
    }

    /// Set the audit level.
    pub fn set_level(&mut self, level: AuditLevel) {
        self.level = level;
    }

    /// Get the current audit level.
    pub fn level(&self) -> AuditLevel {
        self.level
    }

    /// Set the next sequence number (used on startup to resume from catalog).
    pub fn set_next_seq(&mut self, seq: u64) {
        self.next_seq = seq;
        self.published_seq = seq.saturating_sub(1);
        self.persisted_through = self.published_seq;
    }

    /// Set the last hash (used on startup to continue the chain from catalog).
    pub fn set_last_hash(&mut self, hash: String) {
        self.published_hash.clone_from(&hash);
        self.staged_hash = hash;
    }

    /// Test support: record an audit event while the caller keeps the audit lock.
    ///
    /// Production code records through [`super::log_commit::record_concurrent`] or
    /// `SharedState::audit_record`, so no fsync wait runs under the audit lock.
    #[cfg(test)]
    pub fn record(
        &mut self,
        event: AuditEvent,
        tenant_id: Option<TenantId>,
        source: &str,
        detail: &str,
    ) -> u64 {
        self.record_with_auth(
            event,
            tenant_id,
            None,
            source,
            detail,
            &AuditAuth::default(),
        )
    }

    /// Test support: [`Self::record`] with database context.
    #[cfg(test)]
    pub fn record_with_database(
        &mut self,
        event: AuditEvent,
        tenant_id: Option<TenantId>,
        database_id: Option<DatabaseId>,
        source: &str,
        detail: &str,
    ) -> u64 {
        self.record_with_auth(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            &AuditAuth::default(),
        )
    }

    /// Test support: [`Self::record`] with auth context (best-effort durability).
    ///
    /// A WAL sink error is logged and reported, and the entry stays buffered.
    #[cfg(test)]
    pub fn record_with_auth(
        &mut self,
        event: AuditEvent,
        tenant_id: Option<TenantId>,
        database_id: Option<DatabaseId>,
        source: &str,
        detail: &str,
        auth: &AuditAuth,
    ) -> u64 {
        match self.record_with_auth_durable(event, tenant_id, database_id, source, detail, auth) {
            Ok(seq) => seq,
            Err(e) => {
                let seq = self.last_seq();
                tracing::error!(error = %e, seq, "audit WAL append failed, entry is buffered only");
                crate::diag::audit_wal_append_failed(&e, seq);
                seq
            }
        }
    }

    /// Test support: [`Self::record_with_auth`] that returns the WAL error.
    ///
    /// The entry is buffered even when the WAL append fails.
    /// The error tells the caller that the entry is not crash-safe.
    #[cfg(test)]
    pub fn record_with_auth_durable(
        &mut self,
        event: AuditEvent,
        tenant_id: Option<TenantId>,
        database_id: Option<DatabaseId>,
        source: &str,
        detail: &str,
        auth: &AuditAuth,
    ) -> crate::Result<u64> {
        let seqs = self.record_batch_durable([AuditRecordInput {
            event,
            tenant_id,
            database_id,
            source,
            detail,
            auth,
        }])?;
        Ok(seqs.first().copied().unwrap_or_else(|| self.last_seq()))
    }

    /// Filter in-memory entries by database id.
    pub fn query_by_database(&self, database_id: DatabaseId) -> Vec<&AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.database_id == Some(database_id))
            .collect()
    }

    /// Query entries by event type.
    pub fn query_by_event(&self, event: &AuditEvent) -> Vec<&AuditEntry> {
        self.entries.iter().filter(|e| &e.event == event).collect()
    }

    /// Query entries for a specific tenant.
    pub fn query_by_tenant(&self, tenant_id: TenantId) -> Vec<&AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.tenant_id == Some(tenant_id))
            .collect()
    }

    /// Query entries since a given sequence number.
    pub fn since(&self, seq: u64) -> Vec<&AuditEntry> {
        self.entries.iter().filter(|e| e.seq >= seq).collect()
    }

    /// Get all entries in memory.
    pub fn all(&self) -> &VecDeque<AuditEntry> {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn total_recorded(&self) -> u64 {
        self.total_entries
    }

    /// Query entries by auth user ID.
    pub fn query_by_user(&self, auth_user_id: &str) -> Vec<&AuditEntry> {
        self.entries
            .iter()
            .filter(|e| e.auth_user_id == auth_user_id)
            .collect()
    }

    /// Get the separate auth events stream (login/logout/MFA/deactivation).
    pub fn auth_events(&self) -> &VecDeque<AuditEntry> {
        &self.auth_events
    }

    /// Copy the buffered entries for a persistence pass.
    ///
    /// The entries stay buffered until [`Self::evict_persisted`] runs.
    /// A reader that locks the log between the copy and the eviction still sees them.
    pub fn snapshot_for_persistence(&self) -> Vec<AuditEntry> {
        self.entries.iter().cloned().collect()
    }

    /// Remove buffered entries with `seq <= through_seq`.
    ///
    /// The caller must call this only after those entries are committed to the catalog.
    /// Buffered seqs ascend from front to back, so eviction stops at the first higher seq.
    /// Returns the highest WAL position among the evicted entries.
    /// Every buffered WAL record at or below it is in the catalog.
    pub fn evict_persisted(&mut self, through_seq: u64) -> Option<u64> {
        self.persisted_through = self.persisted_through.max(through_seq);
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.seq <= through_seq)
        {
            self.entries.pop_front();
        }
        let mut committed_position = None;
        while let Some(&(seq, position)) = self.wal_positions.front() {
            if seq > through_seq {
                break;
            }
            committed_position = Some(position);
            self.wal_positions.pop_front();
        }
        committed_position
    }

    /// The hash of the last published entry, the tail a chain reader checks against.
    pub fn last_hash(&self) -> &str {
        &self.published_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::entry::hash_entry;

    #[test]
    fn empty_log() {
        let log = AuditLog::new(100);
        assert!(log.is_empty());
        assert_eq!(log.total_recorded(), 0);
    }

    #[test]
    fn record_and_query() {
        let mut log = AuditLog::new(100);

        log.record(
            AuditEvent::AuthSuccess,
            Some(TenantId::new(1)),
            "10.0.0.1",
            "user login",
        );
        log.record(
            AuditEvent::AuthFailure,
            Some(TenantId::new(2)),
            "10.0.0.2",
            "bad password",
        );
        log.record(
            AuditEvent::AuthSuccess,
            Some(TenantId::new(1)),
            "10.0.0.1",
            "api key auth",
        );

        assert_eq!(log.len(), 3);
        assert_eq!(log.total_recorded(), 3);

        let successes = log.query_by_event(&AuditEvent::AuthSuccess);
        assert_eq!(successes.len(), 2);

        let tenant1 = log.query_by_tenant(TenantId::new(1));
        assert_eq!(tenant1.len(), 2);

        let tenant2 = log.query_by_tenant(TenantId::new(2));
        assert_eq!(tenant2.len(), 1);
    }

    #[test]
    fn sequence_numbers_monotonic() {
        let mut log = AuditLog::new(100);
        let s1 = log.record(AuditEvent::AuthSuccess, None, "src", "");
        let s2 = log.record(AuditEvent::AuthFailure, None, "src", "");
        let s3 = log.record(AuditEvent::AdminAction, None, "src", "");
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(s3, 3);
    }

    #[test]
    fn since_query() {
        let mut log = AuditLog::new(100);
        log.record(AuditEvent::AuthSuccess, None, "src", "a");
        log.record(AuditEvent::AuthFailure, None, "src", "b");
        log.record(AuditEvent::AdminAction, None, "src", "c");

        let since2 = log.since(2);
        assert_eq!(since2.len(), 2);
        assert_eq!(since2[0].detail, "b");
        assert_eq!(since2[1].detail, "c");
    }

    #[test]
    fn bound_requests_flush_and_never_drops() {
        let mut log = AuditLog::new(3);
        let total = 3 * BACKPRESSURE_MULTIPLE + 1;
        for i in 0..total {
            log.record(AuditEvent::AuthSuccess, None, "src", &format!("entry-{i}"));
        }

        assert_eq!(
            log.len(),
            total,
            "the buffer must keep every unpersisted entry"
        );
        assert_eq!(log.total_recorded(), total as u64);
        assert_eq!(log.all()[0].detail, "entry-0");
        assert!(log.needs_backpressure());
        assert!(
            log.flush_control().take_flush_request(),
            "reaching the bound must request a flush"
        );

        let through_seq = log.last_seq();
        log.evict_persisted(through_seq);
        assert!(!log.needs_backpressure());
    }

    /// Captures every appended seq, and fails the append for one chosen seq.
    struct RecordingSink {
        appended: std::sync::Mutex<Vec<u64>>,
        fail_seq: u64,
    }

    impl AuditWalSink for RecordingSink {
        fn append_batch(&self, entries: &[AuditEntry]) -> crate::Result<Vec<u64>> {
            if entries.iter().any(|entry| entry.seq == self.fail_seq) {
                return Err(crate::Error::Internal {
                    detail: "injected audit WAL error".into(),
                });
            }
            self.appended
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .extend(entries.iter().map(|entry| entry.seq));
            // The position is the seq times 10, so a test can tell the two apart.
            Ok(entries.iter().map(|entry| entry.seq * 10).collect())
        }

        fn sync_through(&self, _position: u64) -> crate::Result<()> {
            Ok(())
        }

        fn truncate_through(&self, _position: u64) -> crate::Result<()> {
            Ok(())
        }
    }

    /// Counts `sync_through` calls, one per sync.
    #[derive(Default)]
    struct SyncCountingSink {
        syncs: std::sync::atomic::AtomicUsize,
        appended: std::sync::atomic::AtomicUsize,
        panic_on_append: bool,
    }

    impl AuditWalSink for SyncCountingSink {
        fn append_batch(&self, entries: &[AuditEntry]) -> crate::Result<Vec<u64>> {
            if self.panic_on_append {
                panic!("injected audit WAL panic");
            }
            self.appended
                .fetch_add(entries.len(), std::sync::atomic::Ordering::SeqCst);
            Ok(entries.iter().map(|entry| entry.seq).collect())
        }

        fn sync_through(&self, _position: u64) -> crate::Result<()> {
            self.syncs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        fn truncate_through(&self, _position: u64) -> crate::Result<()> {
            Ok(())
        }
    }

    fn batch_inputs<'a>(
        details: &'a [String],
        auth: &'a AuditAuth,
    ) -> impl Iterator<Item = AuditRecordInput<'a>> {
        details.iter().map(move |detail| AuditRecordInput {
            event: AuditEvent::DmlAudit,
            tenant_id: None,
            database_id: None,
            source: "batch",
            detail,
            auth,
        })
    }

    #[test]
    fn a_64_entry_batch_issues_one_sync() {
        let sink = Arc::new(SyncCountingSink::default());
        let mut log = AuditLog::new(1_000);
        log.set_wal_sink(Arc::clone(&sink) as Arc<dyn AuditWalSink>);
        let details: Vec<String> = (0..64).map(|i| format!("dml-{i}")).collect();
        let auth = AuditAuth::default();

        let seqs = log
            .record_batch_durable(batch_inputs(&details, &auth))
            .expect("record batch");

        assert_eq!(
            seqs,
            (1..=64).collect::<Vec<u64>>(),
            "seq order follows input order"
        );
        assert_eq!(sink.syncs.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(sink.appended.load(std::sync::atomic::Ordering::SeqCst), 64);
        let entries = log.all();
        for pair in entries.iter().collect::<Vec<_>>().windows(2) {
            assert_eq!(
                pair[1].prev_hash,
                hash_entry(pair[0]),
                "the batch stays chained"
            );
        }
        assert_eq!(log.last_hash(), hash_entry(&entries[63]));
    }

    #[test]
    fn a_sink_panic_leaves_the_log_untouched() {
        let mut log = AuditLog::new(1_000);
        log.record(AuditEvent::AdminAction, None, "src", "before");
        let (last_seq, last_hash) = (log.last_seq(), log.last_hash().to_string());
        log.set_wal_sink(Arc::new(SyncCountingSink {
            panic_on_append: true,
            ..SyncCountingSink::default()
        }) as Arc<dyn AuditWalSink>);
        let details: Vec<String> = (0..3).map(|i| format!("dml-{i}")).collect();
        let auth = AuditAuth::default();

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            log.record_batch_durable(batch_inputs(&details, &auth))
        }));

        assert!(outcome.is_err(), "the sink panic must propagate");
        assert_eq!(log.len(), 1, "no entry of the batch is buffered");
        assert_eq!(log.last_seq(), last_seq, "no seq of the batch is consumed");
        assert_eq!(log.last_hash(), last_hash);
    }

    #[test]
    fn eviction_reports_the_highest_committed_wal_position() {
        let sink = Arc::new(RecordingSink {
            appended: std::sync::Mutex::new(Vec::new()),
            fail_seq: u64::MAX,
        });
        let mut log = AuditLog::new(100);
        log.set_wal_sink(Arc::clone(&sink) as Arc<dyn AuditWalSink>);
        for detail in ["a", "b", "c"] {
            log.record(AuditEvent::AdminAction, None, "src", detail);
        }
        assert_eq!(log.evict_persisted(2), Some(20));
        assert_eq!(log.evict_persisted(2), None, "nothing new is committed");
        assert_eq!(log.evict_persisted(3), Some(30));
    }

    #[test]
    fn every_record_method_appends_to_the_wal_sink() {
        let sink = Arc::new(RecordingSink {
            appended: std::sync::Mutex::new(Vec::new()),
            fail_seq: u64::MAX,
        });
        let mut log = AuditLog::new(100);
        log.set_wal_sink(Arc::clone(&sink) as Arc<dyn AuditWalSink>);
        log.record(AuditEvent::AdminAction, None, "src", "record");
        log.record_with_database(AuditEvent::DatabaseCreated, None, None, "src", "db");
        log.record_with_auth(
            AuditEvent::AuthzDenied,
            None,
            None,
            "src",
            "auth",
            &AuditAuth::default(),
        );
        let durable = log.record_with_auth_durable(
            AuditEvent::DdlChange,
            None,
            None,
            "src",
            "durable",
            &AuditAuth::default(),
        );
        assert_eq!(durable.ok(), Some(4));
        assert_eq!(*sink.appended.lock().unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn wal_error_keeps_the_entry_buffered_and_reaches_a_durable_caller() {
        let sink = Arc::new(RecordingSink {
            appended: std::sync::Mutex::new(Vec::new()),
            fail_seq: 2,
        });
        let mut log = AuditLog::new(100);
        log.set_wal_sink(Arc::clone(&sink) as Arc<dyn AuditWalSink>);
        log.record(AuditEvent::AdminAction, None, "src", "one");
        let failed = log.record_with_auth_durable(
            AuditEvent::AdminAction,
            None,
            None,
            "src",
            "two",
            &AuditAuth::default(),
        );
        assert!(failed.is_err(), "the WAL error must reach a durable caller");
        assert_eq!(log.len(), 2, "the entry must stay buffered");
        assert_eq!(*sink.appended.lock().unwrap(), vec![1]);
    }

    #[test]
    fn a_full_buffer_keeps_later_entries_in_the_wal_only() {
        let sink = Arc::new(RecordingSink {
            appended: std::sync::Mutex::new(Vec::new()),
            fail_seq: u64::MAX,
        });
        let mut log = AuditLog::new(1);
        log.set_wal_sink(Arc::clone(&sink) as Arc<dyn AuditWalSink>);
        let total = HARD_BOUND_MULTIPLE as u64 + 3;
        for i in 0..total {
            log.record(AuditEvent::AdminAction, None, "src", &format!("entry-{i}"));
        }

        assert_eq!(log.len(), HARD_BOUND_MULTIPLE, "the buffer stays bounded");
        assert!(log.is_full());
        assert_eq!(
            log.last_seq(),
            total,
            "the chain covers the WAL-only entries"
        );
        assert_eq!(
            log.wal_only(),
            Some(WalOnlyRange {
                first_seq: HARD_BOUND_MULTIPLE as u64 + 1,
                through_seq: total,
                position: total * 10,
            })
        );
        assert_eq!(sink.appended.lock().unwrap().len() as u64, total);
    }

    #[test]
    fn a_full_buffer_with_a_refusing_wal_takes_no_seq() {
        let sink = Arc::new(RecordingSink {
            appended: std::sync::Mutex::new(Vec::new()),
            fail_seq: HARD_BOUND_MULTIPLE as u64 + 1,
        });
        let mut log = AuditLog::new(1);
        log.set_wal_sink(Arc::clone(&sink) as Arc<dyn AuditWalSink>);
        for i in 0..HARD_BOUND_MULTIPLE {
            log.record(AuditEvent::AdminAction, None, "src", &format!("entry-{i}"));
        }
        let (seq, hash) = (log.last_seq(), log.last_hash().to_string());

        let lost = log.record_with_auth_durable(
            AuditEvent::AdminAction,
            None,
            None,
            "src",
            "lost",
            &AuditAuth::default(),
        );

        assert!(lost.is_err(), "the caller must see the error");
        assert_eq!(log.last_seq(), seq, "a lost entry takes no seq");
        assert_eq!(log.last_hash(), hash, "the chain stays whole");
        assert_eq!(log.len(), HARD_BOUND_MULTIPLE);
    }

    #[test]
    fn a_committed_range_ends_only_when_nothing_followed_it() {
        let mut log = AuditLog::new(1);
        let committed = WalOnlyRange {
            first_seq: 5,
            through_seq: 7,
            position: 70,
        };
        log.wal_only = Some(WalOnlyRange {
            through_seq: 9,
            position: 90,
            ..committed
        });
        assert!(!log.is_persisted(6));

        log.note_wal_only_committed(committed);
        assert_eq!(
            log.wal_only(),
            Some(WalOnlyRange {
                first_seq: 8,
                through_seq: 9,
                position: 90,
            })
        );
        assert!(log.is_persisted(6));
        assert!(!log.is_persisted(8));

        log.note_wal_only_committed(WalOnlyRange {
            first_seq: 8,
            through_seq: 9,
            position: 90,
        });
        assert_eq!(log.wal_only(), None);
        assert!(log.is_persisted(9));
    }

    #[test]
    fn snapshot_keeps_entries_until_evicted() {
        let mut log = AuditLog::new(100);
        log.record(
            AuditEvent::SnapshotBegin,
            None,
            "node-1",
            "snapshot started",
        );
        log.record(AuditEvent::SnapshotEnd, None, "node-1", "snapshot done");

        let snapshot = log.snapshot_for_persistence();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(log.len(), 2, "a snapshot must not remove buffered entries");

        // An entry recorded after the snapshot must survive eviction of the snapshot.
        log.record(AuditEvent::AdminAction, None, "node-1", "late entry");
        let through_seq = snapshot.last().map(|e| e.seq).expect("non-empty snapshot");
        log.evict_persisted(through_seq);

        assert_eq!(log.len(), 1);
        assert_eq!(log.all()[0].detail, "late entry");
    }

    #[test]
    fn evict_persisted_on_empty_log_is_noop() {
        let mut log = AuditLog::new(100);
        log.evict_persisted(u64::MAX);
        assert!(log.is_empty());
    }

    #[test]
    fn all_event_types_representable() {
        let events = [
            AuditEvent::AuthSuccess,
            AuditEvent::AuthFailure,
            AuditEvent::AuthzDenied,
            AuditEvent::PrivilegeChange,
            AuditEvent::TenantCreated,
            AuditEvent::TenantDeleted,
            AuditEvent::SnapshotBegin,
            AuditEvent::SnapshotEnd,
            AuditEvent::RestoreBegin,
            AuditEvent::RestoreEnd,
            AuditEvent::CertRotation,
            AuditEvent::CertRotationFailed,
            AuditEvent::KeyRotation,
            AuditEvent::ConfigChange,
            AuditEvent::NodeJoined,
            AuditEvent::NodeLeft,
            AuditEvent::AdminAction,
            // New variants (13 additions):
            AuditEvent::DatabaseCreated,
            AuditEvent::DatabaseDropped,
            AuditEvent::DatabaseRenamed,
            AuditEvent::DatabaseQuotaChanged,
            AuditEvent::DatabaseCloned,
            AuditEvent::DatabaseMirrored,
            AuditEvent::DatabasePromoted,
            AuditEvent::DatabaseMaterialized,
            AuditEvent::TenantMoved,
            AuditEvent::DatabaseBackedUp,
            AuditEvent::DatabaseRestored,
            AuditEvent::DmlAudit,
            AuditEvent::DatabaseAuditDmlChanged,
        ];

        let mut log = AuditLog::new(100);
        for event in events {
            log.record(event, None, "test", "");
        }
        assert_eq!(log.len(), 30);
    }

    #[test]
    fn record_with_database_id_propagates() {
        use nodedb_types::DatabaseId;
        let mut log = AuditLog::new(100);
        let db_id = DatabaseId::new(42);
        log.record_with_database(
            AuditEvent::DatabaseCreated,
            None,
            Some(db_id),
            "admin",
            "CREATE DATABASE mydb",
        );
        let entries = log.query_by_database(db_id);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].database_id, Some(db_id));
        assert_eq!(entries[0].event, AuditEvent::DatabaseCreated);
    }

    #[test]
    fn appends_link_each_entry_to_its_predecessor() {
        let mut log = AuditLog::new(100);
        log.record(AuditEvent::AuthSuccess, None, "src", "login");
        log.record(AuditEvent::DdlChange, None, "src", "alter");
        log.record(AuditEvent::AdminAction, None, "src", "op");
        let entries = log.all();
        assert!(entries[0].prev_hash.is_empty());
        assert_eq!(entries[1].prev_hash, hash_entry(&entries[0]));
        assert_eq!(entries[2].prev_hash, hash_entry(&entries[1]));
        assert_eq!(log.last_hash(), hash_entry(&entries[2]));
        assert_eq!(log.last_seq(), 3);
    }
}
