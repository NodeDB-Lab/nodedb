// SPDX-License-Identifier: BUSL-1.1

//! Hand a delivered batch's audit records to the Control Plane recorder.
//!
//! The Event Plane does no storage I/O, so it never records an audit entry itself.
//! It sends the batch over the recorder's bounded channel and awaits the answer.
//! A full channel makes the await wait, which slows the consumer.
//! The ring then fills, and the Event Plane's ring backpressure and WAL catch-up apply.
//! No record is dropped, and no thread blocks.
//!
//! A batch that is not durable holds the consumer here, so its watermark holds too.
//! The retry waits with a backoff, and each attempt runs on the blocking pool.
//! A crash or a shutdown during the retry leaves the events above the watermark.
//! WAL catch-up then delivers them again, and the audit ledger removes the duplicates.

use std::sync::Arc;
use std::time::Duration;

use tracing::error;

use crate::control::security::audit::PendingAudit;
use crate::control::security::audit::recorder::{BatchCommit, RecordFailure};
use crate::control::state::SharedState;

/// First wait before a retry of a batch that is not durable.
const RETRY_BACKOFF_MIN: Duration = Duration::from_millis(10);

/// Longest wait between retries of a batch that is not durable.
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Record `records` through the Control Plane recorder. Returns once every one is durable.
///
/// The caller awaits this before its watermark passes the batch.
/// So a crash before the answer leaves the events above the watermark, and WAL catch-up delivers them again.
/// A batch the disk or the catalog refuses is retried until it lands, as a trigger action's hold is.
pub async fn hold_audits(state: &Arc<SharedState>, records: Vec<PendingAudit>) {
    if records.is_empty() {
        return;
    }
    let mut outcome = state.audit_recorder().record(records).await;
    let mut backoff = RETRY_BACKOFF_MIN;
    while let Err(failure) = outcome {
        error!(
            backoff_ms = backoff.as_millis() as u64,
            failure = failure_name(&failure),
            "audit batch is not durable; the watermark holds, retrying"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
        outcome = match failure {
            RecordFailure::Unrecorded(records) => record_on_blocking_pool(state, records).await,
            RecordFailure::NotDurable { through_seq } => {
                settle_on_blocking_pool(state, through_seq).await
            }
        };
    }
}

fn failure_name(failure: &RecordFailure) -> &'static str {
    match failure {
        RecordFailure::Unrecorded(_) => "unrecorded",
        RecordFailure::NotDurable { .. } => "not_durable",
    }
}

/// Record a batch no attempt has staged. Its answer has the same three states as the recorder's.
///
/// The recorder is gone, or its batch stopped before staging, or the audit buffer is full.
/// The attempt goes to the blocking pool, so this Event Plane task still does no storage I/O.
async fn record_on_blocking_pool(
    state: &Arc<SharedState>,
    records: Vec<PendingAudit>,
) -> Result<(), RecordFailure> {
    let records = Arc::new(records);
    let commit = BatchCommit::default();
    let task_state = Arc::clone(state);
    let task_records = Arc::clone(&records);
    let task_commit = commit.clone();
    let attempt = tokio::task::spawn_blocking(move || {
        task_state.record_pending_audits(&task_records, &task_commit);
    })
    .await;
    if let Err(e) = attempt {
        error!(error = %e, "audit fallback recording panicked");
    }
    commit.outcome(records)
}

/// Ask for a flush that commits a staged batch to the catalog. A commit makes the batch durable.
///
/// A retry never records the batch again, so the audit log holds it once.
async fn settle_on_blocking_pool(
    state: &Arc<SharedState>,
    through_seq: u64,
) -> Result<(), RecordFailure> {
    let task_state = Arc::clone(state);
    let attempt =
        tokio::task::spawn_blocking(move || task_state.settle_audit_through(through_seq)).await;
    match attempt {
        Ok(true) => Ok(()),
        Ok(false) => Err(RecordFailure::NotDurable { through_seq }),
        Err(e) => {
            error!(error = %e, through_seq, "audit settle task panicked");
            Err(RecordFailure::NotDurable { through_seq })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::audit::{AuditEntry, AuditEvent, AuditWalSink, lock_audit_log};
    use crate::wal::WalManager;

    fn pending(detail: &str) -> PendingAudit {
        PendingAudit {
            event: AuditEvent::DmlAudit,
            tenant_id: None,
            database_id: None,
            source: "event_plane_test".into(),
            detail: detail.into(),
        }
    }

    fn test_state(dir: &tempfile::TempDir) -> Arc<SharedState> {
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("hold.wal")).expect("open WAL"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("shared state")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn held_records_are_recorded_before_the_call_returns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = test_state(&dir);

        let batch: Vec<PendingAudit> = (0..200).map(|i| pending(&format!("dml-{i}"))).collect();
        hold_audits(&state, batch).await;

        let recorded = state
            .recent_audit_entries(usize::MAX, |e| e.source == "event_plane_test")
            .expect("read audit");
        assert_eq!(recorded.len(), 200, "every handed-over record is recorded");
    }

    /// Takes every append and fails every sync, as a disk that refuses fsync does.
    struct NoSyncSink;

    impl AuditWalSink for NoSyncSink {
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

    /// A failed sync holds the batch until a flush commits it, and no record is written twice.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_sync_holds_the_batch_until_the_catalog_has_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = test_state(&dir);
        lock_audit_log(&state.audit).set_wal_sink(Arc::new(NoSyncSink) as Arc<dyn AuditWalSink>);
        let control = lock_audit_log(&state.audit).flush_control();
        control.inject_catalog_fault(true);

        let batch: Vec<PendingAudit> = (0..4).map(|i| pending(&format!("dml-{i}"))).collect();
        let hold = {
            let state = Arc::clone(&state);
            tokio::spawn(async move { hold_audits(&state, batch).await })
        };

        // Neither the WAL nor the catalog has the batch, so the hold does not end.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!hold.is_finished(), "the watermark must hold");

        control.inject_catalog_fault(false);
        tokio::time::timeout(Duration::from_secs(30), hold)
            .await
            .expect("the hold ends once the catalog commits the batch")
            .expect("hold task");
        let persisted = state
            .credentials
            .catalog()
            .load_recent_audit_entries(usize::MAX, |e| e.source == "event_plane_test")
            .expect("read catalog");
        assert_eq!(persisted.len(), 4, "the batch reached the catalog once");
    }

    /// A full buffer refuses the batch, and the batch lands once the catalog drains the buffer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_buffer_holds_the_batch_unrecorded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = test_state(&dir);
        let control = {
            let mut log = lock_audit_log(&state.audit);
            log.set_flush_bound(1);
            log.flush_control()
        };
        control.inject_catalog_fault(true);
        let before = lock_audit_log(&state.audit).last_seq();
        let filler: Vec<PendingAudit> = (0..crate::control::security::audit::HARD_BOUND_MULTIPLE)
            .map(|i| pending(&format!("filler-{i}")))
            .collect();
        hold_audits(&state, filler).await;
        assert!(lock_audit_log(&state.audit).is_full());

        let hold = {
            let state = Arc::clone(&state);
            tokio::spawn(async move { hold_audits(&state, vec![pending("late")]).await })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!hold.is_finished(), "a full buffer must refuse the batch");
        assert_eq!(
            lock_audit_log(&state.audit).last_seq(),
            before + crate::control::security::audit::HARD_BOUND_MULTIPLE as u64,
            "a refused batch takes no seq"
        );

        control.inject_catalog_fault(false);
        assert!(state.flush_audit_log(), "the catalog is back");
        tokio::time::timeout(Duration::from_secs(30), hold)
            .await
            .expect("the hold ends once the buffer drains")
            .expect("hold task");
    }
}
