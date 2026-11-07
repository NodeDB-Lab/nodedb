// SPDX-License-Identifier: BUSL-1.1

//! Audit-log consumer for the session-invalidation and user-change buses.
//!
//! This task writes audit rows and does nothing else. A hard revoke ends its
//! sessions on the credential mutation's own path
//! (`CredentialStore::end_sessions_on_hard_revoke`), so a lagging consumer
//! costs audit rows and never a revocation.
//!
//! `spawn_bus_consumer` launches a single Tokio task that:
//!
//! 1. Waits on the two broadcast receivers.
//! 2. Takes every event ready on both as one batch, up to [`MAX_BATCH`].
//! 3. Records the batch with one WAL sync, on a blocking thread, so the wait
//!    never holds a Tokio worker. It waits for that sync before the next batch.
//!
//! Each event maps to one row:
//!
//! - `SessionInvalidated`: `AuditEvent::SessionRevoked`.
//! - `UserChanged`: `AuditEvent::PrivilegeChange`.
//! - A receiver lag (`Lagged`): `AuditEvent::AuditBusLagged`, naming how many
//!   rows it lost, plus a diagnostics report.
//!
//! The task exits once both senders are gone and both receivers are drained.
//!
//! The returned `JoinHandle` is stored as `SharedState::bus_consumer_handle`.
//! Graceful shutdown drops the `SessionInvalidationBus` / `UserChangeBus`
//! senders, which eventually closes both receivers and causes the task to exit.

use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use crate::control::security::audit::event::AuditEvent;
use crate::control::security::audit::log::AuditLog;
use crate::control::security::audit::{AuditAuth, AuditRecordInput, record_batch_best_effort};

use super::session_invalidation::{SESSION_INVALIDATION_CHANNEL_CAPACITY, SessionInvalidated};
use super::user_change::{USER_CHANGE_CHANNEL_CAPACITY, UserChanged};

/// The most rows one sync covers: a full buffer of each bus.
const MAX_BATCH: usize = SESSION_INVALIDATION_CHANNEL_CAPACITY + USER_CHANGE_CHANNEL_CAPACITY;

const SESSION_INVALIDATION_BUS: &str = "session_invalidation_bus";
const USER_CHANGE_BUS: &str = "user_change_bus";

/// One audit row a bus event produces.
struct BusRow {
    event: AuditEvent,
    source: &'static str,
    detail: String,
    /// The bus and event count of a lag this row records.
    lag: Option<(&'static str, u64)>,
}

impl From<&SessionInvalidated> for BusRow {
    fn from(event: &SessionInvalidated) -> Self {
        Self {
            event: AuditEvent::SessionRevoked,
            source: SESSION_INVALIDATION_BUS,
            detail: format!("user_id={} reason={}", event.user_id, event.reason.as_str()),
            lag: None,
        }
    }
}

impl From<&UserChanged> for BusRow {
    fn from(event: &UserChanged) -> Self {
        Self {
            event: AuditEvent::PrivilegeChange,
            source: USER_CHANGE_BUS,
            detail: format!("user_id={} credential_mutation", event.user_id),
            lag: None,
        }
    }
}

/// The row for `dropped` events a lag lost on `bus`.
fn lag_row(bus: &'static str, dropped: u64) -> BusRow {
    BusRow {
        event: AuditEvent::AuditBusLagged,
        source: "bus_consumer",
        detail: format!("{bus} lagged; dropped={dropped}"),
        lag: Some((bus, dropped)),
    }
}

/// One receiver and whether its bus still has senders.
struct BusFeed<T> {
    rx: broadcast::Receiver<T>,
    bus: &'static str,
    open: bool,
}

impl<T: Clone> BusFeed<T>
where
    for<'a> BusRow: From<&'a T>,
{
    fn new(rx: broadcast::Receiver<T>, bus: &'static str) -> Self {
        Self {
            rx,
            bus,
            open: true,
        }
    }

    /// Add the outcome of one awaited receive to `batch`.
    fn note_received(&mut self, result: Result<T, RecvError>, batch: &mut Vec<BusRow>) {
        match result {
            Ok(event) => batch.push(BusRow::from(&event)),
            Err(RecvError::Lagged(dropped)) => batch.push(lag_row(self.bus, dropped)),
            Err(RecvError::Closed) => self.open = false,
        }
    }

    /// Add every event ready now to `batch`, up to [`MAX_BATCH`].
    fn drain_ready(&mut self, batch: &mut Vec<BusRow>) {
        while self.open && batch.len() < MAX_BATCH {
            match self.rx.try_recv() {
                Ok(event) => batch.push(BusRow::from(&event)),
                Err(TryRecvError::Lagged(dropped)) => batch.push(lag_row(self.bus, dropped)),
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Closed) => self.open = false,
            }
        }
    }
}

/// Spawn the bus consumer task.
///
/// - `si_rx`: pre-subscribed receiver for the session-invalidation bus.
/// - `uc_rx`: pre-subscribed receiver for the user-change bus.
/// - `audit`: shared audit log.
///
/// Returns a `JoinHandle` suitable for storing in `SharedState::bus_consumer_handle`,
/// or `None` when there is no Tokio runtime to spawn onto — a `SharedState` built
/// from a synchronous test or tooling context has no reactor, and constructing one
/// must not panic. Production startup always runs inside the Control Plane runtime.
pub fn spawn_bus_consumer(
    si_rx: broadcast::Receiver<SessionInvalidated>,
    uc_rx: broadcast::Receiver<UserChanged>,
    audit: Arc<Mutex<AuditLog>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!(
            "bus_consumer: no tokio runtime; skipping spawn (test or non-async context)"
        );
        return None;
    };
    Some(handle.spawn(run_bus_consumer(si_rx, uc_rx, audit)))
}

async fn run_bus_consumer(
    si_rx: broadcast::Receiver<SessionInvalidated>,
    uc_rx: broadcast::Receiver<UserChanged>,
    audit: Arc<Mutex<AuditLog>>,
) {
    let mut si = BusFeed::new(si_rx, SESSION_INVALIDATION_BUS);
    let mut uc = BusFeed::new(uc_rx, USER_CHANGE_BUS);
    while si.open || uc.open {
        let mut batch = Vec::new();
        tokio::select! {
            biased;

            result = si.rx.recv(), if si.open => si.note_received(result, &mut batch),
            result = uc.rx.recv(), if uc.open => uc.note_received(result, &mut batch),
        }
        si.drain_ready(&mut batch);
        uc.drain_ready(&mut batch);
        record_off_worker(&audit, batch).await;
    }
}

/// Record `batch` on a blocking thread and wait for it.
async fn record_off_worker(audit: &Arc<Mutex<AuditLog>>, batch: Vec<BusRow>) {
    if batch.is_empty() {
        return;
    }
    let rows = batch.len();
    let audit = Arc::clone(audit);
    let recorded = tokio::task::spawn_blocking(move || record_rows(&audit, &batch)).await;
    if let Err(error) = recorded {
        tracing::error!(%error, rows, "bus_consumer: the audit batch task did not finish");
        crate::diag::security_bus_rows_not_recorded(rows);
    }
}

/// Record `rows` in order with one WAL sync, outside the audit lock. A lag row
/// also files its diagnostics report. Runs off the Tokio workers because both
/// the sync and the report wait on disk.
fn record_rows(audit: &Mutex<AuditLog>, rows: &[BusRow]) {
    let auth = AuditAuth::default();
    record_batch_best_effort(
        audit,
        rows.iter().map(|row| AuditRecordInput {
            event: row.event.clone(),
            tenant_id: None,
            database_id: None,
            source: row.source,
            detail: &row.detail,
            auth: &auth,
        }),
    );
    for (bus, dropped) in rows.iter().filter_map(|row| row.lag) {
        crate::diag::security_bus_lagged(bus, dropped);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::buses::session_invalidation::{
        SessionInvalidationBus, SessionInvalidationReason,
    };
    use crate::control::security::buses::user_change::UserChangeBus;

    fn make_audit() -> Arc<Mutex<AuditLog>> {
        Arc::new(Mutex::new(AuditLog::new(1_000)))
    }

    fn invalidated(user_id: u64) -> SessionInvalidated {
        SessionInvalidated {
            user_id,
            reason: SessionInvalidationReason::UserDropped,
        }
    }

    #[tokio::test]
    async fn session_invalidated_produces_audit_row() {
        let audit = make_audit();
        let (si_bus, si_rx) = SessionInvalidationBus::new();
        let (uc_bus, uc_rx) = UserChangeBus::new();

        let handle = spawn_bus_consumer(si_rx, uc_rx, Arc::clone(&audit));

        si_bus.publish(invalidated(99));

        drop(si_bus);
        drop(uc_bus);
        handle
            .expect("bus consumer spawns under #[tokio::test]")
            .await
            .expect("consumer task panicked");

        let log = audit.lock().unwrap();
        let rows = log.query_by_event(&AuditEvent::SessionRevoked);
        assert_eq!(rows.len(), 1, "expected exactly one SessionRevoked row");
        assert!(rows[0].detail.contains("user_id=99"));
        assert!(rows[0].detail.contains("UserDropped"));
    }

    #[tokio::test]
    async fn user_changed_produces_audit_row() {
        let audit = make_audit();
        let (si_bus, si_rx) = SessionInvalidationBus::new();
        let (uc_bus, uc_rx) = UserChangeBus::new();

        let handle = spawn_bus_consumer(si_rx, uc_rx, Arc::clone(&audit));

        uc_bus.publish(UserChanged { user_id: 55 });

        drop(si_bus);
        drop(uc_bus);
        handle
            .expect("bus consumer spawns under #[tokio::test]")
            .await
            .expect("consumer task panicked");

        let log = audit.lock().unwrap();
        let rows = log.query_by_event(&AuditEvent::PrivilegeChange);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].detail.contains("user_id=55"));
    }

    #[tokio::test]
    async fn both_buses_produce_rows() {
        let audit = make_audit();
        let (si_bus, si_rx) = SessionInvalidationBus::new();
        let (uc_bus, uc_rx) = UserChangeBus::new();

        let handle = spawn_bus_consumer(si_rx, uc_rx, Arc::clone(&audit));

        si_bus.publish(SessionInvalidated {
            user_id: 1,
            reason: SessionInvalidationReason::UserDeactivated,
        });
        uc_bus.publish(UserChanged { user_id: 2 });

        drop(si_bus);
        drop(uc_bus);
        handle
            .expect("bus consumer spawns under #[tokio::test]")
            .await
            .expect("consumer task panicked");

        let log = audit.lock().unwrap();
        assert_eq!(log.query_by_event(&AuditEvent::SessionRevoked).len(), 1);
        assert_eq!(log.query_by_event(&AuditEvent::PrivilegeChange).len(), 1);
    }

    /// A publisher that overruns the consumer costs audit rows only. The lost
    /// count reaches the audit log, and the events still held are recorded.
    #[tokio::test]
    async fn a_lag_records_how_many_rows_it_lost() {
        let audit = make_audit();
        let (si_bus, si_rx) = SessionInvalidationBus::new();
        let (uc_bus, uc_rx) = UserChangeBus::new();

        // The consumer starts after the overrun, so the lag is certain.
        let overrun = 3;
        for user_id in 0..(SESSION_INVALIDATION_CHANNEL_CAPACITY as u64 + overrun) {
            si_bus.publish(invalidated(user_id));
        }
        let handle = spawn_bus_consumer(si_rx, uc_rx, Arc::clone(&audit));

        drop(si_bus);
        drop(uc_bus);
        handle
            .expect("bus consumer spawns under #[tokio::test]")
            .await
            .expect("consumer task panicked");

        let log = audit.lock().unwrap();
        let lagged = log.query_by_event(&AuditEvent::AuditBusLagged);
        assert_eq!(lagged.len(), 1);
        assert!(lagged[0].detail.contains(SESSION_INVALIDATION_BUS));
        assert!(lagged[0].detail.contains(&format!("dropped={overrun}")));
        assert_eq!(
            log.query_by_event(&AuditEvent::SessionRevoked).len(),
            SESSION_INVALIDATION_CHANNEL_CAPACITY
        );
    }

    /// A lag on the user-change bus costs its audit rows and nothing else.
    #[tokio::test]
    async fn a_user_change_lag_records_how_many_rows_it_lost() {
        let audit = make_audit();
        let (si_bus, si_rx) = SessionInvalidationBus::new();
        let (uc_bus, uc_rx) = UserChangeBus::new();

        for user_id in 0..(USER_CHANGE_CHANNEL_CAPACITY as u64 + 1) {
            uc_bus.publish(UserChanged { user_id });
        }
        let handle = spawn_bus_consumer(si_rx, uc_rx, Arc::clone(&audit));

        drop(si_bus);
        drop(uc_bus);
        handle
            .expect("bus consumer spawns under #[tokio::test]")
            .await
            .expect("consumer task panicked");

        let log = audit.lock().unwrap();
        let lagged = log.query_by_event(&AuditEvent::AuditBusLagged);
        assert_eq!(lagged.len(), 1);
        assert!(lagged[0].detail.contains(USER_CHANGE_BUS));
    }

    #[test]
    fn bus_consumer_entries_survive_a_crash() {
        use crate::control::security::audit::restore::{audit_with_test_wal, replay_after_crash};

        let dir = tempfile::tempdir().expect("tempdir");
        let (audit, wal) = audit_with_test_wal(dir.path());
        record_rows(
            &audit,
            &[
                BusRow::from(&invalidated(41)),
                BusRow::from(&UserChanged { user_id: 42 }),
                lag_row(USER_CHANGE_BUS, 3),
            ],
        );
        drop(audit);

        let sources: Vec<String> = replay_after_crash(&wal)
            .into_iter()
            .map(|e| e.source)
            .collect();
        for source in [
            "session_invalidation_bus",
            "user_change_bus",
            "bus_consumer",
        ] {
            assert!(
                sources.iter().any(|s| s == source),
                "the {source} entry must replay after a crash: {sources:?}"
            );
        }
    }
}
