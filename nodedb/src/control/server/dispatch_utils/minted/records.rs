// SPDX-License-Identifier: BUSL-1.1

//! The records a write appended to the WAL for one Data-Plane dispatch, held
//! under the outcome-floor window opened before the first append.
//!
//! The window holds the outcome floor below every record until the write's
//! outcome is final. Each path that ends the write picks one close:
//!
//! - [`MintedRecords::settle`]: the records applied, or their outcome at the
//!   core is final.
//! - [`MintedRecords::cancel`]: nothing applied. A `WriteAborted` marker names
//!   each record, and the window settles once the markers are durable.
//! - [`MintedRecords::hold`]: the records have no final outcome in this
//!   process. Restart replay must reach them, so the floor stays below them
//!   until the process exits.
//!
//! Appends go through [`MintedRecords::appender`], which records the LSN of
//! every record it writes. A plan that appends several records is cancelled
//! whole.
//!
//! Records dropped without a close still close. Records never sent to a core
//! are cancelled in place: a `WriteAborted` marker names each one and the
//! window settles. Records a core can hold have no known outcome, so their
//! window leaks and files its report. A caller dropped at any await before
//! the dispatch therefore leaves nothing open. After the dispatch the
//! records belong to the task that waits for the final response.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::bridge::dispatch::{OutcomeFloor, ResendRefusal, WriteWindow};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::wal_dispatch::{WalAppendOutcome, WalAppendRequest, wal_append};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::WalManager;
use crate::wal::manager::{AppendSink, NO_APPLY_KEY, RecordedAppend, WalAppender};

/// Where a write's records live. The abort markers that cancel them carry it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordOwner {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
}

/// The records one write appended, and the window that holds the outcome
/// floor below them.
#[must_use = "minted records hold the outcome floor until they settle, cancel, or hold"]
pub(crate) struct MintedRecords {
    /// `None` once a close took it.
    window: Option<WriteWindow>,
    appended: Mutex<Vec<RecordedAppend>>,
    /// The existing record this set resends. A resent record belongs to the
    /// write that appended it, and only that write can cancel it.
    resent: Option<Lsn>,
    /// The WAL the records went to. The first append stores it.
    wal: OnceLock<Arc<WalManager>>,
    /// Whether a core can hold the records.
    sent: AtomicBool,
}

impl std::fmt::Debug for MintedRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MintedRecords")
            .field("open", &self.window.is_some())
            .field("appended", &*self.recorded())
            .field("resent", &self.resent)
            .field("sent", &self.sent.load(Ordering::Acquire))
            .finish()
    }
}

/// A closed set's parts: the window, every appended record, and the
/// resent LSN.
type Parts = (WriteWindow, Vec<RecordedAppend>, Option<Lsn>);

impl MintedRecords {
    /// Open the window. Call it before the first record is appended.
    pub(crate) fn open(floor: &Arc<OutcomeFloor>) -> Self {
        Self::with_window(floor.open_write(), None)
    }

    /// Hold an existing record at `lsn` that is sent to a core again.
    /// Refused, with the reason, when its outcome is final or a live or held
    /// window carries it to one: a second apply would land below the floor,
    /// or apply the record twice.
    pub(crate) fn resend(floor: &Arc<OutcomeFloor>, lsn: Lsn) -> Result<Self, ResendRefusal> {
        let window = floor.open_existing(lsn)?;
        Ok(Self::with_window(window, Some(lsn)))
    }

    fn with_window(window: WriteWindow, resent: Option<Lsn>) -> Self {
        Self {
            window: Some(window),
            appended: Mutex::new(Vec::new()),
            resent,
            wal: OnceLock::new(),
            sent: AtomicBool::new(false),
        }
    }

    fn recorded(&self) -> MutexGuard<'_, Vec<RecordedAppend>> {
        self.appended.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// An appender whose records carry `apply_key` and join this set.
    pub(crate) fn appender<'a>(
        &'a self,
        wal: &'a Arc<WalManager>,
        apply_key: u64,
    ) -> WalAppender<'a> {
        self.wal.get_or_init(|| Arc::clone(wal));
        wal.recording_appender(apply_key, self)
    }

    /// Append `plan`'s redo records under this window.
    pub(crate) fn append_plan(
        &self,
        wal: &Arc<WalManager>,
        owner: RecordOwner,
        plan: &PhysicalPlan,
    ) -> crate::Result<WalAppendOutcome> {
        wal_append(WalAppendRequest {
            wal: self.appender(wal, NO_APPLY_KEY),
            tenant_id: owner.tenant_id,
            vshard_id: owner.vshard_id,
            database_id: owner.database_id,
            plan,
            credentials: None,
            now_override: None,
        })
    }

    /// Mark the records as held by a core. Call it once the request carrying
    /// them is enqueued, or once they are committed to a path that carries
    /// them to their outcome. Dropped records are never cancelled after it.
    pub(crate) fn mark_sent(&self) {
        self.sent.store(true, Ordering::Release);
    }

    /// The highest appended or resent LSN, or `None` when there is none.
    pub(crate) fn highest(&self) -> Option<Lsn> {
        self.recorded()
            .iter()
            .map(|record| record.lsn)
            .chain(self.resent)
            .max()
    }

    /// Every appended LSN, in append order.
    #[cfg(test)]
    pub(crate) fn lsns(&self) -> Vec<Lsn> {
        self.recorded().iter().map(|record| record.lsn).collect()
    }

    /// Take the window and the records out. `None` when a close already
    /// took them.
    fn take_parts(&mut self) -> Option<Parts> {
        let window = self.window.take()?;
        let appended = std::mem::take(&mut *self.recorded());
        Some((window, appended, self.resent))
    }

    /// The outcome of every record is final.
    pub(crate) fn settle(mut self) {
        if let Some((window, _, _)) = self.take_parts() {
            window.settle();
        }
    }

    /// The records have no final outcome in this process.
    #[track_caller]
    pub(crate) fn hold(mut self) {
        if let Some((window, _, _)) = self.take_parts() {
            window.hold();
        }
    }

    /// Cancel every record with a `WriteAborted` marker that carries
    /// `marker_key`, wait until the markers are durable, then settle.
    ///
    /// A failed append or fsync holds the window and returns the error: the
    /// records stay replayable, so the floor must not pass them.
    ///
    /// A crash before the markers are durable still leaves the records
    /// replayable. The markers make the refusal durable once it is reported.
    ///
    /// A resent record is not cancelled here: the write that appended it
    /// cancels it from its own outcome. The window settles.
    ///
    /// The cancel runs in a task the caller does not own, so a caller
    /// dropped mid-cancel leaves the task to close the window.
    pub(crate) async fn cancel(
        self,
        wal: &Arc<WalManager>,
        owner: RecordOwner,
        marker_key: u64,
    ) -> crate::Result<()> {
        let wal = Arc::clone(wal);
        tokio::spawn(async move { self.cancel_in_place(&wal, owner, marker_key).await })
            .await
            .map_err(|error| crate::Error::Internal {
                detail: format!("the task cancelling a write's records failed: {error}"),
            })?
    }

    async fn cancel_in_place(
        mut self,
        wal: &WalManager,
        owner: RecordOwner,
        marker_key: u64,
    ) -> crate::Result<()> {
        let Some((window, appended, resent)) = self.take_parts() else {
            return Ok(());
        };
        if resent.is_some() {
            window.settle();
            return Ok(());
        }
        let mut last_marker = None;
        for record in &appended {
            match wal.appender(marker_key).append_write_aborted(
                owner.tenant_id,
                owner.vshard_id,
                owner.database_id,
                record.lsn,
            ) {
                Ok(marker) => last_marker = Some(marker),
                Err(error) => {
                    window.hold();
                    return Err(error);
                }
            }
        }
        if let Some(marker) = last_marker
            && let Err(error) = wal.wait_durable(marker).await
        {
            window.hold();
            return Err(error);
        }
        tracing::debug!(
            cancelled = appended.len(),
            "refused write records cancelled in the WAL"
        );
        window.settle();
        Ok(())
    }

    /// Cancel records whose write another path carries to its outcome, such
    /// as a Calvin route or a Raft proposal, in a task the caller does not
    /// own. That path's result stands. A cancel error holds the window,
    /// which files its report, and is logged with `path` naming the route.
    ///
    /// The caller awaits [`Superseded::finish`] once the other path returns.
    /// A caller dropped before then leaves the task to finish the cancel.
    pub(crate) fn supersede(
        self,
        wal: Arc<WalManager>,
        owner: RecordOwner,
        path: &'static str,
    ) -> Superseded {
        Superseded {
            task: tokio::spawn(async move {
                if let Err(error) = self.cancel_in_place(&wal, owner, 0).await {
                    tracing::error!(
                        path,
                        %error,
                        "records of a write carried by another path could not be \
                         cancelled; their window is held until restart"
                    );
                }
            }),
        }
    }

    /// Close records dropped without a close. Runs in the dropping thread and
    /// spawns nothing.
    ///
    /// Records no core holds are cancelled in place, and the window settles
    /// once each marker is appended. The markers are not awaited: nothing
    /// reported an outcome for these records, so a crash that loses a marker
    /// leaves a write whose caller never learned its outcome. A marker that
    /// fails to append holds the window.
    ///
    /// A resent record, or a set with nothing appended, settles. Records a
    /// core can hold leak their window, which files its report.
    fn close_dropped(&mut self) {
        let sent = self.sent.load(Ordering::Acquire);
        let Some((window, appended, resent)) = self.take_parts() else {
            return;
        };
        if sent {
            drop(window);
            return;
        }
        if resent.is_some() || appended.is_empty() {
            window.settle();
            return;
        }
        let Some(wal) = self.wal.get() else {
            // Only `appender` adds records, and it stores the WAL first.
            window.hold();
            return;
        };
        for record in &appended {
            if let Err(error) = wal.appender(NO_APPLY_KEY).append_write_aborted(
                record.tenant_id,
                record.vshard_id,
                record.database_id,
                record.lsn,
            ) {
                tracing::error!(
                    %error,
                    lsn = record.lsn.as_u64(),
                    "records dropped before dispatch could not be cancelled; \
                     their window is held until restart"
                );
                window.hold();
                return;
            }
        }
        tracing::debug!(
            cancelled = appended.len(),
            "records dropped before dispatch cancelled in the WAL"
        );
        window.settle();
    }
}

/// Each append joins the set, and the window owns its LSN from the moment
/// the record exists. A resend of the record is refused while it is owned.
impl AppendSink for MintedRecords {
    fn record(&self, append: RecordedAppend) {
        if let Some(window) = &self.window {
            window.own(append.lsn);
        }
        self.recorded().push(append);
    }
}

impl Drop for MintedRecords {
    fn drop(&mut self) {
        self.close_dropped();
    }
}

/// The task cancelling records another path superseded.
pub(crate) struct Superseded {
    task: tokio::task::JoinHandle<()>,
}

impl Superseded {
    /// Wait until the cancel ended.
    pub(crate) async fn finish(self) {
        if let Err(error) = self.task.await {
            tracing::error!(%error, "the task cancelling superseded records failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::manager::NO_APPLY_KEY;

    fn owner() -> RecordOwner {
        RecordOwner {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
        }
    }

    fn append(wal: &Arc<WalManager>, minted: &MintedRecords, body: &[u8]) -> Lsn {
        minted
            .appender(wal, NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                body,
            )
            .expect("append")
    }

    fn open_wal(dir: &tempfile::TempDir) -> Arc<WalManager> {
        Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"))
    }

    fn replayed(wal: &WalManager) -> Vec<u64> {
        wal.sync().expect("sync");
        wal.replay()
            .expect("replay")
            .iter()
            .map(|record| record.header.lsn)
            .collect()
    }

    #[test]
    fn settled_records_release_the_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let floor = OutcomeFloor::new();
        let minted = MintedRecords::open(&floor);
        let lsn = append(&wal, &minted, b"a");
        assert!(floor.floor() < lsn);
        minted.settle();
        assert_eq!(floor.floor(), lsn);
    }

    #[test]
    fn held_records_keep_the_floor_below_them() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let floor = OutcomeFloor::new();
        let minted = MintedRecords::open(&floor);
        let lsn = append(&wal, &minted, b"a");
        minted.hold();
        assert!(floor.floor() < lsn);
        assert_eq!(floor.leaked_windows(), 0, "a hold is not a leak");
    }

    #[tokio::test]
    async fn a_resent_record_is_never_cancelled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let lsn = wal
            .appender(NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"a",
            )
            .expect("append");
        let resent = MintedRecords::resend(&floor, lsn).expect("the floor is below the record");
        assert!(floor.floor() < lsn);
        resent.cancel(&wal, owner(), 0).await.expect("cancel");
        wal.sync().expect("sync");
        let replayed: Vec<u64> = wal
            .replay()
            .expect("replay")
            .iter()
            .map(|record| record.header.lsn)
            .collect();
        assert!(replayed.contains(&lsn.as_u64()), "no marker names it");
        assert_eq!(floor.floor(), lsn);
        assert!(
            MintedRecords::resend(&floor, lsn).is_err(),
            "the floor passed the record"
        );
    }

    #[tokio::test]
    async fn cancelled_records_are_dropped_from_replay_and_release_the_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let minted = MintedRecords::open(&floor);
        let first = append(&wal, &minted, b"a");
        let second = append(&wal, &minted, b"b");
        assert_eq!(minted.lsns(), vec![first, second]);
        assert_eq!(minted.highest(), Some(second));
        minted.cancel(&wal, owner(), 0).await.expect("cancel");
        assert!(
            wal.durable_through() > second.as_u64(),
            "markers are durable"
        );
        let replayed: Vec<u64> = wal
            .replay()
            .expect("replay")
            .iter()
            .map(|record| record.header.lsn)
            .collect();
        assert!(!replayed.contains(&first.as_u64()));
        assert!(!replayed.contains(&second.as_u64()));
        assert!(floor.floor() >= second, "the window settled");
    }

    #[test]
    fn records_dropped_before_dispatch_are_cancelled_and_release_the_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let floor = OutcomeFloor::new();
        let minted = MintedRecords::open(&floor);
        let first = append(&wal, &minted, b"a");
        let second = append(&wal, &minted, b"b");
        drop(minted);
        let replayed = replayed(&wal);
        assert!(!replayed.contains(&first.as_u64()));
        assert!(!replayed.contains(&second.as_u64()));
        assert!(floor.floor() >= second, "the window settled");
        assert_eq!(floor.leaked_windows(), 0);
        assert_eq!(floor.held_windows(), 0);
    }

    #[test]
    fn records_dropped_after_dispatch_keep_the_floor_below_them() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let floor = OutcomeFloor::new();
        let minted = MintedRecords::open(&floor);
        let lsn = append(&wal, &minted, b"a");
        minted.mark_sent();
        drop(minted);
        assert!(replayed(&wal).contains(&lsn.as_u64()), "no marker names it");
        assert!(floor.floor() < lsn);
        assert_eq!(
            floor.leaked_windows(),
            1,
            "the dropped window files its leak"
        );
    }

    #[test]
    fn a_dropped_set_with_nothing_appended_settles() {
        let floor = OutcomeFloor::new();
        drop(MintedRecords::open(&floor));
        assert_eq!(floor.leaked_windows(), 0);
        assert_eq!(floor.held_windows(), 0);
    }

    #[test]
    fn a_dropped_resend_settles_without_a_marker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let floor = OutcomeFloor::new();
        let lsn = wal
            .appender(NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"a",
            )
            .expect("append");
        drop(MintedRecords::resend(&floor, lsn).expect("the floor is below the record"));
        assert!(replayed(&wal).contains(&lsn.as_u64()), "no marker names it");
        assert_eq!(floor.floor(), lsn);
        assert_eq!(floor.leaked_windows(), 0);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn a_failed_drop_cancel_holds_the_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let floor = OutcomeFloor::new();
        let minted = MintedRecords::open(&floor);
        let lsn = append(&wal, &minted, b"a");
        {
            let _fail =
                crate::fail_point::FailGuard::fail("wal::append_write_aborted", "disk full");
            drop(minted);
        }
        assert!(replayed(&wal).contains(&lsn.as_u64()), "no marker names it");
        assert!(floor.floor() < lsn);
        assert_eq!(floor.held_windows(), 1);
        assert_eq!(floor.leaked_windows(), 0);
    }

    /// A writer that appended a record and has not sent it yet owns it, so a
    /// resend cannot race it to a core.
    #[test]
    fn a_resend_is_refused_while_a_live_window_owns_the_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let floor = OutcomeFloor::new();
        let minted = MintedRecords::open(&floor);
        let lsn = append(&wal, &minted, b"a");

        assert!(floor.floor() < lsn);
        assert!(
            MintedRecords::resend(&floor, lsn).is_err(),
            "the writer owns it"
        );

        minted.settle();
    }

    /// A record whose owner closed has a final outcome, even while an older
    /// window keeps the floor below it.
    #[test]
    fn a_resend_is_refused_after_the_owner_closed_above_the_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(&dir);
        let floor = OutcomeFloor::new();
        let older = MintedRecords::open(&floor);
        append(&wal, &older, b"a");
        let newer = MintedRecords::open(&floor);
        let lsn = append(&wal, &newer, b"b");
        newer.settle();

        assert!(floor.floor() < lsn, "the older window holds the floor");
        assert!(
            MintedRecords::resend(&floor, lsn).is_err(),
            "its outcome is final"
        );

        older.settle();
        assert!(floor.floor() >= lsn);
    }
}
