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

use std::sync::{Arc, Mutex, MutexGuard};

use crate::bridge::dispatch::{OutcomeFloor, WriteWindow};
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::wal_dispatch::{WalAppendOutcome, WalAppendRequest, wal_append};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::WalManager;
use crate::wal::manager::{NO_APPLY_KEY, WalAppender};

/// Where a write's records live. The abort markers that cancel them carry it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordOwner {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
}

/// The records one write appended, and the window that holds the outcome
/// floor below them.
#[derive(Debug)]
#[must_use = "minted records hold the outcome floor until they settle, cancel, or hold"]
pub(crate) struct MintedRecords {
    window: WriteWindow,
    lsns: Mutex<Vec<Lsn>>,
    /// Whether this write appended the records. A resent record belongs to
    /// the write that appended it, and only that write can cancel it.
    appended_here: bool,
}

impl MintedRecords {
    /// Open the window. Call it before the first record is appended.
    pub(crate) fn open(floor: &Arc<OutcomeFloor>) -> Self {
        Self {
            window: floor.open_write(),
            lsns: Mutex::new(Vec::new()),
            appended_here: true,
        }
    }

    /// Hold an existing record at `lsn` that is sent to a core again. `None`
    /// when the floor already passed it: its outcome is final, and a second
    /// apply would land below the floor.
    pub(crate) fn resend(floor: &Arc<OutcomeFloor>, lsn: Lsn) -> Option<Self> {
        let window = floor.open_existing(lsn)?;
        Some(Self {
            window,
            lsns: Mutex::new(vec![lsn]),
            appended_here: false,
        })
    }

    fn recorded(&self) -> MutexGuard<'_, Vec<Lsn>> {
        self.lsns.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// An appender whose records carry `apply_key` and join this set.
    pub(crate) fn appender<'a>(&'a self, wal: &'a WalManager, apply_key: u64) -> WalAppender<'a> {
        wal.recording_appender(apply_key, &self.lsns)
    }

    /// Append `plan`'s redo records under this window.
    pub(crate) fn append_plan(
        &self,
        wal: &WalManager,
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

    /// The highest appended LSN, or `None` when nothing was appended.
    pub(crate) fn highest(&self) -> Option<Lsn> {
        self.recorded().iter().copied().max()
    }

    /// Every appended LSN, in append order.
    #[cfg(test)]
    pub(crate) fn lsns(&self) -> Vec<Lsn> {
        self.recorded().clone()
    }

    /// Note every recorded LSN on the window and take the list out.
    fn into_parts(self) -> (WriteWindow, Vec<Lsn>, bool) {
        let Self {
            window,
            lsns,
            appended_here,
        } = self;
        let lsns = lsns.into_inner().unwrap_or_else(|p| p.into_inner());
        if let Some(highest) = lsns.iter().copied().max() {
            window.note_minted(highest);
        }
        (window, lsns, appended_here)
    }

    /// The outcome of every record is final.
    pub(crate) fn settle(self) {
        let (window, _, _) = self.into_parts();
        window.settle();
    }

    /// The records have no final outcome in this process.
    #[track_caller]
    pub(crate) fn hold(self) {
        let (window, _, _) = self.into_parts();
        window.hold();
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
        self,
        wal: &WalManager,
        owner: RecordOwner,
        marker_key: u64,
    ) -> crate::Result<()> {
        let (window, lsns, appended_here) = self.into_parts();
        if !appended_here {
            window.settle();
            return Ok(());
        }
        let mut last_marker = None;
        for lsn in &lsns {
            match wal.appender(marker_key).append_write_aborted(
                owner.tenant_id,
                owner.vshard_id,
                owner.database_id,
                *lsn,
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
            cancelled = lsns.len(),
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

    fn append(wal: &WalManager, minted: &MintedRecords, body: &[u8]) -> Lsn {
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

    #[test]
    fn settled_records_release_the_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
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
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
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
            MintedRecords::resend(&floor, lsn).is_none(),
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
}
