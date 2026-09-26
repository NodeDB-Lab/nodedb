// SPDX-License-Identifier: BUSL-1.1

//! Close a write's outcome-floor window from the core's final response.
//!
//! A refusal whose code proves nothing applied cancels the records. Any other
//! final response settles them: the core's outcome is final, and restart
//! replay from the floor reproduces it.

use std::sync::Arc;

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::wal::WalManager;

use super::super::write_abort::{refusal_is_final, write_definitely_not_applied};
use super::records::{MintedRecords, RecordOwner};

/// Close `minted` from the core's final `response`.
///
/// `final_refusal_key` is the proposal key a final refusal's marker carries,
/// `0` when no proposal carries this write. A failed cancel returns the error
/// and holds the window.
pub(crate) async fn resolve_on_response(
    wal: &Arc<WalManager>,
    owner: RecordOwner,
    final_refusal_key: u64,
    response: &Response,
    minted: MintedRecords,
) -> crate::Result<()> {
    let refusal = response
        .error_code
        .as_deref()
        .filter(|code| response.status != Status::Ok && write_definitely_not_applied(code));
    match refusal {
        Some(code) => {
            let marker_key = if refusal_is_final(code) {
                final_refusal_key
            } else {
                0
            };
            // A refused sync frame advanced its stream's high-water mark. The
            // frame's record is cancelled, so the mark gets a record of its
            // own, durable with the markers.
            if let ErrorCode::SyncRejected { provenance, .. } = code
                && let Err(error) = wal.appender(marker_key).append_sync_seq_advance(
                    provenance.producer_id,
                    provenance.epoch,
                    provenance.stream_id,
                    provenance.seq,
                )
            {
                minted.hold();
                return Err(error);
            }
            minted.cancel(wal, owner, marker_key).await
        }
        None => {
            minted.settle();
            Ok(())
        }
    }
}

/// Close `minted` once the core's final response arrives on `rx`, after the
/// caller stopped waiting for it.
///
/// A refusal that arrives after the caller timed out still cancels its
/// records, so a restart cannot apply a write the caller never saw applied. A
/// channel that closes before a final response leaves the outcome unknown:
/// the window holds.
pub(crate) async fn resolve_at_final(
    wal: &Arc<WalManager>,
    owner: RecordOwner,
    final_refusal_key: u64,
    mut rx: crate::control::ResponseReceiver,
    minted: MintedRecords,
) {
    loop {
        match rx.recv().await {
            Some(response) if response.partial => continue,
            Some(response) => {
                if let Err(error) =
                    resolve_on_response(wal, owner, final_refusal_key, &response, minted).await
                {
                    tracing::error!(
                        %error,
                        "a late refusal's abort marker failed; its records stay replayable"
                    );
                }
                return;
            }
            None => {
                minted.hold();
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::OutcomeFloor;
    use crate::bridge::envelope::{ErrorCode, Payload};
    use crate::types::{DatabaseId, Lsn, RequestId, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;

    fn owner() -> RecordOwner {
        RecordOwner {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
        }
    }

    fn response(status: Status, code: Option<ErrorCode>, partial: bool) -> Response {
        Response {
            request_id: RequestId::new(1),
            status,
            attempt: 1,
            partial,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: code.map(Box::new),
            read_set_valid: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn refusal() -> Response {
        response(
            Status::Error,
            Some(ErrorCode::RejectedConstraint {
                constraint: "unique".into(),
                detail: "duplicate key".into(),
            }),
            false,
        )
    }

    fn minted_record(wal: &Arc<WalManager>, floor: &Arc<OutcomeFloor>) -> (MintedRecords, Lsn) {
        let minted = MintedRecords::open(floor);
        let lsn = minted
            .appender(wal, NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"row",
            )
            .expect("append");
        (minted, lsn)
    }

    fn replayed(wal: &WalManager) -> Vec<u64> {
        wal.replay()
            .expect("replay")
            .iter()
            .map(|record| record.header.lsn)
            .collect()
    }

    #[tokio::test]
    async fn an_applied_response_settles_and_keeps_the_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (minted, lsn) = minted_record(&wal, &floor);
        let ok = response(Status::Ok, None, false);
        resolve_on_response(&wal, owner(), 0, &ok, minted)
            .await
            .expect("resolve");
        wal.sync().expect("sync");
        assert!(replayed(&wal).contains(&lsn.as_u64()));
        assert_eq!(floor.floor(), lsn);
    }

    #[tokio::test]
    async fn an_ambiguous_failure_settles_and_keeps_the_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (minted, lsn) = minted_record(&wal, &floor);
        let internal = response(
            Status::Error,
            Some(ErrorCode::Internal {
                detail: "io".into(),
            }),
            false,
        );
        resolve_on_response(&wal, owner(), 0, &internal, minted)
            .await
            .expect("resolve");
        wal.sync().expect("sync");
        assert!(replayed(&wal).contains(&lsn.as_u64()));
        assert_eq!(floor.floor(), lsn);
    }

    #[tokio::test]
    async fn a_definite_refusal_cancels_the_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (minted, lsn) = minted_record(&wal, &floor);
        resolve_on_response(&wal, owner(), 0, &refusal(), minted)
            .await
            .expect("resolve");
        assert!(!replayed(&wal).contains(&lsn.as_u64()));
        assert!(floor.floor() >= lsn);
    }

    /// The caller stopped waiting before the core answered. The refusal that
    /// arrives later still writes its abort marker before the window settles.
    #[tokio::test]
    async fn a_refusal_after_the_caller_timed_out_still_writes_its_abort_marker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (minted, lsn) = minted_record(&wal, &floor);
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let rx = crate::control::ResponseReceiver::from_channel(rx);
        let waiter = {
            let wal = Arc::clone(&wal);
            tokio::spawn(async move { resolve_at_final(&wal, owner(), 0, rx, minted).await })
        };
        assert!(floor.floor() < lsn, "the window holds while the core works");

        tx.send(response(Status::Ok, None, true))
            .await
            .expect("send partial");
        tx.send(refusal()).await.expect("send refusal");
        waiter.await.expect("waiter");

        assert!(!replayed(&wal).contains(&lsn.as_u64()));
        assert!(floor.floor() >= lsn);
    }

    #[tokio::test]
    async fn a_channel_closed_before_a_final_response_holds_the_window() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (minted, lsn) = minted_record(&wal, &floor);
        let (tx, rx) = tokio::sync::mpsc::channel::<Response>(4);
        let rx = crate::control::ResponseReceiver::from_channel(rx);
        let waiter = {
            let wal = Arc::clone(&wal);
            tokio::spawn(async move { resolve_at_final(&wal, owner(), 0, rx, minted).await })
        };
        drop(tx);
        waiter.await.expect("waiter");
        assert!(floor.floor() < lsn);
        assert_eq!(floor.leaked_windows(), 0);
    }

    /// A sync frame the gate refused for good is cancelled, and the
    /// high-water mark it advanced is journalled on its own. Restart replay
    /// then restores the mark, never applies the frame, and counts the
    /// refusal as the proposal's outcome.
    #[tokio::test]
    async fn a_refused_sync_frame_journals_its_mark_and_cancels_its_record() {
        const KEY: u64 = 0xAB;
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (minted, lsn) = minted_record(&wal, &floor);
        let rejected = response(
            Status::Error,
            Some(ErrorCode::SyncRejected {
                violation: nodedb_types::sync::violation::ViolationType::PermissionDenied,
                applied_seq: 4,
                provenance: nodedb_types::sync::wire::SyncProvenance {
                    producer_id: 9,
                    epoch: 2,
                    stream_id: 1,
                    seq: 4,
                },
            }),
            false,
        );

        resolve_on_response(&wal, owner(), KEY, &rejected, minted)
            .await
            .expect("resolve");

        wal.sync().expect("sync");
        let records = wal.replay().expect("replay");
        assert!(
            !records
                .iter()
                .any(|record| record.header.lsn == lsn.as_u64()),
            "the refused frame never replays"
        );
        let (maps, _) =
            crate::wal::replay::replay_sync_hwm_records(&records).expect("replay marks");
        assert_eq!(maps.sync_hwm.get(&(9, 1)), Some(&4));
        assert_eq!(maps.producer_epoch_floor.get(&9), Some(&2));
        let ledger = crate::control::distributed_applier::ProposalLedger::from_records(&records, 8);
        assert!(
            ledger.prior(KEY).is_some(),
            "the refusal is the proposal's outcome"
        );
        assert!(floor.floor() >= lsn);
    }
}
