// SPDX-License-Identifier: BUSL-1.1

//! Waiting for a dispatched write's response in a task the caller does not
//! own.
//!
//! Once a write is enqueued, its records close from the core's final
//! response. A caller future dropped mid-wait will drop the records with it
//! and leak their window. The wait and the close run in a spawned task
//! instead. The caller awaits what the task reports, and a dropped caller
//! leaves the task running until the records close.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::oneshot;

use crate::bridge::envelope::Response;
use crate::control::ResponseReceiver;
use crate::control::local_dispatch::{DispatchCollectError, collect_bounded_response};
use crate::wal::WalManager;

use super::records::{MintedRecords, RecordOwner};
use super::resolve::{resolve_at_final, resolve_on_response};

/// How the owned task reads the response it reports.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Collect {
    /// Every frame up to the final one, merged, under a byte budget.
    Merged { max_result_bytes: usize },
}

/// Where the owned task waits, and how the records close.
pub(crate) struct OwnedWait {
    pub wal: Arc<WalManager>,
    pub owner: RecordOwner,
    /// The key a final refusal's abort marker carries, `0` when this write's
    /// refusals are never final.
    pub final_refusal_key: u64,
    /// The instant the caller stops waiting.
    pub deadline: Instant,
    pub collect: Collect,
    /// Where the final response goes when it arrives after the caller
    /// stopped waiting.
    pub late: Option<oneshot::Sender<Response>>,
}

/// What the owned task reports to the caller.
pub(crate) enum OwnedResponse {
    /// A response arrived by the deadline. `closed` is the result of closing
    /// the records from it: a failed cancel returns its error and holds the
    /// window. A partial response reports `Ok` here, and the task closes the
    /// records from the final one.
    Answered {
        response: Response,
        closed: crate::Result<()>,
    },
    /// The deadline passed first. The task closes the records once the
    /// final response arrives.
    DeadlineExceeded,
    /// The merged response outgrew its byte budget. The task closes the
    /// records once the final response arrives.
    OverBudget { bytes: usize },
    /// The channel closed without a final response. The records are held.
    ChannelClosed,
}

/// What a task that owns a dispatched write's records reports. Dropping it
/// leaves the task to close the records.
pub(crate) struct OwnedReport {
    rx: oneshot::Receiver<OwnedResponse>,
}

impl OwnedReport {
    /// Wait for what the task reports.
    pub(crate) async fn recv(self) -> crate::Result<OwnedResponse> {
        self.rx.await.map_err(|_| crate::Error::Internal {
            detail: "the task waiting for a dispatched write's response ended without \
                     reporting"
                .into(),
        })
    }
}

/// Mark `minted` sent and hand it to a spawned task that waits for `rx`'s
/// response and closes the records from it.
///
/// Call it in the same synchronous step as the enqueue of the request that
/// carries the records. No caller future owns them after that, so no drop
/// at a later await leaks their window.
pub(crate) fn spawn_owned_wait(
    wait: OwnedWait,
    rx: ResponseReceiver,
    minted: MintedRecords,
) -> OwnedReport {
    minted.mark_sent();
    let (report_tx, report_rx) = oneshot::channel();
    tokio::spawn(wait_and_close(wait, rx, minted, report_tx));
    OwnedReport { rx: report_rx }
}

async fn wait_and_close(
    wait: OwnedWait,
    mut rx: ResponseReceiver,
    minted: MintedRecords,
    report: oneshot::Sender<OwnedResponse>,
) {
    let OwnedWait {
        wal,
        owner,
        final_refusal_key,
        deadline,
        collect,
        late,
    } = wait;
    let forward = |final_response: Option<Response>| {
        if let (Some(late), Some(response)) = (late, final_response) {
            let _ = late.send(response);
        }
    };
    let until = tokio::time::Instant::from_std(deadline);
    let collected = match collect {
        Collect::Merged { max_result_bytes } => {
            tokio::time::timeout_at(until, collect_bounded_response(&mut rx, max_result_bytes))
                .await
        }
    };
    match collected {
        Ok(Ok(response)) if response.partial => {
            let _ = report.send(OwnedResponse::Answered {
                response,
                closed: Ok(()),
            });
            forward(resolve_at_final(&wal, owner, final_refusal_key, rx, minted).await);
        }
        Ok(Ok(response)) => {
            let closed =
                resolve_on_response(&wal, owner, final_refusal_key, &response, minted).await;
            let _ = report.send(OwnedResponse::Answered { response, closed });
        }
        Ok(Err(DispatchCollectError::OverBudget { bytes })) => {
            let _ = report.send(OwnedResponse::OverBudget { bytes });
            forward(resolve_at_final(&wal, owner, final_refusal_key, rx, minted).await);
        }
        Ok(Err(DispatchCollectError::ChannelClosed)) => {
            minted.hold();
            let _ = report.send(OwnedResponse::ChannelClosed);
        }
        Err(_) => {
            let _ = report.send(OwnedResponse::DeadlineExceeded);
            forward(resolve_at_final(&wal, owner, final_refusal_key, rx, minted).await);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::bridge::dispatch::OutcomeFloor;
    use crate::bridge::envelope::{ErrorCode, Payload, Status};
    use crate::control::RequestTracker;
    use crate::types::{DatabaseId, Lsn, RequestId, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;

    fn owner() -> RecordOwner {
        RecordOwner {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
        }
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
                b"x",
            )
            .expect("append");
        (minted, lsn)
    }

    fn refusal(id: u64) -> Response {
        Response {
            request_id: RequestId::new(id),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: Some(Box::new(ErrorCode::RejectedConstraint {
                constraint: "unique".into(),
                detail: "duplicate key".into(),
            })),
            stage_vote: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn wait(wal: &Arc<WalManager>, deadline: Instant) -> OwnedWait {
        OwnedWait {
            wal: Arc::clone(wal),
            owner: owner(),
            final_refusal_key: 0,
            deadline,
            collect: Collect::Merged {
                max_result_bytes: 1 << 20,
            },
            late: None,
        }
    }

    /// A final response that arrives after the deadline goes to the late
    /// receiver, once the records closed from it.
    #[tokio::test]
    async fn a_late_final_response_is_forwarded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let tracker = RequestTracker::new();
        let rx = tracker.register(RequestId::new(3));
        let (minted, _lsn) = minted_record(&wal, &floor);
        let (late_tx, late_rx) = oneshot::channel();
        let report = spawn_owned_wait(
            OwnedWait {
                late: Some(late_tx),
                ..wait(&wal, Instant::now() + Duration::from_millis(20))
            },
            rx,
            minted,
        );
        let outcome = report.recv().await.expect("report");
        assert!(matches!(outcome, OwnedResponse::DeadlineExceeded));

        let mut ok = refusal(3);
        ok.status = Status::Ok;
        ok.error_code = None;
        assert!(tracker.complete(ok));
        let late = late_rx.await.expect("the late response is forwarded");
        assert_eq!(late.status, Status::Ok);
    }

    /// The caller future is dropped while it waits. The task still closes
    /// the records from the refusal that arrives afterwards.
    #[tokio::test]
    async fn a_dropped_caller_still_closes_its_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (minted, lsn) = minted_record(&wal, &floor);
        let tracker = RequestTracker::new();
        let rx = tracker.register(RequestId::new(1));
        let deadline = Instant::now() + Duration::from_secs(30);

        let caller = tokio::time::timeout(
            Duration::from_millis(10),
            spawn_owned_wait(wait(&wal, deadline), rx, minted).recv(),
        )
        .await;
        assert!(
            caller.is_err(),
            "the caller gave up before the core answered"
        );
        assert!(floor.floor() < lsn, "the window holds while the core works");

        assert!(tracker.complete(refusal(1)));
        for _ in 0..200 {
            if floor.floor() >= lsn {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(floor.floor() >= lsn, "the refusal closed the records");
        assert_eq!(floor.leaked_windows(), 0);
    }

    /// The deadline passes first. The caller hears it at once, and the task
    /// closes the records from the final response that follows.
    #[tokio::test]
    async fn a_deadline_reports_at_once_and_the_records_close_later() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (minted, lsn) = minted_record(&wal, &floor);
        let tracker = RequestTracker::new();
        let rx = tracker.register(RequestId::new(2));

        let outcome = spawn_owned_wait(wait(&wal, Instant::now()), rx, minted)
            .recv()
            .await
            .expect("report");
        assert!(matches!(outcome, OwnedResponse::DeadlineExceeded));
        assert!(floor.floor() < lsn);

        assert!(tracker.complete(refusal(2)));
        for _ in 0..200 {
            if floor.floor() >= lsn {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(floor.floor() >= lsn);
    }

    /// The report is dropped before anyone polls it. The task owns the
    /// records from the spawn, so the answer still settles them, once.
    #[tokio::test]
    async fn a_report_dropped_unpolled_still_settles_the_records_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (minted, lsn) = minted_record(&wal, &floor);
        let tracker = RequestTracker::new();
        let rx = tracker.register(RequestId::new(3));
        let deadline = Instant::now() + Duration::from_secs(30);

        drop(spawn_owned_wait(wait(&wal, deadline), rx, minted));
        assert!(floor.floor() < lsn, "the window holds while the core works");

        let mut applied = refusal(3);
        applied.status = Status::Ok;
        applied.error_code = None;
        assert!(tracker.complete(applied));
        for _ in 0..200 {
            if floor.floor() >= lsn {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(floor.floor() >= lsn, "the answer settled the records");
        wal.sync().expect("sync");
        let replayed: Vec<u64> = wal
            .replay()
            .expect("replay")
            .iter()
            .map(|record| record.header.lsn)
            .collect();
        assert!(replayed.contains(&lsn.as_u64()), "no marker names it");
        assert_eq!(floor.leaked_windows(), 0);
        assert_eq!(floor.held_windows(), 0);
    }
}
