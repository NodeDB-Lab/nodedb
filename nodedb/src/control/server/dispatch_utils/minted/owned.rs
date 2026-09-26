// SPDX-License-Identifier: BUSL-1.1

//! Waiting for a dispatched write's response in a task the caller does not
//! own.
//!
//! Once a write is enqueued, its records close from the core's final
//! response. A caller future dropped mid-wait would drop the records with it
//! and leak their window. The wait and the close run in a spawned task
//! instead. The caller awaits what the task reports, and a dropped caller
//! leaves the task running until the records close.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::oneshot;

use crate::bridge::envelope::Response;
use crate::control::ResponseReceiver;
use crate::wal::WalManager;

use super::super::collect::{DispatchCollectError, collect_bounded_response};
use super::records::{MintedRecords, RecordOwner};
use super::resolve::{resolve_at_final, resolve_on_response};

/// How the owned task reads the response it reports.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Collect {
    /// Every frame up to the final one, merged, under a byte budget.
    Merged { max_result_bytes: usize },
    /// The first frame. A partial first frame leaves the task closing the
    /// records from the final one.
    First,
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

/// Wait for `rx`'s response in a spawned task that owns `minted`, and
/// return what it reports.
pub(crate) async fn await_response_owned(
    wait: OwnedWait,
    rx: ResponseReceiver,
    minted: MintedRecords,
) -> crate::Result<OwnedResponse> {
    let (report_tx, report_rx) = oneshot::channel();
    tokio::spawn(wait_and_close(wait, rx, minted, report_tx));
    report_rx.await.map_err(|_| crate::Error::Internal {
        detail: "the task waiting for a dispatched write's response ended without \
                 reporting"
            .into(),
    })
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
    } = wait;
    let until = tokio::time::Instant::from_std(deadline);
    let collected = match collect {
        Collect::Merged { max_result_bytes } => {
            tokio::time::timeout_at(until, collect_bounded_response(&mut rx, max_result_bytes))
                .await
        }
        Collect::First => {
            tokio::time::timeout_at(until, async {
                rx.recv().await.ok_or(DispatchCollectError::ChannelClosed)
            })
            .await
        }
    };
    match collected {
        Ok(Ok(response)) if response.partial => {
            let _ = report.send(OwnedResponse::Answered {
                response,
                closed: Ok(()),
            });
            resolve_at_final(&wal, owner, final_refusal_key, rx, minted).await;
        }
        Ok(Ok(response)) => {
            let closed =
                resolve_on_response(&wal, owner, final_refusal_key, &response, minted).await;
            let _ = report.send(OwnedResponse::Answered { response, closed });
        }
        Ok(Err(DispatchCollectError::OverBudget { bytes })) => {
            let _ = report.send(OwnedResponse::OverBudget { bytes });
            resolve_at_final(&wal, owner, final_refusal_key, rx, minted).await;
        }
        Ok(Err(DispatchCollectError::ChannelClosed)) => {
            minted.hold();
            let _ = report.send(OwnedResponse::ChannelClosed);
        }
        Err(_) => {
            let _ = report.send(OwnedResponse::DeadlineExceeded);
            resolve_at_final(&wal, owner, final_refusal_key, rx, minted).await;
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
            read_set_valid: None,
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
        }
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
            await_response_owned(wait(&wal, deadline), rx, minted),
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

        let outcome = await_response_owned(wait(&wal, Instant::now()), rx, minted)
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
}
