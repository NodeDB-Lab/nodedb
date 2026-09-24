// SPDX-License-Identifier: BUSL-1.1

//! Fan a post-apply physical plan out to every core on this node.
//!
//! A committed catalog entry whose physical work runs per node dispatches the
//! same plan to each core, matching how the boot seed installs state on every
//! core rather than one vshard. The caller owns the report for an unreached
//! core, because it alone knows which capture site names the mutation.

use std::time::Duration;

use tracing::debug;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
use crate::control::ResponseReceiver;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

/// Deadline for one core's acknowledgement of a post-apply meta op.
pub(super) const DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Scope and naming for one fan-out, as every log line and error reports it.
pub(super) struct CoreFanout<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Collection the plan targets.
    pub collection: &'a str,
    /// Names the mutation in the ack line and the unreached-core error.
    pub what: &'a str,
    /// Extra identity for the ack line. Empty when the collection names it all.
    pub detail: &'a str,
}

/// Every core's answer to one fan-out, as far as the dispatch deadline saw.
pub(super) struct FanoutAnswers {
    /// Cores that were not reached, or that answered anything but `Ok`.
    pub(super) refused: Vec<usize>,
    /// Cores still working at the deadline, each with the receiver its
    /// answer arrives on.
    pub(super) pending: Vec<(usize, ResponseReceiver)>,
}

impl FanoutAnswers {
    /// Every core that did not apply the plan, once each pending core gave
    /// its final answer. Waits with no deadline. A core whose receiver
    /// closes without a final answer counts as not applied.
    pub(super) async fn into_final_refusals(self) -> Vec<usize> {
        let mut refused = self.refused;
        for (core_id, mut rx) in self.pending {
            match final_response(&mut rx).await {
                Some(resp) if resp.status == Status::Ok => {}
                _ => refused.push(core_id),
            }
        }
        refused
    }
}

/// Dispatch `plan` to every core on this node, raising when any core failed to
/// apply it.
pub(super) async fn dispatch_to_every_core(
    shared: &SharedState,
    target: &CoreFanout<'_>,
    plan: &PhysicalPlan,
) -> crate::Result<()> {
    let unreached = unacked_cores(shared, target, plan).await;
    if unreached.is_empty() {
        return Ok(());
    }
    Err(crate::Error::Internal {
        detail: format!("cores did not apply the {}: {unreached:?}", target.what),
    })
}

/// Dispatch `plan` to every core on this node, returning the cores that
/// did not answer `Ok` by the dispatch deadline.
async fn unacked_cores(
    shared: &SharedState,
    target: &CoreFanout<'_>,
    plan: &PhysicalPlan,
) -> Vec<usize> {
    let answers = fan_out(shared, target, plan).await;
    let mut unacked = answers.refused;
    unacked.extend(answers.pending.into_iter().map(|(core_id, _)| core_id));
    unacked
}

/// Dispatch `plan` to every core on this node and collect the answers that
/// arrive by the dispatch deadline.
///
/// A caller that dispatches two plans covering each other reads the two
/// answer sets per core instead of raising on the first refusal.
pub(super) async fn fan_out(
    shared: &SharedState,
    target: &CoreFanout<'_>,
    plan: &PhysicalPlan,
) -> FanoutAnswers {
    let num_cores = {
        let d = shared.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
        d.num_cores()
    };
    let deadline = std::time::Instant::now() + DISPATCH_TIMEOUT;
    let mut receivers = Vec::with_capacity(num_cores);
    let mut refused: Vec<usize> = Vec::new();

    {
        let mut d = shared.dispatcher.lock().unwrap_or_else(|p| p.into_inner());
        for core_id in 0..num_cores {
            let request_id = shared.next_request_id();
            let request = Request {
                request_id,
                tenant_id: TenantId::new(target.tenant_id),
                database_id: DatabaseId::new(target.database_id),
                vshard_id: VShardId::new(core_id as u32),
                plan: plan.clone(),
                deadline,
                priority: Priority::Background,
                trace_id: TraceId::generate(),
                consistency: ReadConsistency::Eventual,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            };
            let rx = shared.tracker.register(request_id);
            if d.dispatch_to_core(core_id, request).is_err() {
                shared.tracker.cancel(&request_id);
                refused.push(core_id);
                continue;
            }
            receivers.push((core_id, rx));
        }
    }

    let mut pending: Vec<(usize, ResponseReceiver)> = Vec::new();
    let wait_until = tokio::time::Instant::from_std(deadline);
    for (core_id, mut rx) in receivers {
        match tokio::time::timeout_at(wait_until, final_response(&mut rx)).await {
            Ok(Some(resp)) if resp.status == Status::Ok => {
                debug!(
                    tenant = target.tenant_id,
                    collection = %target.collection,
                    detail = target.detail,
                    core_id,
                    what = target.what,
                    "post-apply core ack"
                );
            }
            Ok(_) => refused.push(core_id),
            Err(_) => pending.push((core_id, rx)),
        }
    }

    FanoutAnswers { refused, pending }
}

/// The request's final response, skipping partial frames. `None` once the
/// receiver closes without one. Cancel-safe.
async fn final_response(rx: &mut ResponseReceiver) -> Option<Response> {
    while let Some(resp) = rx.recv().await {
        if !resp.partial {
            return Some(resp);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Payload;
    use crate::control::RequestTracker;
    use crate::types::{Lsn, RequestId};

    fn answer(id: u64, status: Status, partial: bool) -> Response {
        Response {
            request_id: RequestId::new(id),
            status,
            attempt: 1,
            partial,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    /// A core that outlived the dispatch deadline counts by its final
    /// answer. Partial frames before it do not decide anything.
    #[tokio::test]
    async fn a_pending_core_counts_by_its_final_answer() {
        let tracker = RequestTracker::new();
        let applied = tracker.register(RequestId::new(1));
        let refused = tracker.register(RequestId::new(2));
        let answers = FanoutAnswers {
            refused: vec![3],
            pending: vec![(0, applied), (1, refused)],
        };
        let waiter = tokio::spawn(answers.into_final_refusals());

        assert!(tracker.complete(answer(1, Status::Partial, true)));
        assert!(tracker.complete(answer(1, Status::Ok, false)));
        assert!(tracker.complete(answer(2, Status::Error, false)));

        assert_eq!(waiter.await.expect("waiter"), vec![3, 1]);
    }

    /// A receiver that closes without a final answer leaves the core's
    /// outcome unknown, so it counts as not applied.
    #[tokio::test]
    async fn a_core_whose_receiver_closes_counts_as_not_applied() {
        let tracker = RequestTracker::new();
        let rx = tracker.register(RequestId::new(4));
        tracker.cancel(&RequestId::new(4));
        let answers = FanoutAnswers {
            refused: Vec::new(),
            pending: vec![(2, rx)],
        };

        assert_eq!(answers.into_final_refusals().await, vec![2]);
    }
}
