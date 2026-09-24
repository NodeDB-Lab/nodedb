// SPDX-License-Identifier: BUSL-1.1

//! The Control-Plane half of the Data Plane shutdown drain.
//!
//! The drain rests on one ordering rule: the enqueue gate closes under the
//! same mutex every enqueue takes. [`Dispatcher::begin_data_plane_drain`] runs
//! with that mutex held, so a `dispatch` call that observed an open gate has
//! already pushed its request by the time the drain starts. From that point no
//! further work can reach a Data Plane core.
//!
//! What is left is work the cores already hold. The Control Plane can see all
//! of it without reaching across the plane boundary:
//!
//! - `wfq.total_depth()` — accepted, staged, not yet in the SPSC ring.
//! - `outstanding` — pushed into the ring and not yet answered. A core answers
//!   only after it executes the task, so this covers a request sitting in the
//!   ring and a request executing on the core right now.
//!
//! Both counters live on the `Dispatcher`, so the drain reads Control-Plane
//! state only. The Data Plane keeps its single channel — the SPSC bridge — and
//! shares no lock with this path.

use tracing::warn;

use crate::bridge::envelope;
use crate::bridge::envelope::{ErrorCode, Payload, Status};
use crate::types::{Lsn, RequestId};

use super::dispatcher::Dispatcher;
use super::enqueue::release_inflight_slot;

/// Work one core still owes the Control Plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorePending {
    /// Index of the Data Plane core.
    pub core_id: usize,
    /// Requests accepted into the weighted-fair queue and not yet pushed into
    /// the SPSC ring.
    pub staged: usize,
    /// Requests pushed into the SPSC ring and not yet answered. Includes the
    /// task the core is executing right now.
    pub outstanding: usize,
}

impl std::fmt::Display for CorePending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "core-{} (staged {}, outstanding {})",
            self.core_id, self.staged, self.outstanding
        )
    }
}

impl Dispatcher {
    /// Close the enqueue gate for every Data Plane core.
    ///
    /// Callers must hold the dispatcher mutex, which is what makes the close
    /// ordered against every concurrent enqueue. Staged work is flushed toward
    /// the ring on the way out so the drain starts with as little backlog as
    /// the ring headroom allows.
    ///
    /// Idempotent: a second call re-flushes and changes nothing else.
    pub fn begin_data_plane_drain(&mut self) {
        self.data_plane_draining = true;
        let outcome_floor = self.outcome_floor.floor();
        for channel in self.cores.iter_mut() {
            channel.flush_wfq(outcome_floor);
        }
    }

    /// Whether the enqueue gate is closed.
    pub fn is_data_plane_draining(&self) -> bool {
        self.data_plane_draining
    }

    /// Refuse a request that arrived after the gate closed.
    ///
    /// The caller answers the client with this error rather than dropping the
    /// request: a node that is shutting down must say so, and the dispatch
    /// class already means "this never reached a core", which is exactly what
    /// happened.
    pub(super) fn reject_if_draining(&self) -> crate::Result<()> {
        if !self.data_plane_draining {
            return Ok(());
        }
        Err(crate::Error::Dispatch {
            detail: "node is draining its Data Plane for shutdown; \
                     the request was not executed and can be retried elsewhere"
                .to_string(),
        })
    }

    /// Every core that still owes work, with its counts. Empty means the Data
    /// Plane holds nothing the Control Plane handed it.
    pub fn data_plane_pending(&self) -> Vec<CorePending> {
        self.cores
            .iter()
            .enumerate()
            .filter_map(|(core_id, channel)| {
                let staged = channel.wfq.total_depth();
                let outstanding = channel.outstanding.len();
                if staged == 0 && outstanding == 0 {
                    return None;
                }
                Some(CorePending {
                    core_id,
                    staged,
                    outstanding,
                })
            })
            .collect()
    }

    /// Wake every core so it observes ring contents now instead of on its next
    /// idle poll timeout. A core with no notifier wired yet is skipped — it has
    /// not started, so it holds nothing.
    pub fn wake_all_cores(&self) {
        for channel in self.cores.iter() {
            if let Some(ref notifier) = channel.wake_notifier {
                notifier.notify();
            }
        }
    }

    /// Fail every request the cores still hold, answering each one instead of
    /// leaving its caller to wait out its deadline.
    ///
    /// Called only after the bounded drain expires. Staged work never reached a
    /// core, and outstanding work reached one that did not answer in time.
    pub fn abandon_data_plane_work(&mut self) -> Vec<envelope::Response> {
        let mut abandoned = Vec::new();
        let mut freed = false;
        for (core_id, channel) in self.cores.iter_mut().enumerate() {
            let mut ids: Vec<u64> = channel
                .wfq
                .drain()
                .into_iter()
                .map(|req| req.request_id.as_u64())
                .collect();
            let mut seen: std::collections::HashSet<u64> = ids.iter().copied().collect();
            for rid in channel.outstanding.drain() {
                if seen.insert(rid) {
                    ids.push(rid);
                }
            }
            if ids.is_empty() {
                continue;
            }
            warn!(
                core_id,
                abandoned = ids.len(),
                "data plane drain deadline expired — failing the requests this core still holds"
            );
            for rid in ids {
                // The node is shutting down, and this core publishes nothing more.
                self.dispatched_lsns.settle(rid);
                freed |=
                    release_inflight_slot(&mut self.request_tenant, &mut self.tenant_inflight, rid);
                abandoned.push(envelope::Response {
                    request_id: RequestId::new(rid),
                    status: Status::Error,
                    attempt: 1,
                    partial: false,
                    payload: Payload::empty(),
                    watermark_lsn: Lsn::ZERO,
                    error_code: Some(Box::new(ErrorCode::Internal {
                        detail: format!(
                            "core-{core_id} did not finish this request before the \
                             shutdown drain deadline"
                        ),
                    })),
                    read_set_valid: None,
                    read_version_lsn: Lsn::ZERO,
                    write_set: Vec::new(),
                });
            }
        }
        if freed {
            self.capacity_freed.notify_waiters();
        }
        abandoned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::test_requests::make_request_for_db;

    #[test]
    fn a_fresh_dispatcher_accepts_work_and_reports_it_pending() {
        let (mut dispatcher, _data_sides) = Dispatcher::new(2, 64);
        assert!(!dispatcher.is_data_plane_draining());
        assert!(dispatcher.data_plane_pending().is_empty());

        dispatcher
            .dispatch(make_request_for_db(0, 0, 1))
            .expect("a running dispatcher accepts work");
        let pending = dispatcher.data_plane_pending();
        assert_eq!(pending.len(), 1, "one core holds the request");
        assert_eq!(pending[0].outstanding, 1);
    }

    #[test]
    fn work_arriving_after_the_gate_closes_is_refused_not_dropped() {
        let (mut dispatcher, _data_sides) = Dispatcher::new(2, 64);
        dispatcher.begin_data_plane_drain();

        let err = dispatcher
            .dispatch(make_request_for_db(0, 0, 1))
            .expect_err("a draining dispatcher must refuse new work");
        assert!(
            matches!(err, crate::Error::Dispatch { .. }),
            "refusal is a dispatch error the caller can answer with: {err}"
        );
        assert!(
            dispatcher.data_plane_pending().is_empty(),
            "a refused request must not be counted as in-flight work"
        );
    }

    #[test]
    fn direct_core_dispatch_is_refused_after_the_gate_closes() {
        let (mut dispatcher, _data_sides) = Dispatcher::new(2, 64);
        dispatcher.begin_data_plane_drain();

        let err = dispatcher
            .dispatch_to_core(0, make_request_for_db(0, 0, 1))
            .expect_err("the direct-to-core path uses the same gate");
        assert!(matches!(err, crate::Error::Dispatch { .. }));
    }

    #[test]
    fn begin_drain_is_idempotent() {
        let (mut dispatcher, _data_sides) = Dispatcher::new(1, 64);
        dispatcher.begin_data_plane_drain();
        dispatcher.begin_data_plane_drain();
        assert!(dispatcher.is_data_plane_draining());
        assert!(
            dispatcher.dispatch(make_request_for_db(0, 0, 1)).is_err(),
            "a second drain start changes nothing"
        );
    }

    #[test]
    fn abandoned_work_is_answered_and_cleared() {
        let (mut dispatcher, _data_sides) = Dispatcher::new(1, 64);
        dispatcher
            .dispatch(make_request_for_db(0, 0, 7))
            .expect("accept before the drain");
        dispatcher.begin_data_plane_drain();

        let abandoned = dispatcher.abandon_data_plane_work();
        assert_eq!(abandoned.len(), 1);
        assert_eq!(abandoned[0].request_id, RequestId::new(7));
        assert_eq!(abandoned[0].status, Status::Error);
        assert!(
            dispatcher.data_plane_pending().is_empty(),
            "abandoning clears the pending set so a second pass answers nothing"
        );
        assert!(dispatcher.abandon_data_plane_work().is_empty());
    }
}
