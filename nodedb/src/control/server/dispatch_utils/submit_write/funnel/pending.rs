// SPDX-License-Identifier: BUSL-1.1

//! A write the funnel enqueued on its core, and the response phase that
//! finishes it.
//!
//! The enqueue and the response phase are separate so a caller can enqueue
//! writes in a fixed order and collect their outcomes in any order. The
//! data-group apply loop does this: it enqueues committed entries in log
//! order and collects each outcome independently, so one parked write never
//! holds back the writes behind it.

use crate::control::state::SharedState;

use super::super::params::{SubmitOutcome, SubmitWrite};
use super::driver::enqueue_write;
use super::response::{ResponsePhaseInput, collect_classify_and_finish};

/// A write past its enqueue.
pub(crate) struct PendingWrite {
    stage: Stage,
}

enum Stage {
    /// The write has its outcome already: the Calvin scheduler applied it.
    Done(SubmitOutcome),
    /// A core holds the write. The response phase collects its outcome.
    Dispatched {
        input: Box<ResponsePhaseInput>,
        /// The write changes a permission-tree source on a node with no
        /// lease, so its ack waits until the local permission cache holds it.
        binds_authorization: bool,
    },
}

impl PendingWrite {
    pub(super) fn done(outcome: SubmitOutcome) -> Self {
        Self {
            stage: Stage::Done(outcome),
        }
    }

    pub(super) fn dispatched(input: ResponsePhaseInput, binds_authorization: bool) -> Self {
        Self {
            stage: Stage::Dispatched {
                input: Box::new(input),
                binds_authorization,
            },
        }
    }

    /// Collect the outcome, classify it, and run every step a completed write
    /// still owes before it is acknowledged.
    ///
    /// See [`SubmitOutcome`] for what comes back.
    pub(crate) async fn finish(self, shared: &SharedState) -> crate::Result<SubmitOutcome> {
        let (input, binds_authorization) = match self.stage {
            Stage::Done(outcome) => return Ok(outcome),
            Stage::Dispatched {
                input,
                binds_authorization,
            } => (input, binds_authorization),
        };
        let max_result_bytes = shared.tuning.network.max_query_result_bytes as usize;
        let outcome = collect_classify_and_finish(shared, max_result_bytes, *input).await?;
        if binds_authorization {
            crate::control::security::auth_lease::await_local_coverage(
                shared,
                std::time::Instant::now()
                    + std::time::Duration::from_secs(shared.tuning.network.default_deadline_secs),
            )
            .await?;
        }
        Ok(outcome)
    }
}

/// Admit, make durable, enqueue, collect, and publish one write.
///
/// See [`SubmitOutcome`] for what comes back.
pub(crate) async fn submit_write(
    shared: &SharedState,
    params: SubmitWrite,
) -> crate::Result<SubmitOutcome> {
    enqueue_write(shared, params).await?.finish(shared).await
}
