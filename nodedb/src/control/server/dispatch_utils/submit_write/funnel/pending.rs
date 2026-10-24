// SPDX-License-Identifier: BUSL-1.1

//! A write the funnel enqueued on its core, and the response phase that
//! finishes it.
//!
//! The enqueue and the response phase are separate so a caller can enqueue
//! writes in a fixed order and collect their outcomes in any order. The
//! data-group apply loop does this: it enqueues committed entries in log
//! order and collects each outcome independently, so one parked write never
//! holds back the writes behind it.

use crate::bridge::envelope::{ErrorCode, Status};
use crate::control::state::SharedState;
use crate::control::write_resolve::MAX_WRITE_RESOLVE_RETRIES;

use super::super::params::{
    ChangeFeedOwner, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering,
};
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
    Dispatched(Box<ResponsePhaseInput>),
}

impl PendingWrite {
    pub(super) fn done(outcome: SubmitOutcome) -> Self {
        Self {
            stage: Stage::Done(outcome),
        }
    }

    pub(super) fn dispatched(input: ResponsePhaseInput) -> Self {
        Self {
            stage: Stage::Dispatched(Box::new(input)),
        }
    }

    /// Collect the outcome, classify it, and run every step a completed write
    /// still owes before it is acknowledged.
    ///
    /// See [`SubmitOutcome`] for what comes back.
    pub(crate) async fn finish(self, shared: &SharedState) -> crate::Result<SubmitOutcome> {
        match self.stage {
            Stage::Done(outcome) => Ok(outcome),
            Stage::Dispatched(input) => collect_classify_and_finish(shared, *input).await,
        }
    }
}

/// Admit, make durable, enqueue, collect, and publish one write.
///
/// A live timeseries ingest resolves its rows before its record is appended
/// (`driver`). Its install refuses with `OllpRetryRequired`, cancelling the
/// record, when a concurrent write changed the collection schema since the
/// resolve. It is then submitted again, up to
/// [`MAX_WRITE_RESOLVE_RETRIES`] times, and resolves against the new schema.
///
/// See [`SubmitOutcome`] for what comes back.
pub(crate) async fn submit_write(
    shared: &SharedState,
    params: SubmitWrite,
) -> crate::Result<SubmitOutcome> {
    let mut params = params;
    let mut attempt: u32 = 0;
    loop {
        let retry = timeseries_retry(&params);
        let outcome = enqueue_write(shared, params).await?.finish(shared).await?;
        match retry {
            Some(next) if refused_for_drift(&outcome) && attempt < MAX_WRITE_RESOLVE_RETRIES => {
                attempt += 1;
                params = next;
            }
            _ => return Ok(outcome),
        }
    }
}

/// A copy of `params` to submit again when its install refuses for schema
/// drift: a live, autocommit, unresolved timeseries ingest the funnel
/// appends. `None` for every other write.
fn timeseries_retry(params: &SubmitWrite) -> Option<SubmitWrite> {
    let WalDurability::AppendHere {
        now_override,
        apply_key: 0,
        commit_hlc,
        change_position: None,
        system_ordinal: None,
    } = &params.durability
    else {
        return None;
    };
    if params.txn_id.is_some()
        || !matches!(params.ordering, WriteOrdering::Gate)
        || !crate::control::write_resolve::is_unresolved_ingest(&params.plan)
    {
        return None;
    }
    let change_feed = match &params.change_feed {
        ChangeFeedOwner::LocalApply => ChangeFeedOwner::LocalApply,
        ChangeFeedOwner::Unowned => ChangeFeedOwner::Unowned,
        ChangeFeedOwner::Replicated { .. } => return None,
    };
    Some(SubmitWrite {
        tenant_id: params.tenant_id,
        database_id: params.database_id,
        vshard_id: params.vshard_id,
        plan: params.plan.clone(),
        trace_id: params.trace_id,
        event_source: params.event_source,
        txn_id: None,
        user_id: params.user_id.clone(),
        user_roles: params.user_roles.clone(),
        durability: WalDurability::AppendHere {
            now_override: *now_override,
            apply_key: 0,
            commit_hlc: *commit_hlc,
            change_position: None,
            system_ordinal: None,
        },
        ordering: WriteOrdering::Gate,
        change_feed,
    })
}

/// Whether the core refused the write because the collection schema moved
/// between its resolve and its install.
fn refused_for_drift(outcome: &SubmitOutcome) -> bool {
    outcome.response.status == Status::Error
        && outcome.response.error_code.as_deref() == Some(&ErrorCode::OllpRetryRequired)
}
