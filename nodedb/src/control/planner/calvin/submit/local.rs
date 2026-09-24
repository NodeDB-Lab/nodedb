// SPDX-License-Identifier: BUSL-1.1

//! Local submit-and-await primitive for the Calvin cross-shard write path.
//!
//! PRECONDITION for everything here: this node is the sequencer-group leader.
//! Its sequencer service assigns and its registry receives the replicated
//! completion ack. A submit on a non-leader is drained and discarded, so
//! non-leader callers must route through [`super::routed`].

use std::time::Duration;

use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::calvin::{AttemptOutcome, TxnId};

use crate::Error;
use crate::bridge::envelope::Response;
use crate::control::planner::calvin::abort_error::calvin_abort_error;
use crate::control::state::{CalvinApplyResult, SharedState};

/// Build a minimal Control-Plane [`Response`] carrying only the RETURNING
/// `payload` bytes forwarded over the cross-node routed-submit RPC.
///
/// The coordinator only reads `.payload` (and derives the plan kind from the
/// task) when shaping RETURNING rows, so the other fields are placeholders: the
/// authoritative status/LSN already lived on the leader that applied the txn.
pub(super) fn synthetic_returning_response(payload_bytes: Vec<u8>) -> Response {
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::{Lsn, RequestId};

    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Payload::from_vec(payload_bytes),
        watermark_lsn: Lsn::ZERO,
        error_code: None,
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}

/// Submit `tx_class` to THIS node's Calvin sequencer inbox and await completion.
///
/// PRECONDITION: this node is the sequencer-group leader (its service assigns;
/// its registry receives the replicated completion ack). Callers that are not
/// the leader MUST route via [`super::routed::submit_calvin_routed`].
///
/// The assignment + completion waits are bounded by
/// `state.tuning.network.default_deadline_secs`.
pub async fn submit_and_await_calvin(
    state: &SharedState,
    tx_class: TxClass,
) -> crate::Result<Option<Response>> {
    let timeout = Duration::from_secs(state.tuning.network.default_deadline_secs);
    submit_and_await_calvin_with_timeout(state, tx_class, timeout).await
}

/// [`submit_and_await_calvin`] with an explicit deadline budget.
///
/// Used by the leader-side RPC handler so the forwarded submit-and-await is
/// bounded by the coordinator's remaining deadline rather than this node's full
/// default deadline.
pub async fn submit_and_await_calvin_with_timeout(
    state: &SharedState,
    tx_class: TxClass,
    timeout: Duration,
) -> crate::Result<Option<Response>> {
    let inbox = state
        .sequencer_inbox
        .get()
        .ok_or(Error::SequencerUnavailable)?;
    let registry = state
        .calvin_completion_registry
        .get()
        .ok_or(Error::SequencerUnavailable)?;

    let inbox_seq = inbox.submit(tx_class).map_err(|e| Error::BadRequest {
        detail: format!("Calvin sequencer rejected transaction: {e}"),
    })?;

    let assignment_rx = registry.register_submission(inbox_seq);
    let (epoch, position, participants) = tokio::time::timeout(timeout, assignment_rx)
        .await
        .map_err(|_| Error::Internal {
            detail: "timed out waiting for Calvin sequencer assignment".to_owned(),
        })?
        .map_err(|_| Error::Internal {
            detail: "Calvin sequencer assignment channel closed".to_owned(),
        })?;

    let completion_rx = registry.register_completion(TxnId::new(epoch, position), participants);
    let outcome = tokio::time::timeout(timeout, completion_rx)
        .await
        .map_err(|_| {
            let err = Error::Internal {
                detail: "timed out waiting for Calvin transaction completion".to_owned(),
            };
            // This timeout is the only signal a silently-never-completed
            // Calvin write ever produces; file it as a structured report at
            // the one site that detects it, since the error alone gives an
            // operator no clue which transaction or participant stalled.
            crate::diag::calvin_completion_timeout(
                &err,
                epoch,
                position,
                participants,
                timeout.as_secs(),
            );
            err
        })?
        .map_err(|_| Error::Internal {
            detail: "Calvin completion channel closed".to_owned(),
        })?;
    // Terminal, NON-retryable: the scheduler rejected the transaction's local
    // plan routing and broadcast `TxnRoutingFailed`. Surface it immediately —
    // falling through to the RETURNING-drain below would silently report
    // `Ok(None)` for a transaction that never applied.
    if let AttemptOutcome::Failed { detail } = &outcome {
        return Err(Error::Internal {
            detail: format!("calvin transaction routing failed: {detail}"),
        });
    }
    // Terminal, NON-retryable: the global cross-shard verdict was ABORT and the
    // writes were dropped. This is a fall-through chain, NOT a match — without
    // this explicit check `Aborted` would fall through to the RETURNING drain
    // below and silently return `Ok(None)`, reporting COMMIT SUCCESS for a
    // transaction that never applied. The verdict's reason picks the error the
    // client retries on.
    if let AttemptOutcome::Aborted { reason } = &outcome {
        return Err(calvin_abort_error(*reason));
    }
    // The static (non-dependent) Calvin path never produces an OLLP mismatch —
    // `note_ollp_mismatch` only fires on the dependent-predicate retry path — so
    // this branch is unreachable at runtime today. It is kept as a typed error
    // (never a panic) so any future mismatch signal on this channel surfaces
    // deterministically instead of crashing.
    if outcome == AttemptOutcome::Mismatch {
        return Err(Error::Internal {
            detail: "OLLP mismatch outcome on non-dependent Calvin path".to_owned(),
        });
    }

    // Completion fired: the scheduler deposited the applied Response (with any
    // RETURNING rows) into the sidecar BEFORE proposing the ack that woke this
    // waiter, so the entry is present now if this write carried RETURNING.
    // Drain it (removing the entry) and hand it back so the coordinator can emit
    // DATA-ROW output instead of a bare command tag. `None` for plain writes; a
    // `Conflict` (>1 RETURNING participant) fails loudly rather than returning a
    // partial cross-shard union.
    let drained = state
        .calvin_apply_results
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&TxnId::new(epoch, position));
    match drained {
        Some(CalvinApplyResult::Single { response, .. }) => {
            // An installed txn whose reply failed to render deposits it as an
            // error for the statement.
            crate::control::server::dispatch_utils::reject_data_plane_error(&response)?;
            Ok(Some(response))
        }
        Some(CalvinApplyResult::Conflict) => Err(Error::Internal {
            detail: "multi-participant cross-shard RETURNING not supported".to_owned(),
        }),
        None => Ok(None),
    }
}
