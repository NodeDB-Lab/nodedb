// SPDX-License-Identifier: BUSL-1.1

//! Coordinator-owned OLLP dependent-read retry loop.
//!
//! The retry loop for dependent-read (OLLP) Calvin transactions is owned by the
//! coordinator (the pgwire handler), not the per-vshard scheduler. On a
//! post-exec predicate-drift mismatch the loop runs a FRESH pre-execution
//! reconnaissance before resubmitting — a stale prediction can never converge
//! under predicate drift. The scheduler's only job on mismatch is to release the
//! aborted attempt's locks and signal the completion registry so this loop wakes.

use nodedb_cluster::calvin::{AttemptOutcome, CalvinCompletionRegistry, TxnId};

use crate::Error;
use crate::control::cluster::calvin::executor::ollp::error::OllpError;
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::planner::calvin::submit::RoutedAssignment;

// ── run_dependent_with_retry ──────────────────────────────────────────────────

/// Coordinator-owned OLLP dependent-read retry loop with FRESH reconnaissance
/// per attempt.
///
/// This is the single owner of the submit → await-assignment → await-completion
/// → (mismatch ? re-scan : done) loop for dependent-read Calvin transactions.
/// On a POST-EXEC predicate-drift mismatch (the executor released the aborted
/// attempt's locks and the scheduler signalled the registry), the loop runs the
/// injected `rescan` closure to produce a FRESH prediction and resubmits — a
/// stale prediction can never converge under predicate drift. On a PRE-ADMISSION
/// failure (`OllpError` from the circuit-breaker / sequencer / tenant budget),
/// nothing executed, so the loop resubmits the SAME prediction after backoff.
///
/// `submit` and `rescan` are injected so this loop is unit-testable WITHOUT a
/// live server/executor: a fake scheduler driving the real
/// [`CalvinCompletionRegistry`] suffices.
pub struct DependentRetryArgs<'a, P, SF, RF> {
    pub registry: &'a CalvinCompletionRegistry,
    pub orchestrator: &'a OllpOrchestrator,
    pub predicate_class_hash: u64,
    pub timeout: std::time::Duration,
    pub ollp_max_retries: u32,
    pub initial_predicted: P,
    pub submit: SF,
    pub rescan: RF,
}

pub async fn run_dependent_with_retry<P, SF, SFut, RF, RFut>(
    args: DependentRetryArgs<'_, P, SF, RF>,
) -> crate::Result<TxnId>
where
    SF: FnMut(&P) -> SFut,
    SFut: std::future::Future<Output = Result<RoutedAssignment, OllpError>>,
    RF: FnMut() -> RFut,
    RFut: std::future::Future<Output = crate::Result<P>>,
{
    let DependentRetryArgs {
        registry,
        orchestrator,
        predicate_class_hash,
        timeout,
        ollp_max_retries,
        initial_predicted,
        mut submit,
        mut rescan,
    } = args;
    let mut predicted = initial_predicted;
    let mut retry: u32 = 0;
    loop {
        let assignment = match submit(&predicted).await {
            Ok(assignment) => assignment,
            Err(_ollp_err) => {
                // PRE-ADMISSION failure (circuit/sequencer/budget). Nothing
                // executed, so there is no aborted attempt to re-scan around —
                // resubmit the SAME prediction after backoff bookkeeping.
                if retry >= ollp_max_retries {
                    return Err(Error::OllpExhausted {
                        retries: ollp_max_retries.min(u8::MAX as u32) as u8,
                    });
                }
                orchestrator
                    .on_retry_required(predicate_class_hash, retry)
                    .await;
                retry += 1;
                continue;
            }
        };

        // The assignment (`epoch`/`position`) is produced by
        // `submit_calvin_routed_assign` inside the injected `submit` closure —
        // which routes to the sequencer-group leader and awaits only the
        // assignment phase. The coordinator then awaits completion on its local
        // registry, which receives the replicated completion ack on every
        // sequencer-group member.
        let txn_id = TxnId::new(assignment.epoch, assignment.position);
        let completion_rx = registry.register_completion(txn_id, assignment.participants);
        let outcome = tokio::time::timeout(timeout, completion_rx)
            .await
            .map_err(|_| Error::Internal {
                detail: "timed out waiting for Calvin completion".into(),
            })?
            .map_err(|_| Error::Internal {
                detail: "Calvin completion channel closed".into(),
            })?;

        match outcome {
            // Return the completed txn's id so the caller can drain the applied
            // Response (RETURNING rows) the scheduler deposited before the ack.
            AttemptOutcome::Completed => return Ok(txn_id),
            // Terminal, NON-retryable: the global cross-shard OCC verdict was
            // ABORT (read-set validation failed). A serialization failure is a
            // terminal verdict, not OLLP predicate drift — a fresh reconnaissance
            // cannot change a committed verdict, so surface it to the client as
            // SQLSTATE 40001 immediately instead of burning retries.
            AttemptOutcome::Aborted => {
                return Err(Error::CalvinSerializationConflict);
            }
            // Terminal, NON-retryable: the scheduler rejected the transaction's
            // local plan routing and broadcast `TxnRoutingFailed`. A fresh
            // reconnaissance can never fix a routing rejection, so surface it
            // to the caller immediately instead of burning retries.
            AttemptOutcome::Failed { detail } => {
                return Err(Error::Internal {
                    detail: format!("calvin transaction routing failed: {detail}"),
                });
            }
            AttemptOutcome::Mismatch => {
                // POST-EXEC predicate drift. The scheduler already released the
                // aborted attempt's locks before signalling the registry, so a
                // FRESH reconnaissance is safe — and necessary, since the stale
                // prediction can never converge under drift.
                if retry >= ollp_max_retries {
                    return Err(Error::OllpExhausted {
                        retries: ollp_max_retries.min(u8::MAX as u32) as u8,
                    });
                }
                orchestrator
                    .on_retry_required(predicate_class_hash, retry)
                    .await;
                retry += 1;
                predicted = rescan().await?;
            }
        }
    }
}

#[cfg(test)]
#[path = "retry_loop_tests.rs"]
mod tests;
