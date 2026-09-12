// SPDX-License-Identifier: BUSL-1.1

//! Write-batch coalescing: amortizes redb fsync across consecutive PointPut tasks.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::{PointPutOutcome, PointPutParams};
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::DocumentOp;

/// Whether a task is a `PointPut` this coalescing path may absorb.
///
/// A put carrying a `RETURNING` projection is excluded: this path answers every
/// batched task with a bare OK, which carries no row payload, so absorbing one
/// would drop the rows the statement asked for. It falls through to `poll_one`
/// and the single-put handler, which projects the stored post-image.
fn is_batchable_put(task: &ExecutionTask) -> bool {
    matches!(
        task.plan(),
        PhysicalPlan::Document(DocumentOp::PointPut {
            returning: None,
            ..
        })
    )
}

impl CoreLoop {
    /// Batch-coalesce consecutive PointPut tasks from the front of the task queue.
    ///
    /// Opens ONE redb WriteTransaction, executes all PointPuts within it,
    /// commits once, and sends individual responses. This amortizes the
    /// fsync cost across N writes instead of paying it per-write.
    ///
    /// Returns the number of tasks processed (0 if the front of the queue
    /// is not a batchable PointPut, in which case the caller should fall
    /// back to `poll_one`).
    pub fn poll_write_batch(&mut self) -> usize {
        // Check if the front of the queue is a non-expired batchable PointPut.
        let front_is_put = self
            .task_queue
            .front()
            .is_some_and(|t| is_batchable_put(t) && !t.is_expired());
        if !front_is_put {
            return 0;
        }

        // Collect consecutive non-expired PointPuts (max 64).
        let mut batch: Vec<ExecutionTask> = Vec::with_capacity(64);
        while batch.len() < 64 {
            let is_put = self
                .task_queue
                .front()
                .is_some_and(|t| is_batchable_put(t) && !t.is_expired());
            if !is_put {
                break;
            }
            if let Some(task) = self.task_queue.pop_front() {
                batch.push(task);
            } else {
                break;
            }
        }

        // Single write: no batching benefit, let poll_one handle it
        // (poll_one also handles idempotency cache and other bookkeeping).
        if batch.len() <= 1 {
            for t in batch.into_iter().rev() {
                self.task_queue.push_front(t);
            }
            return 0;
        }

        // Open ONE transaction for the entire batch.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(_) => {
                // Can't open txn — put tasks back, let poll_one handle individually.
                for t in batch.into_iter().rev() {
                    self.task_queue.push_front(t);
                }
                return 0;
            }
        };

        // Execute each PointPut within the shared transaction.
        // Track per-task success/failure for individual responses, and
        // capture the prior stored bytes per row so the Event Plane emit
        // below can resolve Insert vs Update from the actual mutation.
        let mut results: Vec<Result<PointPutOutcome, crate::bridge::envelope::Response>> =
            Vec::with_capacity(batch.len());
        for task in &batch {
            let PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                value,
                surrogate,
                resolved_sum_targets,
                ..
            }) = task.plan()
            else {
                unreachable!("batch only contains PointPut");
            };
            let tid = task.request.tenant_id.as_u64();
            let db_id = task.request.database_id.as_u64();
            let storage_key = crate::engine::document::store::StorageKey::for_surrogate(*surrogate);
            results.push(
                self.apply_point_put(
                    &txn,
                    PointPutParams {
                        database_id: db_id,
                        tid,
                        collection: collection.as_str(),
                        storage_key,
                        surrogate: *surrogate,
                        value,
                        index_text: true,
                        user_roles: &task.request.user_roles,
                        enforce: true,
                        wal_lsn: task.wal_lsn(),
                        resolved_targets: resolved_sum_targets.as_slice(),
                    },
                )
                .map_err(|e| {
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    )
                }),
            );
        }

        // If ANY write failed hard (document put error), abort the batch.
        let any_hard_failure = results.iter().any(|r| r.is_err());
        if any_hard_failure {
            // Don't commit — transaction is dropped (implicit rollback).
            // Send error responses for failed tasks, put successful ones back.
            let count = batch.len();
            for (task, result) in batch.into_iter().zip(results) {
                let response = match result {
                    Err(err_response) => err_response,
                    Ok(_) => self.response_error(
                        &task,
                        ErrorCode::Internal {
                            detail: "batch aborted due to sibling failure".into(),
                        },
                    ),
                };
                // The transaction was dropped un-committed above, so a lost
                // response costs the caller its error message, not its write.
                self.send_response(response, crate::diag::LostResponseWrite::RolledBack);
            }
            return count;
        }

        // Commit once for all writes.
        let commit_result = txn.commit();

        let count = batch.len();
        for (task, result) in batch.iter().zip(results.iter()) {
            let response = match &commit_result {
                Ok(()) => {
                    // Emit write event for each successful batched PointPut.
                    // The Insert vs Update tag is derived from the prior
                    // bytes captured per row above.
                    if let PhysicalPlan::Document(DocumentOp::PointPut {
                        collection,
                        surrogate,
                        value,
                        ..
                    }) = task.plan()
                    {
                        let tid = task.request.tenant_id.as_u64();
                        let prior = match result {
                            Ok(p) => p.prior_value.as_deref(),
                            Err(_) => None,
                        };
                        let identity =
                            crate::engine::document::store::StorageKey::for_surrogate(*surrogate)
                                .to_identity();
                        self.emit_put_event(task, tid, collection.as_str(), identity, value, prior);
                    }
                    self.response_ok(task)
                }
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("batch commit: {e}"),
                    },
                ),
            };

            // Record idempotency key.
            if let Some(key) = task.request.idempotency_key {
                let succeeded = response.status == crate::bridge::envelope::Status::Ok;
                if self.idempotency_cache.len() >= 16_384
                    && let Some(oldest) = self.idempotency_order.pop_front()
                {
                    self.idempotency_cache.remove(&oldest);
                }
                self.idempotency_cache.insert(key, succeeded);
                self.idempotency_order.push_back(key);
            }

            // A committed batch whose response is lost is the ambiguous case:
            // the row is durable and the caller will see only a deadline.
            let write = match &commit_result {
                Ok(()) => crate::diag::LostResponseWrite::Committed,
                Err(_) => crate::diag::LostResponseWrite::RolledBack,
            };
            self.send_response(response, write);
        }

        if commit_result.is_ok() {
            debug!(core = self.core_id, count, "write batch committed");
        }

        count
    }
}
