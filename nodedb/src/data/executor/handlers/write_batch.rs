//! Write-batch coalescing: amortizes redb fsync across consecutive PointPut tasks.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan};
use crate::bridge::physical_plan::DocumentOp;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

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
        // Check if the front of the queue is a non-expired PointPut.
        let front_is_put = self.task_queue.front().is_some_and(|t| {
            matches!(
                t.plan(),
                PhysicalPlan::Document(DocumentOp::PointPut { .. })
            ) && !t.is_expired()
        });
        if !front_is_put {
            return 0;
        }

        // Collect consecutive non-expired PointPuts (max 64).
        let mut batch: Vec<ExecutionTask> = Vec::with_capacity(64);
        while batch.len() < 64 {
            let is_put = self.task_queue.front().is_some_and(|t| {
                matches!(
                    t.plan(),
                    PhysicalPlan::Document(DocumentOp::PointPut { .. })
                ) && !t.is_expired()
            });
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
        // Track per-task success/failure for individual responses.
        let mut results: Vec<Result<(), crate::bridge::envelope::Response>> =
            Vec::with_capacity(batch.len());
        for task in &batch {
            let PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                document_id,
                value,
            }) = task.plan()
            else {
                unreachable!("batch only contains PointPut");
            };
            let tid = task.request.tenant_id.as_u32();
            results.push(
                self.apply_point_put(&txn, tid, collection, document_id, value)
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
                    Ok(()) => self.response_error(
                        &task,
                        ErrorCode::Internal {
                            detail: "batch aborted due to sibling failure".into(),
                        },
                    ),
                };
                let _ = self
                    .response_tx
                    .try_push(crate::bridge::dispatch::BridgeResponse { inner: response });
            }
            return count;
        }

        // Commit once for all writes.
        let commit_result = txn.commit();

        let count = batch.len();
        for task in &batch {
            let response = match &commit_result {
                Ok(()) => {
                    // Emit write event for each successful batched PointPut.
                    if let PhysicalPlan::Document(DocumentOp::PointPut {
                        collection,
                        document_id,
                        value,
                    }) = task.plan()
                    {
                        self.emit_write_event(
                            task,
                            collection,
                            crate::event::WriteOp::Insert,
                            document_id,
                            Some(value),
                            None,
                        );
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

            let _ = self
                .response_tx
                .try_push(crate::bridge::dispatch::BridgeResponse { inner: response });
        }

        if commit_result.is_ok() {
            debug!(core = self.core_id, count, "write batch committed");
        }

        count
    }
}
