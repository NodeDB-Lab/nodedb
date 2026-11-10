// SPDX-License-Identifier: BUSL-1.1

//! Write-batch coalescing: amortizes redb fsync across consecutive PointPut tasks.

use tracing::debug;

use crate::bridge::dispatch::JournalGroup;
use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::redo_image::{WriteSetRow, versioned_point_images};
use crate::data::executor::handlers::point::apply_put::PointPutOutcome;
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
    /// Whether a put requires the canonical row-image hooks and statement boundary.
    /// Shared batches bypass these hooks, so these puts run through `poll_one`.
    fn requires_point_put_hooks(&self, task: &ExecutionTask) -> bool {
        let PhysicalPlan::Document(DocumentOp::PointPut { collection, .. }) = task.plan() else {
            return false;
        };
        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.as_str().to_string(),
        );
        self.doc_configs.get(&key).is_some_and(|config| {
            config.enforcement.hash_chain || config.enforcement.has_image_enforcement()
        })
    }

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
        let front_is_put = self.task_queue.front().is_some_and(|t| {
            is_batchable_put(t) && !t.is_expired() && !self.requires_point_put_hooks(t)
        });
        // While a staged Calvin transaction owns rows, every write passes the
        // fence in `poll_one` one at a time.
        if !front_is_put || !self.calvin.commit_pending.is_empty() {
            return 0;
        }

        // Collect consecutive non-expired PointPuts (max 64).
        let mut batch: Vec<ExecutionTask> = Vec::with_capacity(64);
        while batch.len() < 64 {
            let is_put = self.task_queue.front().is_some_and(|t| {
                is_batchable_put(t) && !t.is_expired() && !self.requires_point_put_hooks(t)
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

        // A batch with a journalled put runs journalled as a whole: its one
        // commit and the write sets reach stable storage together.
        let journalled = batch.iter().any(|task| self.journals_write_set(task));
        if journalled {
            self.begin_journalled();
        }

        // Open ONE transaction for the entire batch.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(_) => {
                if journalled {
                    self.end_empty_journalled();
                }
                // Can't open txn — put tasks back, let poll_one handle individually.
                for t in batch.into_iter().rev() {
                    self.task_queue.push_front(t);
                }
                return 0;
            }
        };
        let groups: Vec<_> = batch
            .iter()
            .map(|task| self.take_journal_group(task))
            .collect();

        // Execute each PointPut within the shared transaction.
        // Track per-task success/failure for individual responses, and
        // capture the prior stored bytes per row so the Event Plane emit
        // below can resolve Insert vs Update from the actual mutation.
        let applied = self.apply_batched_puts(&txn, &batch);
        let mut results = applied.outcomes;
        let apply_failed = applied.apply_failed;
        let mut prepared_images: Vec<Option<_>> = (0..batch.len()).map(|_| None).collect();
        let mut preparation_error = None;
        for (index, (task, result)) in batch.iter().zip(&results).enumerate() {
            let Ok(outcome) = result else {
                continue;
            };
            let PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                document_id,
                surrogate,
                ..
            }) = task.plan()
            else {
                preparation_error = Some((
                    index,
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "coalesced write does not contain a document put".into(),
                        },
                    ),
                ));
                break;
            };
            let identity =
                crate::engine::document::store::RowIdentity::from_user_key(document_id.as_str());
            let event = match self.prepare_installed_put_event(
                &WriteSetRow {
                    database_id: task.request.database_id.as_u64(),
                    tid: task.request.tenant_id.as_u64(),
                    collection: collection.as_str(),
                    identity: identity.clone(),
                },
                &outcome.installed,
                outcome.prior_value.as_deref(),
            ) {
                Ok(event) => event,
                Err(error) => {
                    preparation_error = Some((index, self.response_error(task, error)));
                    break;
                }
            };
            let image = crate::bridge::envelope::WriteSetEntry::installed_put(
                surrogate.as_u32(),
                identity,
                outcome.installed.clone(),
                event.clone(),
            );
            prepared_images[index] = Some((versioned_point_images(image), event));
        }

        if preparation_error.is_none() {
            for (index, ((task, result), image)) in
                batch.iter().zip(&results).zip(&prepared_images).enumerate()
            {
                if result.is_ok() && image.is_none() {
                    preparation_error = Some((
                        index,
                        self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "coalesced request {} has no prepared capture",
                                    task.request_id().as_u64()
                                ),
                            },
                        ),
                    ));
                    break;
                }
            }
        }

        // Every fallible capture and event preparation precedes the shared commit.
        let any_hard_failure = apply_failed || preparation_error.is_some();
        if any_hard_failure {
            // Application and event errors abandon every provisional write.
            drop(txn);
            let fatal = self.abandon_batched_puts(&batch, &mut results);
            let count = batch.len();
            let mut responses = Vec::with_capacity(count);
            for (index, (task, result)) in batch.iter().zip(results).enumerate() {
                let preparation_response = match preparation_error.take() {
                    Some((failed, response)) if failed == index => Some(response),
                    pending => {
                        preparation_error = pending;
                        None
                    }
                };
                let response = match (&fatal, preparation_response, result) {
                    (Some(code), _, _) => self.response_error(task, code.clone()),
                    (None, Some(response), _) | (None, None, Err(response)) => response,
                    (None, None, Ok(_)) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "batch aborted due to sibling failure".into(),
                        },
                    ),
                };
                // The transaction was dropped un-committed above, so a lost
                // response costs the caller its error message, not its write.
                responses.push((response, crate::diag::LostResponseWrite::RolledBack));
            }
            self.finish_journalled_batch(&batch, groups, responses);
            return count;
        }

        // Commit once for all writes.
        let commit_result = txn.commit();
        if let Err(error) = &commit_result {
            self.seal_journalled();
            self.fail_stop_core(
                crate::data::executor::core_loop::fail_stop::FailStopCause::WriteSetUnpersisted,
                &format!("coalesced document commit outcome is unknown: {error}"),
            );
        }
        let fatal = match commit_result {
            Ok(()) => None,
            Err(_) => self.abandon_batched_puts(&batch, &mut results),
        };

        let count = batch.len();
        let mut responses = Vec::with_capacity(count);
        for ((task, result), prepared_image) in batch.iter().zip(results).zip(prepared_images) {
            let response = match (&commit_result, &fatal, result, prepared_image) {
                (Err(_), Some(code), _, _) => self.response_error(task, code.clone()),
                (Ok(()), _, Ok(_), Some((write_set, event))) => {
                    let mut response = self.response_ok(task);
                    // Emit write event for each successful batched PointPut.
                    // The Insert vs Update tag is derived from the prior
                    // bytes captured per row above.
                    if let PhysicalPlan::Document(DocumentOp::PointPut {
                        collection,
                        document_id,
                        ..
                    }) = task.plan()
                    {
                        // The plan's `document_id` is the row's client identity,
                        // the same one `execute_point_put` emits and the WAL
                        // journals.
                        let identity = crate::engine::document::store::RowIdentity::from_user_key(
                            document_id.as_str(),
                        );
                        response.write_set = write_set;
                        self.emit_prepared_put_event(task, collection.as_str(), identity, &event);
                    }
                    response
                }
                (Ok(()), _, Err(response), _) => response,
                (Ok(()), _, Ok(_), None) => {
                    let error = crate::Error::Storage {
                        engine: "journal".into(),
                        detail: "committed coalesced put lacks its prepared capture; close and reopen the handle".into(),
                    };
                    self.fail_journalled(&error);
                    self.response_error(task, error)
                }
                (Err(e), None, _, _) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("batch commit: {e}"),
                    },
                ),
            };

            // Record idempotency key.
            if let Some(key) = task.request.idempotency_key {
                self.idempotency
                    .record(key, response.status == crate::bridge::envelope::Status::Ok);
            }

            // A committed batch whose response is lost is the ambiguous case:
            // the row is durable and the caller will see only a deadline.
            let write = match &commit_result {
                Ok(()) => crate::diag::LostResponseWrite::Committed,
                Err(_) => crate::diag::LostResponseWrite::RolledBack,
            };
            responses.push((response, write));
        }
        self.finish_journalled_batch(&batch, groups, responses);

        if commit_result.is_ok() {
            debug!(core = self.core_id, count, "write batch committed");
        }

        count
    }

    /// Reverse the in-memory side effects of batched puts whose shared
    /// transaction drops uncommitted: the cache entry each put wrote and the
    /// R-tree, vector and sparse entries of each applied put, last put first.
    ///
    /// Returns `Some(RollbackFailed)` when an entry did not reverse. The
    /// core's state is then unknown, so this fail-stops it, and every task
    /// of the batch reports that code.
    fn abandon_batched_puts(
        &mut self,
        batch: &[ExecutionTask],
        results: &mut [Result<PointPutOutcome, Response>],
    ) -> Option<ErrorCode> {
        let mut undone: crate::Result<()> = Ok(());
        for (task, result) in batch.iter().zip(results.iter_mut()).rev() {
            let PhysicalPlan::Document(DocumentOp::PointPut {
                collection,
                surrogate,
                ..
            }) = task.plan()
            else {
                continue;
            };
            let database_id = task.request.database_id.as_u64();
            let tid = task.request.tenant_id.as_u64();
            let key = crate::engine::document::store::StorageKey::for_surrogate(*surrogate);
            // A failed put can have cached its row before it failed.
            self.doc_cache
                .invalidate(database_id, tid, collection.as_str(), &key);
            if let Ok(outcome) = result {
                let memory_undo = std::mem::take(&mut outcome.memory_undo);
                let row = self.undo_memory_effects(database_id, tid, memory_undo);
                undone = undone.and(row);
            }
        }
        let fatal = ErrorCode::from(undone.err()?);
        self.fail_stop_on_rollback_code(&fatal);
        Some(fatal)
    }

    /// Store the write sets of the journalled tasks of `batch`, finish the
    /// journalled run, then send every response.
    fn finish_journalled_batch(
        &mut self,
        batch: &[ExecutionTask],
        groups: Vec<Option<JournalGroup>>,
        mut responses: Vec<(Response, crate::diag::LostResponseWrite)>,
    ) {
        if groups.iter().any(Option::is_some) {
            let mut captures = Vec::new();
            let mut unstored = None;
            for ((task, group), (response, _)) in batch.iter().zip(groups).zip(&responses) {
                let Some(group) = group else {
                    continue;
                };
                match self.write_set_capture(task, group, response) {
                    Ok(capture) => captures.push(capture),
                    Err(error) => unstored = Some(error),
                }
            }
            let stored = if let Some(error) = unstored {
                self.abandon_journalled(&error);
                Err(error)
            } else {
                self.finish_journalled(&captures)
            };
            if let Err(error) = stored {
                let code = ErrorCode::from(error);
                for (response, _) in &mut responses {
                    response.status = Status::Error;
                    response.error_code = Some(Box::new(code.clone()));
                }
            }
        }
        for (response, write) in responses {
            self.send_response(response, write);
        }
    }
}
