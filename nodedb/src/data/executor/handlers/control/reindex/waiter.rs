// SPDX-License-Identifier: BUSL-1.1

//! A plain REINDEX answers once its rebuilds have cut over.
//!
//! The rebuilds run exactly as for REINDEX CONCURRENTLY: off the core, with
//! the core serving every other request meanwhile. Only the answer waits.
//! The core holds the request's task here and answers it from the tick:
//! the held task builds the response, the response ring carries it, and the
//! task's deadline bounds the wait.
//!
//! - Every rebuild cut over: `Ok`.
//! - A rebuild was discarded, or an HNSW segment rebuild failed: the first
//!   error.
//! - The deadline passed first: `DeadlineExceeded`. The rebuilds are not
//!   cancelled; each one still cuts over on its own tick, or is refused
//!   cleanly with the live index unchanged, as for REINDEX CONCURRENTLY.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use super::dispatch::IndexSelection;
use super::pending::RebuildTarget;
use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::data::executor::task::ExecutionTask;

/// A plain REINDEX waiting for its rebuilds.
pub struct ReindexWaiter {
    task: ExecutionTask,
    target: RebuildTarget,
    /// HNSW indexes with queued segment rebuilds, and each index's failed
    /// build count when the rebuilds were queued.
    vector_failed_before: HashMap<VectorIndexKey, u64>,
    /// The first FTS or CSR rebuild the core discarded.
    first_error: Option<crate::Error>,
}

impl CoreLoop {
    /// Start the rebuilds of a plain REINDEX and hold its task until they
    /// cut over. Returns the task unchanged when it is anything else, or
    /// when it expired before it started.
    pub(in crate::data::executor) fn hold_plain_reindex(
        &mut self,
        task: ExecutionTask,
    ) -> Option<ExecutionTask> {
        let plain = if let PhysicalPlan::Meta(MetaOp::RebuildIndex {
            collection,
            index_name,
            concurrent: false,
        }) = task.plan()
        {
            Some((collection.as_str().to_string(), index_name.clone()))
        } else {
            None
        };
        let Some((collection, index_name)) = plain else {
            return Some(task);
        };
        if past_deadline(&task) {
            // Nothing started, so nothing is left running.
            let response = self.response_error(&task, ErrorCode::DeadlineExceeded);
            self.send_reindex_answer(response);
            return None;
        }
        let target = RebuildTarget {
            database_id: task.request.database_id,
            tenant_id: task.request.tenant_id,
            collection,
        };
        let selection = IndexSelection::from_name(index_name.as_deref());

        if let Err(e) = self.start_rebuilds(&target, selection) {
            let response = self.response_error(&task, e);
            self.send_reindex_answer(response);
            return None;
        }
        let vector_failed_before = if selection.hnsw {
            self.vector_keys_of(&target)
                .into_iter()
                .filter(|key| self.vector_builds.pending_for(key) > 0)
                .map(|key| {
                    let failed = self.vector_builds_failed(&key);
                    (key, failed)
                })
                .collect()
        } else {
            HashMap::new()
        };
        self.maintenance.reindex_waiters.push(ReindexWaiter {
            task,
            target,
            vector_failed_before,
            first_error: None,
        });
        // A collection with nothing to rebuild answers on this tick.
        self.answer_reindex_waiters();
        None
    }

    /// Record a discarded FTS or CSR rebuild against the plain REINDEX that
    /// waits for it.
    pub(super) fn note_reindex_refused(&mut self, target: &RebuildTarget, err: &crate::Error) {
        for waiter in self
            .maintenance
            .reindex_waiters
            .iter_mut()
            .filter(|w| w.target == *target && w.first_error.is_none())
        {
            waiter.first_error = Some(crate::Error::ObjectNotInPrerequisiteState {
                object: format!("indexes of collection \"{}\"", target.collection),
                detail: format!("a rebuild was discarded: {err}"),
            });
        }
    }

    /// Answer every plain REINDEX whose rebuilds finished or whose deadline
    /// passed. Called from `tick()` after the cutovers.
    pub fn answer_reindex_waiters(&mut self) {
        if self.maintenance.reindex_waiters.is_empty() {
            return;
        }
        let waiters = std::mem::take(&mut self.maintenance.reindex_waiters);
        for waiter in waiters {
            if past_deadline(&waiter.task) {
                let response = self.response_error(&waiter.task, ErrorCode::DeadlineExceeded);
                self.send_reindex_answer(response);
                continue;
            }
            let fts_csr_running = self
                .maintenance
                .pending_reindex
                .iter()
                .any(|p| p.target == waiter.target);
            let hnsw_running = waiter
                .vector_failed_before
                .keys()
                .any(|key| self.vector_builds.pending_for(key) > 0);
            if fts_csr_running || hnsw_running {
                self.maintenance.reindex_waiters.push(waiter);
                continue;
            }
            let ReindexWaiter {
                task,
                vector_failed_before,
                first_error,
                ..
            } = waiter;
            let error = first_error.or_else(|| self.failed_vector_rebuilds(&vector_failed_before));
            let response = match error {
                None => self.response_ok(&task),
                Some(e) => self.response_error(&task, e),
            };
            self.send_reindex_answer(response);
        }
    }

    /// An error naming each HNSW index whose failed build count rose
    /// since its rebuilds were queued, or `None` when none did.
    fn failed_vector_rebuilds(
        &self,
        failed_before: &HashMap<VectorIndexKey, u64>,
    ) -> Option<crate::Error> {
        let failed: Vec<&str> = failed_before
            .iter()
            .filter(|(key, before)| self.vector_builds_failed(key) > **before)
            .map(|(key, _)| key.2.as_str())
            .collect();
        if failed.is_empty() {
            return None;
        }
        Some(crate::Error::ObjectNotInPrerequisiteState {
            object: format!("HNSW indexes {failed:?}"),
            detail: "a segment rebuild failed; the segment keeps its previous graph".to_string(),
        })
    }

    /// Failed HNSW builds of `key` so far.
    fn vector_builds_failed(&self, key: &VectorIndexKey) -> u64 {
        self.vector_collections
            .get(key)
            .map_or(0, |coll| coll.stats().builds_failed)
    }

    fn send_reindex_answer(&mut self, response: Response) {
        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            tracing::warn!(
                core = self.core_id,
                error = %e,
                "failed to send a REINDEX answer: response queue full"
            );
        }
    }
}

/// Whether `task`'s request deadline passed. The wait is bounded by the
/// request deadline whatever the request's admission class.
fn past_deadline(task: &ExecutionTask) -> bool {
    // no-determinism: the deadline bounds only when the answer is sent; the
    // rebuilds and their cutovers do not depend on it.
    std::time::Instant::now() > task.request.deadline
}
