use tracing::warn;

use crate::bridge::dispatch::BridgeResponse;
use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};

use super::super::task::{ExecutionTask, TaskState};
use super::CoreLoop;

impl CoreLoop {
    /// Drain incoming requests from the SPSC bridge into the task queue.
    pub fn drain_requests(&mut self) {
        let mut batch = Vec::new();
        self.request_rx.drain_into(&mut batch, 64);
        for br in batch {
            self.task_queue.push_back(ExecutionTask::new(br.inner));
        }
    }

    /// Process the next pending task and send the response back via SPSC.
    pub fn poll_one(&mut self) -> bool {
        let Some(mut task) = self.task_queue.pop_front() else {
            return false;
        };

        if let Some(key) = task.request.idempotency_key
            && let Some(&succeeded) = self.idempotency_cache.get(&key)
        {
            let response = if succeeded {
                self.response_ok(&task)
            } else {
                self.response_error(&task, ErrorCode::DuplicateWrite)
            };
            if let Err(e) = self
                .response_tx
                .try_push(BridgeResponse { inner: response })
            {
                warn!(core = self.core_id, error = %e, "failed to send idempotent response");
            }
            return true;
        }

        let response = if task.is_expired() {
            task.state = TaskState::Failed;
            Response {
                request_id: task.request_id(),
                status: Status::Error,
                attempt: 1,
                partial: false,
                payload: Payload::empty(),
                watermark_lsn: self.watermark,
                error_code: Some(ErrorCode::DeadlineExceeded),
            }
        } else {
            task.state = TaskState::Running;
            let resp = self.execute(&task);
            task.state = TaskState::Completed;
            resp
        };

        if let Some(key) = task.request.idempotency_key {
            let succeeded = response.status == Status::Ok;
            if self.idempotency_cache.len() >= 16_384
                && let Some(oldest_key) = self.idempotency_order.pop_front()
            {
                self.idempotency_cache.remove(&oldest_key);
            }
            self.idempotency_cache.insert(key, succeeded);
            self.idempotency_order.push_back(key);
        }

        // Bound the dangling-edge tracker across all tenants. Count
        // all entries; when the aggregate exceeds the cap, drop the
        // whole map — callers are tolerant of false negatives (an
        // `EdgePut` to a recently-deleted node races the tracker, so
        // the semantics are advisory regardless).
        let total: usize = self.deleted_nodes.values().map(|s| s.len()).sum();
        if total > 100_000 {
            self.deleted_nodes.clear();
        }

        if let Err(e) = self
            .response_tx
            .try_push(BridgeResponse { inner: response })
        {
            warn!(core = self.core_id, error = %e, "failed to send response — response queue full");
        }

        true
    }

    /// Run one iteration of the event loop: drain requests, process tasks.
    pub fn tick(&mut self) -> usize {
        self.poll_build_completions();
        self.drain_requests();
        let mut processed = 0;
        while !self.task_queue.is_empty() {
            let batched = self.poll_write_batch();
            if batched > 0 {
                processed += batched;
                continue;
            }
            if self.poll_one() {
                processed += 1;
            } else {
                break;
            }
        }
        processed
    }
}
