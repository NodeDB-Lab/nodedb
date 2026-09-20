// SPDX-License-Identifier: BUSL-1.1

//! The `{"truncated": n}` reply every engine's `TRUNCATE` handler returns.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Encode `{"truncated": n}` as the reply to `task`.
    pub(in crate::data::executor) fn truncate_response(
        &self,
        task: &ExecutionTask,
        truncated: usize,
    ) -> Response {
        match response_codec::encode_count("truncated", truncated) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
