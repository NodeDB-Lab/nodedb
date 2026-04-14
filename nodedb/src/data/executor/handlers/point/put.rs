//! PointPut: insert or overwrite one document, committing storage + indexes
//! + stats in a single redb transaction via `apply_point_put`.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_point_put(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        document_id: &str,
        value: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "point put");

        // Unified write transaction: document + inverted index + stats in one commit.
        let txn = match self.sparse.begin_write() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        if let Err(e) = self.apply_point_put(&txn, tid, collection, document_id, value) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            );
        }

        if let Err(e) = txn.commit() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("commit: {e}"),
                },
            );
        }

        self.checkpoint_coordinator.mark_dirty("sparse", 1);

        // Emit write event to Event Plane (after successful commit).
        // For strict collections, convert Binary Tuple → msgpack so the
        // Event Plane can deserialize the payload for trigger dispatch.
        let event_value = self.resolve_event_payload(tid, collection, value);
        self.emit_write_event(
            task,
            collection,
            crate::event::WriteOp::Insert,
            document_id,
            Some(event_value.as_deref().unwrap_or(value)),
            None,
        );

        self.response_ok(task)
    }
}
