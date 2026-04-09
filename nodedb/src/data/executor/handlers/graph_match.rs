//! Graph pattern matching handler — executes MATCH queries on the Data Plane.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::pattern::ast::MatchQuery;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_match(
        &self,
        task: &ExecutionTask,
        query_bytes: &[u8],
    ) -> Response {
        debug!(core = self.core_id, "graph match execution");

        // Deserialize the MatchQuery from MessagePack.
        let query: MatchQuery = match zerompk::from_msgpack(query_bytes) {
            Ok(q) => q,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to deserialize MatchQuery");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid match query: {e}"),
                    },
                );
            }
        };

        // Execute the pattern match on CSR + EdgeStore.
        match crate::engine::graph::pattern::executor::execute(&query, &self.csr, &self.edge_store)
        {
            Ok(rows) => match crate::engine::graph::pattern::executor::rows_to_msgpack(&rows) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, ErrorCode::from(e)),
            },
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
