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
        tid: u32,
        query_bytes: &[u8],
    ) -> Response {
        debug!(core = self.core_id, tid, "graph match execution");

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

        // Execute the pattern match on the caller's CSR partition +
        // EdgeStore. An absent partition means "this tenant has no
        // graph state" — return the empty row set rather than error.
        let partition = match self.csr_partition(tid) {
            Some(p) => p,
            None => {
                let payload = match crate::engine::graph::pattern::executor::rows_to_msgpack(&[]) {
                    Ok(p) => p,
                    Err(e) => return self.response_error(task, ErrorCode::from(e)),
                };
                return self.response_with_payload(task, payload);
            }
        };
        match crate::engine::graph::pattern::executor::execute(&query, partition, &self.edge_store)
        {
            Ok(outcome) => {
                match crate::engine::graph::pattern::executor::rows_to_msgpack(&outcome.rows) {
                    Ok(payload) => {
                        if outcome.truncated {
                            // Variable-length expansion hit a hard cap.
                            // Surface via the envelope's `partial` flag
                            // so merging / client response paths can see
                            // the incomplete result — silent truncation
                            // is not allowed (§CLAUDE.md "Do the Ripple").
                            self.response_partial(task, payload)
                        } else {
                            self.response_with_payload(task, payload)
                        }
                    }
                    Err(e) => self.response_error(task, ErrorCode::from(e)),
                }
            }
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }
}
