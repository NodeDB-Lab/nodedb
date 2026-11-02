// SPDX-License-Identifier: BUSL-1.1

//! The rows an aggregate folds: the input sub-plan's rows, or a per-shard
//! scan of the named collection.

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Aggregate input rows as `(doc_id, msgpack map)`.
pub(in crate::data::executor) type AggregateDocs = Vec<(String, Vec<u8>)>;

impl CoreLoop {
    /// Execute `sub_plan` and decode its rows.
    ///
    /// A child error response is returned unchanged, so its error code
    /// reaches the client. An empty payload is zero rows. A non-empty payload
    /// that is not a MessagePack row array is an internal error, never zero
    /// rows.
    pub(in crate::data::executor) fn input_docs(
        &mut self,
        task: &ExecutionTask,
        sub_plan: &nodedb_physical::physical_plan::PhysicalPlan,
    ) -> Result<AggregateDocs, Box<Response>> {
        let sub_response = self.execute_plan(task, sub_plan);
        if sub_response.status == Status::Error {
            return Err(Box::new(sub_response));
        }
        if sub_response.payload.is_empty() {
            return Ok(Vec::new());
        }
        match crate::data::executor::response_codec::decode_response_to_docs(&sub_response) {
            Some(docs) => Ok(docs),
            None => Err(Box::new(
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "aggregate input rows failed to decode: payload is not a \
                             MessagePack row array"
                            .to_string(),
                    },
                ),
            )),
        }
    }

    /// Scan every row of `collection` on this core for an aggregate, under
    /// the scan byte budget.
    ///
    /// Reads through [`CoreLoop::scan_collection`]: the current version of
    /// every row, as `(doc_id, msgpack)`.
    pub(in crate::data::executor) fn scan_for_aggregate(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
    ) -> Result<AggregateDocs, Box<Response>> {
        let did = task.request.database_id.as_u64();
        if let Err(e) = self.prepare_collection_scan(did, tid, collection) {
            return Err(Box::new(self.response_error(task, ErrorCode::from(e))));
        }
        // Every row is folded: the read is bounded by the scan byte budget
        // alone, and a collection past it fails loudly, never as a prefix.
        let budget = self.query_tuning.max_scan_result_bytes;
        let scan_limit = super::super::scan_budget::fetch_limit_for(usize::MAX, 0, budget);
        let scanned = self.scan_collection(did, tid, collection, scan_limit);
        let docs = scanned.map_err(|e| Box::new(self.response_error(task, ErrorCode::from(e))))?;
        if super::super::scan_budget::scan_bytes_exceeded(&docs, budget) {
            return Err(Box::new(
                self.response_error(task, ErrorCode::ResourcesExhausted),
            ));
        }
        Ok(docs)
    }
}
