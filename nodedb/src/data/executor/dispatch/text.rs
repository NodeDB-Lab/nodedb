//! Text (FTS) operation dispatch.

use crate::bridge::envelope::Response;
use crate::bridge::physical_plan::TextOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_text(&mut self, task: &ExecutionTask, op: &TextOp) -> Response {
        let tid = task.request.tenant_id.as_u32();
        match op {
            TextOp::Search {
                collection,
                query,
                top_k,
                fuzzy,
                prefilter,
                rls_filters,
            } => self.execute_text_search(
                task,
                tid,
                collection,
                query,
                *top_k,
                *fuzzy,
                prefilter.as_ref(),
                rls_filters,
            ),

            TextOp::HybridSearch {
                collection,
                query_vector,
                query_text,
                top_k,
                ef_search,
                fuzzy,
                vector_weight,
                filter_bitmap,
                rls_filters,
            } => self.execute_hybrid_search(
                task,
                tid,
                collection,
                query_vector,
                query_text,
                *top_k,
                *ef_search,
                *fuzzy,
                *vector_weight,
                filter_bitmap.as_ref(),
                rls_filters,
            ),
        }
    }
}
