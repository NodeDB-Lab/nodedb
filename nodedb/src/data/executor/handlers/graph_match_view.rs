// SPDX-License-Identifier: BUSL-1.1

//! The CSR, node documents and staged overlay one MATCH reads.

use nodedb_physical::physical_plan::{GraphReadScope, GraphSystemCut};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::pattern::ast::MatchQuery;

/// What one MATCH reads: the CSR at the plan's cut, the node collection's
/// edge-store name, how its documents are read, and the staged overlay of a
/// live read.
pub(super) struct MatchView<'a> {
    pub(super) csr: &'a crate::engine::graph::csr::CsrIndex,
    pub(super) collection: Option<String>,
    pub(super) document_cut: crate::engine::graph::pattern::executor::DocumentCut,
    pub(super) overlay: Option<crate::engine::graph::csr::GraphOverlayDelta>,
}

impl CoreLoop {
    /// The edge-store name of the collection `IN '<collection>'` names.
    pub(super) fn match_collection(task: &ExecutionTask, query: &MatchQuery) -> Option<String> {
        query
            .collection
            .as_deref()
            .map(|c| super::graph_algo_edges::algo_collection(task.request.database_id, c))
    }

    /// Build or reuse the CSR a MATCH at `scope` reads.
    pub(super) fn prepare_match_view(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        query: &MatchQuery,
        scope: &GraphReadScope,
    ) -> Result<(), crate::Error> {
        let collection = Self::match_collection(task, query);
        self.prepare_csr_view(
            task.request.database_id.as_u64(),
            tid,
            collection.as_deref(),
            scope,
        )
    }

    /// The view a MATCH at `scope` reads. `Ok(None)` when the tenant has no
    /// live graph state on this core: the answer is the empty row set.
    ///
    /// `IN '<collection>'` names the collection bare. Its edges, documents
    /// and CSR collection ids carry the database-qualified name, so the view
    /// qualifies it. A historical read walks the CSR of that collection at
    /// the cut and sees committed versions only. A bitemporal node
    /// collection's documents are read at the same cut.
    pub(super) fn match_view(
        &self,
        task: &ExecutionTask,
        tid: u64,
        query: &MatchQuery,
        scope: &GraphReadScope,
    ) -> Result<Option<MatchView<'_>>, crate::Error> {
        let database_id = task.request.database_id.as_u64();
        let collection = Self::match_collection(task, query);
        let Some(csr) = self.csr_view(database_id, tid, collection.as_deref(), scope)? else {
            return Ok(None);
        };
        let document_cut = match collection.as_deref() {
            Some(c) if self.is_bitemporal(database_id, tid, c) => {
                crate::engine::graph::pattern::executor::DocumentCut::Versioned {
                    system_as_of: match scope.system {
                        GraphSystemCut::AtOrdinal(ordinal) => Some(ordinal),
                        GraphSystemCut::Live | GraphSystemCut::AllVersions => None,
                    },
                    valid_at_ms: scope.valid_at_ms,
                }
            }
            _ => crate::engine::graph::pattern::executor::DocumentCut::Plain,
        };
        let overlay = self.live_overlay_delta(task, tid, scope);
        Ok(Some(MatchView {
            csr,
            collection,
            document_cut,
            overlay,
        }))
    }
}
