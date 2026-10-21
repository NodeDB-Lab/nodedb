// SPDX-License-Identifier: BUSL-1.1

//! The CSR a graph algorithm stage reads from this core's edges.
//!
//! A historical run over every label of the collection reads the core's
//! historical CSR cache, so every superstep of a BSP run reuses one build.
//! A label-filtered or live run builds its CSR from the edge store.

use std::ops::Deref;

use nodedb_graph::CsrIndex;
use nodedb_physical::physical_plan::GraphReadScope;
use nodedb_types::DatabaseId;

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::graph::algo::params::AlgoParams;

use super::graph_algo::build_csr_for_collection;
use super::graph_algo_edges::algo_collection;

/// An algorithm's CSR: borrowed from the historical cache, or built.
pub(in crate::data::executor) enum AlgoCsr<'a> {
    View(&'a CsrIndex),
    Built(Box<CsrIndex>),
}

impl Deref for AlgoCsr<'_> {
    type Target = CsrIndex;

    fn deref(&self) -> &CsrIndex {
        match self {
            Self::View(csr) => csr,
            Self::Built(csr) => csr,
        }
    }
}

/// The edge-store collection whose historical CSR an algorithm at `scope`
/// reads through the cache, or `None` when it builds its own. The
/// dispatcher prepares that CSR before the handler runs.
pub(in crate::data::executor) fn algo_view_collection(
    database: DatabaseId,
    params: &AlgoParams,
    scope: &GraphReadScope,
) -> Option<String> {
    (params.edge_label.is_none() && !scope.is_live())
        .then(|| algo_collection(database, &params.collection))
}

impl CoreLoop {
    /// The CSR of `params.collection` at `scope` on this core.
    pub(in crate::data::executor) fn algo_csr(
        &self,
        database: DatabaseId,
        tid: u64,
        params: &AlgoParams,
        scope: &GraphReadScope,
    ) -> crate::Result<AlgoCsr<'_>> {
        if let Some(collection) = algo_view_collection(database, params, scope)
            && let Some(csr) = self.csr_view(database.as_u64(), tid, Some(&collection), scope)?
        {
            return Ok(AlgoCsr::View(csr));
        }
        let memory = nodedb_mem::ScopedMemory::new(
            self.governor.clone(),
            database,
            crate::types::TenantId::new(tid),
            nodedb_mem::EngineId::Graph,
        );
        build_csr_for_collection(
            &self.edge_store,
            database.as_u64(),
            tid,
            &params.collection,
            params.edge_label.as_deref(),
            scope,
            memory,
        )
        .map(|csr| AlgoCsr::Built(Box::new(csr)))
    }
}
