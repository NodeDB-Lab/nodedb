// SPDX-License-Identifier: BUSL-1.1

//! `PhysicalTaskVisitor` adapter for the Data Plane `CoreLoop`.
//!
//! Bridges `nodedb_physical::dispatch` to the per-engine `dispatch_*` methods
//! on `CoreLoop`. Each trait method is a one-liner; all routing logic lives
//! in the individual sub-dispatchers.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ClusterEventOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp,
    QueryOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};

/// Adapter that implements [`PhysicalTaskVisitor`] for the Data Plane.
///
/// Holds mutable access to the `CoreLoop` plus the per-request context
/// needed by every handler: the `ExecutionTask` and the pre-extracted
/// tenant id (`tid`).
pub(super) struct DataPlaneVisitor<'a, 'b> {
    pub(super) core_loop: &'a mut CoreLoop,
    pub(super) task: &'b ExecutionTask,
    pub(super) tid: u64,
}

impl<'a, 'b> PhysicalTaskVisitor for DataPlaneVisitor<'a, 'b> {
    type Output = Response;
    type Error = std::convert::Infallible;

    fn document(&mut self, op: &DocumentOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_document(self.task, op))
    }

    fn vector(&mut self, op: &VectorOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_vector(self.task, op))
    }

    fn crdt(&mut self, op: &CrdtOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_crdt(self.task, op))
    }

    fn graph(&mut self, op: &GraphOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_graph(self.task, op))
    }

    fn text(&mut self, op: &TextOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_text(self.task, op))
    }

    fn array(&mut self, op: &ArrayOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_array(self.task, op))
    }

    fn query(&mut self, op: &QueryOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_query(self.task, self.tid, op))
    }

    fn meta(&mut self, op: &MetaOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_meta(self.task, self.tid, op))
    }

    fn columnar(&mut self, op: &ColumnarOp) -> Result<Response, Self::Error> {
        let response = self.core_loop.dispatch_columnar(self.task, op);
        // A write moves the collection's history: its charge follows.
        self.core_loop.recharge_columnar_written(self.task, op);
        Ok(response)
    }

    fn timeseries(&mut self, op: &TimeseriesOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_timeseries(self.task, op))
    }

    fn spatial(&mut self, op: &SpatialOp) -> Result<Response, Self::Error> {
        Ok(self.core_loop.dispatch_spatial(self.task, self.tid, op))
    }

    fn kv(&mut self, op: &KvOp) -> Result<Response, Self::Error> {
        let did = self.task.request.database_id.as_u64();
        Ok(self.core_loop.dispatch_kv(self.task, did, self.tid, op))
    }

    fn cluster_array(&mut self, _op: &ClusterArrayOp) -> Result<Response, Self::Error> {
        unreachable!("ClusterArray plans must not be dispatched to the Data Plane")
    }

    fn cluster_event(&mut self, _op: &ClusterEventOp) -> Result<Response, Self::Error> {
        unreachable!("ClusterEvent plans must not be dispatched to the Data Plane")
    }
}
