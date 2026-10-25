// SPDX-License-Identifier: Apache-2.0

//! The input stage of [`super::op::GraphOp::Algo`].
//!
//! Graph edges are partitioned across cores and nodes by endpoint key. An
//! algorithm that needs the whole graph runs in two stages: every core that
//! holds edges exports them, then one core runs the algorithm over the union.

/// One weighted edge of a collection's graph.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AlgoEdge {
    pub src: String,
    pub label: String,
    pub dst: String,
    /// The edge's `weight` property, `1.0` when it carries none.
    pub weight: f64,
}

/// Where an algorithm reads its graph from.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum AlgoStage {
    /// Run over this core's own edges.
    Local,
    /// Run nothing. Answer this core's edges of the collection, after the
    /// label filter, as a msgpack array of [`AlgoEdge`], read at the `Algo`
    /// plan's scope.
    ExportEdges,
    /// Run over these edges, gathered from every core that holds the
    /// collection. The core reads no edges of its own.
    Gathered { edges: Vec<AlgoEdge> },
}
