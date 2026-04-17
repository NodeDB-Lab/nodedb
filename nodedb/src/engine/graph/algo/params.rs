//! Graph algorithm enum and parameter bag.
//!
//! `GraphAlgorithm` identifies which algorithm to run.
//! `AlgoParams` carries the union of all algorithm parameters — each
//! algorithm validates and extracts what it needs.

use serde::{Deserialize, Serialize};

/// Supported graph algorithms.
///
/// Each variant maps to a standalone algorithm implementation under
/// `src/engine/graph/algo/`. Used by `PhysicalPlan::GraphAlgo` to
/// identify which algorithm to dispatch.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum GraphAlgorithm {
    /// PageRank — link analysis (power iteration).
    PageRank,
    /// Weakly Connected Components — union-find.
    Wcc,
    /// Community Detection — label propagation.
    LabelPropagation,
    /// Local Clustering Coefficient — per-node triangle density.
    Lcc,
    /// Single-Source Shortest Path — weighted Dijkstra.
    Sssp,
    /// Betweenness Centrality — Brandes' algorithm.
    Betweenness,
    /// Closeness Centrality — inverse distance sum.
    Closeness,
    /// Harmonic Centrality — inverse distance harmonic mean.
    Harmonic,
    /// Degree Centrality — normalized degree.
    Degree,
    /// Louvain Community Detection — modularity optimization.
    Louvain,
    /// Triangle Counting — global or per-node.
    Triangles,
    /// Graph Diameter / Eccentricity.
    Diameter,
    /// k-Core Decomposition — peeling algorithm.
    KCore,
}

impl GraphAlgorithm {
    /// Human-readable name for progress reporting and result column headers.
    pub fn name(&self) -> &'static str {
        match self {
            Self::PageRank => "pagerank",
            Self::Wcc => "wcc",
            Self::LabelPropagation => "label_propagation",
            Self::Lcc => "lcc",
            Self::Sssp => "sssp",
            Self::Betweenness => "betweenness",
            Self::Closeness => "closeness",
            Self::Harmonic => "harmonic",
            Self::Degree => "degree",
            Self::Louvain => "louvain",
            Self::Triangles => "triangles",
            Self::Diameter => "diameter",
            Self::KCore => "kcore",
        }
    }

    /// Whether this algorithm is iterative (emits progress per iteration).
    pub fn is_iterative(&self) -> bool {
        matches!(
            self,
            Self::PageRank | Self::LabelPropagation | Self::Louvain
        )
    }

    /// Result column schema: `(column_name, column_type)`.
    ///
    /// Used by the Arrow result builder to construct RecordBatches and by
    /// the DDL layer to advertise result columns.
    pub fn result_schema(&self) -> &'static [(&'static str, AlgoColumnType)] {
        use AlgoColumnType::*;
        match self {
            Self::PageRank => &[("node_id", Text), ("rank", Float64)],
            Self::Wcc => &[("node_id", Text), ("component_id", Int64)],
            Self::LabelPropagation => &[("node_id", Text), ("community_id", Int64)],
            Self::Lcc => &[("node_id", Text), ("coefficient", Float64)],
            Self::Sssp => &[("node_id", Text), ("distance", Float64)],
            Self::Betweenness => &[("node_id", Text), ("centrality", Float64)],
            Self::Closeness => &[("node_id", Text), ("centrality", Float64)],
            Self::Harmonic => &[("node_id", Text), ("centrality", Float64)],
            Self::Degree => &[("node_id", Text), ("centrality", Float64)],
            Self::Louvain => &[
                ("node_id", Text),
                ("community_id", Int64),
                ("modularity", Float64),
            ],
            Self::Triangles => &[("node_id", Text), ("triangles", Int64)],
            Self::Diameter => &[("diameter", Int64), ("radius", Int64)],
            Self::KCore => &[("node_id", Text), ("coreness", Int64)],
        }
    }
}

/// Column type for algorithm result schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlgoColumnType {
    Text,
    Float64,
    Int64,
}

/// Generic parameter bag for all graph algorithms.
///
/// Each algorithm validates and extracts the parameters it needs,
/// ignoring the rest. Unknown parameters are silently ignored rather
/// than rejected — this allows forward-compatible DDL extensions.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AlgoParams {
    /// Target collection name.
    pub collection: String,

    /// PageRank damping factor (default: 0.85).
    pub damping: Option<f64>,

    /// Maximum iterations for iterative algorithms (PageRank, LabelProp, Louvain).
    pub max_iterations: Option<usize>,

    /// Convergence tolerance for PageRank (default: 1e-7).
    pub tolerance: Option<f64>,

    /// Source node for SSSP.
    pub source_node: Option<String>,

    /// Sample size for approximate centrality (betweenness, closeness).
    /// `None` = exact computation.
    pub sample_size: Option<usize>,

    /// Direction for degree centrality: "in", "out", "both".
    pub direction: Option<String>,

    /// Resolution parameter for Louvain (default: 1.0).
    pub resolution: Option<f64>,

    /// Mode for triangle counting / diameter: "global", "per_node", "exact", "approximate".
    pub mode: Option<String>,
}

impl AlgoParams {
    /// PageRank damping factor, validated to (0.0, 1.0).
    pub fn damping_factor(&self) -> f64 {
        self.damping.unwrap_or(0.85).clamp(0.01, 0.99)
    }

    /// Max iterations with sensible default per algorithm.
    ///
    /// Defence-in-depth: the pgwire handler clamps `ITERATIONS` to
    /// `MAX_ITERATIONS_CAP` before dispatch, but any alternate entry
    /// point (native protocol, internal dispatch) also lands here, so
    /// we enforce the ceiling at the engine boundary too.
    pub fn iterations(&self, default: usize) -> usize {
        const ITERATIONS_HARD_CAP: usize = 1_000;
        self.max_iterations
            .unwrap_or(default)
            .clamp(1, ITERATIONS_HARD_CAP)
    }

    /// Convergence tolerance, validated to positive.
    pub fn convergence_tolerance(&self) -> f64 {
        let t = self.tolerance.unwrap_or(1e-7);
        if t > 0.0 { t } else { 1e-7 }
    }

    /// Louvain resolution parameter, validated to positive.
    pub fn louvain_resolution(&self) -> f64 {
        let r = self.resolution.unwrap_or(1.0);
        if r > 0.0 { r } else { 1.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs;

    #[test]
    fn algorithm_names() {
        assert_eq!(GraphAlgorithm::PageRank.name(), "pagerank");
        assert_eq!(GraphAlgorithm::Wcc.name(), "wcc");
        assert_eq!(GraphAlgorithm::KCore.name(), "kcore");
    }

    #[test]
    fn iterative_algorithms() {
        assert!(GraphAlgorithm::PageRank.is_iterative());
        assert!(GraphAlgorithm::LabelPropagation.is_iterative());
        assert!(GraphAlgorithm::Louvain.is_iterative());
        assert!(!GraphAlgorithm::Wcc.is_iterative());
        assert!(!GraphAlgorithm::Sssp.is_iterative());
    }

    #[test]
    fn result_schema_columns() {
        let schema = GraphAlgorithm::PageRank.result_schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0], ("node_id", AlgoColumnType::Text));
        assert_eq!(schema[1], ("rank", AlgoColumnType::Float64));
    }

    #[test]
    fn louvain_schema_has_three_columns() {
        let schema = GraphAlgorithm::Louvain.result_schema();
        assert_eq!(schema.len(), 3);
    }

    #[test]
    fn params_defaults() {
        let p = AlgoParams::default();
        assert_eq!(p.damping_factor(), 0.85);
        assert_eq!(p.iterations(20), 20);
        assert_eq!(p.convergence_tolerance(), 1e-7);
        assert_eq!(p.louvain_resolution(), 1.0);
    }

    #[test]
    fn params_clamping() {
        let p = AlgoParams {
            damping: Some(2.0),
            tolerance: Some(-1.0),
            resolution: Some(0.0),
            ..Default::default()
        };
        assert_eq!(p.damping_factor(), 0.99);
        assert_eq!(p.convergence_tolerance(), 1e-7);
        assert_eq!(p.louvain_resolution(), 1.0);
    }

    #[test]
    fn params_serde_roundtrip() {
        let p = AlgoParams {
            collection: "users".into(),
            damping: Some(0.9),
            max_iterations: Some(30),
            source_node: Some("alice".into()),
            ..Default::default()
        };
        let json = sonic_rs::to_string(&p).unwrap();
        let p2: AlgoParams = sonic_rs::from_str(&json).unwrap();
        assert_eq!(p2.collection, "users");
        assert_eq!(p2.damping, Some(0.9));
        assert_eq!(p2.max_iterations, Some(30));
        assert_eq!(p2.source_node, Some("alice".into()));
    }
}
