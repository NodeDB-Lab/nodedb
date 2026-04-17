//! Per-query graph traversal configuration.
//!
//! Adaptive fan-out uses a two-tier limit with optional graceful
//! degradation instead of a hard kill.

/// Largest accepted value for any graph-DSL depth parameter
/// (`DEPTH`, `MAX_DEPTH`, `EXPANSION_DEPTH`).
///
/// Enforced at every ingress (pgwire, native protocol) and at the
/// engine boundary as defence-in-depth so a single statement cannot
/// saturate `cross_core_bfs`, `csr.shortest_path`, or the subgraph
/// materializer with an unbounded fan-out per hop.
pub const MAX_GRAPH_TRAVERSAL_DEPTH: usize = 64;

use serde::{Deserialize, Serialize};

/// Per-query graph traversal configuration.
///
/// Controls fan-out limits, partial result handling, and visited node caps
/// for scatter-gather graph queries across shards.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct GraphTraversalOptions {
    /// Soft warning threshold (shards per hop).
    ///
    /// When the number of shards reached in a single hop exceeds this value,
    /// a fan-out warning is emitted but execution continues.
    /// Default: 12
    pub fan_out_soft: u16,

    /// Hard limit (shards per hop).
    ///
    /// Maximum number of shards that can be queried in a single hop.
    /// If exceeded and `fan_out_partial` is false, returns FAN_OUT_EXCEEDED error.
    /// Default: 16
    pub fan_out_hard: u16,

    /// If true, return partial results instead of FAN_OUT_EXCEEDED error.
    ///
    /// When the hard limit is exceeded, instead of failing with FAN_OUT_EXCEEDED,
    /// this flag allows the response to be marked as truncated with partial results.
    /// Default: false
    pub fan_out_partial: bool,

    /// Cap on total visited nodes across all shards.
    ///
    /// Once this limit is reached, no further node exploration occurs.
    /// Default: 100_000
    pub max_visited: usize,
}

impl Default for GraphTraversalOptions {
    fn default() -> Self {
        Self {
            fan_out_soft: 12,
            fan_out_hard: 16,
            fan_out_partial: false,
            max_visited: 100_000,
        }
    }
}

impl GraphTraversalOptions {
    /// Create a new `GraphTraversalOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Response metadata for scatter-gather graph query results.
///
/// Tracks how many shards were reached, skipped, and whether results are
/// complete or truncated due to adaptive fan-out limits.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphResponseMeta {
    /// Number of shards that were queried and returned results.
    pub shards_reached: u16,

    /// Number of shards that were skipped due to fan-out limits.
    pub shards_skipped: u16,

    /// Whether results are incomplete (true) or complete (false).
    pub truncated: bool,

    /// Fan-out warning message if soft limit was exceeded.
    ///
    /// Format: "X/Y" where X is shards_reached and Y is fan_out_hard.
    /// None if no warning.
    pub fan_out_warning: Option<String>,

    /// Whether results gathered beyond the soft limit are approximate.
    ///
    /// Set to true when shards_reached > fan_out_soft.
    pub approximate: bool,
}

impl GraphResponseMeta {
    /// Check if this response has no warnings or truncation.
    ///
    /// Returns true if:
    /// - No fan-out warning
    /// - Not truncated
    /// - Not approximate
    pub fn is_clean(&self) -> bool {
        self.fan_out_warning.is_none() && !self.truncated && !self.approximate
    }

    /// Create response metadata with a fan-out warning.
    ///
    /// Indicates that the soft limit was exceeded but execution continued.
    /// Creates a warning string like "12/16" showing reached vs hard limit.
    pub fn with_warning(shards_reached: u16, shards_skipped: u16, fan_out_hard: u16) -> Self {
        Self {
            shards_reached,
            shards_skipped,
            truncated: false,
            fan_out_warning: Some(format!("{}/{}", shards_reached, fan_out_hard)),
            approximate: true,
        }
    }

    /// Create response metadata for truncated results.
    ///
    /// Indicates that results were incomplete due to fan-out limits.
    pub fn with_truncation(shards_reached: u16, shards_skipped: u16) -> Self {
        Self {
            shards_reached,
            shards_skipped,
            truncated: true,
            fan_out_warning: None,
            approximate: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs;

    #[test]
    fn default_options_have_expected_values() {
        let opts = GraphTraversalOptions::default();
        assert_eq!(opts.fan_out_soft, 12);
        assert_eq!(opts.fan_out_hard, 16);
        assert!(!opts.fan_out_partial);
        assert_eq!(opts.max_visited, 100_000);
    }

    #[test]
    fn new_returns_defaults() {
        let opts = GraphTraversalOptions::new();
        assert_eq!(opts, GraphTraversalOptions::default());
    }

    #[test]
    fn default_meta_is_clean() {
        let meta = GraphResponseMeta::default();
        assert!(meta.is_clean());
        assert_eq!(meta.shards_reached, 0);
        assert_eq!(meta.shards_skipped, 0);
        assert!(!meta.truncated);
        assert!(meta.fan_out_warning.is_none());
        assert!(!meta.approximate);
    }

    #[test]
    fn with_warning_generates_correct_string() {
        let meta = GraphResponseMeta::with_warning(12, 4, 16);
        assert_eq!(meta.shards_reached, 12);
        assert_eq!(meta.shards_skipped, 4);
        assert!(!meta.truncated);
        assert_eq!(meta.fan_out_warning, Some("12/16".to_string()));
        assert!(meta.approximate);
    }

    #[test]
    fn with_truncation_sets_flags() {
        let meta = GraphResponseMeta::with_truncation(10, 6);
        assert_eq!(meta.shards_reached, 10);
        assert_eq!(meta.shards_skipped, 6);
        assert!(meta.truncated);
        assert!(meta.fan_out_warning.is_none());
        assert!(meta.approximate);
    }

    #[test]
    fn with_warning_is_not_clean() {
        let meta = GraphResponseMeta::with_warning(12, 4, 16);
        assert!(!meta.is_clean());
    }

    #[test]
    fn with_truncation_is_not_clean() {
        let meta = GraphResponseMeta::with_truncation(10, 6);
        assert!(!meta.is_clean());
    }

    #[test]
    fn serialization_roundtrip() {
        let opts = GraphTraversalOptions {
            fan_out_soft: 8,
            fan_out_hard: 12,
            fan_out_partial: true,
            max_visited: 50_000,
        };
        let json = sonic_rs::to_string(&opts).unwrap();
        let deserialized: GraphTraversalOptions = sonic_rs::from_str(&json).unwrap();
        assert_eq!(opts.fan_out_soft, deserialized.fan_out_soft);
        assert_eq!(opts.fan_out_hard, deserialized.fan_out_hard);
        assert_eq!(opts.fan_out_partial, deserialized.fan_out_partial);
        assert_eq!(opts.max_visited, deserialized.max_visited);
    }

    #[test]
    fn meta_serialization_roundtrip() {
        let meta = GraphResponseMeta::with_warning(15, 1, 16);
        let json = sonic_rs::to_string(&meta).unwrap();
        let deserialized: GraphResponseMeta = sonic_rs::from_str(&json).unwrap();
        assert_eq!(meta.shards_reached, deserialized.shards_reached);
        assert_eq!(meta.shards_skipped, deserialized.shards_skipped);
        assert_eq!(meta.truncated, deserialized.truncated);
        assert_eq!(meta.fan_out_warning, deserialized.fan_out_warning);
        assert_eq!(meta.approximate, deserialized.approximate);
    }
}
