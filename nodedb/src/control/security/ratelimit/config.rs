//! Rate limiting configuration: tiers, endpoint costs, defaults.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Rate limit configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Whether rate limiting is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Default rate limit for unauthenticated/untied requests.
    #[serde(default = "default_qps")]
    pub default_qps: u64,

    /// Default burst capacity.
    #[serde(default = "default_burst")]
    pub default_burst: u64,

    /// Named tiers: `{ "free": { qps: 50, burst: 100 }, "pro": { qps: 5000, burst: 10000 } }`.
    /// Selected by `$auth.metadata.plan` claim or explicit config.
    #[serde(default)]
    pub tiers: HashMap<String, RateLimitTier>,

    /// Per-endpoint cost multipliers. Key = operation name.
    /// Default cost is 1. `vector_search` = 20 means each search costs 20 tokens.
    #[serde(default)]
    pub endpoint_costs: HashMap<String, u64>,
}

/// A named rate limit tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitTier {
    /// Sustained queries per second.
    pub qps: u64,
    /// Burst capacity.
    pub burst: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_qps: 100,
            default_burst: 200,
            tiers: HashMap::new(),
            endpoint_costs: default_endpoint_costs(),
        }
    }
}

fn default_qps() -> u64 {
    100
}
fn default_burst() -> u64 {
    200
}

/// Default per-endpoint cost multipliers.
fn default_endpoint_costs() -> HashMap<String, u64> {
    let mut m = HashMap::new();
    m.insert("point_get".into(), 1);
    m.insert("point_put".into(), 1);
    m.insert("document_scan".into(), 5);
    m.insert("vector_search".into(), 20);
    m.insert("text_search".into(), 10);
    m.insert("hybrid_search".into(), 25);
    m.insert("graph_hop".into(), 10);
    m.insert("graph_path".into(), 15);
    m.insert("aggregate".into(), 10);
    m.insert("kv_get".into(), 1);
    m.insert("kv_put".into(), 1);
    m.insert("kv_scan".into(), 5);
    m
}

impl RateLimitConfig {
    /// Get the cost of an operation. Returns 1 if not configured.
    pub fn operation_cost(&self, operation: &str) -> u64 {
        *self.endpoint_costs.get(operation).unwrap_or(&1)
    }

    /// Resolve a tier by name. Returns `None` if tier not found.
    pub fn tier(&self, name: &str) -> Option<&RateLimitTier> {
        self.tiers.get(name)
    }
}
