//! Retention policy type definitions.
//!
//! A retention policy defines tiered data lifecycle for a timeseries collection:
//! raw data → downsampled aggregates → archive → drop.

use serde::{Deserialize, Serialize};

use crate::engine::timeseries::continuous_agg::AggregateExpr;

/// Complete definition of a tiered retention policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicyDef {
    /// Owning tenant.
    pub tenant_id: u32,
    /// Policy name (unique per tenant).
    pub name: String,
    /// Target collection (must be timeseries).
    pub collection: String,
    /// Ordered tiers: tier 0 is always RAW, subsequent tiers downsample.
    pub tiers: Vec<TierDef>,
    /// Whether automatic query routing across tiers is enabled.
    pub auto_tier: bool,
    /// Whether this policy is actively enforced.
    pub enabled: bool,
    /// Evaluation interval in milliseconds (how often the policy runs).
    /// Default: 3_600_000 (1 hour).
    pub eval_interval_ms: u64,
    /// Who created this policy.
    pub owner: String,
    /// Unix timestamp (seconds) when created.
    pub created_at: u64,
}

/// A single tier in a retention policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierDef {
    /// Tier index (0 = RAW, 1+ = downsampled).
    pub tier_index: u32,
    /// Downsample resolution in milliseconds.
    /// 0 for RAW tier (no aggregation).
    pub resolution_ms: u64,
    /// Aggregate expressions to compute for this tier.
    /// Empty for RAW tier.
    pub aggregates: Vec<AggregateExpr>,
    /// How long to retain data in this tier (milliseconds).
    /// 0 = forever.
    pub retain_ms: u64,
    /// Optional archive target for expired data from this tier.
    pub archive: Option<ArchiveTarget>,
}

/// Where to archive data after it expires from a tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ArchiveTarget {
    /// Archive to S3-compatible object storage.
    S3 { url: String },
}

impl TierDef {
    /// Whether this is the raw (unsampled) tier.
    pub fn is_raw(&self) -> bool {
        self.resolution_ms == 0
    }
}

impl RetentionPolicyDef {
    /// Default evaluation interval: 1 hour.
    pub const DEFAULT_EVAL_INTERVAL_MS: u64 = 3_600_000;

    /// Get the raw tier (always tier 0).
    pub fn raw_tier(&self) -> Option<&TierDef> {
        self.tiers.first().filter(|t| t.is_raw())
    }

    /// Get all downsample tiers (tier 1+).
    pub fn downsample_tiers(&self) -> &[TierDef] {
        if self.tiers.len() > 1 {
            &self.tiers[1..]
        } else {
            &[]
        }
    }

    /// Name of the auto-generated continuous aggregate for a given tier.
    pub fn aggregate_name(&self, tier_index: u32) -> String {
        format!("_policy_{}_tier{}", self.name, tier_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_tier_detection() {
        let raw = TierDef {
            tier_index: 0,
            resolution_ms: 0,
            aggregates: Vec::new(),
            retain_ms: 604_800_000,
            archive: None,
        };
        assert!(raw.is_raw());

        let downsample = TierDef {
            tier_index: 1,
            resolution_ms: 60_000,
            aggregates: Vec::new(),
            retain_ms: 7_776_000_000,
            archive: None,
        };
        assert!(!downsample.is_raw());
    }

    #[test]
    fn aggregate_name_generation() {
        let def = RetentionPolicyDef {
            tenant_id: 1,
            name: "sensor_policy".into(),
            collection: "sensor_data".into(),
            tiers: vec![
                TierDef {
                    tier_index: 0,
                    resolution_ms: 0,
                    aggregates: Vec::new(),
                    retain_ms: 604_800_000,
                    archive: None,
                },
                TierDef {
                    tier_index: 1,
                    resolution_ms: 60_000,
                    aggregates: Vec::new(),
                    retain_ms: 7_776_000_000,
                    archive: None,
                },
            ],
            auto_tier: false,
            enabled: true,
            eval_interval_ms: RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
            owner: "admin".into(),
            created_at: 0,
        };
        assert_eq!(def.aggregate_name(1), "_policy_sensor_policy_tier1");
        assert!(def.raw_tier().is_some());
        assert_eq!(def.downsample_tiers().len(), 1);
    }
}
