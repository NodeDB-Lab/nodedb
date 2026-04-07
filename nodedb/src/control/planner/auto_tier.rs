//! AUTO_TIER query routing for tiered retention policies.
//!
//! When a timeseries collection has an enabled retention policy with `auto_tier = true`,
//! queries are automatically routed to the highest-resolution tier that covers each
//! time segment. A 6-month query reads hourly aggregates for old data and raw data
//! for recent data — transparent to the client.
//!
//! The routing decision uses the policy's tier definitions and their `retain_ms`
//! to estimate which tier covers which time range. Watermark-based routing would
//! be more precise but requires a Data Plane round-trip; retain_ms-based routing
//! is instantaneous and correct for steady-state operation.

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::TimeseriesOp;
use crate::engine::timeseries::retention_policy::RetentionPolicyDef;
use crate::types::{TenantId, VShardId};

use super::physical::PhysicalTask;

/// A time segment assigned to a specific tier.
#[derive(Debug)]
struct TierSegment {
    /// Tier index (0 = RAW, 1+ = downsample).
    tier_index: u32,
    /// Collection name to query (raw collection or aggregate name).
    collection: String,
    /// Time range for this segment.
    time_range: (i64, i64),
    /// Bucket interval (0 for raw, tier resolution_ms for aggregates).
    bucket_interval_ms: i64,
}

/// Plan a tiered scan: split the query time range across tiers and return
/// one `PhysicalTask` per tier segment.
///
/// Each task targets either the raw collection or a tier's auto-created
/// continuous aggregate (`_policy_{name}_tier{N}`).
pub(super) fn plan_tiered_scan(
    policy: &RetentionPolicyDef,
    tenant_id: TenantId,
    query_time_range: (i64, i64),
    filters: Vec<u8>,
    group_by: Vec<String>,
    aggregates: Vec<(String, String)>,
    gap_fill: String,
) -> Vec<PhysicalTask> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let segments = compute_tier_segments(policy, query_time_range, now_ms);

    if segments.is_empty() {
        // Fallback: single raw scan (shouldn't happen, but defensive).
        return vec![build_scan_task(
            tenant_id,
            &policy.collection,
            query_time_range,
            0,
            &filters,
            &group_by,
            &aggregates,
            &gap_fill,
        )];
    }

    segments
        .iter()
        .map(|seg| {
            build_scan_task(
                tenant_id,
                &seg.collection,
                seg.time_range,
                seg.bucket_interval_ms,
                &filters,
                &group_by,
                &aggregates,
                &gap_fill,
            )
        })
        .collect()
}

/// Determine which tier covers which time segment.
///
/// Works backwards from now: the most recent `retain_ms` goes to the
/// highest-resolution tier (tier 0 = raw), then the next tier, etc.
///
/// Example for policy: RAW 7d, 1m 90d, 1h 2y:
/// - [now-7d, now] → RAW (tier 0)
/// - [now-90d, now-7d] → 1m aggregate (tier 1)
/// - [now-2y, now-90d] → 1h aggregate (tier 2)
fn compute_tier_segments(
    policy: &RetentionPolicyDef,
    query_range: (i64, i64),
    now_ms: i64,
) -> Vec<TierSegment> {
    let (query_start, query_end) = query_range;
    let mut segments = Vec::new();

    // Build tier boundaries working backward from now.
    // Each tier covers from (now - cumulative_retain) to (now - previous_cumulative).
    let mut boundary = now_ms; // Right edge of current tier.

    for tier in &policy.tiers {
        if tier.retain_ms == 0 {
            // "forever" retention — this tier covers everything older.
            let tier_start = i64::MIN;
            let seg_start = query_start.max(tier_start);
            let seg_end = query_end.min(boundary);
            if seg_start < seg_end {
                segments.push(TierSegment {
                    tier_index: tier.tier_index,
                    collection: collection_for_tier(policy, tier.tier_index),
                    time_range: (seg_start, seg_end),
                    bucket_interval_ms: tier.resolution_ms as i64,
                });
            }
            break; // Forever tier consumes all remaining time.
        }

        let tier_start = boundary - tier.retain_ms as i64;

        // Intersect with query range.
        let seg_start = query_start.max(tier_start);
        let seg_end = query_end.min(boundary);

        if seg_start < seg_end {
            segments.push(TierSegment {
                tier_index: tier.tier_index,
                collection: collection_for_tier(policy, tier.tier_index),
                time_range: (seg_start, seg_end),
                bucket_interval_ms: tier.resolution_ms as i64,
            });
        }

        boundary = tier_start;
    }

    // Reverse so segments are ordered oldest → newest (natural time order).
    segments.reverse();
    segments
}

/// Get the collection name for a tier.
fn collection_for_tier(policy: &RetentionPolicyDef, tier_index: u32) -> String {
    if tier_index == 0 {
        policy.collection.clone()
    } else {
        policy.aggregate_name(tier_index)
    }
}

/// Explain the tier selection for a query — returns human-readable tier plan.
///
/// Used by EXPLAIN to show which tiers are selected for each time segment.
pub(crate) fn explain_tier_selection(
    policy: &RetentionPolicyDef,
    query_time_range: (i64, i64),
) -> String {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let segments = compute_tier_segments(policy, query_time_range, now_ms);

    if segments.is_empty() {
        return format!(
            "AUTO_TIER on '{}': no tier covers the requested range",
            policy.collection
        );
    }

    let mut lines = vec![format!(
        "AUTO_TIER plan for '{}' ({} tiers, {} segments):",
        policy.collection,
        policy.tiers.len(),
        segments.len()
    )];

    for seg in &segments {
        let tier_label = if seg.tier_index == 0 {
            "RAW".to_string()
        } else {
            format!(
                "tier{} ({}ms buckets)",
                seg.tier_index, seg.bucket_interval_ms
            )
        };
        lines.push(format!(
            "  [{} .. {}] → {} ({})",
            seg.time_range.0, seg.time_range.1, tier_label, seg.collection,
        ));
    }

    lines.join("\n")
}

/// Build a single `PhysicalTask` for a tier segment.
#[allow(clippy::too_many_arguments)]
fn build_scan_task(
    tenant_id: TenantId,
    collection: &str,
    time_range: (i64, i64),
    bucket_interval_ms: i64,
    filters: &[u8],
    group_by: &[String],
    aggregates: &[(String, String)],
    gap_fill: &str,
) -> PhysicalTask {
    PhysicalTask {
        tenant_id,
        vshard_id: VShardId::from_collection(collection),
        plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: collection.to_string(),
            time_range,
            projection: Vec::new(),
            limit: usize::MAX,
            filters: filters.to_vec(),
            bucket_interval_ms,
            group_by: group_by.to_vec(),
            aggregates: aggregates.to_vec(),
            gap_fill: gap_fill.to_string(),
            rls_filters: Vec::new(),
        }),
        post_dedup: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::retention_policy::types::TierDef;

    fn make_policy() -> RetentionPolicyDef {
        RetentionPolicyDef {
            tenant_id: 1,
            name: "sensor_policy".into(),
            collection: "sensor_data".into(),
            tiers: vec![
                TierDef {
                    tier_index: 0,
                    resolution_ms: 0,
                    aggregates: Vec::new(),
                    retain_ms: 604_800_000, // 7 days
                    archive: None,
                },
                TierDef {
                    tier_index: 1,
                    resolution_ms: 60_000, // 1 minute
                    aggregates: Vec::new(),
                    retain_ms: 7_776_000_000, // 90 days
                    archive: None,
                },
                TierDef {
                    tier_index: 2,
                    resolution_ms: 3_600_000, // 1 hour
                    aggregates: Vec::new(),
                    retain_ms: 63_072_000_000, // 2 years
                    archive: None,
                },
            ],
            auto_tier: true,
            enabled: true,
            eval_interval_ms: 3_600_000,
            owner: "admin".into(),
            created_at: 0,
        }
    }

    #[test]
    fn segments_cover_full_range() {
        let policy = make_policy();
        let now = 100_000_000_000i64; // ~3 years from epoch
        let query_start = now - 30 * 86_400_000; // 30 days ago
        let query_end = now;

        let segments = compute_tier_segments(&policy, (query_start, query_end), now);

        // Should produce 2 segments: raw (last 7d) + tier1 (7d-30d).
        assert_eq!(segments.len(), 2);

        // First segment (oldest) = tier1 aggregate.
        assert_eq!(segments[0].tier_index, 1);
        assert_eq!(segments[0].collection, "_policy_sensor_policy_tier1");
        assert_eq!(segments[0].bucket_interval_ms, 60_000);

        // Second segment (newest) = raw.
        assert_eq!(segments[1].tier_index, 0);
        assert_eq!(segments[1].collection, "sensor_data");
        assert_eq!(segments[1].bucket_interval_ms, 0);
    }

    #[test]
    fn recent_query_uses_raw_only() {
        let policy = make_policy();
        let now = 100_000_000_000i64;
        let query_start = now - 3_600_000; // 1 hour ago
        let query_end = now;

        let segments = compute_tier_segments(&policy, (query_start, query_end), now);

        // Entirely within raw tier.
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].tier_index, 0);
        assert_eq!(segments[0].collection, "sensor_data");
    }

    #[test]
    fn old_query_uses_highest_tier() {
        let policy = make_policy();
        let now = 100_000_000_000i64;
        let query_start = now - 365 * 86_400_000; // 1 year ago
        let query_end = now - 180 * 86_400_000; // 6 months ago

        let segments = compute_tier_segments(&policy, (query_start, query_end), now);

        // Entirely within tier2 (1h aggregates, 90d-2y).
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].tier_index, 2);
        assert_eq!(segments[0].collection, "_policy_sensor_policy_tier2");
    }

    #[test]
    fn plan_produces_tasks_per_segment() {
        let policy = make_policy();
        // Use wall-clock now since plan_tiered_scan uses SystemTime internally.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let tasks = plan_tiered_scan(
            &policy,
            TenantId::new(1),
            (now - 30 * 86_400_000, now),
            Vec::new(),
            Vec::new(),
            vec![("avg".into(), "temperature".into())],
            String::new(),
        );

        assert!(tasks.len() >= 2); // At least raw + tier1.
    }
}
