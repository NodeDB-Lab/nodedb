//! `SHOW RETENTION POLICY` DDL handler.
//!
//! Syntax:
//! ```sql
//! SHOW RETENTION POLICY ON <collection>
//! SHOW RETENTION POLICIES
//! ```

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::engine::timeseries::retention_policy::types::ArchiveTarget;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

/// SHOW RETENTION POLICY ON <collection>
/// SHOW RETENTION POLICIES
pub fn show_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u32();
    // Determine if filtering by collection.
    let collection_filter = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("ON") {
        Some(parts[4].to_lowercase())
    } else {
        None
    };

    let policies = state.retention_policy_registry.list_for_tenant(tenant_id);

    let schema = Arc::new(vec![
        text_field("policy_name"),
        text_field("collection"),
        text_field("enabled"),
        text_field("auto_tier"),
        int8_field("tier_count"),
        text_field("tiers"),
        text_field("eval_interval"),
        text_field("owner"),
        int8_field("created_at"),
    ]);

    let mut rows = Vec::new();
    for policy in &policies {
        // Apply collection filter.
        if let Some(ref coll) = collection_filter
            && &policy.collection != coll
        {
            continue;
        }

        let tiers_desc = format_tiers(&policy.tiers);
        let eval_interval = format_duration_ms(policy.eval_interval_ms);

        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&policy.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&policy.collection)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&policy.enabled.to_string())
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&policy.auto_tier.to_string())
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(policy.tiers.len() as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&tiers_desc)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&eval_interval)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&policy.owner)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(policy.created_at as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// Format tiers into a compact human-readable string.
fn format_tiers(tiers: &[crate::engine::timeseries::retention_policy::TierDef]) -> String {
    let mut parts = Vec::new();
    for tier in tiers {
        let resolution = if tier.is_raw() {
            "RAW".to_string()
        } else {
            format_duration_ms(tier.resolution_ms)
        };
        let retain = if tier.retain_ms == 0 {
            "forever".to_string()
        } else {
            format_duration_ms(tier.retain_ms)
        };
        let archive = match &tier.archive {
            Some(ArchiveTarget::S3 { url }) => format!(" → {url}"),
            None => String::new(),
        };
        parts.push(format!("{resolution} ({retain}){archive}"));
    }
    parts.join(" → ")
}

/// Format milliseconds as a human-readable duration.
fn format_duration_ms(ms: u64) -> String {
    const SECOND: u64 = 1_000;
    const MINUTE: u64 = 60 * SECOND;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;
    const YEAR: u64 = 365 * DAY;

    if ms == 0 {
        return "0".to_string();
    }
    if ms.is_multiple_of(YEAR) {
        return format!("{}y", ms / YEAR);
    }
    if ms.is_multiple_of(DAY) {
        return format!("{}d", ms / DAY);
    }
    if ms.is_multiple_of(HOUR) {
        return format!("{}h", ms / HOUR);
    }
    if ms.is_multiple_of(MINUTE) {
        return format!("{}m", ms / MINUTE);
    }
    if ms.is_multiple_of(SECOND) {
        return format!("{}s", ms / SECOND);
    }
    format!("{ms}ms")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_duration() {
        assert_eq!(format_duration_ms(60_000), "1m");
        assert_eq!(format_duration_ms(3_600_000), "1h");
        assert_eq!(format_duration_ms(86_400_000), "1d");
        assert_eq!(format_duration_ms(604_800_000), "7d");
        assert_eq!(format_duration_ms(31_536_000_000), "1y");
        assert_eq!(format_duration_ms(1_500), "1500ms");
        assert_eq!(format_duration_ms(5_000), "5s");
    }

    #[test]
    fn format_tiers_display() {
        use crate::engine::timeseries::retention_policy::types::{ArchiveTarget, TierDef};

        let tiers = vec![
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
            TierDef {
                tier_index: 2,
                resolution_ms: 3_600_000,
                aggregates: Vec::new(),
                retain_ms: 63_072_000_000,
                archive: Some(ArchiveTarget::S3 {
                    url: "s3://bucket/data/".into(),
                }),
            },
        ];
        let desc = format_tiers(&tiers);
        assert_eq!(desc, "RAW (7d) → 1m (90d) → 1h (2y) → s3://bucket/data/");
    }
}
