// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW RETENTION POLICY` DDL handler.
//!
//! Ported from the pgwire `ddl::retention_policy::show` handler. The registry
//! listing, the optional `ON <collection>` filter, the tier / duration
//! formatting, and the exact column set are preserved verbatim; only the result
//! construction changed from pgwire `Response` / `QueryResponse` /
//! `DataRowEncoder` to the protocol-neutral [`DdlResult::Rows`] over
//! [`ShapedRows`]. The mixed text/`int8` column OIDs (`tier_count` and
//! `created_at` are `int8`, every other column is text) are reproduced by
//! building `column_types` manually so the RowDescription stays byte-identical;
//! the `int8` cells are emitted as their decimal text form, the same bytes the
//! pgwire `DataRowEncoder::encode_field(&i64)` produced.
//!
//! Syntax:
//! ```sql
//! SHOW RETENTION POLICY ON <collection>
//! SHOW RETENTION POLICIES
//! ```

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::engine::timeseries::retention_policy::types::ArchiveTarget;

use super::super::super::result::{DdlError, DdlResult};

/// SHOW RETENTION POLICY ON <collection>
/// SHOW RETENTION POLICIES
pub fn show_retention_policy(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    // Determine if filtering by collection.
    let collection_filter = if parts.len() >= 5 && parts[3].eq_ignore_ascii_case("ON") {
        Some(parse_ident_token(parts[4])?)
    } else {
        None
    };

    let policies = state
        .retention_policy_registry
        .list_for_tenant_in_database(database_id.as_u64(), tenant_id);

    let columns = vec![
        "policy_name".to_string(),
        "collection".to_string(),
        "enabled".to_string(),
        "auto_tier".to_string(),
        "tier_count".to_string(),
        "tiers".to_string(),
        "eval_interval".to_string(),
        "owner".to_string(),
        "created_at".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Text,
        DdlColType::Int8,
    ];

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

        let mut row = Map::new();
        row.insert(
            "policy_name".to_string(),
            JsonValue::String(policy.name.clone()),
        );
        row.insert(
            "collection".to_string(),
            JsonValue::String(policy.collection.clone()),
        );
        row.insert(
            "enabled".to_string(),
            JsonValue::String(policy.enabled.to_string()),
        );
        row.insert(
            "auto_tier".to_string(),
            JsonValue::String(policy.auto_tier.to_string()),
        );
        row.insert(
            "tier_count".to_string(),
            JsonValue::String((policy.tiers.len() as i64).to_string()),
        );
        row.insert("tiers".to_string(), JsonValue::String(tiers_desc));
        row.insert(
            "eval_interval".to_string(),
            JsonValue::String(eval_interval),
        );
        row.insert("owner".to_string(), JsonValue::String(policy.owner.clone()));
        row.insert(
            "created_at".to_string(),
            JsonValue::String((policy.created_at as i64).to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
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
