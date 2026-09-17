// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `EXPLAIN TIERS ON <collection>` handler.
//!
//! Shows the AUTO_TIER routing plan for a collection's retention policy. The
//! handler builds [`DdlResult`] directly and carries no pgwire types.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};

/// Execute `EXPLAIN TIERS ON <collection> [RANGE <start_ms> <end_ms>]`.
pub fn explain_tiers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // EXPLAIN TIERS ON <collection> [RANGE <start_ms> <end_ms>]
    if parts.len() < 4 || !parts[2].eq_ignore_ascii_case("ON") {
        return Err(err(
            "42601",
            "syntax: EXPLAIN TIERS ON <collection> [RANGE <start_ms> <end_ms>]",
        ));
    }
    let collection = parse_ident_token(parts[3])?;
    let tenant_id = identity.tenant_id.as_u64();

    let policy = state
        .retention_policy_registry
        .get_for_collection(database_id.as_u64(), tenant_id, &collection)
        .ok_or_else(|| {
            err(
                "42704",
                &format!("no retention policy found for '{collection}'"),
            )
        })?;

    if !policy.auto_tier {
        return Err(err(
            "42809",
            &format!("AUTO_TIER is not enabled on '{collection}'"),
        ));
    }

    // Optional RANGE clause: EXPLAIN TIERS ON coll RANGE 1700000000 1710000000
    let time_range = if parts.len() >= 7 && parts[4].eq_ignore_ascii_case("RANGE") {
        let start = parts[5]
            .parse::<i64>()
            .map_err(|_| err("42601", &format!("invalid RANGE start: {}", parts[5])))?;
        let end = parts[6]
            .parse::<i64>()
            .map_err(|_| err("42601", &format!("invalid RANGE end: {}", parts[6])))?;
        (start, end)
    } else {
        (0i64, i64::MAX)
    };
    let explanation =
        crate::control::planner::auto_tier::explain_tier_selection(&policy, time_range);

    let columns = vec!["plan".to_string()];
    let mut rows = Vec::new();
    for line in explanation.lines() {
        let mut row = Map::new();
        row.insert("plan".to_string(), JsonValue::String(line.to_string()));
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(columns, rows))])
}

/// Build a [`DdlError`] from a SQLSTATE + message.
fn err(sqlstate: &str, message: &str) -> DdlError {
    DdlError::new(sqlstate, message)
}
