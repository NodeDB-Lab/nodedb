// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral migration DDL command: SHOW MIGRATIONS.
//!
//! Ported from the pgwire `ddl::cluster::migration` handler. The migration
//! tracker read is preserved verbatim; only the result construction changed
//! from pgwire `Response` / `QueryResponse` to the protocol-neutral
//! `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// SHOW MIGRATIONS — list active and recent migrations.
///
/// Superuser only.
pub fn show_migrations(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(ddl_err(
            "42501",
            "permission denied: only superuser can view migrations",
        ));
    }

    let tracker = match &state.migration_tracker {
        Some(t) => t,
        None => {
            return Err(ddl_err(
                "55000",
                "cluster mode not enabled (single-node instance)",
            ));
        }
    };

    let snapshots = tracker.snapshot();

    let columns = vec![
        "vshard_id".to_string(),
        "phase".to_string(),
        "elapsed_ms".to_string(),
        "active".to_string(),
    ];
    let column_types = vec![
        DdlColType::Int8,
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Text,
    ];

    let mut rows = Vec::new();
    for s in &snapshots {
        let active_str = if s.is_active { "yes" } else { "no" };

        let mut row = Map::new();
        row.insert(
            "vshard_id".to_string(),
            JsonValue::String((s.vshard_id as i64).to_string()),
        );
        row.insert("phase".to_string(), JsonValue::String(s.phase.clone()));
        row.insert(
            "elapsed_ms".to_string(),
            JsonValue::String((s.elapsed_ms as i64).to_string()),
        );
        row.insert(
            "active".to_string(),
            JsonValue::String(active_str.to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}
