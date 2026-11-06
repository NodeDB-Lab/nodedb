// SPDX-License-Identifier: BUSL-1.1

//! Handler for `SHOW DATABASE MIRROR STATUS [FOR <name>]`.
//!
//! The tenant-admin gate, catalog list, mirror-only filtering, `FOR <name>`
//! filter, status / mode / lag rendering, `mirror_cut_state` reads, and the
//! not-found error for a specific name run here. The result is the
//! protocol-neutral [`DdlResult`] over `ShapedRows`. Every column is `Text`.
//! `lag_ms` is empty until a cut applied, and `failure` is empty unless the
//! link failed.

use serde_json::{Map, Value as JsonValue};

use nodedb_types::{MirrorMode, MirrorStatus};

use crate::control::security::catalog::database_types::DatabaseStatus;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::result::{DdlError, DdlResult};
use super::super::gate::require_tenant_admin;
use super::super::support::{ddl_err, text_rows};

/// Nanoseconds in one millisecond, for the wall clock.
const NANOS_PER_MS: u64 = 1_000_000;

/// Handle `SHOW DATABASE MIRROR STATUS [FOR <name>]`.
pub fn show_database_mirror_status(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show database mirror status")?;

    let catalog = state.credentials.catalog();

    let all_databases = catalog
        .list_databases()
        .map_err(|e| DdlError::from_error_in_context("catalog list failed", &e))?;

    let columns = vec![
        "name".to_string(),
        "source_cluster".to_string(),
        "source_database".to_string(),
        "source_database_name".to_string(),
        "mode".to_string(),
        "status".to_string(),
        "failure".to_string(),
        "bytes_done".to_string(),
        "bytes_total".to_string(),
        "lag_ms".to_string(),
        "ready_cut".to_string(),
        "applied_cut".to_string(),
        "applied_cut_hlc_ns".to_string(),
    ];

    let now_ms = nodedb_types::wall_now_ns() / NANOS_PER_MS;

    let mut rows: Vec<Map<String, JsonValue>> = Vec::new();

    for db in &all_databases {
        // Filter: only mirror databases (Mirroring status or Active with Promoted origin).
        let origin = match &db.mirror_origin {
            Some(o) => o,
            None => continue,
        };

        // Apply FOR <name> filter if specified.
        if let Some(filter) = name
            && !db.name.eq_ignore_ascii_case(filter)
        {
            continue;
        }

        // Check that the database status is consistent with a mirror lifecycle.
        match db.status {
            DatabaseStatus::Mirroring | DatabaseStatus::Active => {}
            DatabaseStatus::Deactivated | DatabaseStatus::Cloning => continue,
        }

        let mode_str = match origin.mode {
            MirrorMode::Sync => "sync",
            MirrorMode::Async => "async",
        };

        let (status_str, bytes_done, bytes_total, failure) = match &origin.status {
            MirrorStatus::Bootstrapping {
                bytes_done,
                bytes_total,
            } => ("bootstrapping", *bytes_done, *bytes_total, String::new()),
            MirrorStatus::Following => ("following", 0, 0, String::new()),
            MirrorStatus::Degraded => ("degraded", 0, 0, String::new()),
            MirrorStatus::Disconnected => ("disconnected", 0, 0, String::new()),
            MirrorStatus::Promoting { .. } => ("promoting", 0, 0, String::new()),
            MirrorStatus::Failed { failure } => ("failed", 0, 0, failure.to_string()),
            MirrorStatus::Promoted => ("promoted", 0, 0, String::new()),
        };

        let cut_state = catalog
            .get_mirror_cut_state(db.id)
            .map_err(|e| DdlError::from_error_in_context("mirror cut state read failed", &e))?
            .unwrap_or_default();
        // A promoted mirror keeps its cut state and follows nothing.
        let lag_ms = match origin.status {
            MirrorStatus::Promoted => String::new(),
            MirrorStatus::Bootstrapping { .. }
            | MirrorStatus::Following
            | MirrorStatus::Degraded
            | MirrorStatus::Disconnected
            | MirrorStatus::Promoting { .. }
            | MirrorStatus::Failed { .. } => cut_state
                .lag_ms(now_ms)
                .map_or_else(String::new, |lag_ms| lag_ms.to_string()),
        };

        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(db.name.clone()));
        row.insert(
            "source_cluster".to_string(),
            JsonValue::String(origin.source_cluster.clone()),
        );
        row.insert(
            "source_database".to_string(),
            JsonValue::String(origin.source_database.as_u64().to_string()),
        );
        row.insert(
            "source_database_name".to_string(),
            JsonValue::String(origin.source_database_name.clone()),
        );
        row.insert("mode".to_string(), JsonValue::String(mode_str.to_string()));
        row.insert(
            "status".to_string(),
            JsonValue::String(status_str.to_string()),
        );
        row.insert("failure".to_string(), JsonValue::String(failure));
        row.insert(
            "bytes_done".to_string(),
            JsonValue::String(bytes_done.to_string()),
        );
        row.insert(
            "bytes_total".to_string(),
            JsonValue::String(bytes_total.to_string()),
        );
        row.insert("lag_ms".to_string(), JsonValue::String(lag_ms));
        row.insert(
            "ready_cut".to_string(),
            JsonValue::String(cut_state.ready_cut.to_string()),
        );
        row.insert(
            "applied_cut".to_string(),
            JsonValue::String(cut_state.applied_cut.to_string()),
        );
        row.insert(
            "applied_cut_hlc_ns".to_string(),
            JsonValue::String(cut_state.applied_cut_hlc_ns.to_string()),
        );
        rows.push(row);
    }

    // When a specific name was requested and no rows were found, return an error.
    if let Some(filter) = name
        && rows.is_empty()
    {
        return Err(ddl_err(
            "42P01",
            format!("mirror database '{filter}' not found"),
        ));
    }

    Ok(text_rows(columns, rows))
}
