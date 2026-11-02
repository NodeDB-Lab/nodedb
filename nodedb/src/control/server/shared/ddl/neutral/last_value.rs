// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `LAST_VALUE` and `LAST_VALUES` query handlers.
//!
//! Syntax:
//! ```sql
//! SELECT LAST_VALUES('<collection>')
//! SELECT LAST_VALUE('<collection>', <series_id>)
//! ```
//!
//! These dispatch `MetaOp::QueryLastValues` / `QueryLastValue` to the Data Plane
//! and return results as protocol-neutral rows. Inside a transaction block
//! both read at the block's snapshot, so a value committed after it stays
//! invisible to the block.

use std::time::Duration;

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::server::shared::session::{DmlTxnCtx, StatementDeadline};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::MetaOp;

use super::super::result::{DdlError, DdlResult};

/// The system time a last-value read of `txn_ctx`'s statement reads at:
/// the block's snapshot inside a transaction block, else the current state.
async fn read_scope(
    state: &SharedState,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<nodedb_types::SystemTimeScope, DdlError> {
    let snapshot = txn_ctx
        .txn_snapshot(state)
        .await
        .map_err(|e| DdlError::from_error_in_context("transaction snapshot", &e))?;
    Ok(snapshot.map_or(nodedb_types::SystemTimeScope::Current, |cut| cut.scope()))
}

/// `SELECT LAST_VALUES('<collection>')` — returns all cached last values.
pub async fn query_last_values(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let plan = PhysicalPlan::Meta(MetaOp::QueryLastValues {
        collection: nodedb_types::QualifiedCollection::new(database_id, collection),
        system_time: read_scope(state, txn_ctx).await?,
    });

    // Only reached through `shared::ddl::dispatch`, which native/pgwire/HTTP
    // each call after their own single per-request admission gate.
    let payload = crate::control::server::shared::ddl::user_dispatch::dispatch_for_identity(
        crate::control::server::shared::ddl::user_dispatch::DispatchRequest {
            state,
            identity,
            database_id,
            collection,
            plan,
            deadline: StatementDeadline::after(Duration::from_secs(5)),
            // Admitted at this request's own transport entry; this handler
            // has no session or peer information of its own.
            admission: crate::control::server::shared::ddl::user_dispatch::RequestAdmission::AlreadyAdmitted,
            // No session reaches this handler, so its read takes the strong
            // default (see `DmlTxnCtx::linearizable_reads`).
            linearizable: true,
        },
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("dispatch failed", &e))?;

    // `meta_query_last_values` encodes with `response_codec::encode`, which is
    // MessagePack — `decode_payload` is its counterpart. A JSON parser on those
    // bytes fails on the first one, and defaulting that failure away reports
    // "this collection caches no last values" for every collection that has
    // them.
    let entries: Vec<(u64, i64, f64)> =
        crate::data::executor::response_codec::decode_payload(&payload)
            .map_err(|e| DdlError::from_error_in_context("LAST_VALUES reply", &e))?;

    let mut rows = Vec::with_capacity(entries.len());
    for (series_id, ts, value) in &entries {
        let mut row = Map::new();
        row.insert(
            "series_id".to_string(),
            JsonValue::String((*series_id as i64).to_string()),
        );
        row.insert(
            "timestamp_ms".to_string(),
            JsonValue::String(ts.to_string()),
        );
        row.insert(
            "value".to_string(),
            JsonValue::String(format!("{value:.6}")),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        vec![
            "series_id".to_string(),
            "timestamp_ms".to_string(),
            "value".to_string(),
        ],
        vec![DdlColType::Int8, DdlColType::Int8, DdlColType::Text],
        rows,
    ))])
}

/// `SELECT LAST_VALUE('<collection>', <series_id>)` — returns single series value.
pub async fn query_last_value(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
    series_id: u64,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let plan = PhysicalPlan::Meta(MetaOp::QueryLastValue {
        collection: nodedb_types::QualifiedCollection::new(database_id, collection),
        series_id,
        system_time: read_scope(state, txn_ctx).await?,
    });

    // Only reached through `shared::ddl::dispatch`, which native/pgwire/HTTP
    // each call after their own single per-request admission gate.
    let payload = crate::control::server::shared::ddl::user_dispatch::dispatch_for_identity(
        crate::control::server::shared::ddl::user_dispatch::DispatchRequest {
            state,
            identity,
            database_id,
            collection,
            plan,
            deadline: StatementDeadline::after(Duration::from_secs(5)),
            // Admitted at this request's own transport entry; this handler
            // has no session or peer information of its own.
            admission: crate::control::server::shared::ddl::user_dispatch::RequestAdmission::AlreadyAdmitted,
            // No session reaches this handler, so its read takes the strong
            // default (see `DmlTxnCtx::linearizable_reads`).
            linearizable: true,
        },
    )
    .await
    .map_err(|e| DdlError::from_error_in_context("dispatch failed", &e))?;

    // MessagePack, as in `query_last_values` above — an absent series is
    // encoded as a null (decoding to `None`), which is a different fact from a
    // payload that cannot be read at all.
    let entry: Option<(i64, f64)> = crate::data::executor::response_codec::decode_payload(&payload)
        .map_err(|e| DdlError::from_error_in_context("LAST_VALUE reply", &e))?;

    let mut rows = Vec::new();
    if let Some((ts, value)) = entry {
        let mut row = Map::new();
        row.insert(
            "timestamp_ms".to_string(),
            JsonValue::String(ts.to_string()),
        );
        row.insert(
            "value".to_string(),
            JsonValue::String(format!("{value:.6}")),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        vec!["timestamp_ms".to_string(), "value".to_string()],
        vec![DdlColType::Int8, DdlColType::Text],
        rows,
    ))])
}
