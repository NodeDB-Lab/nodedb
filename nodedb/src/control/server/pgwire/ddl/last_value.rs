//! `LAST_VALUE` and `LAST_VALUES` query handlers.
//!
//! Syntax:
//! ```sql
//! SELECT LAST_VALUES('<collection>')
//! SELECT LAST_VALUE('<collection>', <series_id>)
//! ```
//!
//! These dispatch `MetaOp::QueryLastValues` / `QueryLastValue` to the Data Plane
//! and return results as pgwire rows.

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::MetaOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{int8_field, sqlstate_error, text_field};

/// `SELECT LAST_VALUES('<collection>')` — returns all cached last values.
pub async fn query_last_values(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;
    let plan = PhysicalPlan::Meta(MetaOp::QueryLastValues {
        collection: collection.to_string(),
    });

    let payload = crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        collection,
        plan,
        Duration::from_secs(5),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("dispatch failed: {e}")))?;

    let entries: Vec<(u64, i64, f64)> = sonic_rs::from_slice(&payload).unwrap_or_default();

    let schema = Arc::new(vec![
        int8_field("series_id"),
        int8_field("timestamp_ms"),
        text_field("value"),
    ]);

    let mut rows = Vec::new();
    for (series_id, ts, value) in &entries {
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&(*series_id as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(ts)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&format!("{value:.6}"))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// `SELECT LAST_VALUE('<collection>', <series_id>)` — returns single series value.
pub async fn query_last_value(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    collection: &str,
    series_id: u64,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id;
    let plan = PhysicalPlan::Meta(MetaOp::QueryLastValue {
        collection: collection.to_string(),
        series_id,
    });

    let payload = crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        collection,
        plan,
        Duration::from_secs(5),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &format!("dispatch failed: {e}")))?;

    let entry: Option<(i64, f64)> = sonic_rs::from_slice(&payload).unwrap_or_default();

    let schema = Arc::new(vec![int8_field("timestamp_ms"), text_field("value")]);

    let mut rows = Vec::new();
    if let Some((ts, value)) = entry {
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&ts)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&format!("{value:.6}"))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}
