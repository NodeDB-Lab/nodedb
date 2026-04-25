//! CRDT operations exposed as SQL-like functions.
//!
//! - `SELECT crdt_state('collection', 'doc_id')` → read CRDT snapshot
//! - `SELECT crdt_apply('collection', 'doc_id', 'delta_hex')` → apply CRDT delta

use std::sync::Arc;
use std::time::Duration;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::CrdtOp;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::types::{hex_decode, sqlstate_error, text_field};

/// Parse function arguments from SQL like `SELECT func('arg1', 'arg2')`.
fn parse_function_args(sql: &str) -> Vec<String> {
    // Find content between first '(' and last ')'.
    let start = match sql.find('(') {
        Some(i) => i + 1,
        None => return Vec::new(),
    };
    let end = match sql.rfind(')') {
        Some(i) => i,
        None => return Vec::new(),
    };
    if start >= end {
        return Vec::new();
    }

    let args_str = &sql[start..end];
    args_str
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .collect()
}

/// `SELECT crdt_state('collection', 'doc_id')`
///
/// Returns the CRDT document snapshot as a text result.
pub async fn crdt_state(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_function_args(sql);
    if args.len() < 2 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SELECT crdt_state('collection', 'doc_id')",
        ));
    }

    let collection = &args[0];
    let document_id = &args[1];
    let tenant_id = identity.tenant_id;

    let plan = PhysicalPlan::Crdt(CrdtOp::Read {
        collection: collection.clone(),
        document_id: document_id.clone(),
    });

    // Synchronous dispatch via the blocking bridge.
    let result = super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        collection,
        plan,
        Duration::from_secs(state.tuning.network.default_deadline_secs),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let schema = Arc::new(vec![text_field("crdt_state")]);
    let mut encoder = DataRowEncoder::new(schema.clone());

    if result.is_empty() {
        return Ok(vec![Response::Query(QueryResponse::new(
            schema,
            stream::empty(),
        ))]);
    }

    let text = String::from_utf8_lossy(&result).into_owned();
    encoder
        .encode_field(&text)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}

/// `SELECT crdt_apply('collection', 'doc_id', 'delta_hex')`
///
/// Applies a CRDT delta and returns the result.
pub async fn crdt_apply(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = parse_function_args(sql);
    if args.len() < 3 {
        return Err(sqlstate_error(
            "42601",
            "syntax: SELECT crdt_apply('collection', 'doc_id', 'delta_hex_or_base64')",
        ));
    }

    let collection = &args[0];
    let document_id = &args[1];
    let delta_str = &args[2];

    // Try hex decode first, then treat as raw bytes.
    let delta = hex_decode(delta_str).unwrap_or_else(|| delta_str.as_bytes().to_vec());

    let tenant_id = identity.tenant_id;

    let surrogate = state
        .surrogate_assigner
        .assign(collection, document_id.as_bytes())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: collection.clone(),
        document_id: document_id.clone(),
        delta,
        peer_id: identity.user_id,
        mutation_id: 0,
        surrogate,
    });

    super::sync_dispatch::dispatch_async(
        state,
        tenant_id,
        collection,
        plan,
        Duration::from_secs(state.tuning.network.default_deadline_secs),
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;

    let schema = Arc::new(vec![text_field("result")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&"OK")
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    let row = encoder.take_row();

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(row)]),
    ))])
}
