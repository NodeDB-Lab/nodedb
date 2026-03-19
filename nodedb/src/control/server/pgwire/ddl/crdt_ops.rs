//! CRDT operations exposed as SQL-like functions.
//!
//! - `SELECT crdt_state('collection', 'doc_id')` → read CRDT snapshot
//! - `SELECT crdt_apply('collection', 'doc_id', 'delta_hex')` → apply CRDT delta

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, VShardId};

use super::super::types::{hex_decode, sqlstate_error, text_field};

/// Default deadline for CRDT operations.
const CRDT_DEADLINE: Duration = Duration::from_secs(30);

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
pub fn crdt_state(
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

    let plan = PhysicalPlan::CrdtRead {
        collection: collection.clone(),
        document_id: document_id.clone(),
    };

    // Synchronous dispatch via the blocking bridge.
    let result = dispatch_sync(state, tenant_id, collection, plan)?;

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
pub fn crdt_apply(
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

    let plan = PhysicalPlan::CrdtApply {
        collection: collection.clone(),
        document_id: document_id.clone(),
        delta,
        peer_id: identity.user_id,
    };

    dispatch_sync(state, tenant_id, collection, plan)?;

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

/// Synchronous dispatch helper for DDL context (blocking on oneshot).
fn dispatch_sync(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    collection: &str,
    plan: PhysicalPlan,
) -> PgWireResult<Vec<u8>> {
    let vshard_id = VShardId::from_collection(collection);
    let request_id = RequestId::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );

    let request = Request {
        request_id,
        tenant_id,
        vshard_id,
        plan,
        deadline: Instant::now() + CRDT_DEADLINE,
        priority: Priority::Normal,
        trace_id: 0,
        consistency: ReadConsistency::Strong,
    };

    let rx = state.tracker.register(request_id);

    match state.dispatcher.lock() {
        Ok(mut d) => d
            .dispatch(request)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?,
        Err(p) => p
            .into_inner()
            .dispatch(request)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?,
    };

    // Block on the response (this is in an async context but DDL dispatch
    // uses sync patterns — the response arrives quickly from the Data Plane).
    let resp = rx
        .blocking_recv()
        .map_err(|_| sqlstate_error("XX000", "response channel closed"))?;

    if resp.status != Status::Ok {
        let detail = resp
            .error_code
            .as_ref()
            .map(|c| format!("{c:?}"))
            .unwrap_or_else(|| String::from_utf8_lossy(&resp.payload).into_owned());
        return Err(sqlstate_error("XX000", &detail));
    }

    Ok(resp.payload.to_vec())
}
