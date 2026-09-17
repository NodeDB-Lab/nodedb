// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral CRDT operations exposed as SQL-like functions.
//!
//! - `SELECT crdt_state('collection', 'doc_id')` → read CRDT snapshot
//! - `SELECT crdt_apply('collection', 'doc_id', 'delta_hex')` → apply CRDT delta
//!
//! Handlers build [`DdlResult`](super::super::result::DdlResult) directly and
//! carry no pgwire types.

use std::time::Duration;

use serde_json::{Map, Value as JsonValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::crdt_post_image_policy::ExternalCrdtPostImagePolicy;
use crate::control::planner::sql_plan_convert::convert::db_qualified;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::server::shared::authorization::{authorize_collection, authorize_task_set};
use crate::control::server::shared::ddl::sql_parse::hex_decode;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::result::{DdlError, DdlResult};

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
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql);
    if args.len() < 2 {
        return Err(DdlError::new(
            "42601",
            "syntax: SELECT crdt_state('collection', 'doc_id')",
        ));
    }

    let collection = &args[0];
    let document_id = &args[1];

    let plan = PhysicalPlan::Crdt(CrdtOp::Read {
        collection: nodedb_types::QualifiedCollection::new(database_id, collection),
        document_id: document_id.clone(),
    });

    // Synchronous dispatch via the blocking bridge.
    // A CRDT state read is a read of the collection's rows, so it carries the
    // same authorization and row-level security as any other read of them.
    // This handler is only ever reached through `shared::ddl::dispatch`,
    // which native/pgwire/HTTP each call after their own single per-request
    // admission gate — so this request was already admitted once.
    let result = crate::control::server::shared::ddl::user_dispatch::dispatch_for_identity(
        crate::control::server::shared::ddl::user_dispatch::DispatchRequest {
            state,
            identity,
            database_id,
            collection,
            plan,
            timeout: Duration::from_secs(state.tuning.network.default_deadline_secs),
            // This handler receives no session or peer information; the
            // request was admitted at its own transport entry, and
            // `AlreadyAdmitted` carries no address precisely so there is
            // nothing to invent here.
            admission: crate::control::server::shared::ddl::user_dispatch::RequestAdmission::AlreadyAdmitted,
        },
    )
    .await
    .map_err(|e| DdlError::new("XX000", e.to_string()))?;

    let columns = vec!["crdt_state".to_string()];

    if result.is_empty() {
        return Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
            columns,
            Vec::new(),
        ))]);
    }

    let text = String::from_utf8_lossy(&result).into_owned();
    let mut row = Map::new();
    row.insert("crdt_state".to_string(), JsonValue::String(text));

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        columns,
        vec![row],
    ))])
}

/// `SELECT crdt_apply('collection', 'doc_id', 'delta_hex')`
///
/// Applies a CRDT delta and returns the result.
pub async fn crdt_apply(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let args = parse_function_args(sql);
    if args.len() < 3 {
        return Err(DdlError::new(
            "42601",
            "syntax: SELECT crdt_apply('collection', 'doc_id', 'delta_hex_or_base64')",
        ));
    }

    let collection = &args[0];
    let document_id = &args[1];
    let delta_str = &args[2];
    if delta_str.len() > nodedb_crdt::DEFAULT_MAX_DELTA_BYTES.saturating_mul(2) {
        return Err(DdlError::new("54000", "CRDT delta exceeds maximum size"));
    }

    // Try hex decode first, then treat as raw bytes.
    let delta = hex_decode(delta_str).unwrap_or_else(|| delta_str.as_bytes().to_vec());
    if delta.len() > nodedb_crdt::DEFAULT_MAX_DELTA_BYTES {
        return Err(DdlError::new("54000", "CRDT delta exceeds maximum size"));
    }

    let tenant_id = identity.tenant_id;
    let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        collection,
        Permission::Write,
        &state.permissions,
        &state.roles,
        &audit,
    )
    .map_err(|error| DdlError::new("42501", format!("permission denied: {}", error.resource())))?;

    let surrogate = state
        .surrogate_assigner
        .assign(database_id, tenant_id, collection, document_id.as_bytes())
        .map_err(|e| DdlError::new("XX000", e.to_string()))?;

    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: nodedb_types::QualifiedCollection::new(database_id, collection),
        document_id: document_id.clone(),
        delta,
        peer_id: identity.user_id,
        mutation_id: 0,
        surrogate,
        provenance: None,
        // Local pgwire write, not a replicated peer sync — no constraint fence.
        constraint_version_required: 0,
        expected_frontier_digest: None,
    });
    let task = PhysicalTask {
        tenant_id,
        vshard_id: crate::types::VShardId::from_collection_in_database(database_id, collection),
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let authorized = authorize_task_set(
        identity,
        std::slice::from_ref(&task),
        &state.permissions,
        &state.roles,
        &audit,
    )
    .map_err(|error| DdlError::new("42501", format!("permission denied: {}", error.resource())))?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| DdlError::new("XX000", "authorization returned no capability"))?;

    // Route through the Raft proposer gate so the delta is quorum-durable under
    // replication. A local-only dispatch would land the delta on the receiving
    // node only — it would be lost to every follower and entirely on failover.
    //
    // RLS write policies are stored keyed by `db_qualified(database_id,
    // collection)`, so the policy must be handed that same key or it silently
    // misses a policy on a non-default database.
    let qualified_collection = db_qualified(database_id, collection);
    let policy = ExternalCrdtPostImagePolicy::from_identity(
        tenant_id,
        database_id,
        &qualified_collection,
        identity,
        "sql".into(),
        &state.rls,
        &audit,
    );
    crate::control::crdt_admission::dispatch_authorized_crdt_apply_admitted(
        state,
        crate::control::crdt_admission::AuthorizedCrdtApplyAdmissionRequest {
            authorized,
            collection,
            timeout: Duration::from_secs(state.tuning.network.default_deadline_secs),
            event_source: crate::event::EventSource::User,
            policy: &policy,
        },
    )
    .await
    .map_err(|e| DdlError::new("XX000", e.to_string()))?;

    let columns = vec!["result".to_string()];
    let mut row = Map::new();
    row.insert("result".to_string(), JsonValue::String("OK".to_string()));

    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        columns,
        vec![row],
    ))])
}
