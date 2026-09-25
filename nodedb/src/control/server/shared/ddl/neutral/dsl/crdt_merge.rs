// SPDX-License-Identifier: BUSL-1.1

//! `CRDT MERGE INTO` DSL handler.

use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::crdt_post_image_policy::ExternalCrdtPostImagePolicy;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::pgwire::types::error_map::error_to_sqlstate;
use crate::control::server::shared::authorization::{authorize_collection, authorize_task_set};
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::CrdtOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// CRDT MERGE INTO <collection> FROM '<source_id>' TO '<target_id>'
pub async fn crdt_merge(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 7 {
        return Err(ddl_err(
            "42601",
            "syntax: CRDT MERGE INTO <collection> FROM '<source_id>' TO '<target_id>'",
        ));
    }

    let collection = parts[3];
    let tenant_id = identity.tenant_id;

    let from_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("FROM"))
        .ok_or_else(|| ddl_err("42601", "expected FROM keyword"))?;
    let to_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("TO"))
        .ok_or_else(|| ddl_err("42601", "expected TO keyword"))?;

    let source_id = parts
        .get(from_idx + 1)
        .map(|s| s.trim_matches('\'').trim_matches('"'))
        .ok_or_else(|| ddl_err("42601", "missing source document ID"))?;
    let target_id = parts
        .get(to_idx + 1)
        .map(|s| s.trim_matches('\'').trim_matches('"'))
        .ok_or_else(|| ddl_err("42601", "missing target document ID"))?;

    let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    for permission in [Permission::Read, Permission::Write] {
        authorize_collection(
            identity,
            database_id,
            collection,
            permission,
            &state.permissions,
            &state.roles,
            &audit,
        )
        .map_err(|error| ddl_err("42501", format!("permission denied: {}", error.resource())))?;
    }

    let source_plan = PhysicalPlan::Crdt(CrdtOp::Read {
        collection: nodedb_types::QualifiedCollection::new(database_id, collection),
        document_id: source_id.to_string(),
    });

    // Only reached through `shared::ddl::dispatch`, which native/pgwire/HTTP
    // each call after their own single per-request admission gate.
    let source_bytes = crate::control::server::shared::ddl::user_dispatch::dispatch_for_identity(
        crate::control::server::shared::ddl::user_dispatch::DispatchRequest {
            state,
            identity,
            database_id,
            collection,
            plan: source_plan,
            timeout: Duration::from_secs(state.tuning.network.default_deadline_secs),
            // Admitted at this request's own transport entry; this handler
            // has no session or peer information of its own.
            admission: crate::control::server::shared::ddl::user_dispatch::RequestAdmission::AlreadyAdmitted,
        },
    )
    .await
    .map_err(|e| {
        let (_, sqlstate, message) = error_to_sqlstate(&e);
        ddl_err(sqlstate, message)
    })?;
    if source_bytes.is_empty() {
        return Err(ddl_err(
            "02000",
            format!("source document '{source_id}' not found"),
        ));
    }

    let target_surrogate = state
        .surrogate_assigner
        .assign(database_id, tenant_id, collection, target_id.as_bytes())
        .map_err(|e| {
            let (_, sqlstate, message) = error_to_sqlstate(&e);
            ddl_err(sqlstate, message)
        })?;

    let apply_plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: nodedb_types::QualifiedCollection::new(database_id, collection),
        document_id: target_id.to_string(),
        delta: source_bytes,
        peer_id: identity.user_id,
        mutation_id: 0,
        surrogate: target_surrogate,
        provenance: None,
        // Server-side merge result, not a replicated peer sync — no fence.
        constraint_version_required: 0,
        expected_frontier_digest: None,
    });
    let task = PhysicalTask {
        tenant_id,
        vshard_id: crate::types::VShardId::from_collection_in_database(database_id, collection),
        database_id,
        plan: apply_plan,
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
    .map_err(|error| ddl_err("42501", format!("permission denied: {}", error.resource())))?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| ddl_err("XX000", "authorization returned no capability"))?;

    // Route the merge result through the Raft proposer gate so the applied delta
    // is quorum-durable under replication, not lost to followers on failover.
    //
    // RLS write policies are stored keyed by `db_qualified(database_id,
    // collection)`, so the policy is handed that same key or it silently
    // misses a policy on a non-default database. `collection` itself stays
    // bare: it also feeds vShard routing and the admission request's equality
    // check against this same (unqualified) plan.
    let qualified_collection =
        crate::control::planner::sql_plan_convert::convert::db_qualified(database_id, collection);
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
    .map_err(|e| {
        let (_, sqlstate, message) = error_to_sqlstate(&e);
        ddl_err(sqlstate, message)
    })?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("CRDT merge: {source_id} → {target_id} in '{collection}'"),
    );

    Ok(vec![DdlResult::Status {
        command: "CRDT MERGE".to_string(),
        rows_affected: None,
    }])
}
