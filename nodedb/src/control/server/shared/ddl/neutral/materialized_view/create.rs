// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE MATERIALIZED VIEW` handler — replicates through the
//! metadata raft group via `CatalogEntry::PutMaterializedView`.
//!
//! The catalog path (`propose_and_apply` for the view definition, then
//! `propose_and_apply` for the target collection, then
//! `register_proposed_collection`), the duplicate / source-existence checks,
//! and the target-collection descriptor run here. The result is the
//! protocol-neutral [`DdlResult`] / [`DdlError`].

use nodedb_types::DatabaseId;

use crate::control::security::catalog::{StoredCollection, StoredMaterializedView};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::catalog::propose_and_apply_async;
use super::super::super::result::{DdlError, DdlResult};

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// `CREATE MATERIALIZED VIEW <name> ON <source> AS SELECT ... [WITH (...)]`
pub async fn create_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    source: &str,
    query_sql: &str,
    refresh_mode: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let name = name.to_string();
    let source = source.to_string();
    let query_sql = query_sql.to_string();
    let refresh_mode = refresh_mode.to_string();

    let tenant_id = identity.tenant_id;
    crate::control::relation_name::check_relation_name("materialized view", &name)?;

    // Streaming MVs are Event-Plane objects: they source from a change stream
    // (named in the query's FROM clause) and maintain per-group partial
    // aggregates in `mv_registry`. They deliberately skip the periodic path
    // below — no `PutMaterializedView` proposal, no target collection, and no
    // `ON` source-collection existence check (those are periodic-only).
    if refresh_mode.eq_ignore_ascii_case("STREAMING") {
        return create_streaming_mv(state, identity, database_id, &name, &query_sql).await;
    }

    // Validate source collection exists.
    {
        let catalog = state.credentials.catalog();
        match catalog.get_collection(database_id, tenant_id.as_u64(), &source) {
            Ok(Some(_)) => {}
            _ => {
                return Err(err(
                    "42P01",
                    format!("source collection '{source}' does not exist"),
                ));
            }
        }

        // A catalog-read fault must abort the CREATE: proceeding can adopt a
        // same-name target over an object whose existence check transiently
        // failed.
        if catalog
            .get_materialized_view(database_id.as_u64(), tenant_id.as_u64(), &name)
            .map_err(|error| DdlError::from_error(&error))?
            .is_some()
        {
            return Err(err(
                "42P07",
                format!("materialized view '{name}' already exists"),
            ));
        }
        if catalog
            .get_collection(database_id, tenant_id.as_u64(), &name)
            .map_err(|error| DdlError::from_error(&error))?
            .is_some()
        {
            return Err(err("42P07", format!("collection '{name}' already exists")));
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let view = StoredMaterializedView {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
        source: source.clone(),
        query_sql,
        refresh_mode,
        owner: identity.username.clone(),
        created_at: now,
        // Stamped by the metadata applier at commit time.
        descriptor_version: 0,
        modification_hlc: nodedb_types::Hlc::ZERO,
    };

    // Propose through the metadata raft group. Every node's
    // applier writes the definition to local `SystemCatalog` redb
    // so the view is visible cluster-wide. The refresh loop picks
    // it up on its next tick.
    let entry =
        crate::control::catalog_entry::CatalogEntry::PutMaterializedView(Box::new(view.clone()));
    propose_and_apply_async(state, &entry).await?;

    // Create the implementation-owned target collection so REFRESH can insert
    // into it and clients can SELECT from it. The pre-check rejects every
    // same-name collection; DROP can therefore purge this target without ever
    // deleting a user-owned collection.
    let target = StoredCollection {
        tenant_id: tenant_id.as_u64(),
        name: name.clone(),
        owner: identity.username.clone(),
        created_at: now,
        deactivated_at_ns: 0,
        storage_conversion: None,
        descriptor_version: 0,
        constraint_version: 0,
        crdt_signing_required: false,
        modification_hlc: nodedb_types::Hlc::ZERO,
        incarnation: nodedb_types::Hlc::ZERO,
        fields: Vec::new(),
        field_defs: Vec::new(),
        event_defs: Vec::new(),
        collection_type: nodedb_types::CollectionType::document(),
        timeseries_config: None,
        conflict_policy: None,
        is_active: true,
        append_only: false,
        hash_chain: false,
        balanced: None,
        last_chain_hash: None,
        period_lock: None,
        retention_period: None,
        legal_holds: Vec::new(),
        state_constraints: Vec::new(),
        transition_checks: Vec::new(),
        type_guards: Vec::new(),
        check_constraints: Vec::new(),
        materialized_sums: Vec::new(),
        lvc_enabled: false,
        bitemporal: false,
        crdt: false,
        permission_tree_def: None,
        indexes: Vec::new(),
        size_bytes_estimate: 0,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        database_id,
        cloned_from: None,
        clone_status: nodedb_types::CloneStatus::default(),
        has_implicit_edges: false,
        declared_primary_key: None,
    };
    let coll_entry =
        crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(target.clone()));
    let outcome = propose_and_apply_async(state, &coll_entry).await?;
    super::super::collection::register_proposed_collection(state, outcome, &target)
        .await
        .map_err(|e| DdlError::from_error(&e))?;

    tracing::info!(
        view = name,
        source,
        tenant = tenant_id.as_u64(),
        "materialized view created"
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE MATERIALIZED VIEW".to_string(),
        rows_affected: None,
    }])
}

/// `CREATE MATERIALIZED VIEW <name> [ON <coll>] STREAMING AS SELECT ... FROM <stream> ...`
///
/// The handler runs the tenant-admin gate, source-stream existence check,
/// duplicate guard, catalog persist, in-memory registration, and buffer
/// backfill. The result / error types are the protocol-neutral [`DdlResult`] /
/// [`DdlError`]. The source is the change stream named in the query's FROM
/// clause, not the `ON` lineage collection.
async fn create_streaming_mv(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    query_sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    super::super::auth_support::require_tenant_admin(
        identity,
        "create streaming materialized views",
    )?;

    let parsed = super::streaming_parse::parse_streaming_mv(query_sql)?;
    let tenant_id = identity.tenant_id.as_u64();

    // Verify the source change stream exists.
    let Some(source_def) = state
        .stream_registry
        .get(database_id, tenant_id, &parsed.source_stream)
    else {
        return Err(err(
            "42704",
            format!("change stream '{}' does not exist", parsed.source_stream),
        ));
    };

    // The view's group key and aggregate state are written to storage from the
    // stored columns of each event, where no result-path mask reaches them. A
    // definition over a column some policy protects is refused here, before the
    // definition is proposed, so nothing is ever maintained from it. A wildcard
    // stream names no single source collection, so its columns are matched
    // across the tenant instead of passing unchecked.
    let source_collection = (!source_def.is_wildcard()).then_some(source_def.collection.as_str());
    crate::control::planner::redaction_refusal::refuse_redacted_streaming_mv(
        &state.redaction,
        tenant_id,
        source_collection,
        &parsed.group_by_columns,
        &parsed.aggregates,
    )
    .map_err(|error| err("42501", error.to_string()))?;

    // Reject a duplicate streaming MV.
    if state
        .mv_registry
        .get_def(database_id, tenant_id, name)
        .is_some()
    {
        return Err(err(
            "42710",
            format!("streaming MV '{name}' already exists"),
        ));
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let source_stream = parsed.source_stream.clone();
    let def = crate::event::streaming_mv::types::StreamingMvDef {
        database_id,
        tenant_id,
        name: name.to_string(),
        source_stream: parsed.source_stream,
        group_by_columns: parsed.group_by_columns,
        aggregates: parsed.aggregates,
        filter_expr: parsed.filter_expr,
        owner: identity.username.clone(),
        created_at: now,
    };

    let entry =
        crate::control::catalog_entry::CatalogEntry::PutStreamingMaterializedView(Box::new(def));
    crate::control::metadata_proposer::propose_catalog_entry_async(state, &entry)
        .await
        .map_err(|error| DdlError::from_error_in_context("metadata propose", &error))?;

    // Backfill: replay events already in the source stream's buffer so the MV
    // bootstraps with historical data instead of only future events.
    if let Some(mv_state) = state.mv_registry.get_state(database_id, tenant_id, name)
        && let Some(buffer) = state
            .cdc_router
            .get_buffer(database_id, tenant_id, &source_stream)
    {
        crate::event::streaming_mv::processor::backfill_from_buffer(&mv_state, &buffer);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE MATERIALIZED VIEW {name} STREAMING"),
    );

    tracing::info!(
        view = name,
        stream = source_stream,
        tenant = tenant_id,
        "streaming materialized view created"
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE MATERIALIZED VIEW".to_string(),
        rows_affected: None,
    }])
}
