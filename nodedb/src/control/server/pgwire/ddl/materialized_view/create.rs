//! `CREATE MATERIALIZED VIEW` handler — replicates through the
//! metadata raft group via `CatalogEntry::PutMaterializedView`.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::catalog::{StoredCollection, StoredMaterializedView};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::sqlstate_error;
use super::parse::parse_create_mv;

/// `CREATE MATERIALIZED VIEW <name> ON <source> AS SELECT ... [WITH (...)]`
pub async fn create_materialized_view(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let (name, source, query_sql, refresh_mode) = parse_create_mv(sql)?;

    let tenant_id = identity.tenant_id;

    // Validate source collection exists.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &source) {
            Ok(Some(_)) => {}
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("source collection '{source}' does not exist"),
                ));
            }
        }

        if let Ok(Some(_)) = catalog.get_materialized_view(tenant_id.as_u32(), &name) {
            return Err(sqlstate_error(
                "42P07",
                &format!("materialized view '{name}' already exists"),
            ));
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let view = StoredMaterializedView {
        tenant_id: tenant_id.as_u32(),
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
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
    if log_index == 0
        && let Some(catalog) = state.credentials.catalog()
    {
        catalog
            .put_materialized_view(&view)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    // Create the view's target collection so REFRESH can insert into it
    // and clients can SELECT from it like any other collection. Skipped
    // when a collection of the same name already exists (idempotent).
    let target_exists = match state.credentials.catalog() {
        Some(catalog) => matches!(
            catalog.get_collection(tenant_id.as_u32(), &name),
            Ok(Some(c)) if c.is_active
        ),
        None => false,
    };
    if !target_exists {
        let target = StoredCollection {
            tenant_id: tenant_id.as_u32(),
            name: name.clone(),
            owner: identity.username.clone(),
            created_at: now,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
            fields: Vec::new(),
            field_defs: Vec::new(),
            event_defs: Vec::new(),
            collection_type: nodedb_types::CollectionType::document(),
            timeseries_config: None,
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
            permission_tree_def: None,
            indexes: Vec::new(),
        };
        let coll_entry =
            crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(target.clone()));
        let coll_log_index =
            crate::control::metadata_proposer::propose_catalog_entry(state, &coll_entry)
                .map_err(|e| sqlstate_error("XX000", &format!("metadata propose: {e}")))?;
        if coll_log_index == 0
            && let Some(catalog) = state.credentials.catalog()
        {
            catalog
                .put_collection(&target)
                .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        }
        // Register the target with this node's Data Plane so writes
        // encode correctly and scans can find the collection.
        super::super::collection::dispatch_register_from_stored(state, &target).await;
    }

    tracing::info!(
        view = name,
        source,
        tenant = tenant_id.as_u32(),
        "materialized view created"
    );

    Ok(vec![Response::Execution(Tag::new(
        "CREATE MATERIALIZED VIEW",
    ))])
}
