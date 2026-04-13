//! Dispatch `DocumentOp::Register` to this node's Data Plane
//! after a collection has been committed.
//!
//! Two entry points:
//! - [`dispatch_register_if_needed`] — leader-side, called from
//!   the pgwire handler path. Parses the FIELDS clause from
//!   `parts` to derive index paths.
//! - [`dispatch_register_from_stored`] — applier-side, called
//!   from the metadata applier's post-apply hook after a
//!   `CatalogEntry::PutCollection` commits. Derives index paths
//!   from `coll.fields`.
//!
//! Both funnel into [`dispatch_register_from_stored_inner`]
//! which builds the storage-mode + enforcement-options
//! `EnforcementOptions` value and dispatches to the Data Plane.

use crate::control::security::catalog::StoredCollection;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::enforcement::{build_generated_column_specs, find_materialized_sum_bindings};

/// Dispatch a `DocumentOp::Register` to the Data Plane after
/// collection creation (leader-side pgwire path). Looks up the
/// just-created collection from catalog and parses the FIELDS
/// clause from `parts` for index paths.
pub async fn dispatch_register_if_needed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) {
    let name = parts.get(2).map(|s| s.to_lowercase()).unwrap_or_default();
    let tenant_id = identity.tenant_id;

    let Some(catalog) = state.credentials.catalog() else {
        return;
    };
    let Ok(Some(coll)) = catalog.get_collection(tenant_id.as_u32(), &name) else {
        return;
    };
    let (fields, _serial_fields) =
        super::super::super::schema_validation::parse_fields_clause(parts);
    let index_paths: Vec<String> = fields
        .iter()
        .map(|(name, _ty)| format!("$.{name}"))
        .collect();
    let _ = sql; // Reserved for future CRDT detection from SQL.
    dispatch_register_from_stored_inner(state, tenant_id, &coll, index_paths).await;
}

/// Applier-side entry point: dispatch `DocumentOp::Register` using
/// a fully-populated [`StoredCollection`]. Called from the
/// production `MetadataCommitApplier` after it materializes a
/// replicated `CatalogEntry::PutCollection` into local
/// `SystemCatalog` redb, so every follower's Data Plane knows
/// about the collection before the first cross-node INSERT
/// arrives.
pub async fn dispatch_register_from_stored(state: &SharedState, coll: &StoredCollection) {
    let tenant_id = crate::types::TenantId::new(coll.tenant_id);
    let index_paths: Vec<String> = coll
        .fields
        .iter()
        .map(|(name, _ty)| format!("$.{name}"))
        .collect();
    dispatch_register_from_stored_inner(state, tenant_id, coll, index_paths).await;
}

async fn dispatch_register_from_stored_inner(
    state: &SharedState,
    tenant_id: crate::types::TenantId,
    coll: &StoredCollection,
    index_paths: Vec<String>,
) {
    let name = coll.name.clone();
    let Some(catalog) = state.credentials.catalog() else {
        return;
    };

    // Determine storage mode from collection type — exhaustive
    // match ensures new CollectionType variants get a compile
    // error here.
    let storage_mode = match &coll.collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            crate::bridge::physical_plan::StorageMode::Strict {
                schema: schema.clone(),
            }
        }
        nodedb_types::CollectionType::KeyValue(config) => {
            crate::bridge::physical_plan::StorageMode::Strict {
                schema: config.schema.clone(),
            }
        }
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        | nodedb_types::CollectionType::Columnar(_) => {
            crate::bridge::physical_plan::StorageMode::Schemaless
        }
    };

    let crdt_enabled = false;

    let enforcement = crate::bridge::physical_plan::EnforcementOptions {
        append_only: coll.append_only,
        hash_chain: coll.hash_chain,
        balanced: coll
            .balanced
            .as_ref()
            .map(|b| crate::bridge::physical_plan::BalancedDef {
                group_key_column: b.group_key_column.clone(),
                entry_type_column: b.entry_type_column.clone(),
                debit_value: b.debit_value.clone(),
                credit_value: b.credit_value.clone(),
                amount_column: b.amount_column.clone(),
            }),
        period_lock: coll.period_lock.as_ref().map(|pl| {
            crate::bridge::physical_plan::PeriodLockConfig {
                period_column: pl.period_column.clone(),
                ref_table: pl.ref_table.clone(),
                ref_pk: pl.ref_pk.clone(),
                status_column: pl.status_column.clone(),
                allowed_statuses: pl.allowed_statuses.clone(),
            }
        }),
        retention: coll.retention_period.as_ref().and_then(|s| {
            crate::data::executor::enforcement::retention::parse_retention_period(s).ok()
        }),
        has_legal_hold: !coll.legal_holds.is_empty(),
        state_constraints: coll.state_constraints.clone(),
        transition_checks: coll.transition_checks.clone(),
        materialized_sum_sources: find_materialized_sum_bindings(
            catalog,
            tenant_id.as_u32(),
            &name,
        ),
        generated_columns: build_generated_column_specs(coll),
    };

    let vshard = crate::types::VShardId::from_collection(&name);
    let plan = crate::bridge::envelope::PhysicalPlan::Document(
        crate::bridge::physical_plan::DocumentOp::Register {
            collection: name.clone(),
            index_paths,
            crdt_enabled,
            storage_mode,
            enforcement: Box::new(enforcement),
        },
    );

    if let Err(e) = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state, tenant_id, vshard, plan, 0,
    )
    .await
    {
        tracing::warn!(
            %name,
            error = %e,
            "failed to dispatch Register to Data Plane (non-fatal)"
        );
    }
}
