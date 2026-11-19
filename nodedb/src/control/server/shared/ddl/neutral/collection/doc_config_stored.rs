// SPDX-License-Identifier: BUSL-1.1

//! The wire form of a document config, derived from the durable catalog.
//!
//! The config builder and the proposer's config pins both derive the wire
//! form here, so a pin's digest equals the digest the core stores.

use nodedb_physical::physical_plan::{
    BalancedDef, EnforcementOptions, PeriodLockConfig, RegisteredIndex, StorageMode,
};

use crate::bootstrap::constraint_reconcile::CollectionSource;
use crate::control::security::catalog::StoredCollection;

use super::DocConfigWire;
use super::enforcement::{build_generated_column_specs, find_materialized_sum_bindings};

/// The storage mode `coll` registers with.
fn storage_mode(coll: &StoredCollection) -> StorageMode {
    // Exhaustive, so a new collection type is a compile error here.
    match &coll.collection_type {
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Strict(schema)) => {
            StorageMode::Strict {
                schema: schema.clone(),
            }
        }
        nodedb_types::CollectionType::KeyValue(config) => StorageMode::Strict {
            schema: config.schema.clone(),
        },
        nodedb_types::CollectionType::Document(nodedb_types::DocumentMode::Schemaless)
        | nodedb_types::CollectionType::Columnar(_) => StorageMode::Schemaless,
    }
}

/// The enforcement options `coll` registers with. `name` is its qualified
/// name, which the materialized-sum bindings name as their source.
fn enforcement<S: CollectionSource + ?Sized>(
    catalog: &S,
    tenant_id: crate::types::TenantId,
    coll: &StoredCollection,
    name: &str,
) -> EnforcementOptions {
    EnforcementOptions {
        append_only: coll.append_only,
        hash_chain: coll.hash_chain,
        balanced: coll.balanced.as_ref().map(|b| BalancedDef {
            group_key_column: b.group_key_column.clone(),
            entry_type_column: b.entry_type_column.clone(),
            debit_value: b.debit_value.clone(),
            credit_value: b.credit_value.clone(),
            amount_column: b.amount_column.clone(),
        }),
        period_lock: coll.period_lock.as_ref().map(|pl| PeriodLockConfig {
            period_column: pl.period_column.clone(),
            ref_table: pl.ref_table.clone(),
            ref_pk: pl.ref_pk.clone(),
            status_column: pl.status_column.clone(),
            allowed_statuses: pl.allowed_statuses.clone(),
        }),
        retention: coll.retention_period.as_ref().and_then(|s| {
            crate::data::executor::enforcement::retention::parse_retention_period(s).ok()
        }),
        has_legal_hold: !coll.legal_holds.is_empty(),
        state_constraints: coll.state_constraints.clone(),
        transition_checks: coll.transition_checks.clone(),
        materialized_sum_sources: find_materialized_sum_bindings(
            catalog,
            tenant_id.as_u64(),
            name,
            coll.database_id,
        ),
        generated_columns: build_generated_column_specs(coll),
        // Typeguards exist on schemaless collections only.
        type_guards: if coll.collection_type.is_schemaless() {
            coll.type_guards.clone()
        } else {
            Vec::new()
        },
        storage_conversion: coll.storage_conversion.clone(),
    }
}

impl DocConfigWire {
    /// The wire form of `coll` with `indexes`, as `catalog` holds it.
    ///
    /// `catalog` answers the materialized-sum bindings other collections
    /// declare with `coll` as their source.
    pub(crate) fn from_stored<S: CollectionSource + ?Sized>(
        catalog: &S,
        tenant_id: crate::types::TenantId,
        coll: &StoredCollection,
        indexes: &[RegisteredIndex],
    ) -> Self {
        let name = crate::control::planner::sql_plan_convert::convert::db_qualified(
            coll.database_id,
            &coll.name,
        );
        Self {
            bitemporal: coll.bitemporal,
            enforcement: enforcement(catalog, tenant_id, coll, &name),
            storage_mode: storage_mode(coll),
            index_paths: indexes.to_vec(),
            declared_key: crate::control::planner::catalog_adapter::document_declared_key(coll),
            vector_primary: coll.vector_primary.clone(),
        }
    }
}
