// SPDX-License-Identifier: BUSL-1.1

//! Compile a policy's `USING` text against its collection's declared columns.
//!
//! One entry point, [`compile_policy_predicate`], serves `CREATE RLS POLICY`
//! and every load of a stored policy (boot replay, Raft apply, recovery
//! reload, schema change), so a policy is typed against the collection's
//! current declared columns wherever it is materialized. The compiled
//! predicate is never persisted: the catalog keeps the text, and every
//! reader recompiles.

use nodedb_sql::types::ColumnInfo;
use nodedb_types::DatabaseId;

use crate::control::planner::catalog_adapter::convert_collection_type;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::predicate::RlsPredicate;
use crate::control::security::predicate_parser::{parse_predicate, validate_auth_refs};
use crate::control::security::predicate_typing::type_predicate_literals;

/// Parse, validate, and type `text` against `columns`.
///
/// Errors: a parse error, an unknown `$auth.*` reference, or a literal the
/// declared column type cannot read. Each names what was refused.
pub fn compile_policy_predicate(text: &str, columns: &[ColumnInfo]) -> crate::Result<RlsPredicate> {
    let parsed = parse_predicate(text).map_err(|error| crate::Error::BadRequest {
        detail: format!("predicate parse error: {error}"),
    })?;
    validate_auth_refs(&parsed)?;
    type_predicate_literals(parsed, columns)
}

/// The declared columns of the active collection `collection` in
/// `database_id` / `tenant_id`, as the planner sees them.
///
/// Errors with `CollectionNotFound` when the collection is absent or
/// dropped: a policy cannot be typed without its schema.
pub fn declared_columns(
    catalog: &SystemCatalog,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
) -> crate::Result<Vec<ColumnInfo>> {
    let stored = catalog
        .get_collection(database_id, tenant_id, collection)?
        .filter(|stored| stored.is_active)
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id: crate::types::TenantId::new(tenant_id),
            collection: collection.to_string(),
        })?;
    let (_, columns, _) = convert_collection_type(&stored);
    Ok(columns)
}

impl super::store::RlsPolicyStore {
    /// Recompile every stored policy on `collection` against its current
    /// declared columns and reinstall it.
    ///
    /// For a schema change: a policy literal typed against a column that
    /// `ALTER COLLECTION` added, dropped, or renamed must follow the new
    /// schema without waiting for a restart. A row that no longer compiles
    /// installs as a restrictive deny-all (`StoredRlsPolicy::rehydrate`).
    pub fn recompile_for_collection(
        &self,
        catalog: &SystemCatalog,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<()> {
        let qualified = nodedb_types::QualifiedCollection::new(database_id, collection);
        for stored in catalog.list_rls_policies_for_collection(tenant_id, qualified.as_str())? {
            self.install_replicated_policy(stored.rehydrate(catalog));
        }
        Ok(())
    }
}
