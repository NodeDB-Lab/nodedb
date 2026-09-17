// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for strict-schema-altering DDL.
//!
//! `ALTER COLUMN TYPE`, `DROP COLUMN`, and `RENAME COLUMN` all open
//! with the same prelude — look up the catalog, fetch the active
//! strict collection, deserialize its `StrictSchema` blob — and close
//! with the same coda — package the mutated `StoredCollection` into a
//! `PutCollection` entry, replicate it through the metadata raft group,
//! refresh the Data Plane register, and bump the schema version.
//!
//! Ported verbatim from the pgwire
//! `ddl::collection::alter::strict_schema` module; only the result type
//! changed from pgwire `PgWireResult` / `sqlstate_error` to the
//! protocol-neutral [`DdlError`]. The catalog lookup, engine gate, schema
//! (de)serialization, propose + register + version-bump ordering, and the
//! SQLSTATE codes / messages are unchanged.

use nodedb_types::DatabaseId;

use crate::control::security::catalog::StoredCollection;
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;

use super::support::{err, load_active_collection};

/// Look up the active strict collection `name` for `tenant_id` and
/// return it together with its deserialized `StrictSchema`. Returns
/// the appropriate error if the catalog is missing, the collection is
/// absent / inactive, the engine is not strict, or the embedded
/// `timeseries_config` JSON fails to parse.
pub(super) fn load_strict_collection(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
    operation: &str,
) -> Result<(StoredCollection, nodedb_types::columnar::StrictSchema), DdlError> {
    let coll = load_active_collection(state, database_id, tenant_id, name)?;

    if !coll.collection_type.is_strict() {
        return Err(err(
            "0A000",
            format!("{operation} is only supported on strict document collections"),
        ));
    }

    let schema: nodedb_types::columnar::StrictSchema = coll
        .timeseries_config
        .as_deref()
        .and_then(|s| sonic_rs::from_str(s).ok())
        .ok_or_else(|| err("XX000", "strict schema missing or malformed"))?;

    Ok((coll, schema))
}

/// Re-serialize `schema` into `coll.timeseries_config` and set
/// `coll.collection_type` to the matching `Strict(...)` variant.
pub(super) fn write_schema_back(
    coll: &mut StoredCollection,
    schema: nodedb_types::columnar::StrictSchema,
) {
    coll.collection_type = nodedb_types::CollectionType::strict(schema.clone());
    coll.timeseries_config = sonic_rs::to_string(&schema).ok();
}

/// Retype a column's entry in `coll.fields`, the catalog's record of the
/// *declared* type string each column was created with.
///
/// `fields` is not redundant with the strict schema: `ColumnType` collapses
/// every integer width onto one `Int64` variant, so the declared spelling is
/// the only surviving record of how wide the author said the column was. That
/// spelling drives the column's advertised wire OID and the range accepted on
/// write, which is exactly why `ALTER COLUMN TYPE` — whose only supported use
/// *is* an alias change such as `INT` → `BIGINT` — has to update it. Leaving
/// it stale would make the alter a silent no-op for the case it exists to
/// serve, and would keep rejecting writes the new type allows.
pub(super) fn retype_field(coll: &mut StoredCollection, column: &str, new_type: &str) {
    for (name, type_str) in coll.fields.iter_mut() {
        if name.eq_ignore_ascii_case(column) {
            *type_str = new_type.to_string();
        }
    }
}

/// Rename a column's entry in `coll.fields`, keeping its declared type.
pub(super) fn rename_field(coll: &mut StoredCollection, old_name: &str, new_name: &str) {
    for (name, _) in coll.fields.iter_mut() {
        if name.eq_ignore_ascii_case(old_name) {
            *name = new_name.to_string();
        }
    }
}

/// Remove a column's entry from `coll.fields`.
pub(super) fn remove_field(coll: &mut StoredCollection, column: &str) {
    coll.fields
        .retain(|(name, _)| !name.eq_ignore_ascii_case(column));
}

/// Append a column's declared type to `coll.fields`, replacing any existing
/// entry of the same name.
pub(super) fn add_field(coll: &mut StoredCollection, column: &str, declared_type: &str) {
    remove_field(coll, column);
    coll.fields
        .push((column.to_string(), declared_type.to_string()));
}

/// Replicate the mutated collection through the metadata raft group,
/// refresh this node's Data Plane register so the in-memory shape
/// catches up with the new schema, recompile the collection's RLS
/// policies against it, then bump `schema_version`.
pub(super) async fn persist_schema_change(
    state: &SharedState,
    updated: &StoredCollection,
) -> Result<(), DdlError> {
    let entry =
        crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(updated.clone()));
    super::support::propose_and_apply(state, &entry)?;

    super::super::register::dispatch_register_from_stored(state, updated)
        .await
        .map_err(|e| err("XX000", e.to_string()))?;
    recompile_rls_policies(state, updated)?;
    state.schema_version.bump();
    Ok(())
}

/// Recompile the RLS policies on `updated` against its new declared columns.
///
/// A policy literal is typed against the column it compares with, so a
/// column the statement added, dropped, or renamed changes what the policy
/// compiles to. The Raft post-apply recompiles on every node in cluster mode;
/// this call covers the single-node path, where no post-apply runs.
pub(super) fn recompile_rls_policies(
    state: &SharedState,
    updated: &StoredCollection,
) -> Result<(), DdlError> {
    state
        .rls
        .recompile_for_collection(
            state.credentials.catalog(),
            updated.database_id,
            updated.tenant_id,
            &updated.name,
        )
        .map_err(|e| err("XX000", format!("rls recompile: {e}")))
}
