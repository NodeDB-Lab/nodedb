//! `ALTER COLLECTION <name> RENAME COLUMN <old> TO <new>` — rename a
//! column in a strict-document collection's schema.
//!
//! Binary-tuple layout is positional, so a rename is pure metadata: no row
//! re-encoding is required. The schema version is bumped so the Data Plane
//! picks up the new name on the next register dispatch.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;

pub async fn alter_collection_rename_column(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let name = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "ALTER COLLECTION requires a name"))?
        .to_lowercase();
    let tenant_id = identity.tenant_id;

    // Expect: ALTER COLLECTION <name> RENAME COLUMN <old> TO <new>.
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))
        .ok_or_else(|| sqlstate_error("42601", "expected RENAME COLUMN <old> TO <new>"))?;
    let old_name = parts
        .get(col_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "missing old column name"))?
        .to_lowercase();
    // TO keyword.
    match parts.get(col_idx + 2) {
        Some(tok) if tok.eq_ignore_ascii_case("TO") => {}
        _ => {
            return Err(sqlstate_error(
                "42601",
                "expected TO between old and new name",
            ));
        }
    }
    let new_name = parts
        .get(col_idx + 3)
        .ok_or_else(|| sqlstate_error("42601", "missing new column name"))?
        .trim_end_matches(';')
        .to_lowercase();

    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    let coll = catalog
        .get_collection(tenant_id.as_u32(), &name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .filter(|c| c.is_active)
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' does not exist")))?;

    if !coll.collection_type.is_strict() {
        return Err(sqlstate_error(
            "0A000",
            "RENAME COLUMN is only supported on strict document collections",
        ));
    }

    let mut schema: nodedb_types::columnar::StrictSchema = coll
        .timeseries_config
        .as_deref()
        .and_then(|s| sonic_rs::from_str(s).ok())
        .ok_or_else(|| sqlstate_error("XX000", "strict schema missing or malformed"))?;

    if schema
        .columns
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(&new_name))
    {
        return Err(sqlstate_error(
            "42P07",
            &format!("column '{new_name}' already exists on '{name}'"),
        ));
    }

    let col = schema
        .columns
        .iter_mut()
        .find(|c| c.name.eq_ignore_ascii_case(&old_name))
        .ok_or_else(|| {
            sqlstate_error(
                "42703",
                &format!("column '{old_name}' does not exist on '{name}'"),
            )
        })?;
    col.name = new_name.clone();
    schema.version = schema.version.saturating_add(1);

    let mut updated = coll;
    updated.collection_type = nodedb_types::CollectionType::strict(schema.clone());
    updated.timeseries_config = sonic_rs::to_string(&schema).ok();

    let entry =
        crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(updated.clone()));
    let log_index = crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    if log_index == 0 {
        catalog
            .put_collection(&updated)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    }

    super::super::create::dispatch_register_if_needed(state, identity, parts, sql).await;
    state.schema_version.bump();

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER COLLECTION '{name}' RENAME COLUMN '{old_name}' TO '{new_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}
