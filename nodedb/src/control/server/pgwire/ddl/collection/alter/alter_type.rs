//! `ALTER COLLECTION <name> ALTER COLUMN <col> TYPE <type>` — change a
//! column's declared type in a strict-document collection's schema.
//!
//! The current implementation only accepts type changes that map to the
//! same underlying `ColumnType` discriminant as the existing column —
//! equivalently, no-op aliases like INT → BIGINT (both map to `Int64`).
//! Widening across discriminants would require a full online rewrite and
//! is tracked as a separate enhancement.

use std::str::FromStr;

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;

pub async fn alter_collection_alter_column_type(
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

    // Expect: ALTER COLLECTION <name> ALTER COLUMN <col> TYPE <type>.
    let col_idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))
        .ok_or_else(|| sqlstate_error("42601", "expected ALTER COLUMN <name> TYPE <type>"))?;
    let column_name = parts
        .get(col_idx + 1)
        .ok_or_else(|| sqlstate_error("42601", "missing column name"))?
        .to_lowercase();
    match parts.get(col_idx + 2) {
        Some(tok) if tok.eq_ignore_ascii_case("TYPE") => {}
        _ => return Err(sqlstate_error("42601", "expected TYPE keyword")),
    }
    let new_type_str = parts
        .get(col_idx + 3)
        .ok_or_else(|| sqlstate_error("42601", "missing new type"))?
        .trim_end_matches(';');

    let new_type = nodedb_types::columnar::ColumnType::from_str(new_type_str)
        .map_err(|e| sqlstate_error("42601", &format!("invalid type '{new_type_str}': {e}")))?;

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
            "ALTER COLUMN TYPE is only supported on strict document collections",
        ));
    }

    let mut schema: nodedb_types::columnar::StrictSchema = coll
        .timeseries_config
        .as_deref()
        .and_then(|s| sonic_rs::from_str(s).ok())
        .ok_or_else(|| sqlstate_error("XX000", "strict schema missing or malformed"))?;

    let col = schema
        .columns
        .iter_mut()
        .find(|c| c.name.eq_ignore_ascii_case(&column_name))
        .ok_or_else(|| {
            sqlstate_error(
                "42703",
                &format!("column '{column_name}' does not exist on '{name}'"),
            )
        })?;

    // Reject a true type change that would require re-encoding existing rows.
    if std::mem::discriminant(&col.column_type) != std::mem::discriminant(&new_type) {
        return Err(sqlstate_error(
            "0A000",
            &format!(
                "cross-type change from {:?} to {:?} requires an online rewrite; \
                 only alias type changes (e.g. INT ↔ BIGINT) are supported today",
                col.column_type, new_type
            ),
        ));
    }
    col.column_type = new_type;
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
        &format!("ALTER COLLECTION '{name}' ALTER COLUMN '{column_name}' TYPE {new_type_str}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}
