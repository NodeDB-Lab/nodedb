//! `ALTER COLLECTION <name> DROP COLUMN <col>` — remove a column from a
//! strict-document collection's schema.
//!
//! Current scope: strict collections only. The column is removed from the
//! schema metadata and the schema version is bumped; existing rows are not
//! re-encoded, so any row written before the drop retains the column's
//! physical bytes. Reads of those rows will see trailing bytes as extra
//! tuple elements — acceptable for the "fix after drop, new writes work"
//! workflow. A full online rewrite is tracked as a separate enhancement.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;

pub async fn alter_collection_drop_column(
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

    // Locate "DROP COLUMN <col>" — `DROP` then `COLUMN` then the name.
    let column_name = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("COLUMN"))
        .and_then(|i| parts.get(i + 1))
        .ok_or_else(|| sqlstate_error("42601", "expected DROP COLUMN <name>"))?
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
            "DROP COLUMN is only supported on strict document collections",
        ));
    }

    let mut schema: nodedb_types::columnar::StrictSchema = coll
        .timeseries_config
        .as_deref()
        .and_then(|s| sonic_rs::from_str(s).ok())
        .ok_or_else(|| sqlstate_error("XX000", "strict schema missing or malformed"))?;

    let idx = schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(&column_name))
        .ok_or_else(|| {
            sqlstate_error(
                "42703",
                &format!("column '{column_name}' does not exist on '{name}'"),
            )
        })?;

    if schema.columns[idx].primary_key {
        return Err(sqlstate_error(
            "42601",
            &format!("cannot drop primary key column '{column_name}'"),
        ));
    }

    let dropped_def = schema.columns.remove(idx);
    let new_version = schema.version.saturating_add(1);
    schema
        .dropped_columns
        .push(nodedb_types::columnar::DroppedColumn {
            def: dropped_def,
            position: idx,
            dropped_at_version: new_version,
        });
    schema.version = new_version;

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
        &format!("ALTER COLLECTION '{name}' DROP COLUMN '{column_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER COLLECTION"))])
}
