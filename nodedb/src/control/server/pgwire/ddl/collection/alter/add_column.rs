//! `ALTER {TABLE,COLLECTION} <name> ADD [COLUMN] <def>` — append a column
//! to a strict-document / columnar collection's schema.

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;
use super::super::helpers::parse_origin_column_def;

/// ALTER TABLE <name> ADD [COLUMN] <name> <type> [NOT NULL] [DEFAULT ...]
pub async fn alter_table_add_column(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let table_name = parts
        .get(2)
        .ok_or_else(|| sqlstate_error("42601", "ALTER TABLE requires a table name"))?
        .to_lowercase();
    let tenant_id = identity.tenant_id;

    // Find column def after ADD [COLUMN].
    let upper = sql.to_uppercase();
    let add_pos = upper
        .find("ADD COLUMN ")
        .map(|p| p + 11)
        .or_else(|| upper.find("ADD ").map(|p| p + 4))
        .ok_or_else(|| sqlstate_error("42601", "expected ADD [COLUMN]"))?;

    let col_def_str = sql[add_pos..].trim();
    let column = parse_origin_column_def(col_def_str).map_err(|e| sqlstate_error("42601", &e))?;
    let column_name = column.name.clone();

    // Validate: new column must be nullable or have a default.
    if !column.nullable && column.default.is_none() {
        return Err(sqlstate_error(
            "42601",
            &format!(
                "ALTER ADD COLUMN '{}': non-nullable column must have a DEFAULT",
                column.name
            ),
        ));
    }

    // Verify collection exists.
    if let Some(catalog) = state.credentials.catalog() {
        match catalog.get_collection(tenant_id.as_u32(), &table_name) {
            Ok(Some(coll)) if coll.is_active => {
                if coll.collection_type.is_strict()
                    && let Some(config_json) = &coll.timeseries_config
                    && let Ok(mut schema) =
                        sonic_rs::from_str::<nodedb_types::columnar::StrictSchema>(config_json)
                {
                    if schema.columns.iter().any(|c| c.name == column.name) {
                        return Err(sqlstate_error(
                            "42P07",
                            &format!("column '{}' already exists", column.name),
                        ));
                    }
                    let new_version = schema.version.saturating_add(1);
                    let mut column = column;
                    column.added_at_version = new_version;
                    schema.columns.push(column);
                    schema.version = new_version;

                    let mut updated = coll;
                    updated.collection_type = nodedb_types::CollectionType::strict(schema.clone());
                    updated.timeseries_config = sonic_rs::to_string(&schema).ok();
                    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(
                        Box::new(updated.clone()),
                    );
                    let log_index =
                        crate::control::metadata_proposer::propose_catalog_entry(state, &entry)
                            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                    if log_index == 0 {
                        catalog
                            .put_collection(&updated)
                            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
                    }
                }
            }
            _ => {
                return Err(sqlstate_error(
                    "42P01",
                    &format!("collection '{table_name}' does not exist"),
                ));
            }
        }
    }

    super::super::create::dispatch_register_if_needed(state, identity, parts, sql).await;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER TABLE '{table_name}' ADD COLUMN '{column_name}'"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER TABLE"))])
}
