// SPDX-License-Identifier: BUSL-1.1

//! `ALTER {TABLE,COLLECTION} <name> ADD [COLUMN] <def>` — append a column
//! to a strict-document / columnar collection's schema.
//!
//! The result type is the protocol-neutral [`DdlResult`] / [`DdlError`]. The
//! multi-version add (`added_at_version` stamp + `schema.version` bump),
//! duplicate-column check, propose + register, and audit run here, and the
//! command tag is `ALTER TABLE`.

use nodedb_types::DatabaseId;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::neutral::collection::helpers::parse_origin_column_def;
use crate::control::server::shared::ddl::neutral::column_default::{
    DeclaredColumn, validate_column_default,
};
use crate::control::server::shared::ddl::neutral::declared_typmod::validate_declared_typmod;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::support::{err, status};

/// ALTER TABLE/COLLECTION <name> ADD [COLUMN] <name> <type> [NOT NULL] [DEFAULT ...]
pub(super) async fn alter_table_add_column(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    table_name: &str,
    col_def_str: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    // The declared type as written, with its modifiers: `SMALLINT NOT NULL`
    // from `age SMALLINT NOT NULL`, the same text `CREATE` records for a
    // column. `ColumnDef::column_type` cannot supply this: it has one `Int64`
    // variant for every integer width. A spaced parameter list such as
    // `DECIMAL(10, 2)` stays whole.
    let written_type = col_def_str
        .trim_start()
        .split_once(char::is_whitespace)
        .map(|(name, declared)| (name, declared.trim()));
    // An invalid `DECIMAL(p,s)` typmod is refused with the SQLSTATE `CREATE`
    // gives it.
    if let Some((name, declared)) = written_type {
        validate_declared_typmod(name, declared)?;
    }
    let column = parse_origin_column_def(col_def_str).map_err(|e| err("42601", e.to_string()))?;
    let column_name = column.name.clone();
    // Falls back to the resolved type's own name when the definition has no
    // separate type text to quote.
    let declared_type = written_type
        .map(|(_, declared)| declared.to_string())
        .unwrap_or_else(|| column.column_type.to_string());

    // Validate: new column must be nullable or have a default.
    if !column.nullable && column.default.is_none() {
        return Err(err(
            "42601",
            format!(
                "ALTER ADD COLUMN '{}': non-nullable column must have a DEFAULT",
                column.name
            ),
        ));
    }
    // A DEFAULT passes the same gate `CREATE` applies: evaluable, and a
    // literal the declared type can hold.
    if let Some(expr) = &column.default {
        validate_column_default(
            &DeclaredColumn {
                name: &column.name,
                declared_type: &declared_type,
                primary_key: column.primary_key,
            },
            expr,
        )?;
    }

    let updated = {
        let catalog = state.credentials.catalog();
        match catalog.get_collection(database_id, tenant_id.as_u64(), table_name) {
            Ok(Some(coll)) if coll.is_active => {
                if coll.collection_type.is_strict()
                    && let Some(config_json) = &coll.timeseries_config
                    && let Ok(mut schema) =
                        sonic_rs::from_str::<nodedb_types::columnar::StrictSchema>(config_json)
                {
                    if schema.columns.iter().any(|c| c.name == column.name) {
                        return Err(err(
                            "42P07",
                            format!("column '{}' already exists", column.name),
                        ));
                    }
                    if column.generated_expr.is_some() {
                        super::super::generated_join::refuse_bound_generated_join_column(
                            catalog,
                            database_id,
                            tenant_id.as_u64(),
                            table_name,
                            std::slice::from_ref(&column.name),
                        )?;
                    }
                    let new_version = schema.version.saturating_add(1);
                    let mut col = column;
                    col.added_at_version = new_version;
                    schema.columns.push(col);
                    schema.version = new_version;

                    let mut updated = coll;
                    updated.collection_type = nodedb_types::CollectionType::strict(schema.clone());
                    updated.timeseries_config = sonic_rs::to_string(&schema).ok();
                    // Record the column's *declared* type alongside the schema
                    // column — see `strict_schema::retype_field`. Catalog
                    // introspection reads the width from this spelling.
                    super::strict_schema::add_field(
                        &mut updated,
                        &column_name,
                        declared_type.as_str(),
                    );
                    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(
                        Box::new(updated.clone()),
                    );
                    // Offload the durable catalog commit (redb `fsync`) off the
                    // Tokio worker so this online ALTER never stalls concurrent
                    // INSERTs on the same runtime.
                    let outcome = super::support::propose_and_apply_async(state, entry).await?;
                    Some((updated, outcome))
                } else {
                    None
                }
            }
            _ => {
                return Err(err(
                    "42P01",
                    format!("collection '{table_name}' does not exist"),
                ));
            }
        }
    };

    if let Some((ref coll, outcome)) = updated {
        super::super::register::register_proposed_collection(state, outcome, coll)
            .await
            .map_err(|e| DdlError::from_error(&e))?;
        super::strict_schema::recompile_rls_policies(state, coll)?;
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER TABLE '{table_name}' ADD COLUMN '{column_name}'"),
    );

    Ok(status("ALTER TABLE"))
}
