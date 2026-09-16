// SPDX-License-Identifier: BUSL-1.1

//! `ALTER {TABLE,COLLECTION} <name> ADD [COLUMN] <def>` — append a column
//! to a strict-document / columnar collection's schema.
//!
//! Ported verbatim from the pgwire `ddl::collection::alter::add_column`
//! handler; only the result type changed from pgwire `PgWireResult` /
//! `Response` to the protocol-neutral [`DdlResult`] / [`DdlError`]. The
//! multi-version add (`added_at_version` stamp + `schema.version` bump),
//! duplicate-column check, propose + register, and audit are unchanged, as
//! is the `ALTER TABLE` command tag.

use nodedb_types::DatabaseId;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::neutral::collection::helpers::parse_origin_column_def;
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

    let column = parse_origin_column_def(col_def_str).map_err(|e| err("42601", e.to_string()))?;
    let column_name = column.name.clone();
    // The declared DEFAULT passes the same gate a CREATE column passes: an
    // unregistered function name, or an expression that names another column,
    // is refused at the declaration rather than at the first insert.
    super::super::super::column_default::validate_column_defaults(&[(
        column_name.clone(),
        col_def_str.to_string(),
    )])?;
    // The declared type as written, e.g. `SMALLINT` from `age SMALLINT NOT
    // NULL`. `ColumnDef::column_type` cannot supply this: it has one `Int64`
    // variant for every integer width. Falls back to the resolved type's own
    // name when the definition has no separate type token to quote.
    let declared_type = col_def_str
        .split_whitespace()
        .nth(1)
        .map(str::to_string)
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
                    let new_version = schema.version.saturating_add(1);
                    let mut col = column;
                    col.added_at_version = new_version;
                    schema.columns.push(col);
                    schema.version = new_version;

                    let mut updated = coll;
                    updated.collection_type = nodedb_types::CollectionType::strict(schema.clone());
                    updated.timeseries_config = sonic_rs::to_string(&schema).ok();
                    // Record the column's *declared* type alongside the
                    // resolved one — see `strict_schema::retype_field`. Without
                    // this the added column has no declared width and falls
                    // back to `BIGINT` on the wire, unlike an identical column
                    // declared at CREATE time.
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
                    super::support::propose_and_apply_async(state, entry).await?;
                    Some(updated)
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

    if let Some(ref coll) = updated {
        super::super::register::dispatch_register_from_stored(state, coll)
            .await
            .map_err(|e| err("XX000", e.to_string()))?;
    }

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER TABLE '{table_name}' ADD COLUMN '{column_name}'"),
    );

    Ok(status("ALTER TABLE"))
}
