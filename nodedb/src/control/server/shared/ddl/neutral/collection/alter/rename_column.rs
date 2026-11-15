// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION <name> RENAME COLUMN <old> TO <new>` — rename a
//! column in a strict-document collection's schema.
//!
//! The result type is the protocol-neutral [`DdlResult`] / [`DdlError`]. The
//! duplicate-name guard, positional rename + version bump, persist, and
//! audit run here, and the command tag is `ALTER COLLECTION`.

use nodedb_types::DatabaseId;

use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::strict_schema::{
    load_strict_collection, persist_schema_change, rename_field, write_schema_back,
};
use super::support::{err, status};
use super::vector_model::move_vector_model_row;

/// ALTER COLLECTION <name> RENAME COLUMN <old_name> TO <new_name>
pub(super) async fn alter_collection_rename_column(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    old_name: &str,
    new_name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id;

    let (coll, mut schema) = load_strict_collection(
        state,
        database_id,
        tenant_id.as_u64(),
        name,
        "RENAME COLUMN",
    )?;

    if schema
        .columns
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(new_name))
    {
        return Err(err(
            "42P07",
            format!("column '{new_name}' already exists on '{name}'"),
        ));
    }

    let col = schema
        .columns
        .iter_mut()
        .find(|c| c.name.eq_ignore_ascii_case(old_name))
        .ok_or_else(|| {
            err(
                "42703",
                format!("column '{old_name}' does not exist on '{name}'"),
            )
        })?;
    if col.generated_expr.is_some() {
        super::super::generated_join::refuse_bound_generated_join_column(
            state.credentials.catalog(),
            database_id,
            tenant_id.as_u64(),
            name,
            &[new_name.to_string()],
        )?;
    }
    col.name = new_name.to_string();
    schema.version = schema.version.saturating_add(1);

    let mut updated = coll;
    write_schema_back(&mut updated, schema);
    rename_field(&mut updated, old_name, new_name);
    persist_schema_change(state, &updated).await?;

    // The embedding-model row is keyed by column name, so it stays under the
    // old name unless the rename moves it.
    move_vector_model_row(
        state,
        database_id,
        tenant_id.as_u64(),
        name,
        old_name,
        new_name,
    )
    .await?;

    state.audit_record(
        AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("ALTER COLLECTION '{name}' RENAME COLUMN '{old_name}' TO '{new_name}'"),
    );

    Ok(status("ALTER COLLECTION"))
}
