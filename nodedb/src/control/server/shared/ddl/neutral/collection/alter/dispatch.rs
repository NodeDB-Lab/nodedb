// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION` dispatcher: maps every `AlterCollectionOp` variant to its
//! protocol-neutral handler.
//!
//! This is the total match over `AlterCollectionOp` — no `_ =>` fallthrough —
//! so the neutral router owns every `ALTER COLLECTION` sub-command. Ported from
//! the pgwire `router::ast::alter::dispatch_alter_collection`; the ADD COLUMN
//! `col_def` string assembly (`<name> <type> [NOT NULL] [DEFAULT ...]`) is
//! preserved verbatim. `SetOnConflict` continues to route to the
//! `conflict_policy` family; every other variant routes to its sibling handler
//! in this directory.

use nodedb_sql::ddl_ast::AlterCollectionOp;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

pub async fn dispatch_alter_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    name: &str,
    operation: &AlterCollectionOp,
) -> Result<Vec<DdlResult>, DdlError> {
    match operation {
        AlterCollectionOp::AddColumn {
            column_name,
            column_type,
            not_null,
            default_expr,
        } => {
            let mut col_def = format!("{column_name} {column_type}");
            if *not_null {
                col_def.push_str(" NOT NULL");
            }
            if let Some(def) = default_expr {
                col_def.push_str(&format!(" DEFAULT {def}"));
            }
            super::add_column::alter_table_add_column(state, identity, name, &col_def).await
        }

        AlterCollectionOp::DropColumn { column_name } => {
            super::drop_column::alter_collection_drop_column(state, identity, name, column_name)
                .await
        }

        AlterCollectionOp::RenameColumn { old_name, new_name } => {
            super::rename_column::alter_collection_rename_column(
                state, identity, name, old_name, new_name,
            )
            .await
        }

        AlterCollectionOp::AlterColumnType {
            column_name,
            new_type,
        } => {
            super::alter_type::alter_collection_alter_column_type(
                state,
                identity,
                name,
                column_name,
                new_type,
            )
            .await
        }

        AlterCollectionOp::OwnerTo { new_owner } => {
            super::ownership::alter_collection_owner(state, identity, database_id, name, new_owner)
        }

        AlterCollectionOp::SetRetention { value } => {
            super::enforcement::alter_collection_set_retention(state, identity, name, value)
        }

        AlterCollectionOp::SetAppendOnly => {
            super::enforcement::alter_collection_set_append_only(state, identity, name)
        }

        AlterCollectionOp::SetLastValueCache { enabled } => {
            super::enforcement::alter_collection_set_last_value_cache(
                state, identity, name, *enabled,
            )
        }

        AlterCollectionOp::SetLegalHold { enabled, tag } => {
            super::enforcement::alter_collection_set_legal_hold(
                state, identity, name, *enabled, tag,
            )
        }

        AlterCollectionOp::AddMaterializedSum {
            target_collection,
            target_column,
            source_collection,
            join_column,
            value_expr,
        } => super::materialized_sum::add_materialized_sum(
            state,
            identity,
            target_collection,
            target_column,
            source_collection,
            join_column,
            value_expr,
        ),

        AlterCollectionOp::SetOnConflict {
            policy,
            constraint_kind,
        } => {
            super::super::super::conflict_policy::alter_set_on_conflict(
                state,
                identity,
                database_id,
                name,
                policy,
                constraint_kind,
            )
            .await
        }
    }
}
