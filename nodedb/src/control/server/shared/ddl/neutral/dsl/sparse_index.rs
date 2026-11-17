// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SPARSE INDEX` DSL handler.

use crate::control::security::catalog::IndexKind;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::index_registry::{
    IndexRegistration, index_name_holder, propose_index_record,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::options::{ColumnMode, HeaderSpec, NameMode, parse_index_statement};
use super::support::ddl_err;

const CONTEXT: &str = "CREATE SPARSE INDEX";
const LEADING: &[&str] = &["CREATE", "SPARSE", "INDEX"];

const SYNTAX: &str = "CREATE SPARSE INDEX [IF NOT EXISTS] [<name>] ON <collection> [(<field>)]";

/// Substituted by the parser when the statement names no index.
const PLACEHOLDER_NAME: &str = "_auto_sparse";

const HEADER: HeaderSpec = HeaderSpec {
    name: NameMode::Optional {
        fallback: PLACEHOLDER_NAME,
    },
    columns: ColumnMode::AtMostOne,
    syntax: SYNTAX,
};

/// The field a sparse index covers when the statement names none.
const DEFAULT_FIELD: &str = "_sparse";

/// `CREATE SPARSE INDEX [IF NOT EXISTS] [<name>] ON <collection> [(<field>)]`
pub async fn create_sparse_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    // This surface carries no options, so any trailing token is a statement
    // the handler does not implement rather than one it can ignore.
    let stmt = parse_index_statement(sql, LEADING, &HEADER, &[], CONTEXT)?;

    let index_name = &stmt.header.name;
    let collection = &stmt.header.collection;
    let field = match stmt.header.column() {
        "" => DEFAULT_FIELD,
        named => named,
    };
    let tenant_id = identity.tenant_id;

    // The parser substitutes a placeholder when the name is omitted; a
    // tenant-global placeholder will collide across collections and leave
    // only one of them droppable, so it resolves per collection and field.
    let index_name = if index_name == PLACEHOLDER_NAME {
        format!("{collection}_{field}_sparse_idx")
    } else {
        index_name.clone()
    };
    if let Some(taken) = index_name_holder(state, database_id, tenant_id, &index_name, CONTEXT)? {
        if stmt.header.if_not_exists && taken.kind == IndexKind::Sparse {
            return Ok(vec![DdlResult::Status {
                command: CONTEXT.to_string(),
                rows_affected: None,
            }]);
        }
        return Err(ddl_err(
            "42710",
            format!(
                "{CONTEXT}: index '{index_name}' already exists on '{}' ({})",
                taken.collection,
                taken.kind.display_type()
            ),
        ));
    }

    propose_index_record(
        state,
        &IndexRegistration {
            database_id,
            tenant_id,
            name: &index_name,
            kind: IndexKind::Sparse,
            collection,
            fields: vec![field.to_string()],
            state: crate::control::security::catalog::IndexRecordState::Ready,
        },
    )
    .await?;
    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        IndexKind::Sparse.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        &index_name,
        &identity.username,
    )
    .await?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created sparse index '{index_name}' on '{collection}' ({field})"),
    );

    Ok(vec![DdlResult::Status {
        command: CONTEXT.to_string(),
        rows_affected: None,
    }])
}

#[cfg(test)]
mod tests {
    use super::super::options::IndexStatement;
    use super::*;

    fn parse(sql: &str) -> Result<IndexStatement, DdlError> {
        parse_index_statement(sql, LEADING, &HEADER, &[], CONTEXT)
    }

    #[test]
    fn name_is_optional() {
        assert_eq!(
            parse("CREATE SPARSE INDEX ON docs (terms)")
                .unwrap()
                .header
                .name,
            "_auto_sparse"
        );
        assert_eq!(
            parse("CREATE SPARSE INDEX idx ON docs (terms)")
                .unwrap()
                .header
                .name,
            "idx"
        );
    }

    #[test]
    fn field_is_optional() {
        assert_eq!(
            parse("CREATE SPARSE INDEX ON docs")
                .unwrap()
                .header
                .column(),
            ""
        );
    }

    #[test]
    fn unrecognized_trailing_tokens_are_rejected() {
        assert!(parse("CREATE SPARSE INDEX ON docs (terms) USING SOMETHING").is_err());
    }
}
