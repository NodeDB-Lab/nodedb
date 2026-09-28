// SPDX-License-Identifier: Apache-2.0

//! Plan a `DROP INDEX` statement parsed by sqlparser.

use crate::types::DropIndexPlan;
use sqlparser::ast;

use crate::SqlPlan;
use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_object_name_checked;

/// Plan a `DROP INDEX [IF EXISTS] name [ON collection]` statement.
///
/// A missing or schema-qualified index name is a hard error — silently
/// defaulting to an empty string would produce cryptic "index not found"
/// failures downstream.
pub fn plan_drop_index(stmt: &ast::Statement) -> Result<SqlPlan> {
    let ast::Statement::Drop {
        object_type: ast::ObjectType::Index,
        names,
        if_exists,
        table,
        ..
    } = stmt
    else {
        return Err(SqlError::Parse {
            detail: "DROP INDEX: unexpected statement shape".into(),
        });
    };

    let name = names.first().ok_or_else(|| SqlError::Parse {
        detail: "DROP INDEX: an index name is required".into(),
    })?;
    let index_name = normalize_object_name_checked(name)?;
    if index_name.is_empty() {
        return Err(SqlError::Parse {
            detail: "DROP INDEX: empty index name".into(),
        });
    }

    let collection = match table.as_ref() {
        Some(t) => Some(normalize_object_name_checked(t)?),
        None => None,
    };

    Ok(SqlPlan::DropIndex(DropIndexPlan {
        index_name,
        collection,
        if_exists: *if_exists,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;

    fn plan(sql: &str) -> Result<SqlPlan> {
        let stmts = parse_sql(sql).expect("parse");
        plan_drop_index(&stmts[0])
    }

    #[test]
    fn basic_drop() {
        let SqlPlan::DropIndex(DropIndexPlan {
            index_name,
            collection,
            if_exists,
        }) = plan("DROP INDEX idx_users_email").unwrap()
        else {
            panic!("expected DropIndex");
        };
        assert_eq!(index_name, "idx_users_email");
        assert!(collection.is_none());
        assert!(!if_exists);
    }

    #[test]
    fn if_exists_honored() {
        let SqlPlan::DropIndex(DropIndexPlan { if_exists, .. }) =
            plan("DROP INDEX IF EXISTS idx").unwrap()
        else {
            panic!("expected DropIndex");
        };
        assert!(if_exists);
    }

    #[test]
    fn schema_qualified_name_rejected() {
        let err = plan("DROP INDEX public.idx").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }), "{err:?}");
    }
}
