// SPDX-License-Identifier: BUSL-1.1

//! Missing strict-column defaults materialized before generated expressions.
//!
//! A volatile DEFAULT (`NOW()`, a UUID generator, `nextval`) takes its value
//! once, on the proposer, before its record is logged. An apply of a logged
//! record never evaluates one: a column it finds unset there is an invariant
//! break, refused before anything is written.

use nodedb_types::{Value, columnar::StrictSchema};
use std::collections::HashMap;

/// The strict collection a body fills, and whether this apply installs a
/// logged record (`ApplyScope::applies_logged_record`).
pub(super) struct StrictDefaults<'a> {
    pub collection: &'a str,
    pub schema: &'a StrictSchema,
    pub logged: bool,
}

pub(super) fn materialize_strict_defaults(
    defaults: StrictDefaults<'_>,
    fields: &mut HashMap<String, Value>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<()> {
    let StrictDefaults {
        collection,
        schema,
        logged,
    } = defaults;
    for column in &schema.columns {
        if fields.contains_key(&column.name)
            || nodedb_types::columnar::schema::is_reserved_bitemporal_column(&column.name)
        {
            continue;
        }
        let Some(text) = column.default.as_deref() else {
            continue;
        };
        if logged && is_volatile(&column.name, text)? {
            return Err(unbound_volatile_default(collection, &column.name, text));
        }
        let invalid = |error: nodedb_sql::SqlError| crate::Error::BadRequest {
            detail: format!(
                "column '{}' DEFAULT '{text}' cannot compile: {error}. Correct the default expression",
                column.name
            ),
        };
        let parsed = nodedb_sql::parse_expr_string(text).map_err(invalid)?;
        let expression = nodedb_sql::query_expr::convert_sql_expr(&parsed).map_err(invalid)?;
        let referenced = nodedb_query::expr_parse::referenced_columns(&expression);
        if !referenced.is_empty() {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "column '{}' DEFAULT '{text}' references row columns {referenced:?}. Use a row-independent expression",
                    column.name
                ),
            });
        }
        let value = expression
            .eval(&Value::Null, crs)
            .map_err(crate::bridge::envelope::ErrorCode::from)
            .map_err(crate::Error::DataPlane)?;
        fields.insert(column.name.clone(), value);
    }
    Ok(())
}

/// Whether the DEFAULT `text` on `column` yields a fresh value each time, by
/// the classification the proposer binds with.
fn is_volatile(column: &str, text: &str) -> crate::Result<bool> {
    nodedb_sql::planner::defaults::CompiledDefault::compile(column, text)
        .map(|compiled| compiled.is_volatile())
        .map_err(|error| crate::Error::BadRequest {
            detail: format!(
                "column '{column}' DEFAULT '{text}' cannot compile: {error}. Correct the default expression"
            ),
        })
}

/// The invariant break a logged apply reports for an unbound volatile DEFAULT.
fn unbound_volatile_default(collection: &str, column: &str, text: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!(
            "internal invariant break: a logged apply on collection '{collection}' found strict \
             column '{column}' unset, and its DEFAULT '{text}' is volatile. The proposer binds \
             every volatile DEFAULT before the record is logged, and an apply never evaluates \
             one, so the record was written without its bound value"
        ),
    }
}
