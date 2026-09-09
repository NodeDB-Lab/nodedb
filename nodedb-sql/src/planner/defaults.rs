// SPDX-License-Identifier: Apache-2.0

//! Column DEFAULT expression evaluation at insert time.
//!
//! Supports ID generation functions (UUIDv4/v7, ULID, CUID2, NANOID), `NOW()`,
//! sequence accessors (`nextval`, `currval`), and literal values. Anything
//! else routes through the plan-time const-folder.
//!
//! A DEFAULT that cannot be evaluated raises [`SqlError::UnevaluableDefault`].
//! The column is never omitted: an omitted column stores NULL where the
//! declaration promised a value, and nothing reports it.
//!
//! Lives in the SQL crate rather than beside one engine's converter because
//! every engine that materializes a DEFAULT has to produce the SAME value for
//! the same expression — a `DEFAULT now()` that means one thing on a document
//! collection and another on a key-value one would be a difference nobody
//! declared. The key-value planner also needs it BEFORE its declared-type
//! coercion and range checks run, so a materialized default is validated
//! exactly like a supplied one.

use crate::catalog::SqlCatalog;
use crate::error::SqlError;
use crate::types::{SqlExpr, SqlValue};

/// Evaluate `expr`, the DEFAULT declared on `column`, to one value.
///
/// `catalog` resolves the sequence accessors `nextval` and `currval`. It is
/// required rather than optional: a catalog-free evaluator silently dropped
/// every sequence-backed DEFAULT.
///
/// The absence of a DEFAULT is the caller's `ColumnInfo::default` being `None`
/// and never reaches here. Every call therefore either yields a value or
/// raises.
pub fn evaluate_default_expr(
    expr: &str,
    column: &str,
    catalog: &dyn SqlCatalog,
) -> crate::Result<nodedb_types::Value> {
    let upper = expr.trim().to_uppercase();
    if let Some(value) = eval_keyword_default(&upper) {
        return Ok(value);
    }
    if let Some(value) = eval_parametric_or_literal(expr, &upper)? {
        return Ok(value);
    }
    evaluate_parsed_default(expr, column, catalog)
}

/// Check that `expr`, the DEFAULT declared on `column`, can be evaluated.
///
/// DDL calls this to refuse an unevaluable DEFAULT at declaration time. It
/// classifies the expression through the same arms `evaluate_default_expr`
/// uses, then parses anything left over. Parsing runs the resolver's
/// `FunctionRegistry` gate, so an unregistered function name raises
/// [`SqlError::UndefinedFunction`].
///
/// A sequence accessor is parsed, never called, so declaring a column must
/// never advance a sequence.
pub fn validate_default_expr(expr: &str, column: &str) -> crate::Result<()> {
    let upper = expr.trim().to_uppercase();
    if eval_keyword_default(&upper).is_some() {
        return Ok(());
    }
    if eval_parametric_or_literal(expr, &upper)?.is_some() {
        return Ok(());
    }
    let sql_expr = crate::parse_expr_string(expr)?;
    reject_setval_default(&sql_expr, column)
}

/// Evaluate the keyword-spelled defaults: the ID generators and `NOW()`.
///
/// Returns `None` for every other expression. This is the one list of
/// keyword forms; the DDL gate classifies through it rather than repeating it.
fn eval_keyword_default(upper: &str) -> Option<nodedb_types::Value> {
    let value = match upper {
        "UUID_V7" | "UUIDV7" | "GEN_UUID_V7()" | "UUID_V7()" => {
            nodedb_types::Value::String(nodedb_types::id_gen::uuid_v7())
        }
        "UUID_V4" | "UUIDV4" | "UUID" | "GEN_UUID_V4()" | "UUID_V4()" => {
            nodedb_types::Value::String(nodedb_types::id_gen::uuid_v4())
        }
        "ULID" | "GEN_ULID()" | "ULID()" => {
            nodedb_types::Value::String(nodedb_types::id_gen::ulid())
        }
        "CUID2" | "CUID2()" => nodedb_types::Value::String(nodedb_types::id_gen::cuid2()),
        "NANOID" | "NANOID()" => nodedb_types::Value::String(nodedb_types::id_gen::nanoid()),
        "NOW()" => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            nodedb_types::Value::String(
                chrono::DateTime::from_timestamp_millis(now.as_millis() as i64)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| now.as_millis().to_string()),
            )
        }
        _ => return None,
    };
    Some(value)
}

/// Evaluate the parametric ID generators and the bare literals.
///
/// Returns `Ok(None)` when `expr` is none of them, leaving it to the parser.
/// This is the one list of literal forms; the DDL gate reuses it.
fn eval_parametric_or_literal(
    expr: &str,
    upper: &str,
) -> crate::Result<Option<nodedb_types::Value>> {
    // NANOID(N) — custom length.
    if upper.starts_with("NANOID(") && upper.ends_with(')') {
        let len_str = &upper[7..upper.len() - 1];
        if let Ok(len) = len_str.parse::<usize>() {
            return Ok(Some(nodedb_types::Value::String(
                nodedb_types::id_gen::nanoid_with_length(len),
            )));
        }
    }
    // CUID2(N) — custom length; validates length range and surfaces planning errors.
    if upper.starts_with("CUID2(") && upper.ends_with(')') {
        let len_str = &upper[6..upper.len() - 1];
        if let Ok(len) = len_str.parse::<usize>() {
            let id = nodedb_types::id_gen::cuid2_with_length(len).map_err(|e| SqlError::Parse {
                detail: format!("CUID2({len}) default expression is invalid: {e}"),
            })?;
            return Ok(Some(nodedb_types::Value::String(id)));
        }
    }
    // Numeric literal.
    if let Ok(i) = expr.trim().parse::<i64>() {
        return Ok(Some(nodedb_types::Value::Integer(i)));
    }
    if let Ok(f) = expr.trim().parse::<f64>() {
        return Ok(Some(nodedb_types::Value::Float(f)));
    }
    // Quoted string literal.
    let trimmed = expr.trim();
    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        return Ok(Some(nodedb_types::Value::String(
            trimmed[1..trimmed.len() - 1].to_string(),
        )));
    }

    Ok(None)
}

/// Parse the DEFAULT as SQL, then resolve it against the catalog or the folder.
fn evaluate_parsed_default(
    expr: &str,
    column: &str,
    catalog: &dyn SqlCatalog,
) -> crate::Result<nodedb_types::Value> {
    let sql_expr = crate::parse_expr_string(expr).map_err(|_| unevaluable(column, expr))?;
    if let Some(value) = evaluate_sequence_default(&sql_expr, column, catalog)? {
        return Ok(sql_value_to_ndb(value));
    }
    // `Once`: a materialized DEFAULT serves this insert only, and an INSERT
    // plan carrying a volatile DEFAULT is never admitted to the plan cache.
    let folded = crate::planner::const_fold::fold_constant_scoped(
        &sql_expr,
        crate::planner::const_fold::default_registry(),
        crate::planner::const_fold::FoldScope::Once,
    )
    .map_err(|_| unevaluable(column, expr))?
    .ok_or_else(|| unevaluable(column, expr))?;
    Ok(sql_value_to_ndb(folded))
}

/// Resolve `nextval` / `currval` through the catalog; refuse `setval`.
///
/// Returns `Ok(None)` for every other expression, leaving it to the folder.
fn evaluate_sequence_default(
    expr: &SqlExpr,
    column: &str,
    catalog: &dyn SqlCatalog,
) -> crate::Result<Option<SqlValue>> {
    reject_setval_default(expr, column)?;
    super::catalog_expr_fold::eval_sequence_accessor(expr, catalog)
}

/// Refuse `setval` as a column DEFAULT.
///
/// `setval` moves a sequence rather than reading one, so a column cannot take
/// its result as a value. Both the evaluator and the DDL gate call this.
fn reject_setval_default(expr: &SqlExpr, column: &str) -> crate::Result<()> {
    if let SqlExpr::Function { name, .. } = expr
        && name.eq_ignore_ascii_case("setval")
    {
        return Err(SqlError::SetvalInColumnDefault {
            column: column.to_string(),
        });
    }
    Ok(())
}

fn unevaluable(column: &str, expr: &str) -> SqlError {
    SqlError::UnevaluableDefault {
        column: column.to_string(),
        expr: expr.to_string(),
    }
}

fn sql_value_to_ndb(v: SqlValue) -> nodedb_types::Value {
    match v {
        SqlValue::Null => nodedb_types::Value::Null,
        SqlValue::Bool(b) => nodedb_types::Value::Bool(b),
        SqlValue::Int(i) => nodedb_types::Value::Integer(i),
        SqlValue::Float(f) => nodedb_types::Value::Float(f),
        SqlValue::Decimal(d) => nodedb_types::Value::Decimal(d),
        SqlValue::String(s) => nodedb_types::Value::String(s),
        SqlValue::Bytes(b) => nodedb_types::Value::Bytes(b),
        SqlValue::Array(a) => {
            nodedb_types::Value::Array(a.into_iter().map(sql_value_to_ndb).collect())
        }
        SqlValue::Timestamp(dt) => nodedb_types::Value::NaiveDateTime(dt),
        SqlValue::Timestamptz(dt) => nodedb_types::Value::DateTime(dt),
    }
}

/// Convert an evaluated default back into the planner's literal type.
///
/// The inverse of `sql_value_to_ndb` above, which is the only producer of
/// these values — so every shape the evaluator can emit has an exact
/// counterpart here. Anything else raises rather than rendering through
/// `Debug`: a `DEFAULT` that stored `Uuid("…")` as its own debug text is the
/// same class of defect as dropping it, and harder to notice because the
/// column looks populated.
pub fn default_value_to_sql(column: &str, value: nodedb_types::Value) -> crate::Result<SqlValue> {
    Ok(match value {
        nodedb_types::Value::Null => SqlValue::Null,
        nodedb_types::Value::Bool(b) => SqlValue::Bool(b),
        nodedb_types::Value::Integer(i) => SqlValue::Int(i),
        nodedb_types::Value::Float(f) => SqlValue::Float(f),
        nodedb_types::Value::Decimal(d) => SqlValue::Decimal(d),
        nodedb_types::Value::String(s) => SqlValue::String(s),
        nodedb_types::Value::Bytes(b) => SqlValue::Bytes(b),
        nodedb_types::Value::NaiveDateTime(dt) => SqlValue::Timestamp(dt),
        nodedb_types::Value::DateTime(dt) => SqlValue::Timestamptz(dt),
        nodedb_types::Value::Array(items) => SqlValue::Array(
            items
                .into_iter()
                .map(|item| default_value_to_sql(column, item))
                .collect::<crate::Result<Vec<_>>>()?,
        ),
        other => {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "default for column '{column}' evaluates to a value with no SQL literal \
                     form: {other:?}"
                ),
            });
        }
    })
}

/// Fill in every column of `row` that declares a DEFAULT and the statement omitted.
///
/// `column_defaults` is the catalog's `(column_name, default_expr)` list.
/// Each entry evaluates at most once per row, so a `nextval` DEFAULT allocates
/// exactly one value per row.
///
/// A column the statement supplied stays untouched, an explicit `NULL`
/// included: `NULL` is a value the author chose, and overwriting it with the
/// default makes storing one impossible.
///
/// A DEFAULT the evaluator cannot resolve raises
/// [`SqlError::UnevaluableDefault`] rather than leaving the column out.
pub fn materialize_row_defaults(
    row: &mut Vec<(String, SqlValue)>,
    column_defaults: &[(String, String)],
    catalog: &dyn SqlCatalog,
) -> crate::Result<()> {
    for (column, default_expr) in column_defaults {
        if row.iter().any(|(name, _)| name == column) {
            continue;
        }
        let evaluated = evaluate_default_expr(default_expr, column, catalog)?;
        row.push((column.clone(), default_value_to_sql(column, evaluated)?));
    }
    Ok(())
}
