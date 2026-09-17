// SPDX-License-Identifier: Apache-2.0

//! MERGE `WHEN ... THEN` clause conversion.
//!
//! Every literal an action writes to the target passes the declared-type
//! coercion here, once, the same pass an ordinary `INSERT VALUES` or
//! `UPDATE SET` literal gets. The document-schemaless engine stores the
//! planner's value verbatim, so a literal left uncoerced would reach storage
//! as whatever the author typed: a bare integer under a `TIMESTAMP` column,
//! text under an `INT` column.
//!
//! Only literals are coerced. A source column reference or a function call
//! is evaluated on the Data Plane against the source row and carries no
//! value at plan time.

use sqlparser::ast::{self, MergeAction, MergeClauseKind as AstMergeClauseKind, MergeInsertKind};

use super::super::ast_helpers::strip_and_convert_filters;
use super::super::declared_type_coerce::coerce_assignments_to_declared_types;
use super::super::dml_helpers::{
    check_declared_float_ranges_in_assignments, check_declared_int_ranges_in_assignments,
    coerce_and_check_rows,
};
use crate::error::{Result, SqlError};
use crate::parser::normalize::normalize_object_name_checked;
use crate::resolver::ColumnScope;
use crate::resolver::columns::TableScope;
use crate::resolver::expr::convert_expr;
use crate::types::*;

/// Convert every `WHEN` clause of a MERGE statement.
///
/// `scope` addresses both target and source, `target_scope` the target
/// alone, and `target` carries the declared columns the action literals are
/// coerced against.
pub(super) fn convert_merge_clauses(
    clauses: &[ast::MergeClause],
    target_ref: &str,
    scope: &TableScope,
    target_scope: &TableScope,
    target: &CollectionInfo,
) -> Result<Vec<MergePlanClause>> {
    clauses
        .iter()
        .map(|c| convert_one_clause(c, target_ref, scope, target_scope, target))
        .collect()
}

fn convert_one_clause(
    clause: &ast::MergeClause,
    target_ref: &str,
    scope: &TableScope,
    target_scope: &TableScope,
    target: &CollectionInfo,
) -> Result<MergePlanClause> {
    let kind = match clause.clause_kind {
        AstMergeClauseKind::Matched => MergeClauseKind::Matched,
        AstMergeClauseKind::NotMatched | AstMergeClauseKind::NotMatchedByTarget => {
            MergeClauseKind::NotMatched
        }
        AstMergeClauseKind::NotMatchedBySource => MergeClauseKind::NotMatchedBySource,
    };

    let extra_predicate = match &clause.predicate {
        Some(expr) => strip_and_convert_filters(vec![expr.clone()], target_ref, scope)?,
        None => Vec::new(),
    };

    let action = convert_merge_action(&clause.action, scope, target_scope, target)?;

    Ok(MergePlanClause {
        kind,
        extra_predicate,
        action,
    })
}

/// Convert one `THEN` action, coercing every literal it writes to the
/// declared type of the target column it lands in.
pub(super) fn convert_merge_action(
    action: &MergeAction,
    scope: &TableScope,
    target_scope: &TableScope,
    target: &CollectionInfo,
) -> Result<MergePlanAction> {
    match action {
        MergeAction::Update(update_expr) => {
            let mut assignments = update_expr
                .assignments
                .iter()
                .map(|a| {
                    let col = match &a.target {
                        ast::AssignmentTarget::ColumnName(name) => {
                            normalize_object_name_checked(name)
                        }
                        ast::AssignmentTarget::Tuple(_) => Err(SqlError::Unsupported {
                            detail: "tuple assignment target in MERGE UPDATE is not supported"
                                .into(),
                        }),
                    }?;
                    target_scope.check_name(None, &col)?;
                    let val = convert_expr(&a.value, &ColumnScope::Relations(scope))?;
                    Ok((col, val))
                })
                .collect::<Result<Vec<_>>>()?;
            // `SET col = <literal>` rewrites the stored row through the same
            // path as `UPDATE ... SET`, so it carries the same declared-type
            // contract, primary-key exemption included.
            coerce_assignments_to_declared_types(
                &target.columns,
                &mut assignments,
                target.primary_key.as_deref(),
            )?;
            check_declared_int_ranges_in_assignments(&target.columns, &assignments)?;
            check_declared_float_ranges_in_assignments(&target.columns, &assignments)?;
            Ok(MergePlanAction::Update { assignments })
        }
        MergeAction::Delete { .. } => Ok(MergePlanAction::Delete),
        MergeAction::Insert(insert_expr) => {
            let columns: Vec<String> = insert_expr
                .columns
                .iter()
                .map(|c| {
                    let col = normalize_object_name_checked(c)?;
                    target_scope.check_name(None, &col)?;
                    Ok(col)
                })
                .collect::<Result<Vec<_>>>()?;

            let mut values: Vec<SqlExpr> = match &insert_expr.kind {
                MergeInsertKind::Values(vals) => {
                    if vals.rows.len() != 1 {
                        return Err(SqlError::Unsupported {
                            detail: format!(
                                "MERGE INSERT VALUES must have exactly one row; got {}",
                                vals.rows.len()
                            ),
                        });
                    }
                    vals.rows[0]
                        .iter()
                        .map(|e| convert_expr(e, &ColumnScope::Relations(scope)))
                        .collect::<Result<Vec<_>>>()?
                }
                MergeInsertKind::Row => {
                    return Err(SqlError::Unsupported {
                        detail: "MERGE INSERT ROW is not supported; use explicit VALUES".into(),
                    });
                }
            };

            if !columns.is_empty() && columns.len() != values.len() {
                return Err(SqlError::Parse {
                    detail: format!(
                        "MERGE INSERT column list ({}) and VALUES ({}) lengths do not match",
                        columns.len(),
                        values.len()
                    ),
                });
            }

            let columns = bind_positional_columns(columns, values.len(), target)?;
            coerce_insert_literals(&columns, &mut values, target)?;
            Ok(MergePlanAction::Insert { columns, values })
        }
    }
}

/// Bind a column-less `INSERT VALUES (...)` arm to the target's declared
/// column order, the way a positional `INSERT INTO t VALUES (...)` binds.
///
/// A target with no declared columns has no order to bind to and keeps the
/// empty list. More values than declared columns is refused: there is no
/// column name for the overflow to land under.
fn bind_positional_columns(
    columns: Vec<String>,
    value_count: usize,
    target: &CollectionInfo,
) -> Result<Vec<String>> {
    if !columns.is_empty() || target.columns.is_empty() {
        return Ok(columns);
    }
    if value_count > target.columns.len() {
        return Err(SqlError::InsertColumnArityMismatch {
            collection: target.name.clone(),
            given: value_count,
            declared: target.columns.len(),
        });
    }
    Ok(target
        .columns
        .iter()
        .take(value_count)
        .map(|c| c.name.clone())
        .collect())
}

/// Coerce every literal of an INSERT arm to the declared type of the target
/// column it is bound to, and range-check it, through the one row-typing
/// pass every `VALUES` clause takes.
///
/// Non-literal expressions are left in place: they are evaluated against the
/// source row on the Data Plane and carry no value here.
fn coerce_insert_literals(
    columns: &[String],
    values: &mut [SqlExpr],
    target: &CollectionInfo,
) -> Result<()> {
    let mut literal_row: Vec<(String, SqlValue)> = Vec::new();
    let mut literal_slots: Vec<usize> = Vec::new();
    for (slot, (column, expr)) in columns.iter().zip(values.iter()).enumerate() {
        if let SqlExpr::Literal(value) = expr {
            literal_row.push((column.clone(), value.clone()));
            literal_slots.push(slot);
        }
    }
    if literal_row.is_empty() {
        return Ok(());
    }
    let mut rows = [literal_row];
    coerce_and_check_rows(target, &mut rows)?;
    let [literal_row] = rows;
    for (slot, (_, value)) in literal_slots.into_iter().zip(literal_row) {
        values[slot] = SqlExpr::Literal(value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::statement::parse_sql;
    use crate::resolver::columns::ResolvedTable;
    use nodedb_types::columnar::IntWidth;
    use nodedb_types::datetime::NdbDateTime;

    /// `2020-03-05T10:00:00Z` as microseconds since the Unix epoch.
    const EARLY_MICROS: i64 = 1_583_402_400_000_000;

    fn column(name: &str, data_type: SqlDataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        }
    }

    /// `(id INT PRIMARY KEY, n INT, at TIMESTAMP, s SMALLINT)` on the
    /// schemaless engine, the engine that stores the planner's value verbatim.
    fn target() -> CollectionInfo {
        let mut id = column("id", SqlDataType::Int64);
        id.is_primary_key = true;
        let mut small = column("s", SqlDataType::Int64);
        small.int_width = Some(IntWidth::I16);
        CollectionInfo {
            name: "t".into(),
            engine: EngineType::DocumentSchemaless,
            columns: vec![
                id,
                column("n", SqlDataType::Int64),
                column("at", SqlDataType::Timestamp),
                small,
            ],
            primary_key: Some("id".into()),
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
        }
    }

    /// Plan the first WHEN clause of `MERGE INTO t t USING t s ON t.id = s.id
    /// <when>`.
    fn action(when: &str) -> Result<MergePlanAction> {
        let sql = format!("MERGE INTO t t USING t s ON t.id = s.id {when}");
        let statements = parse_sql(&sql)?;
        let ast::Statement::Merge(merge) = &statements[0] else {
            panic!("expected a MERGE statement");
        };
        let target = target();
        let relation = |alias: &str| ResolvedTable {
            name: "t".into(),
            alias: Some(alias.into()),
            info: target.clone(),
        };
        let target_scope = TableScope::single(relation("t"))?;
        let mut scope = TableScope::new();
        scope.add(relation("t"))?;
        scope.add_qualified_only(relation("s"))?;
        convert_merge_action(&merge.clauses[0].action, &scope, &target_scope, &target)
    }

    fn literal(expr: &SqlExpr) -> SqlValue {
        match expr {
            SqlExpr::Literal(value) => value.clone(),
            other => panic!("expected a literal, got {other:?}"),
        }
    }

    /// The INSERT arm coerces each literal to its bound column's declared
    /// type and leaves a source column reference untouched.
    #[test]
    fn insert_arm_literals_take_the_declared_column_types() {
        let action =
            action("WHEN NOT MATCHED THEN INSERT (id, n, at) VALUES (s.id, '1', 1583402400000)")
                .expect("plans");
        let MergePlanAction::Insert { columns, values } = action else {
            panic!("expected an INSERT action, got {action:?}");
        };
        assert_eq!(columns, vec!["id", "n", "at"]);
        assert!(
            matches!(values[0], SqlExpr::Column { .. }),
            "a source reference stays an expression: {:?}",
            values[0]
        );
        assert_eq!(literal(&values[1]), SqlValue::Int(1));
        assert_eq!(
            literal(&values[2]),
            SqlValue::Timestamp(NdbDateTime::from_micros(EARLY_MICROS))
        );
    }

    /// The UPDATE arm coerces its literals the same way.
    #[test]
    fn update_arm_literals_take_the_declared_column_types() {
        let action =
            action("WHEN MATCHED THEN UPDATE SET n = '2', at = 1583402400000").expect("plans");
        let MergePlanAction::Update { assignments } = action else {
            panic!("expected an UPDATE action, got {action:?}");
        };
        assert_eq!(literal(&assignments[0].1), SqlValue::Int(2));
        assert_eq!(
            literal(&assignments[1].1),
            SqlValue::Timestamp(NdbDateTime::from_micros(EARLY_MICROS))
        );
    }

    /// A literal the column cannot hold is refused naming the column, on
    /// both arms: text into INT, a boolean into TIMESTAMP, and an integer
    /// past the declared SMALLINT width.
    #[test]
    fn a_literal_the_column_cannot_hold_is_refused_naming_the_column() {
        let err = action("WHEN NOT MATCHED THEN INSERT (id, n) VALUES (s.id, 'abc')")
            .expect_err("'abc' does not fit INT");
        assert!(err.to_string().contains("'n'"), "{err}");

        let err =
            action("WHEN MATCHED THEN UPDATE SET at = true").expect_err("true is not an instant");
        assert!(err.to_string().contains("'at'"), "{err}");

        let err = action("WHEN NOT MATCHED THEN INSERT (id, s) VALUES (s.id, 999999)")
            .expect_err("999999 does not fit SMALLINT");
        assert!(
            matches!(err, SqlError::IntegerOutOfRange { ref column, .. } if column == "s"),
            "{err}"
        );
    }

    /// The primary key keeps its literal as written on both arms, like every
    /// `VALUES` and `SET` path.
    #[test]
    fn the_primary_key_literal_is_exempt() {
        let action = action("WHEN NOT MATCHED THEN INSERT (id, n) VALUES ('7', 1)").expect("plans");
        let MergePlanAction::Insert { values, .. } = action else {
            panic!("expected an INSERT action, got {action:?}");
        };
        assert_eq!(literal(&values[0]), SqlValue::String("7".into()));
    }

    /// A column-less INSERT arm binds to the declared column order, so its
    /// literals still find their declared types.
    #[test]
    fn a_column_less_insert_arm_binds_positionally() {
        let planned = action("WHEN NOT MATCHED THEN INSERT VALUES (s.id, '3', 1583402400000)")
            .expect("plans");
        let MergePlanAction::Insert { columns, values } = planned else {
            panic!("expected an INSERT action, got {planned:?}");
        };
        assert_eq!(columns, vec!["id", "n", "at"]);
        assert_eq!(literal(&values[1]), SqlValue::Int(3));
        assert_eq!(
            literal(&values[2]),
            SqlValue::Timestamp(NdbDateTime::from_micros(EARLY_MICROS))
        );

        let err = action("WHEN NOT MATCHED THEN INSERT VALUES (1, 2, 3, 4, 5)")
            .expect_err("more values than declared columns");
        assert!(
            matches!(err, SqlError::InsertColumnArityMismatch { .. }),
            "{err}"
        );
    }
}
