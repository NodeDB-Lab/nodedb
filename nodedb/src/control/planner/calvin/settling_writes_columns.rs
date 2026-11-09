// SPDX-License-Identifier: BUSL-1.1

//! The columns an UPDATE assigns, a predicate reads and a materialized sum
//! settles from. The rule of
//! [`refuse_predicate_after_source_write`](super::refuse_predicate_after_source_write)
//! uses them to pass an earlier UPDATE that cannot move a later write's
//! settlement.
//!
//! Every reader answers `None` or `false` for a column it cannot name: the
//! whole row, a qualified or nested path, an `EXISTS` filter or a filter that
//! does not decode. The rule then refuses.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::{
    DocumentOp, MaterializedSumBinding, PhysicalPlan, UpdateValue,
};
use nodedb_query::expr::{SqlExpr, WHOLE_ROW_COLUMN};

use crate::bridge::scan_filter::{FilterOp, ScanFilter};

/// The source columns of one materialized-sum binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BindingColumns {
    /// The source column that names the target row.
    pub(super) join_column: String,
    /// Every source column the binding's value expression reads.
    pub(super) value_columns: BTreeSet<String>,
}

/// The source columns of `binding`, or `None` when one cannot be named.
pub(super) fn binding_columns(binding: &MaterializedSumBinding) -> Option<BindingColumns> {
    let mut value_columns = BTreeSet::new();
    if !is_plain_column(&binding.join_column)
        || !expr_columns(&binding.value_expr, &mut value_columns)
    {
        return None;
    }
    Some(BindingColumns {
        join_column: binding.join_column.clone(),
        value_columns,
    })
}

/// The assignments of an UPDATE and its declared primary key.
pub(super) type UpdateAssignments<'a> = (&'a [(String, UpdateValue)], Option<&'a str>);

/// The assignments of a point or predicate UPDATE, and its declared primary
/// key. `None` for every other plan.
pub(super) fn update_assignments(plan: &PhysicalPlan) -> Option<UpdateAssignments<'_>> {
    match plan {
        PhysicalPlan::Document(
            DocumentOp::PointUpdate {
                updates,
                declared_primary_key,
                ..
            }
            | DocumentOp::BulkUpdate {
                updates,
                declared_primary_key,
                ..
            },
        ) => Some((updates.as_slice(), declared_primary_key.as_deref())),
        _ => None,
    }
}

/// What a predicate write or a TRUNCATE matches and assigns.
pub(super) struct MatchShape<'a> {
    /// The serialized `Vec<ScanFilter>` of the statement.
    pub(super) filters: &'a [u8],
    /// The serialized row-level security filters.
    pub(super) rls_filters: &'a [u8],
    /// The `SET` assignments. Empty for a delete or a TRUNCATE.
    pub(super) updates: &'a [(String, UpdateValue)],
    /// A delete or a TRUNCATE settles every binding of its source.
    pub(super) removes_rows: bool,
    pub(super) declared_primary_key: Option<&'a str>,
}

/// The shape of a `BulkUpdate`, `BulkDelete` or `Truncate`, or `None` for any
/// other plan.
pub(super) fn match_shape(plan: &PhysicalPlan) -> Option<MatchShape<'_>> {
    match plan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            filters,
            rls_filters,
            updates,
            declared_primary_key,
            ..
        }) => Some(MatchShape {
            filters,
            rls_filters,
            updates,
            removes_rows: false,
            declared_primary_key: declared_primary_key.as_deref(),
        }),
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            filters,
            rls_filters,
            declared_primary_key,
            ..
        }) => Some(MatchShape {
            filters,
            rls_filters,
            updates: &[],
            removes_rows: true,
            declared_primary_key: declared_primary_key.as_deref(),
        }),
        PhysicalPlan::Document(DocumentOp::Truncate {
            declared_primary_key,
            ..
        }) => Some(MatchShape {
            filters: &[],
            rls_filters: &[],
            updates: &[],
            removes_rows: true,
            declared_primary_key: declared_primary_key.as_deref(),
        }),
        _ => None,
    }
}

/// Every column a serialized filter set reads. Empty bytes match every row
/// and read no column.
pub(super) fn filter_columns(bytes: &[u8]) -> Option<BTreeSet<String>> {
    let mut out = BTreeSet::new();
    if bytes.is_empty() {
        return Some(out);
    }
    let filters: Vec<ScanFilter> = zerompk::from_msgpack(bytes).ok()?;
    filters
        .iter()
        .all(|filter| scan_filter_columns(filter, &mut out))
        .then_some(out)
}

fn scan_filter_columns(filter: &ScanFilter, out: &mut BTreeSet<String>) -> bool {
    let own = match filter.op {
        FilterOp::MatchAll | FilterOp::Or => true,
        FilterOp::Expr => filter.expr.is_some(),
        FilterOp::Exists | FilterOp::NotExists => false,
        FilterOp::GtColumn
        | FilterOp::GteColumn
        | FilterOp::LtColumn
        | FilterOp::LteColumn
        | FilterOp::EqColumn
        | FilterOp::NeColumn => match &filter.value {
            nodedb_types::Value::String(other) => {
                plain_column(&filter.field, out) && plain_column(other, out)
            }
            _ => false,
        },
        FilterOp::Eq
        | FilterOp::Ne
        | FilterOp::Gt
        | FilterOp::Gte
        | FilterOp::Lt
        | FilterOp::Lte
        | FilterOp::Contains
        | FilterOp::Like
        | FilterOp::NotLike
        | FilterOp::Ilike
        | FilterOp::NotIlike
        | FilterOp::In
        | FilterOp::NotIn
        | FilterOp::IsNull
        | FilterOp::IsNotNull
        | FilterOp::ArrayContains
        | FilterOp::ArrayContainsAll
        | FilterOp::ArrayOverlap => plain_column(&filter.field, out),
    };
    own && filter
        .clauses
        .iter()
        .flatten()
        .all(|clause| scan_filter_columns(clause, out))
        && filter
            .expr
            .as_ref()
            .is_none_or(|expr| expr_columns(expr, out))
}

/// Add every column `expr` reads to `out`. Returns `false` when one cannot
/// be named.
pub(super) fn expr_columns(expr: &SqlExpr, out: &mut BTreeSet<String>) -> bool {
    match expr {
        SqlExpr::Column(name) | SqlExpr::OldColumn(name) | SqlExpr::ExcludedColumn(name) => {
            plain_column(name, out)
        }
        SqlExpr::Literal(_) => true,
        SqlExpr::BinaryOp { left, right, .. } => {
            expr_columns(left, out) && expr_columns(right, out)
        }
        SqlExpr::Negate(inner) | SqlExpr::Cast { expr: inner, .. } => expr_columns(inner, out),
        SqlExpr::IsNull { expr: inner, .. } => expr_columns(inner, out),
        SqlExpr::Function { args, .. } | SqlExpr::Coalesce(args) => {
            args.iter().all(|arg| expr_columns(arg, out))
        }
        SqlExpr::NullIf(first, second) => expr_columns(first, out) && expr_columns(second, out),
        SqlExpr::Case {
            operand,
            when_thens,
            else_expr,
        } => {
            operand.as_ref().is_none_or(|op| expr_columns(op, out))
                && when_thens
                    .iter()
                    .all(|(when, then)| expr_columns(when, out) && expr_columns(then, out))
                && else_expr.as_ref().is_none_or(|e| expr_columns(e, out))
        }
    }
}

/// Add `name` to `out` when [`is_plain_column`] accepts it.
fn plain_column(name: &str, out: &mut BTreeSet<String>) -> bool {
    if !is_plain_column(name) {
        return false;
    }
    out.insert(name.to_owned());
    true
}

/// Whether `name` names one top-level column. The whole row, an empty name
/// and a dotted path do not.
pub(super) fn is_plain_column(name: &str) -> bool {
    !name.is_empty() && !name.contains('.') && name != WHOLE_ROW_COLUMN
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(field: &str, op: FilterOp, value: nodedb_types::Value) -> ScanFilter {
        ScanFilter {
            field: field.to_owned(),
            op,
            value,
            clauses: Vec::new(),
            expr: None,
        }
    }

    fn encode(filters: Vec<ScanFilter>) -> Vec<u8> {
        zerompk::to_msgpack_vec(&filters).expect("encode test filters")
    }

    fn names(columns: &BTreeSet<String>) -> Vec<&str> {
        columns.iter().map(String::as_str).collect()
    }

    #[test]
    fn a_plain_filter_reads_its_field() {
        let bytes = encode(vec![filter("amount", FilterOp::Gt, "8".into())]);
        let columns = filter_columns(&bytes).expect("a plain filter names its column");
        assert_eq!(names(&columns), ["amount"]);
    }

    #[test]
    fn an_or_filter_reads_every_clause() {
        let mut or = filter("", FilterOp::Or, nodedb_types::Value::Null);
        or.clauses = vec![
            vec![filter("account_id", FilterOp::Eq, "acc".into())],
            vec![filter("fee", FilterOp::Lt, "2".into())],
        ];
        let columns = filter_columns(&encode(vec![or])).expect("each clause names its column");
        assert_eq!(names(&columns), ["account_id", "fee"]);
    }

    #[test]
    fn a_column_comparison_reads_both_columns() {
        let bytes = encode(vec![filter("amount", FilterOp::GtColumn, "fee".into())]);
        let columns = filter_columns(&bytes).expect("both columns are named");
        assert_eq!(names(&columns), ["amount", "fee"]);
    }

    #[test]
    fn empty_filter_bytes_read_no_column() {
        assert_eq!(filter_columns(&[]), Some(BTreeSet::new()));
    }

    #[test]
    fn an_unnamed_column_is_unknown() {
        let nested = encode(vec![filter("meta.kind", FilterOp::Eq, "x".into())]);
        assert_eq!(filter_columns(&nested), None);
        let exists = encode(vec![filter(
            "fee",
            FilterOp::Exists,
            nodedb_types::Value::Null,
        )]);
        assert_eq!(filter_columns(&exists), None);
        assert_eq!(filter_columns(&[0xc1]), None, "bytes that do not decode");
        let whole_row = SqlExpr::Column(WHOLE_ROW_COLUMN.to_owned());
        assert!(!expr_columns(&whole_row, &mut BTreeSet::new()));
    }

    #[test]
    fn a_binding_names_its_join_and_value_columns() {
        let binding = MaterializedSumBinding {
            target_collection: "ptv_acct".to_owned(),
            target_column: "balance".to_owned(),
            join_column: "account_id".to_owned(),
            value_expr: SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Column("amount".to_owned())),
                op: nodedb_query::expr::BinaryOp::Sub,
                right: Box::new(SqlExpr::Column("fee".to_owned())),
            },
            declared_primary_key: None,
        };
        let columns = binding_columns(&binding).expect("every column is named");
        assert_eq!(columns.join_column, "account_id");
        assert_eq!(names(&columns.value_columns), ["amount", "fee"]);
    }
}
