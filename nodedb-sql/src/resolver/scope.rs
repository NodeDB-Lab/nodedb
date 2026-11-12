// SPDX-License-Identifier: Apache-2.0

//! The column namespace an expression converts against.

use crate::error::Result;
use crate::resolver::column_naming::BareColumn;
use crate::resolver::columns::TableScope;
use crate::types::ColumnInfo;

/// The relations an identifier in this expression can name.
#[derive(Debug, Clone, Copy)]
pub enum ColumnScope<'a> {
    /// No relation is in scope, so no identifier is checkable here.
    ///
    /// Used where an expression has no FROM clause behind it: stored DEFAULT
    /// expressions, partial-index predicates, and the constant folders. A
    /// column reference reaching one of those fails in constant folding as
    /// `SqlError::Unsupported`, which is the correct answer there — the value
    /// is not row-dependent.
    Unchecked,
    /// Identifiers resolve against these relations. One that resolves against
    /// no relation, output alias, or synthetic column raises
    /// `SqlError::UnknownColumn`.
    Relations(&'a TableScope),
}

impl ColumnScope<'_> {
    /// Reject `column`, optionally qualified by `table_ref`, when it names
    /// nothing in this scope.
    pub fn check_column(&self, table_ref: Option<&str>, column: &str) -> Result<()> {
        match self {
            Self::Unchecked => Ok(()),
            Self::Relations(scope) => scope.check_name(table_ref, column),
        }
    }

    /// Check `column`, optionally qualified by `table_ref`, and return the
    /// qualifier its converted reference carries.
    ///
    /// Outside a join's input clauses the qualifier is kept as written. In
    /// them, an unqualified column gets its owning relation's qualifier, so
    /// it reads the `qualifier.column` key of the merged row.
    pub fn column_qualifier(
        &self,
        table_ref: Option<&str>,
        column: &str,
    ) -> Result<Option<String>> {
        match self {
            Self::Unchecked => Ok(table_ref.map(str::to_owned)),
            Self::Relations(scope) => match table_ref {
                None => scope.bare_column_qualifier(column),
                Some(table) => {
                    scope.check_name(Some(table), column)?;
                    Ok(scope.qualified_column_qualifier(table))
                }
            },
        }
    }

    /// The declared column a converted reference reads, optionally qualified
    /// by `table_ref`.
    ///
    /// A qualifier names one of this scope's relations. A bare name reads
    /// the relation it resolves to. A bare name that is also an output name
    /// reads the relation that declares it, so a clause that sees output
    /// names types it as the SELECT list does. `None` under `Unchecked`, for
    /// an enclosing query's column, and for a column no relation declares.
    pub fn declared_column(&self, table_ref: Option<&str>, column: &str) -> Option<&ColumnInfo> {
        let Self::Relations(scope) = self else {
            return None;
        };
        let col = column.to_lowercase();
        let table = match table_ref {
            Some(table) => scope.table_by_ref(table)?,
            None => match scope.resolve_bare(&col).ok()? {
                BareColumn::Relation(table) => table,
                BareColumn::Output => return scope.declared_column(None, &col),
                BareColumn::Outer => return None,
            },
        };
        table.info.columns.iter().find(|c| c.name == col)
    }

    /// Whether an expression here is evaluated once per row of some relation.
    ///
    /// `Unchecked` stands behind stored DEFAULTs, index predicates, and the
    /// constant folders, none of which iterate rows.
    pub fn is_row_scope(&self) -> bool {
        match self {
            Self::Unchecked => false,
            Self::Relations(scope) => scope.is_row_scope(),
        }
    }

    /// Whether a sequence accessor resolves here although rows are in scope.
    ///
    /// Only a SELECT-list scope over a relation says yes; the Control Plane
    /// evaluates that item per output row. `Unchecked` never iterates rows,
    /// so the question does not arise there.
    pub fn allows_cp_functions(&self) -> bool {
        match self {
            Self::Unchecked => false,
            Self::Relations(scope) => scope.allows_cp_functions(),
        }
    }
}
