// SPDX-License-Identifier: Apache-2.0

//! The column namespace an expression converts against.

use crate::error::Result;
use crate::resolver::columns::TableScope;

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
}
