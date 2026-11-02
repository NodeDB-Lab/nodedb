// SPDX-License-Identifier: Apache-2.0

//! How a column reference names its field in the rows a clause reads.
//!
//! A join's merged rows key every field `qualifier.column`, so a clause read
//! over them rewrites each column reference to its owning relation's
//! qualifier. A join aggregate's group rows key fields by bare column name,
//! so a clause read over them drops the qualifier.

use crate::error::Result;
use crate::resolver::columns::{ResolvedTable, TableScope};

/// How a column reference to a relation of the scope names its field in the
/// rows the clause reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ColumnNaming {
    /// The reference keeps the qualifier it is written with.
    #[default]
    AsWritten,
    /// The rows are a join's merged rows, keyed `qualifier.column`. Every
    /// reference carries its owning relation's row qualifier.
    JoinRow,
    /// The rows are a join aggregate's output rows, keyed by bare column
    /// name. Every reference drops its relation qualifier.
    OutputRow,
    /// The rows are a join write's merged rows (`UPDATE ... FROM`, MERGE).
    /// The write target is the scope's first relation, and its fields are
    /// keyed by bare column name. Every other relation's fields are keyed
    /// `ref_name.column`.
    JoinWriteRow,
}

/// Where an unqualified column reference resolves.
#[derive(Debug, Clone, Copy)]
pub enum BareColumn<'a> {
    /// A SELECT output name.
    Output,
    /// A relation of this scope.
    Relation(&'a ResolvedTable),
    /// A relation of an enclosing query.
    Outer,
}

impl TableScope {
    /// The qualifier an unqualified column reference is rewritten to.
    ///
    /// `Some` only over a join's merged rows, for a column of one of this
    /// scope's own relations that the merged row keys by qualifier. The
    /// reference is checked in every naming.
    pub fn bare_column_qualifier(&self, column: &str) -> Result<Option<String>> {
        let resolved = self.resolve_bare(column)?;
        Ok(match (self.column_naming(), resolved) {
            (ColumnNaming::JoinRow, BareColumn::Relation(table)) => {
                table.row_qualifier().map(str::to_owned)
            }
            (ColumnNaming::JoinWriteRow, BareColumn::Relation(table)) => {
                self.join_write_qualifier(table)
            }
            (
                ColumnNaming::JoinRow | ColumnNaming::JoinWriteRow,
                BareColumn::Output | BareColumn::Outer,
            )
            | (ColumnNaming::AsWritten | ColumnNaming::OutputRow, _) => None,
        })
    }

    /// The qualifier a qualified column reference converts with.
    ///
    /// A qualifier naming one of this scope's own relations becomes that
    /// relation's row qualifier over a join's merged rows, and is dropped
    /// over a join aggregate's output rows. Any other qualifier, and every
    /// qualifier in `AsWritten` naming, is kept.
    pub fn qualified_column_qualifier(&self, table_ref: &str) -> Option<String> {
        match (self.column_naming(), self.table_by_ref(table_ref)) {
            (ColumnNaming::JoinRow, Some(table)) => table.row_qualifier().map(str::to_owned),
            (ColumnNaming::JoinWriteRow, Some(table)) => self.join_write_qualifier(table),
            (ColumnNaming::OutputRow, Some(_)) => None,
            (ColumnNaming::AsWritten, _)
            | (
                ColumnNaming::JoinRow | ColumnNaming::OutputRow | ColumnNaming::JoinWriteRow,
                None,
            ) => Some(table_ref.to_owned()),
        }
    }

    /// The qualifier a join write's merged row keys `table`'s fields by:
    /// none for the write target, the reference name for any other relation.
    fn join_write_qualifier(&self, table: &ResolvedTable) -> Option<String> {
        let is_target = self
            .first_relation()
            .is_some_and(|target| target.ref_name() == table.ref_name());
        (!is_target).then(|| table.ref_name().to_owned())
    }
}
