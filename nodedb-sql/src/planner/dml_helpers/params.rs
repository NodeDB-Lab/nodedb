// SPDX-License-Identifier: Apache-2.0

//! Parameter structs passed into DML planning helpers.

use sqlparser::ast;

use crate::catalog::SqlCatalog;
use crate::types::*;

/// Parameters for [`super::build_kv_insert_plan`], shared by plain `INSERT`,
/// `UPSERT`, and `INSERT ... ON CONFLICT (key) DO UPDATE` against the KV
/// engine — the three paths differ only in `intent` and
/// `on_conflict_updates`.
pub(crate) struct KvInsertParams<'a> {
    pub collection: String,
    pub columns: &'a [String],
    pub rows_ast: &'a [ast::Parens<Vec<ast::Expr>>],
    pub intent: KvInsertIntent,
    pub on_conflict_updates: Vec<(String, SqlExpr)>,
    /// Schema-defined primary-key column name from `CollectionInfo::primary_key`.
    /// When supplied, that column is used as the KV key regardless of
    /// whether it is named `"key"`. Falls back to the literal name `"key"`
    /// when `None` (legacy / generic KV collections).
    pub pk_col: Option<&'a str>,
    pub declared_columns: &'a [ColumnInfo],
    pub catalog: &'a dyn SqlCatalog,
}
