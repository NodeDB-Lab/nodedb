// SPDX-License-Identifier: Apache-2.0

//! Column DEFAULTs compiled once per statement and evaluated per row.

use super::convert::{default_value_to_sql, sql_value_to_ndb};
use super::kind::{DefaultKind, keyword_generator, parametric_or_literal};
use crate::catalog::SqlCatalog;
use crate::error::SqlError;
use crate::types::{ColumnInfo, SqlExpr, SqlValue};

/// One column DEFAULT, classified and parsed.
///
/// Compilation is the only step that reads the declaration text, so a caller
/// holding a `CompiledDefault` cannot parse the expression a second time.
#[derive(Debug, Clone)]
pub struct CompiledDefault {
    column: String,
    /// The declaration text, kept for the `UnevaluableDefault` message only.
    text: String,
    kind: DefaultKind,
    volatile: bool,
}

impl CompiledDefault {
    /// Compile the DEFAULT declared on `column` without evaluating it.
    ///
    /// A parse error propagates verbatim, so the DDL gate can map an
    /// unregistered function name onto SQLSTATE `42883`.
    ///
    /// A sequence accessor is parsed, never called, so declaring a column must
    /// never advance a sequence.
    pub fn declare(column: &str, expr: &str) -> crate::Result<Self> {
        let kind = classify(column, expr)?;
        let volatile = match &kind {
            DefaultKind::Generator(_) => true,
            DefaultKind::Literal(_) => false,
            DefaultKind::Expr(parsed) => crate::types::plan::expr_is_volatile(parsed),
        };
        Ok(Self {
            column: column.to_string(),
            text: expr.to_string(),
            kind,
            volatile,
        })
    }

    /// Compile the DEFAULT declared on `column` for row materialization.
    ///
    /// An expression the parser refuses raises [`SqlError::UnevaluableDefault`]
    /// rather than a parse error: DDL already refused those at declaration, so
    /// a statement reaching here found one the catalog cannot resolve.
    pub fn compile(column: &str, expr: &str) -> crate::Result<Self> {
        Self::declare(column, expr).map_err(|error| match error {
            SqlError::Parse { .. } | SqlError::UndefinedFunction { .. } => {
                unevaluable(column, expr)
            }
            other => other,
        })
    }

    /// The column this DEFAULT fills.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Whether this DEFAULT produces a fresh value on every evaluation.
    ///
    /// A plan carrying one is never admitted to the plan cache, or the cache
    /// replays one execution's value into every later one.
    pub fn is_volatile(&self) -> bool {
        self.volatile
    }

    /// Evaluate to one value. `catalog` resolves `nextval` and `currval`.
    ///
    /// Takes no expression text, so the per-row path cannot re-parse.
    pub fn evaluate(&self, catalog: &dyn SqlCatalog) -> crate::Result<nodedb_types::Value> {
        match &self.kind {
            DefaultKind::Generator(generator) => generator.generate(),
            DefaultKind::Literal(value) => Ok(value.clone()),
            DefaultKind::Expr(parsed) => self.evaluate_expr(parsed, catalog),
        }
    }

    /// Resolve a parsed DEFAULT through the catalog, then the const-folder.
    fn evaluate_expr(
        &self,
        parsed: &SqlExpr,
        catalog: &dyn SqlCatalog,
    ) -> crate::Result<nodedb_types::Value> {
        if let Some(value) =
            crate::planner::catalog_expr_fold::eval_sequence_accessor(parsed, catalog)?
        {
            return Ok(sql_value_to_ndb(value));
        }
        // `Once`: a materialized DEFAULT serves this insert only, and an INSERT
        // plan carrying a volatile DEFAULT is never admitted to the plan cache.
        let folded = crate::planner::const_fold::fold_constant_scoped(
            parsed,
            crate::planner::const_fold::default_registry(),
            crate::planner::const_fold::FoldScope::Once,
        )
        .map_err(|_| unevaluable(&self.column, &self.text))?
        .ok_or_else(|| unevaluable(&self.column, &self.text))?;
        Ok(sql_value_to_ndb(folded))
    }
}

/// Every declared DEFAULT of one collection, compiled once for a statement.
///
/// Build this OUTSIDE the row loop. `materialize_row` then fills each row from
/// the parsed forms, so a multi-row `VALUES` clause parses each declaration
/// exactly once however many rows it carries.
#[derive(Debug, Clone, Default)]
pub struct ColumnDefaults {
    defaults: Vec<CompiledDefault>,
}

impl ColumnDefaults {
    /// Compile the catalog's `(column_name, default_expr)` list.
    pub fn compile_pairs(pairs: &[(String, String)]) -> crate::Result<Self> {
        let defaults = pairs
            .iter()
            .map(|(column, expr)| CompiledDefault::compile(column, expr))
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(Self { defaults })
    }

    /// Compile every declared column that carries a DEFAULT.
    pub fn compile_columns(columns: &[ColumnInfo]) -> crate::Result<Self> {
        let defaults = columns
            .iter()
            .filter_map(|column| {
                column
                    .default
                    .as_deref()
                    .map(|expr| CompiledDefault::compile(&column.name, expr))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(Self { defaults })
    }

    /// Whether no column declares a DEFAULT.
    pub fn is_empty(&self) -> bool {
        self.defaults.is_empty()
    }

    /// Fill in every column of `row` that declares a DEFAULT and the statement
    /// omitted.
    ///
    /// Each entry evaluates at most once per row, so a `nextval` DEFAULT
    /// allocates exactly one value per row.
    ///
    /// A column the statement supplied stays untouched, an explicit `NULL`
    /// included: `NULL` is a value the author chose, and overwriting it with
    /// the default makes storing one impossible.
    ///
    /// Returns whether any default it materialized was volatile, so the caller
    /// can keep the plan out of the plan cache.
    pub fn materialize_row(
        &self,
        row: &mut Vec<(String, SqlValue)>,
        catalog: &dyn SqlCatalog,
    ) -> crate::Result<bool> {
        let mut volatile = false;
        for default in &self.defaults {
            if row.iter().any(|(name, _)| name == default.column()) {
                continue;
            }
            let evaluated = default.evaluate(catalog)?;
            let value = default_value_to_sql(default.column(), evaluated)?;
            volatile |= default.is_volatile();
            row.push((default.column().to_string(), value));
        }
        Ok(volatile)
    }
}

/// Check that `expr`, the DEFAULT declared on `column`, can be evaluated.
///
/// DDL calls this to refuse an unevaluable DEFAULT at declaration time. It
/// classifies and parses the expression, and evaluates nothing. Parsing runs
/// the resolver's `FunctionRegistry` gate, so an unregistered function name
/// raises [`SqlError::UndefinedFunction`].
pub fn validate_default_expr(expr: &str, column: &str) -> crate::Result<()> {
    CompiledDefault::declare(column, expr).map(|_| ())
}

/// Classify a DEFAULT into its compiled form.
fn classify(column: &str, expr: &str) -> crate::Result<DefaultKind> {
    let upper = expr.trim().to_uppercase();
    if let Some(generator) = keyword_generator(&upper) {
        return Ok(DefaultKind::Generator(generator));
    }
    if let Some(kind) = parametric_or_literal(expr, &upper)? {
        return Ok(kind);
    }
    let parsed = crate::parse_expr_string(expr)?;
    reject_setval_default(&parsed, column)?;
    Ok(DefaultKind::Expr(parsed))
}

/// Refuse `setval` as a column DEFAULT.
///
/// `setval` moves a sequence rather than reading one, so a column cannot take
/// its result as a value.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqlCatalogError;
    use std::cell::Cell;

    /// A catalog whose only state is a sequence counter, so a test can count
    /// exactly how many times a DEFAULT reached `nextval`.
    #[derive(Default)]
    struct CountingCatalog {
        nextval_calls: Cell<i64>,
    }

    impl SqlCatalog for CountingCatalog {
        fn get_collection(
            &self,
            _database_id: nodedb_types::DatabaseId,
            _name: &str,
        ) -> std::result::Result<Option<crate::types::CollectionInfo>, SqlCatalogError> {
            Ok(None)
        }

        fn sequence_nextval(
            &self,
            _database_id: nodedb_types::DatabaseId,
            _tenant_id: u64,
            _name: &str,
        ) -> crate::Result<i64> {
            let next = self.nextval_calls.get() + 1;
            self.nextval_calls.set(next);
            Ok(next)
        }
    }

    fn pairs(column: &str, expr: &str) -> Vec<(String, String)> {
        vec![(column.to_string(), expr.to_string())]
    }

    #[test]
    fn one_compilation_still_generates_a_fresh_value_per_row() {
        let catalog = CountingCatalog::default();
        let compiled = ColumnDefaults::compile_pairs(&pairs("u", "UUID_V7()")).expect("compiles");
        let mut first = Vec::new();
        let mut second = Vec::new();
        compiled.materialize_row(&mut first, &catalog).expect("row");
        compiled
            .materialize_row(&mut second, &catalog)
            .expect("row");
        assert_ne!(first[0].1, second[0].1, "each row needs its own UUID");
    }

    #[test]
    fn nextval_allocates_exactly_one_value_per_row() {
        let catalog = CountingCatalog::default();
        let compiled =
            ColumnDefaults::compile_pairs(&pairs("id", "nextval('s')")).expect("compiles");
        let mut first = Vec::new();
        let mut second = Vec::new();
        compiled.materialize_row(&mut first, &catalog).expect("row");
        compiled
            .materialize_row(&mut second, &catalog)
            .expect("row");
        assert_eq!(first[0].1, SqlValue::Int(1));
        assert_eq!(second[0].1, SqlValue::Int(2));
        assert_eq!(catalog.nextval_calls.get(), 2);
    }

    #[test]
    fn declaring_a_sequence_default_never_advances_it() {
        let catalog = CountingCatalog::default();
        validate_default_expr("nextval('s')", "id").expect("declaration is valid");
        assert_eq!(
            catalog.nextval_calls.get(),
            0,
            "the DDL gate must parse, never call"
        );
    }

    #[test]
    fn a_supplied_column_keeps_its_value() {
        let catalog = CountingCatalog::default();
        let compiled =
            ColumnDefaults::compile_pairs(&pairs("id", "nextval('s')")).expect("compiles");
        let mut row = vec![("id".to_string(), SqlValue::Int(7))];
        compiled.materialize_row(&mut row, &catalog).expect("row");
        assert_eq!(row, vec![("id".to_string(), SqlValue::Int(7))]);
        assert_eq!(catalog.nextval_calls.get(), 0);
    }

    #[test]
    fn volatility_is_decided_once_at_compile_time() {
        assert!(
            CompiledDefault::declare("u", "UUID_V7()")
                .expect("compiles")
                .is_volatile()
        );
        assert!(
            CompiledDefault::declare("id", "nextval('s')")
                .expect("compiles")
                .is_volatile()
        );
        assert!(
            !CompiledDefault::declare("s", "'active'")
                .expect("compiles")
                .is_volatile()
        );
        assert!(
            !CompiledDefault::declare("n", "1 + 2")
                .expect("compiles")
                .is_volatile()
        );
    }

    #[test]
    fn setval_is_refused_as_a_default() {
        let error = CompiledDefault::declare("id", "setval('s', 10)").expect_err("setval refused");
        assert!(matches!(error, SqlError::SetvalInColumnDefault { .. }));
    }

    #[test]
    fn an_unregistered_function_is_refused_at_declaration() {
        let error = CompiledDefault::declare("a", "no_such_function_here('x')")
            .expect_err("unknown function refused");
        assert!(matches!(error, SqlError::UndefinedFunction { .. }));
    }

    #[test]
    fn an_unregistered_function_is_unevaluable_at_insert() {
        let error = CompiledDefault::compile("a", "no_such_function_here('x')")
            .expect_err("unknown function refused");
        assert!(matches!(error, SqlError::UnevaluableDefault { .. }));
    }
}
