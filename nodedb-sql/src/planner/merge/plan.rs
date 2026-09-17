// SPDX-License-Identifier: Apache-2.0

//! MERGE statement planning: target and source resolution, the ON clause,
//! and dispatch to the engine rules.
//!
//! Translates `sqlparser::ast::Statement::Merge` into `SqlPlan::Merge`.
//! Supported engines: `document_schemaless`, `document_strict`.
//! All other engines return `SqlError::Unsupported`.

use nodedb_types::DatabaseId;
use sqlparser::ast;

use super::super::ast_helpers::qualified_ident_pair;
use super::actions::convert_merge_clauses;
use crate::engine_rules::{self, MergeParams, ScanParams};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::resolver::columns::{ResolvedTable, TableScope};
use crate::temporal::TemporalScope;
use crate::types::*;

/// Plan a `MERGE INTO target USING source ON ... WHEN ... THEN ...` statement.
pub fn plan_merge(stmt: &ast::Statement, catalog: &dyn SqlCatalog) -> Result<Vec<SqlPlan>> {
    let ast::Statement::Merge(merge) = stmt else {
        return Err(SqlError::Parse {
            detail: "expected MERGE statement".into(),
        });
    };

    if merge.clauses.is_empty() {
        return Err(SqlError::Parse {
            detail: "MERGE statement requires at least one WHEN arm".into(),
        });
    }

    // ── Resolve target ──
    let (target_name, target_alias) = extract_table_factor_name_alias(&merge.table)?;
    // The ON clause uses the alias (or table name if no alias) as the qualifier.
    let target_ref = target_alias.as_deref().unwrap_or(target_name.as_str());
    let target_info = catalog
        .get_collection(DatabaseId::DEFAULT, &target_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: target_name.clone(),
        })?;

    // ── Resolve source ──
    let source_plan = plan_merge_source(&merge.source, catalog)?;
    let source_alias = merge_source_alias(&merge.source, &source_plan)?;

    // ── Column namespace: target and source are both addressable ──
    let target_table = ResolvedTable {
        name: target_name.clone(),
        alias: target_alias.clone(),
        info: target_info.clone(),
    };
    let target_scope = TableScope::single(target_table.clone())?;
    let mut scope = TableScope::new();
    scope.add(target_table)?;
    // The source is qualified-only: a bare name in an ON or WHEN clause
    // resolves to the target column, matching the engine's MERGE semantics.
    scope.add_qualified_only(merge_source_relation(
        &merge.source,
        &source_alias,
        catalog,
    )?)?;

    // ── Parse ON clause into equi-join columns ──
    let (target_join_col, source_join_col) =
        extract_merge_equijoin(&merge.on, target_ref, &source_alias, &scope)?;

    // ── Convert WHEN clauses ──
    let clauses = convert_merge_clauses(
        &merge.clauses,
        target_ref,
        &scope,
        &target_scope,
        &target_info,
    )?;

    // ── Dispatch to engine rules ──
    let rules = engine_rules::resolve_engine_rules(target_info.engine);
    rules.plan_merge(MergeParams {
        collection: target_name,
        source: Box::new(source_plan),
        target_join_col,
        source_join_col,
        source_alias,
        clauses,
        // sqlparser folds MSSQL's `OUTPUT ... [INTO tbl]` and Postgres'
        // `RETURNING ...` into one `output` field. Only the RETURNING form
        // sends rows back to the client; `OUTPUT ... INTO` redirects them into
        // another table and must not be reported as a row-returning statement.
        returning: matches!(&merge.output, Some(ast::OutputClause::Returning { .. })),
    })
}

// ── Source planning ────────────────────────────────────────────────────────

/// Plan the USING <source> clause.
///
/// Supports:
/// - Table name: `USING src_table ON ...`
/// - Derived subquery: `USING (SELECT ...) AS alias ON ...`
/// - VALUES constructor: treated as a subquery alias.
fn plan_merge_source(factor: &ast::TableFactor, catalog: &dyn SqlCatalog) -> Result<SqlPlan> {
    match factor {
        ast::TableFactor::Table { name, alias, .. } => {
            let source_name = normalize_object_name_checked(name)?;
            let source_info = catalog
                .get_collection(DatabaseId::DEFAULT, &source_name)?
                .ok_or_else(|| SqlError::UnknownTable {
                    name: source_name.clone(),
                })?;
            let alias_str = alias
                .as_ref()
                .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
                .transpose()?;
            let source_rules = engine_rules::resolve_engine_rules(source_info.engine);
            source_rules.plan_scan(ScanParams {
                collection: source_name,
                alias: alias_str,
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
                window_functions: Vec::new(),
                indexes: Vec::new(),
                temporal: TemporalScope::default(),
                bitemporal: source_info.bitemporal,
            })
        }
        ast::TableFactor::Derived {
            lateral: _,
            subquery,
            alias,
            sample: _,
        } => {
            use crate::functions::registry::FunctionRegistry;
            let alias_name = alias
                .as_ref()
                .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
                .transpose()?
                .unwrap_or_else(|| "source".to_string());
            let functions = FunctionRegistry::new();
            let plan = crate::planner::select::plan_query(
                subquery,
                catalog,
                &functions,
                TemporalScope::default(),
            )?;
            // Wrap in an alias scan-like node; for Merge we pass the sub-plan
            // directly. The alias is tracked separately via `source_alias`.
            let _ = alias_name;
            Ok(plan)
        }
        other => Err(SqlError::Unsupported {
            detail: format!(
                "MERGE USING source type not supported: {other}; \
                 use a table name or a subquery"
            ),
        }),
    }
}

/// The source relation as it appears in the MERGE column namespace.
///
/// A named table resolves through the catalog. A derived subquery or VALUES
/// constructor has no declared schema, so it exposes whatever it projects.
fn merge_source_relation(
    factor: &ast::TableFactor,
    source_alias: &str,
    catalog: &dyn SqlCatalog,
) -> Result<ResolvedTable> {
    if let ast::TableFactor::Table { name, .. } = factor {
        let source_name = normalize_object_name_checked(name)?;
        let info = catalog
            .get_collection(DatabaseId::DEFAULT, &source_name)?
            .ok_or_else(|| SqlError::UnknownTable {
                name: source_name.clone(),
            })?;
        return Ok(ResolvedTable {
            name: source_name,
            alias: Some(source_alias.to_string()),
            info,
        });
    }
    Ok(ResolvedTable {
        name: source_alias.to_string(),
        alias: None,
        info: CollectionInfo {
            name: source_alias.to_string(),
            engine: EngineType::DocumentSchemaless,
            columns: Vec::new(),
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            // The alias exposes whatever the subquery projects.
            open_schema: true,
        },
    })
}

/// Determine the alias used to qualify source-column references in WHEN arms.
fn merge_source_alias(factor: &ast::TableFactor, source_plan: &SqlPlan) -> Result<String> {
    match factor {
        ast::TableFactor::Table { name, alias, .. } => {
            if let Some(alias) = alias {
                crate::reserved::check_ast_identifier(&alias.name)
            } else {
                normalize_object_name_checked(name)
            }
        }
        ast::TableFactor::Derived { alias, .. } => alias
            .as_ref()
            .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
            .transpose()
            .map(|alias| alias.unwrap_or_else(|| "source".to_string())),
        _ => Ok(match source_plan {
            SqlPlan::Scan {
                collection: _,
                alias: Some(alias),
                ..
            } => alias.clone(),
            SqlPlan::Scan { collection, .. } => collection.clone(),
            _ => "source".to_string(),
        }),
    }
}

// ── ON clause parsing ──────────────────────────────────────────────────────

/// Extract a single equi-join predicate of the form `target.col = source.col`
/// from the MERGE ON expression.  Returns `(target_col, source_col)`.
fn extract_merge_equijoin(
    on: &ast::Expr,
    target_ref: &str,
    source_ref: &str,
    scope: &TableScope,
) -> Result<(String, String)> {
    if let ast::Expr::BinaryOp {
        left,
        op: ast::BinaryOperator::Eq,
        right,
    } = on
    {
        let lhs = qualified_ident_pair(left);
        let rhs = qualified_ident_pair(right);
        match (lhs, rhs) {
            (Some((lt, lc)), Some((rt, rc))) => {
                if lt == target_ref && rt == source_ref {
                    scope.check_name(Some(&lt), &lc)?;
                    scope.check_name(Some(&rt), &rc)?;
                    return Ok((lc, rc));
                }
                if lt == source_ref && rt == target_ref {
                    scope.check_name(Some(&lt), &lc)?;
                    scope.check_name(Some(&rt), &rc)?;
                    return Ok((rc, lc));
                }
            }
            // Unqualified bare-column references: assume target.col = source.col
            // pattern when one side is unqualified.
            (Some((t, c)), None) if t == source_ref => {
                if let ast::Expr::Identifier(ident) = right.as_ref() {
                    let target_col = normalize_ident(ident);
                    scope.check_name(Some(&t), &c)?;
                    scope.check_name(Some(target_ref), &target_col)?;
                    return Ok((target_col, c));
                }
            }
            (None, Some((t, c))) if t == source_ref => {
                if let ast::Expr::Identifier(ident) = left.as_ref() {
                    let target_col = normalize_ident(ident);
                    scope.check_name(Some(&t), &c)?;
                    scope.check_name(Some(target_ref), &target_col)?;
                    return Ok((target_col, c));
                }
            }
            _ => {}
        }
    }
    Err(SqlError::Unsupported {
        detail: format!(
            "MERGE ON clause must be a single equi-join predicate of the form \
             `{target_ref}.col = {source_ref}.col`; complex ON expressions are not \
             yet supported"
        ),
    })
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn extract_table_factor_name_alias(factor: &ast::TableFactor) -> Result<(String, Option<String>)> {
    match factor {
        ast::TableFactor::Table { name, alias, .. } => {
            let table_name = normalize_object_name_checked(name)?;
            let alias_str = alias
                .as_ref()
                .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
                .transpose()?;
            Ok((table_name, alias_str))
        }
        other => Err(SqlError::Unsupported {
            detail: format!("MERGE target must be a plain table name, not: {other}"),
        }),
    }
}
