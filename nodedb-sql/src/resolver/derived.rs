// SPDX-License-Identifier: Apache-2.0

//! Output-column inference for derived, LATERAL, and CTE relations.

use sqlparser::ast::{self, Expr, SelectItem, SelectItemQualifiedWildcardKind, SetExpr};

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::resolver::columns::TableScope;
use crate::temporal::TemporalScope;
use crate::types::{CollectionInfo, ColumnInfo, EngineType, SqlCatalog, SqlDataType, SqlPlan};

/// The relation a subquery alias exposes.
///
/// A synthesized relation carries `EngineType::DocumentSchemaless`: it is a
/// MessagePack row stream, and the CTE lowering depends on that. Openness
/// rides on `CollectionInfo::open_schema`, not on the engine.
///
/// `plan` is the subquery's own plan. The search cells (`distance`,
/// `_surrogate`) are read from it, never re-derived from the AST: the planner
/// is the only layer that decides whether an operator form routes to a search.
/// A correlated LATERAL subquery cannot be planned at scope time and carries
/// no plan: it routes no search cells either (the lateral planner handles its
/// shapes directly).
pub fn infer_subquery_relation(
    catalog: &dyn SqlCatalog,
    alias: &str,
    query: &ast::Query,
    plan: Option<&SqlPlan>,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<CollectionInfo> {
    let (mut columns, open_schema) = infer_projection(catalog, query, functions, temporal)?;
    // A search-shaped plan answers with the source columns plus two synthetic
    // cells: `distance` (Float64) and `_surrogate`, the internal row id
    // resolved to the user primary key. Neither is a declared column of a
    // closed-schema source, so derived-relation inference must declare them:
    // without it `s.distance` and `s._surrogate` resolve against no relation
    // and are refused with 42703, while the same projection over an
    // open-schema source runs.
    if plan.is_some_and(SqlPlan::carries_search_cells) {
        if !columns.iter().any(|c| c.name == "distance") {
            columns.push(synthetic_column("distance"));
        }
        if !columns.iter().any(|c| c.name == "_surrogate") {
            columns.push(synthetic_column("_surrogate"));
        }
    }
    Ok(CollectionInfo {
        name: alias.to_string(),
        engine: EngineType::DocumentSchemaless,
        columns,
        primary_key: Some("id".into()),
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        open_schema,
    })
}

/// A relation whose output shape is not inferable, named `alias`.
///
/// Any column name resolves against it. The recursive arm of a `WITH
/// RECURSIVE` names the working table while planning its own body, so that
/// arm's shape is not known yet.
pub fn open_subquery_relation(alias: &str) -> CollectionInfo {
    CollectionInfo {
        name: alias.to_string(),
        engine: EngineType::DocumentSchemaless,
        columns: Vec::new(),
        primary_key: Some("id".into()),
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        open_schema: true,
    }
}

/// Rename an inferred relation's columns to an alias column list.
///
/// `WITH c(a, b) AS (...)` and `FROM (...) AS t(a, b)` name the output
/// positionally. The declared names replace the inferred ones; a type the
/// inferred column carried survives at the same position.
pub fn rename_output_columns(mut info: CollectionInfo, names: &[String]) -> CollectionInfo {
    if names.is_empty() {
        return info;
    }
    let inferred = std::mem::take(&mut info.columns);
    info.columns = names
        .iter()
        .enumerate()
        .map(|(index, name)| match inferred.get(index) {
            Some(column) => ColumnInfo {
                name: name.clone(),
                ..column.clone()
            },
            None => synthetic_column(name),
        })
        .collect();
    info
}

/// The columns a query projects, and whether a name outside them resolves
/// against it.
fn infer_projection(
    catalog: &dyn SqlCatalog,
    query: &ast::Query,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<(Vec<ColumnInfo>, bool)> {
    infer_body(catalog, &query.body, functions, temporal)
}

fn infer_body(
    catalog: &dyn SqlCatalog,
    body: &SetExpr,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<(Vec<ColumnInfo>, bool)> {
    match body {
        SetExpr::Select(select) => infer_select_projection(catalog, select, functions, temporal),
        SetExpr::Query(query) => infer_projection(catalog, query, functions, temporal),
        // A set operation takes its output names from the left arm.
        SetExpr::SetOperation { left, .. } => infer_body(catalog, left, functions, temporal),
        // A row constructor, a `TABLE` command, and a DML body carry no
        // projection list to read names from.
        SetExpr::Values(_)
        | SetExpr::Table(_)
        | SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Delete(_)
        | SetExpr::Merge(_) => Ok((Vec::new(), true)),
    }
}

fn infer_select_projection(
    catalog: &dyn SqlCatalog,
    select: &ast::Select,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<(Vec<ColumnInfo>, bool)> {
    let scope = TableScope::resolve_from(catalog, functions, temporal, &select.from)?;
    let mut columns = Vec::new();
    let mut open = false;

    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for table in scope.tables_in_order() {
                    columns.extend(table.info.columns.iter().cloned());
                    open |= table.info.open_schema;
                }
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                let table_ref = match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        normalize_object_name_checked(name)?
                    }
                    // `STRUCT<...>('x').*` expands a value, not a relation, so
                    // the names it yields are not readable from the FROM clause.
                    SelectItemQualifiedWildcardKind::Expr(_) => {
                        open = true;
                        continue;
                    }
                };
                match scope.table_by_ref(&table_ref) {
                    Some(table) => {
                        columns.extend(table.info.columns.iter().cloned());
                        open |= table.info.open_schema;
                    }
                    // The qualifier names a relation of an enclosing query.
                    None => open = true,
                }
            }
            SelectItem::ExprWithAlias { alias, .. } => {
                columns.push(synthetic_column(&normalize_ident(alias)));
            }
            SelectItem::UnnamedExpr(expr) => columns.push(unnamed_column(&scope, expr)),
            SelectItem::ExprWithAliases { aliases, .. } => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "multi-alias projection ('AS' with {} names) is not supported; \
                         give the expression a single alias",
                        aliases.len()
                    ),
                });
            }
        }
    }

    Ok((columns, open))
}

/// The column an unaliased projection item exposes.
///
/// A bare or two-part column reference keeps the source column's declared
/// type. Anything else takes its rendered form as its name, matching how the
/// projection converter names a computed output.
fn unnamed_column(scope: &TableScope, expr: &Expr) -> ColumnInfo {
    let named = match expr {
        Expr::Identifier(ident) => Some((None, normalize_ident(ident))),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((Some(normalize_ident(&parts[0])), normalize_ident(&parts[1])))
        }
        _ => None,
    };
    match named {
        Some((qualifier, name)) => match scope.declared_column(qualifier.as_deref(), &name) {
            Some(column) => ColumnInfo {
                name,
                ..column.clone()
            },
            None => synthetic_column(&name),
        },
        None => synthetic_column(&format!("{expr}").to_lowercase()),
    }
}

/// A column that resolves by name and declares no type.
fn synthetic_column(name: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type: SqlDataType::Unknown,
        nullable: true,
        is_primary_key: false,
        default: None,
        raw_type: None,
        int_width: None,
        float_width: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SqlCatalogError;
    use nodedb_types::DatabaseId;
    use sqlparser::ast::Statement;

    struct TestCatalog;

    fn strict(name: &str, columns: &[&str]) -> CollectionInfo {
        CollectionInfo {
            name: name.into(),
            engine: EngineType::DocumentStrict,
            columns: columns
                .iter()
                .map(|c| ColumnInfo {
                    name: (*c).into(),
                    data_type: SqlDataType::Int64,
                    nullable: false,
                    is_primary_key: false,
                    default: None,
                    raw_type: None,
                    int_width: None,
                    float_width: None,
                })
                .collect(),
            primary_key: Some("a".into()),
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
        }
    }

    impl SqlCatalog for TestCatalog {
        fn get_collection(
            &self,
            _database_id: DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok(match name {
                "src" => Some(strict("src", &["a", "b"])),
                "loose" => {
                    let mut info = strict("loose", &["x"]);
                    info.engine = EngineType::DocumentSchemaless;
                    info.open_schema = true;
                    Some(info)
                }
                _ => None,
            })
        }
    }

    fn parse_query(sql: &str) -> ast::Query {
        let stmts = crate::parser::statement::parse_sql(sql).expect("parse failed");
        match &stmts[0] {
            Statement::Query(query) => (**query).clone(),
            other => panic!("expected a query, got {other:?}"),
        }
    }

    fn infer(sql: &str) -> CollectionInfo {
        let query = parse_query(sql);
        let functions = crate::functions::registry::FunctionRegistry::new();
        let plan = crate::planner::select::plan_query(
            &query,
            &TestCatalog,
            &functions,
            TemporalScope::default(),
        )
        .expect("plan failed");
        infer_subquery_relation(
            &TestCatalog,
            "t",
            &query,
            Some(&plan),
            &functions,
            TemporalScope::default(),
        )
        .expect("inference failed")
    }

    #[test]
    fn bare_identifier_keeps_the_declared_type() {
        let info = infer("SELECT a FROM src");
        assert_eq!(info.columns.len(), 1);
        assert_eq!(info.columns[0].name, "a");
        assert_eq!(info.columns[0].data_type, SqlDataType::Int64);
        assert!(!info.open_schema);
    }

    #[test]
    fn qualified_identifier_exposes_the_unqualified_name() {
        let info = infer("SELECT i.b FROM src AS i");
        assert_eq!(info.columns.len(), 1);
        assert_eq!(info.columns[0].name, "b");
    }

    #[test]
    fn alias_names_the_output_column() {
        let info = infer("SELECT a + 1 AS total FROM src");
        assert_eq!(info.columns[0].name, "total");
        assert_eq!(info.columns[0].data_type, SqlDataType::Unknown);
    }

    #[test]
    fn star_over_a_closed_source_stays_closed() {
        let info = infer("SELECT * FROM src");
        let names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
        assert!(!info.open_schema);
    }

    #[test]
    fn star_over_an_open_source_stays_open() {
        let info = infer("SELECT * FROM loose");
        assert!(info.open_schema);
    }

    #[test]
    fn set_operation_takes_the_left_arm_shape() {
        let info = infer("SELECT a FROM src UNION ALL SELECT b FROM src");
        assert_eq!(info.columns.len(), 1);
        assert_eq!(info.columns[0].name, "a");
    }

    #[test]
    fn alias_column_list_renames_positionally() {
        let info = rename_output_columns(infer("SELECT a, b FROM src"), &["p".into(), "q".into()]);
        let names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["p", "q"]);
        assert_eq!(info.columns[0].data_type, SqlDataType::Int64);
    }

    #[test]
    fn vector_search_projection_declares_the_synthetic_distance_column() {
        // `SEARCH c USING VECTOR(...)` preprocesses to `ORDER BY
        // vector_distance(...)`; the response layer appends a `distance`
        // cell, so the derived relation must name it even when the source
        // schema is closed.
        let info = infer("SELECT * FROM src ORDER BY vector_distance(b, ARRAY[0.1, 0.2]) LIMIT 2");
        let names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"distance"), "columns: {names:?}");
    }

    #[test]
    fn plain_ordered_projection_has_no_distance_column() {
        let info = infer("SELECT * FROM src ORDER BY b LIMIT 2");
        let names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(!names.contains(&"distance"), "columns: {names:?}");
    }

    /// Every function that routes an ORDER BY to a vector search produces a
    /// `distance` cell, so every one of them must declare it.
    #[test]
    fn every_vector_search_function_declares_the_distance_column() {
        for call in [
            "vector_distance(b, ARRAY[0.1])",
            "vector_cosine_distance(b, ARRAY[0.1])",
            "vector_neg_inner_product(b, ARRAY[0.1])",
        ] {
            let info = infer(&format!("SELECT * FROM src ORDER BY {call} LIMIT 2"));
            let names: Vec<&str> = info.columns.iter().map(|c| c.name.as_str()).collect();
            assert!(names.contains(&"distance"), "{call}: columns {names:?}");
        }
    }
}
