// SPDX-License-Identifier: Apache-2.0

//! `UPDATE target SET ... FROM src WHERE target.col = src.col ...` planning.

use core::ops::ControlFlow;

use nodedb_types::DatabaseId;
use sqlparser::ast::{self, Visit, Visitor};

use super::super::ast_helpers::{
    flatten_and_expr, qualified_ident_pair, strip_and_convert_filters,
};
use super::super::dml_helpers::{
    extract_table_name_from_table_with_joins, refuse_vector_primary_shape,
};
use super::update_delete::convert_assignments;
use crate::engine_rules::{self, UpdateFromParams};
use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name_checked};
use crate::resolver::columns::{BareColumn, ColumnNaming, ResolvedTable, TableScope};
use crate::types::*;

/// Plan `UPDATE target SET ... FROM src WHERE target.col = src.col ...`.
pub(super) fn plan_update_from(
    update: &ast::Update,
    from_kind: &ast::UpdateTableFromKind,
    catalog: &dyn SqlCatalog,
) -> Result<Vec<SqlPlan>> {
    let target_name = extract_table_name_from_table_with_joins(&update.table)?;

    // Extract alias for the target table if present.
    let target_alias: Option<String> = match &update.table.relation {
        ast::TableFactor::Table { alias, .. } => alias
            .as_ref()
            .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
            .transpose()?,
        _ => None,
    };
    let target_ref = target_alias.as_deref().unwrap_or(target_name.as_str());

    let from_tables: &Vec<ast::TableWithJoins> = match from_kind {
        ast::UpdateTableFromKind::AfterSet(tables)
        | ast::UpdateTableFromKind::BeforeSet(tables) => tables,
    };

    // Reject multi-table FROM.
    if from_tables.len() > 1 {
        return Err(SqlError::Unsupported {
            detail: format!(
                "UPDATE ... FROM with {} source tables is not supported; \
                 only a single FROM table is accepted",
                from_tables.len()
            ),
        });
    }
    let from_table = from_tables.first().ok_or_else(|| SqlError::Parse {
        detail: "UPDATE ... FROM requires at least one source table".into(),
    })?;

    // Reject subquery in FROM.
    let source_name = match &from_table.relation {
        ast::TableFactor::Table { name, .. } => normalize_object_name_checked(name)?,
        ast::TableFactor::Derived { .. } => {
            return Err(SqlError::Unsupported {
                detail: "UPDATE ... FROM (subquery) is not supported; \
                     use a CTE: WITH cte AS (SELECT ...) UPDATE t SET ... FROM cte WHERE ..."
                    .into(),
            });
        }
        _ => {
            return Err(SqlError::Unsupported {
                detail: "non-table relation in UPDATE ... FROM is not supported".into(),
            });
        }
    };
    // Reject joins in the FROM source.
    if !from_table.joins.is_empty() {
        return Err(SqlError::Unsupported {
            detail: "JOIN in UPDATE ... FROM source is not supported; \
                     use a CTE to pre-join the source"
                .into(),
        });
    }

    let source_alias: Option<String> = match &from_table.relation {
        ast::TableFactor::Table { alias, .. } => alias
            .as_ref()
            .map(|alias| crate::reserved::check_ast_identifier(&alias.name))
            .transpose()?,
        _ => None,
    };
    let source_ref = source_alias.clone().unwrap_or_else(|| source_name.clone());

    // Validate that the target and source collections exist.
    let target_info = catalog
        .get_collection(DatabaseId::DEFAULT, &target_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: target_name.clone(),
        })?;
    let source_info = catalog
        .get_collection(DatabaseId::DEFAULT, &source_name)?
        .ok_or_else(|| SqlError::UnknownTable {
            name: source_name.clone(),
        })?;
    refuse_vector_primary_shape(&target_info, "UPDATE ... FROM")?;

    // The SET target belongs to the update target; its right-hand side can
    // read either relation.
    let target_scope = TableScope::single(ResolvedTable {
        name: target_name.clone(),
        alias: target_alias.clone(),
        info: target_info.clone(),
    })?;
    let source_scope = TableScope::single(ResolvedTable {
        name: source_name.clone(),
        alias: source_alias.clone(),
        info: source_info.clone(),
    })?;
    let mut join_scope = TableScope::new();
    join_scope.add(ResolvedTable {
        name: target_name.clone(),
        alias: target_alias.clone(),
        info: target_info.clone(),
    })?;
    join_scope.add(ResolvedTable {
        name: source_name.clone(),
        alias: source_alias.clone(),
        info: source_info.clone(),
    })?;

    // The SET values read the merged row: target fields bare, source fields
    // keyed `<source_ref>.column`.
    let value_scope = join_scope
        .clone()
        .with_column_naming(ColumnNaming::JoinWriteRow);
    let assigns = convert_assignments(&update.assignments, &target_scope, &value_scope)?;

    // Split the WHERE clause into the one equi-join predicate linking target
    // and source (required), the conjuncts over the source only, and the
    // rest, which apply to the target.
    let selection = update.selection.as_ref().ok_or_else(|| SqlError::Parse {
        detail: "UPDATE ... FROM requires a WHERE clause with an equi-join predicate \
                 linking the target and source tables"
            .into(),
    })?;
    let split = extract_join_predicate(
        selection,
        JoinRefs {
            target_ref,
            source_ref: &source_ref,
        },
        SplitScopes {
            join: &join_scope,
            target: &target_scope,
            source: &source_scope,
        },
    )?;

    // Plan the source as a scan carrying its own conjuncts, so the join map
    // holds only the source rows the statement selects.
    let source_rules = engine_rules::resolve_engine_rules(source_info.engine);
    let source_plan = source_rules.plan_scan(crate::engine_rules::ScanParams {
        collection: source_name,
        alias: source_alias,
        filters: split.source_filters,
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        window_functions: Vec::new(),
        indexes: Vec::new(),
        temporal: crate::temporal::TemporalScope::default(),
        bitemporal: source_info.bitemporal,
        has_auto_tier: source_info.has_auto_tier,
    })?;

    let rules = engine_rules::resolve_engine_rules(target_info.engine);
    rules.plan_update_from(UpdateFromParams {
        collection: target_name,
        source: Box::new(source_plan),
        source_alias: source_ref,
        target_join_col: split.target_col,
        source_join_col: split.source_col,
        assignments: assigns,
        target_filters: split.target_filters,
        returning: update.returning.is_some(),
    })
}

/// The names the statement gives the target and the source.
#[derive(Clone, Copy)]
struct JoinRefs<'a> {
    target_ref: &'a str,
    source_ref: &'a str,
}

/// The column namespaces the WHERE clause is split and converted against.
#[derive(Clone, Copy)]
struct SplitScopes<'a> {
    /// Target and source together: an unqualified column resolves here, and
    /// a name both may hold is refused as ambiguous.
    join: &'a TableScope,
    /// The target alone: a target conjunct, qualifier stripped, reads the
    /// target row.
    target: &'a TableScope,
    /// The source alone: a source conjunct, qualifier stripped, reads the
    /// source row.
    source: &'a TableScope,
}

/// The WHERE clause of an `UPDATE ... FROM`, split by the relation each
/// conjunct reads.
struct JoinSplit {
    target_col: String,
    source_col: String,
    /// Conjuncts over the target only (or unqualified), qualifier stripped.
    target_filters: Vec<Filter>,
    /// Conjuncts over the source only, qualifier stripped.
    source_filters: Vec<Filter>,
}

/// Extract a single equi-join predicate of the form `target_table.col =
/// source_table.col` (or the reverse) from a WHERE expression, and sort the
/// other conjuncts by the relation they read.
///
/// An unqualified column resolves over target and source together, as
/// PostgreSQL does: it belongs to the one relation that has it, and a name
/// both may hold is refused as ambiguous. A conjunct over the source only
/// filters the source rows before the join. A conjunct that reads both
/// relations has no relation to run on before the join, and is refused
/// rather than evaluated against the target row alone, where its source
/// columns are missing.
fn extract_join_predicate(
    expr: &ast::Expr,
    refs: JoinRefs<'_>,
    scopes: SplitScopes<'_>,
) -> Result<JoinSplit> {
    let JoinRefs {
        target_ref,
        source_ref,
    } = refs;
    let join_scope = scopes.join;
    // Flatten the top-level AND chain.
    let mut conjuncts: Vec<ast::Expr> = Vec::new();
    flatten_and_expr(expr, &mut conjuncts);

    // Find the first conjunct that is an equi-join between target and source.
    let mut join: Option<(usize, String, String)> = None;
    for (i, conjunct) in conjuncts.iter().enumerate() {
        if let Some((tc, sc)) = try_equijoin_pair(conjunct, target_ref, source_ref) {
            join_scope.check_name(Some(target_ref), &tc)?;
            join_scope.check_name(Some(source_ref), &sc)?;
            join = Some((i, tc, sc));
            break;
        }
    }

    let (join_idx, target_col, source_col) = join.ok_or_else(|| SqlError::Parse {
        detail: format!(
            "UPDATE ... FROM requires a WHERE clause equi-join predicate of the form \
             `{target_ref}.col = {source_ref}.col`; none found"
        ),
    })?;

    conjuncts.remove(join_idx);

    let mut target_conjuncts = Vec::new();
    let mut source_conjuncts = Vec::new();
    for conjunct in conjuncts {
        match conjunct_side(&conjunct, refs, join_scope)? {
            ConjunctSide::Target => target_conjuncts.push(conjunct),
            ConjunctSide::Source => source_conjuncts.push(conjunct),
            ConjunctSide::Both => {
                return Err(SqlError::Unsupported {
                    detail: format!(
                        "UPDATE ... FROM condition `{conjunct}` reads both '{target_ref}' and \
                         '{source_ref}'; only the join equality may compare the two, so move \
                         the condition into a CTE source or split it per relation"
                    ),
                });
            }
        }
    }

    // Strip the table qualifier so `uf_target.score` becomes `score` —
    // documents store bare field names. Each side's conjuncts then read that
    // side's row alone, so they convert against that side's scope.
    let target_filters = strip_and_convert_filters(target_conjuncts, target_ref, scopes.target)?;
    let source_filters = strip_and_convert_filters(source_conjuncts, source_ref, scopes.source)?;

    Ok(JoinSplit {
        target_col,
        source_col,
        target_filters,
        source_filters,
    })
}

/// The relation a WHERE conjunct reads.
#[derive(Debug, PartialEq, Eq)]
enum ConjunctSide {
    /// The target only, or no column at all.
    Target,
    /// The source only.
    Source,
    /// Both relations.
    Both,
}

/// Classify `conjunct` by the relations of the columns it names.
///
/// A qualified column belongs to the relation its qualifier names, and a
/// qualifier naming neither relation counts as the target. An unqualified
/// column belongs to the relation `join_scope` resolves it to. A name both
/// relations may hold is refused as ambiguous. A conjunct holding a subquery
/// stays with the target: the subquery's own columns are not the
/// statement's relations.
fn conjunct_side(
    conjunct: &ast::Expr,
    refs: JoinRefs<'_>,
    join_scope: &TableScope,
) -> Result<ConjunctSide> {
    let mut reads = RelationReads {
        refs,
        join_scope,
        target: false,
        source: false,
        subquery: false,
        ambiguous: None,
    };
    let _ = conjunct.visit(&mut reads);
    if let Some(err) = reads.ambiguous {
        return Err(err);
    }
    if reads.subquery {
        return Ok(ConjunctSide::Target);
    }
    Ok(match (reads.target, reads.source) {
        (_, false) => ConjunctSide::Target,
        (false, true) => ConjunctSide::Source,
        (true, true) => ConjunctSide::Both,
    })
}

struct RelationReads<'a> {
    refs: JoinRefs<'a>,
    join_scope: &'a TableScope,
    target: bool,
    source: bool,
    subquery: bool,
    /// The refusal for an unqualified name both relations may hold.
    ambiguous: Option<SqlError>,
}

impl Visitor for RelationReads<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &ast::Expr) -> ControlFlow<()> {
        match expr {
            ast::Expr::Subquery(_) | ast::Expr::Exists { .. } | ast::Expr::InSubquery { .. } => {
                self.subquery = true;
                return ControlFlow::Break(());
            }
            ast::Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                if normalize_ident(&parts[0]) == self.refs.source_ref {
                    self.source = true;
                } else {
                    self.target = true;
                }
            }
            ast::Expr::Identifier(ident) => {
                // A name no relation accepts, such as a keyword function
                // (`current_date`), stays with the target. Its conversion
                // reports an unknown column there.
                match self.join_scope.resolve_bare(&normalize_ident(ident)) {
                    Ok(BareColumn::Relation(table)) if table.ref_name() == self.refs.source_ref => {
                        self.source = true;
                    }
                    Err(err @ SqlError::AmbiguousColumn { .. }) => {
                        self.ambiguous = Some(err);
                        return ControlFlow::Break(());
                    }
                    Ok(_) | Err(_) => self.target = true,
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Try to extract `(target_col, source_col)` from an equality expression
/// where one side is `target_ref.col` and the other is `source_ref.col`.
/// Also handles unqualified names by assuming they belong to the target.
fn try_equijoin_pair(
    expr: &ast::Expr,
    target_ref: &str,
    source_ref: &str,
) -> Option<(String, String)> {
    let ast::Expr::BinaryOp {
        left,
        op: ast::BinaryOperator::Eq,
        right,
    } = expr
    else {
        return None;
    };

    let lhs = qualified_ident_pair(left);
    let rhs = qualified_ident_pair(right);

    match (lhs, rhs) {
        (Some((lt, lc)), Some((rt, rc))) => {
            if lt == target_ref && rt == source_ref {
                Some((lc, rc))
            } else if lt == source_ref && rt == target_ref {
                Some((rc, lc))
            } else {
                None
            }
        }
        // One side is unqualified — treat it as belonging to target.
        (Some((t, c)), None) if t == source_ref => {
            if let ast::Expr::Identifier(ident) = right.as_ref() {
                Some((normalize_ident(ident), c))
            } else {
                None
            }
        }
        (None, Some((t, c))) if t == source_ref => {
            if let ast::Expr::Identifier(ident) = left.as_ref() {
                Some((normalize_ident(ident), c))
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqlCatalogError;
    use crate::parser::statement::parse_sql;

    fn info(name: &str) -> CollectionInfo {
        let mut id = ColumnInfo {
            name: "id".into(),
            data_type: SqlDataType::String,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        };
        id.is_primary_key = true;
        let column = |name: &str, data_type| ColumnInfo {
            name: name.into(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        };
        CollectionInfo {
            name: name.into(),
            engine: EngineType::DocumentSchemaless,
            columns: vec![
                id,
                column("v", SqlDataType::String),
                column("flag", SqlDataType::Bool),
            ],
            primary_key: Some("id".into()),
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: false,
        }
    }

    struct Catalog;

    impl SqlCatalog for Catalog {
        fn get_collection(
            &self,
            _: DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok(matches!(name, "t" | "s").then(|| info(name)))
        }
    }

    fn plan(sql: &str) -> Result<SqlPlan> {
        let statements = parse_sql(sql)?;
        let mut plans = super::super::update_delete::plan_update(&statements[0], &Catalog)?;
        Ok(plans.remove(0))
    }

    /// A source-only condition filters the source rows before the join. It
    /// was evaluated against the target row, where `s.flag` is missing, so
    /// the statement updated nothing.
    #[test]
    fn a_source_only_condition_filters_the_source() {
        let plan =
            plan("UPDATE t SET v = s.v FROM s WHERE t.id = s.id AND s.flag = true").expect("plans");
        let SqlPlan::UpdateFrom {
            source,
            source_alias,
            target_filters,
            ..
        } = plan
        else {
            panic!("expected UpdateFrom, got {plan:?}");
        };
        assert_eq!(source_alias, "s");
        assert!(target_filters.is_empty(), "{target_filters:?}");
        let SqlPlan::Scan { filters, .. } = *source else {
            panic!("expected a source scan");
        };
        assert_eq!(filters.len(), 1);
    }

    #[test]
    fn a_target_condition_stays_with_the_target() {
        // An aliased source is named by its alias: `s.v` would name a relation
        // the statement no longer has.
        let plan =
            plan("UPDATE t SET v = src.v FROM s AS src WHERE t.id = src.id AND t.flag = true")
                .expect("plans");
        let SqlPlan::UpdateFrom {
            source,
            source_alias,
            target_filters,
            ..
        } = plan
        else {
            panic!("expected UpdateFrom, got {plan:?}");
        };
        assert_eq!(source_alias, "src");
        assert_eq!(target_filters.len(), 1);
        let SqlPlan::Scan { filters, .. } = *source else {
            panic!("expected a source scan");
        };
        assert!(filters.is_empty());
    }

    /// A SET value reads the merged row: a target column bare, a source
    /// column under the source's reference name, however the statement
    /// qualifies it.
    #[test]
    fn set_values_name_the_merged_row_fields() {
        let target_read =
            plan("UPDATE t SET v = t.v FROM s AS src WHERE t.id = src.id").expect("plans");
        let SqlPlan::UpdateFrom { assignments, .. } = target_read else {
            panic!("expected UpdateFrom, got {target_read:?}");
        };
        assert!(
            matches!(
                &assignments[0].1,
                SqlExpr::Column { table: None, name } if name == "v"
            ),
            "a target-qualified column reads the bare target field: {:?}",
            assignments[0].1
        );

        let source_read =
            plan("UPDATE t SET v = src.v FROM s AS src WHERE t.id = src.id").expect("plans");
        let SqlPlan::UpdateFrom { assignments, .. } = source_read else {
            panic!("expected UpdateFrom, got {source_read:?}");
        };
        assert!(
            matches!(
                &assignments[0].1,
                SqlExpr::Column { table: Some(table), name } if table == "src" && name == "v"
            ),
            "a source column reads the source's merged-row field: {:?}",
            assignments[0].1
        );
    }

    #[test]
    fn a_condition_over_both_relations_is_refused() {
        let err = plan("UPDATE t SET v = s.v FROM s WHERE t.id = s.id AND t.v <> s.v")
            .expect_err("a mixed condition has no relation to run on");
        assert!(matches!(err, SqlError::Unsupported { .. }), "{err}");
    }

    /// A target-only condition on a column both relations declare reads the
    /// target row: once its qualifier is stripped it names the target alone.
    #[test]
    fn a_qualified_target_condition_on_a_shared_column_plans() {
        let plan =
            plan("UPDATE t SET v = s.v FROM s WHERE t.id = s.id AND t.flag = true").expect("plans");
        let SqlPlan::UpdateFrom { target_filters, .. } = plan else {
            panic!("expected UpdateFrom, got {plan:?}");
        };
        assert_eq!(target_filters.len(), 1);
    }

    /// An unqualified column both relations declare is ambiguous, as in
    /// PostgreSQL.
    #[test]
    fn an_unqualified_shared_column_is_refused() {
        let err = plan("UPDATE t SET v = s.v FROM s WHERE t.id = s.id AND flag = true")
            .expect_err("both relations declare flag");
        assert!(
            matches!(err, SqlError::AmbiguousColumn { ref column } if column == "flag"),
            "{err:?}"
        );
    }

    fn with_column(mut info: CollectionInfo, name: &str) -> CollectionInfo {
        info.columns.push(ColumnInfo {
            name: name.into(),
            data_type: SqlDataType::String,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        });
        info
    }

    #[test]
    fn conjunct_side_reads_qualifiers_and_owners() {
        let refs = JoinRefs {
            target_ref: "t",
            source_ref: "s",
        };
        let mut scope = TableScope::new();
        for (name, info) in [
            ("t", with_column(info("t"), "only_t")),
            ("s", with_column(info("s"), "only_s")),
        ] {
            scope
                .add(ResolvedTable {
                    name: name.into(),
                    alias: None,
                    info,
                })
                .expect("add");
        }
        let side = |sql: &str| {
            let statements = parse_sql(&format!("SELECT 1 FROM t WHERE {sql}")).expect("parses");
            let ast::Statement::Query(query) = &statements[0] else {
                panic!("expected a query");
            };
            let ast::SetExpr::Select(select) = query.body.as_ref() else {
                panic!("expected a select");
            };
            conjunct_side(select.selection.as_ref().expect("where"), refs, &scope)
        };
        assert_eq!(side("s.flag = true").expect("side"), ConjunctSide::Source);
        assert_eq!(side("t.flag = true").expect("side"), ConjunctSide::Target);
        assert_eq!(side("only_t = 'x'").expect("side"), ConjunctSide::Target);
        assert_eq!(side("only_s = 'x'").expect("side"), ConjunctSide::Source);
        assert_eq!(side("t.v = s.v").expect("side"), ConjunctSide::Both);
        assert!(matches!(
            side("flag = true"),
            Err(SqlError::AmbiguousColumn { .. })
        ));
    }
}
