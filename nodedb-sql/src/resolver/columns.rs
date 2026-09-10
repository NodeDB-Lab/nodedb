// SPDX-License-Identifier: Apache-2.0

//! Column and table resolution against the catalog.

use std::collections::HashMap;

use nodedb_types::DatabaseId;

use crate::error::{Result, SqlError};
use crate::parser::normalize::table_name_from_factor;
use crate::types::{CollectionInfo, ColumnInfo, SqlCatalog};

/// Synthetic temporal columns an audit read injects into every version row.
/// They are not declared columns, so a bitemporal relation resolves them by
/// name.
const BITEMPORAL_AUDIT_COLUMNS: [&str; 3] = ["_ts_system", "_ts_valid_from", "_ts_valid_until"];

/// Resolved table reference: name, alias, and catalog info.
#[derive(Debug, Clone)]
pub struct ResolvedTable {
    pub name: String,
    pub alias: Option<String>,
    pub info: CollectionInfo,
}

impl ResolvedTable {
    /// The name to use for qualified column references.
    pub fn ref_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Context built during FROM clause resolution.
#[derive(Debug, Default, Clone)]
pub struct TableScope {
    /// Tables by reference name (alias or table name).
    pub tables: HashMap<String, ResolvedTable>,
    /// Insertion order for unambiguous column resolution.
    order: Vec<String>,
    /// Names resolvable here that belong to no relation: SELECT output
    /// aliases visible to ORDER BY, GROUP BY, and HAVING, plus the output
    /// names substituted for aggregate calls.
    output_names: Vec<String>,
    /// The enclosing query's scope, for a correlated subquery. A qualifier
    /// naming no relation here resolves there instead. Boxed rather than
    /// borrowed: a lifetime on `TableScope` would ripple through every
    /// planner signature that stores or returns one.
    outer: Option<Box<TableScope>>,
    /// Relations a bare column name never resolves against, reachable only
    /// through their qualifier. A MERGE source and the `excluded` relation of
    /// `ON CONFLICT DO UPDATE` are both qualified-only, so a bare name in a
    /// WHEN or SET clause names the target column.
    qualified_only: Vec<String>,
}

impl TableScope {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a resolved table. Returns error if name conflicts.
    pub fn add(&mut self, table: ResolvedTable) -> Result<()> {
        let key = table.ref_name().to_string();
        if self.tables.contains_key(&key) {
            return Err(SqlError::Parse {
                detail: format!("duplicate table reference: {key}"),
            });
        }
        self.order.push(key.clone());
        self.tables.insert(key, table);
        Ok(())
    }

    /// Add a relation that only a qualified reference reaches.
    ///
    /// A bare column name skips it, so a name both this relation and a
    /// bare-resolvable one declare is not ambiguous.
    pub fn add_qualified_only(&mut self, table: ResolvedTable) -> Result<()> {
        let key = table.ref_name().to_string();
        self.add(table)?;
        self.qualified_only.push(key);
        Ok(())
    }

    fn column_exists(&self, table: &ResolvedTable, column: &str) -> bool {
        if table.info.open_schema {
            return true;
        }
        if table.info.bitemporal && BITEMPORAL_AUDIT_COLUMNS.contains(&column) {
            return true;
        }
        let rules = crate::engine_rules::resolve_engine_rules(table.info.engine);
        if rules.implicit_columns().contains(&column) {
            return true;
        }
        // A key-only collection on an engine with a free value-column name
        // has not fixed that name yet, so any name resolves.
        if rules.value_column_name_is_free()
            && !table.info.columns.iter().any(|c| !c.is_primary_key)
        {
            return true;
        }
        table.info.columns.iter().any(|c| c.name == column)
    }

    fn validate_column(&self, table: &ResolvedTable, column: &str) -> Result<()> {
        if self.column_exists(table, column) {
            Ok(())
        } else {
            Err(SqlError::UnknownColumn {
                table: table.name.clone(),
                column: column.into(),
            })
        }
    }

    /// Get the single table in scope (for single-table queries).
    pub fn single_table(&self) -> Option<&ResolvedTable> {
        if self.tables.len() == 1 {
            self.tables.values().next()
        } else {
            Option::None
        }
    }

    /// Whether an expression here is evaluated once per row of some relation.
    ///
    /// A scope with no relation of its own and no enclosing query stands
    /// behind a FROM-less `SELECT`, which produces exactly one row and
    /// evaluates its projection at plan time.
    pub fn is_row_scope(&self) -> bool {
        !self.tables.is_empty() || self.outer.is_some()
    }

    /// A copy of this scope nested inside `outer`, for planning a correlated
    /// subquery body.
    pub fn nested_in(mut self, outer: TableScope) -> Self {
        self.outer = Some(Box::new(outer));
        self
    }

    /// A copy of this scope widened with output column names.
    ///
    /// An ORDER BY, GROUP BY, or HAVING identifier resolves against input
    /// columns first and output names second, so this only widens: a name
    /// that already resolves to a column keeps resolving to it.
    pub fn with_output_names(&self, names: impl IntoIterator<Item = String>) -> Self {
        let mut out = self.clone();
        out.output_names.extend(names);
        out
    }

    /// A single-relation scope, for the DML planners that resolve one
    /// collection and build no FROM clause.
    pub fn single(table: ResolvedTable) -> Result<Self> {
        let mut scope = Self::new();
        scope.add(table)?;
        Ok(scope)
    }

    /// Reject a column reference that names nothing in scope.
    pub fn check_name(&self, table_ref: Option<&str>, column: &str) -> Result<()> {
        let col = column.to_lowercase();

        if let Some(tref) = table_ref {
            let tref_lower = tref.to_lowercase();
            return match self.tables.get(&tref_lower) {
                Some(table) => self.validate_column(table, &col),
                // A qualifier naming no relation here belongs to the outer
                // query of a correlated subquery. With no outer scope it is
                // an unknown relation, not an unknown column.
                None => match &self.outer {
                    Some(outer) => outer.check_name(Some(&tref_lower), &col),
                    None => Err(SqlError::UnknownTable { name: tref_lower }),
                },
            };
        }

        if self.output_names.iter().any(|n| n == &col) {
            return Ok(());
        }

        // A relation that declares the column wins over one that merely
        // accepts any name. An open-schema relation alongside a closed one
        // that declares the column is not an ambiguity.
        let bare: Vec<&ResolvedTable> = self
            .order
            .iter()
            .filter(|key| !self.qualified_only.contains(*key))
            .map(|key| &self.tables[key])
            .collect();
        let declared = bare
            .iter()
            .filter(|table| table.info.columns.iter().any(|c| c.name == col))
            .count();
        // An open-schema relation contributes a maybe, never a yes. With no
        // relation declaring the column, one that accepts any name resolves
        // it: neither ambiguity nor absence is provable.
        if declared == 0 && bare.iter().any(|table| self.column_exists(table, &col)) {
            return Ok(());
        }
        match declared {
            0 => match &self.outer {
                Some(outer) => outer.check_name(None, &col),
                None => Err(SqlError::UnknownColumn {
                    table: self
                        .order
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "<unknown>".into()),
                    column: col,
                }),
            },
            1 => Ok(()),
            _ => Err(SqlError::AmbiguousColumn { column: col }),
        }
    }

    /// The resolved tables, in the order the FROM clause introduced them.
    pub fn tables_in_order(&self) -> impl Iterator<Item = &ResolvedTable> {
        self.order.iter().map(|key| &self.tables[key])
    }

    /// The relation registered under `ref_name`, alias or table name.
    pub fn table_by_ref(&self, ref_name: &str) -> Option<&ResolvedTable> {
        self.tables.get(&ref_name.to_lowercase())
    }

    /// The declared column a reference names, when the relation declares it.
    pub fn declared_column(&self, table_ref: Option<&str>, column: &str) -> Option<&ColumnInfo> {
        let col = column.to_lowercase();
        match table_ref {
            Some(tref) => self
                .table_by_ref(tref)?
                .info
                .columns
                .iter()
                .find(|c| c.name == col),
            None => self
                .tables_in_order()
                .find_map(|t| t.info.columns.iter().find(|c| c.name == col)),
        }
    }

    /// Resolve tables from a FROM clause.
    pub fn resolve_from(
        catalog: &dyn SqlCatalog,
        from: &[sqlparser::ast::TableWithJoins],
    ) -> Result<Self> {
        let mut scope = Self::new();
        for table_with_joins in from {
            scope.resolve_table_factor(catalog, &table_with_joins.relation)?;
            for join in &table_with_joins.joins {
                scope.resolve_table_factor(catalog, &join.relation)?;
            }
        }
        Ok(scope)
    }

    fn resolve_table_factor(
        &mut self,
        catalog: &dyn SqlCatalog,
        factor: &sqlparser::ast::TableFactor,
    ) -> Result<()> {
        // ARRAY_*(...) table-valued function: synthesize a ResolvedTable
        // from the array's dim+attr schema so equi-join keys against the
        // TVF's output rows resolve.
        if let Some(resolved) = crate::resolver::array_tvf::resolve_array_tvf(catalog, factor)? {
            self.add(resolved)?;
            return Ok(());
        }
        // Derived subquery, LATERAL or not: register the alias as the relation
        // its projection list exposes, so a column reference on the alias
        // resolves without a catalog lookup. The inner plan is built
        // separately.
        if let sqlparser::ast::TableFactor::Derived {
            subquery,
            alias: Some(alias),
            ..
        } = factor
        {
            let alias_str = crate::reserved::check_ast_identifier(&alias.name)?;
            let declared: Vec<String> = alias
                .columns
                .iter()
                .map(|column| crate::reserved::check_ast_identifier(&column.name))
                .collect::<Result<_>>()?;
            let info =
                crate::resolver::derived::infer_subquery_relation(catalog, &alias_str, subquery)?;
            self.add(ResolvedTable {
                name: alias_str.clone(),
                alias: Some(alias_str),
                info: crate::resolver::derived::rename_output_columns(info, &declared),
            })?;
            return Ok(());
        }
        if let Some((name, alias)) = table_name_from_factor(factor)? {
            let info = catalog
                .resolve_relation(DatabaseId::DEFAULT, &name)?
                .ok_or_else(|| SqlError::UnknownTable { name: name.clone() })?;
            self.add(ResolvedTable { name, alias, info })?;
        }
        Ok(())
    }
}

/// Scope builders shared by the planner unit tests.
#[cfg(test)]
pub(crate) mod test_support {
    use super::{ResolvedTable, TableScope};
    use crate::types::{CollectionInfo, EngineType};

    /// A one-relation scope over `collection` that accepts any column name.
    pub(crate) fn open_scope(collection: &str) -> TableScope {
        let info = CollectionInfo {
            name: collection.into(),
            engine: EngineType::DocumentSchemaless,
            columns: Vec::new(),
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
        };
        TableScope::single(ResolvedTable {
            name: info.name.clone(),
            alias: None,
            info,
        })
        .expect("single-relation scope")
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::open_scope;
    use super::*;
    use crate::types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType};
    use nodedb_types::PrimaryEngine;

    fn strict_collection(name: &str, columns: Vec<&str>) -> CollectionInfo {
        CollectionInfo {
            name: name.into(),
            engine: EngineType::DocumentStrict,
            columns: columns
                .into_iter()
                .map(|c| ColumnInfo {
                    name: c.into(),
                    data_type: SqlDataType::String,
                    nullable: true,
                    is_primary_key: false,
                    default: None,
                    raw_type: None,
                    int_width: None,
                    float_width: None,
                })
                .collect(),
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
        }
    }

    fn scope_with(info: CollectionInfo) -> TableScope {
        let mut scope = TableScope::new();
        scope
            .add(ResolvedTable {
                name: info.name.clone(),
                alias: None,
                info,
            })
            .expect("add failed");
        scope
    }

    /// A double-quoted identifier resolves as a column name.
    /// `"userId"` is parsed by the SQL layer as `Expr::Identifier` with
    /// `quote_style = Some('"')` and `value = "userId"`. At the `TableScope`
    /// level the column name arrives lowercase, because strict schema columns
    /// are stored lowercase.
    ///
    /// This test covers the resolution path, not just `convert_expr`.
    #[test]
    fn quoted_identifier_resolves_as_column() {
        let scope = scope_with(strict_collection("users", vec!["userid", "email"]));
        scope.check_name(None, "userid").expect("must resolve");
    }

    /// An unrecognized column in a strict collection yields
    /// `SqlError::UnknownColumn`, never `SqlError::Unsupported`.
    ///
    /// A double-quoted identifier like `"ghost_col"` maps to
    /// `SqlExpr::Column { name: "ghost_col" }`. Resolving it against a strict
    /// schema must name the missing column.
    #[test]
    fn unknown_column_in_strict_collection_yields_unknown_column_error() {
        let scope = scope_with(strict_collection("users", vec!["id", "email"]));
        let err = scope
            .check_name(None, "ghost_col")
            .expect_err("must reject an unknown column");
        assert!(
            matches!(err, SqlError::UnknownColumn { ref column, .. } if column == "ghost_col"),
            "expected UnknownColumn(ghost_col), got {err:?}"
        );
        // Unsupported is the wrong error variant here.
        assert!(
            !matches!(err, SqlError::Unsupported { .. }),
            "must not surface Unsupported for a missing column"
        );
    }

    /// Schemaless collections accept any column, including ones that look
    /// like they could be misidentified double-quoted identifiers.
    #[test]
    fn any_column_accepted_in_schemaless_collection() {
        let scope = open_scope("events");
        scope
            .check_name(None, "ghost_col")
            .expect("a schemaless relation must accept any column");
    }

    /// Qualified column reference: `"t"."col"` → table `t`, column `col`.
    #[test]
    fn qualified_column_resolves_correctly() {
        let scope = scope_with(strict_collection("t", vec!["col", "other"]));
        scope
            .check_name(Some("t"), "col")
            .expect("a qualified column must resolve");
    }

    /// Qualified reference to an unknown column in a strict collection must
    /// yield `SqlError::UnknownColumn`, not `Unsupported`.
    #[test]
    fn qualified_unknown_column_in_strict_collection() {
        let scope = scope_with(strict_collection("t", vec!["id"]));
        let err = scope
            .check_name(Some("t"), "missing")
            .expect_err("must reject an unknown column");
        assert!(
            matches!(err, SqlError::UnknownColumn { .. }),
            "expected UnknownColumn, got {err:?}"
        );
    }
}
