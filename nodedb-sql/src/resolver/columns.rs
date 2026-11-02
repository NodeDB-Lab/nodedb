// SPDX-License-Identifier: Apache-2.0

//! Column and table resolution against the catalog.

use std::collections::HashMap;

use nodedb_types::DatabaseId;

use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::parser::normalize::table_name_from_factor;
use crate::temporal::TemporalScope;
use crate::types::{CollectionInfo, ColumnInfo, EngineType, SqlCatalog};

pub use super::column_naming::{BareColumn, ColumnNaming};

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

    /// The qualifier a join's merged row gives this relation's columns.
    ///
    /// A collection or catalog relation's columns are keyed
    /// `ref_name.column`. An `ARRAY_*` table function's rows keep bare
    /// column names, so it has none.
    pub fn row_qualifier(&self) -> Option<&str> {
        match self.info.engine {
            EngineType::Array => None,
            EngineType::DocumentSchemaless
            | EngineType::DocumentStrict
            | EngineType::KeyValue
            | EngineType::Columnar
            | EngineType::Timeseries
            | EngineType::Spatial
            | EngineType::ContinuousAggregate => Some(self.ref_name()),
        }
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
    /// Whether the SELECT this scope backs produces the statement's result
    /// rows. Only such a SELECT list may hold a Control-Plane-computed item:
    /// the Control Plane evaluates the statement's output rows and nothing
    /// deeper. A subquery, CTE, UNION branch, derived table, INSERT source,
    /// or MERGE source keeps the default. A scope nested inside this one
    /// never inherits the flag.
    statement_output: bool,
    /// Whether a sequence accessor resolves here although rows are in scope.
    /// Set for a SELECT-list item over a relation: the Control Plane
    /// evaluates that item once per output row. Every other row-scope clause
    /// keeps the default and refuses the call. A scope nested inside this one
    /// never inherits the flag.
    cp_functions: bool,
    /// How a column reference to one of this scope's relations names its
    /// field. A scope nested inside this one never inherits it.
    column_naming: ColumnNaming,
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
    /// subquery body. The body's own clauses refuse a sequence accessor even
    /// when the enclosing SELECT list allows one.
    pub fn nested_in(mut self, outer: TableScope) -> Self {
        self.outer = Some(Box::new(outer));
        self.statement_output = false;
        self.cp_functions = false;
        self.column_naming = ColumnNaming::AsWritten;
        self
    }

    /// This scope marked as backing the SELECT that produces the statement's
    /// result rows.
    pub fn as_statement_output(mut self) -> Self {
        self.statement_output = true;
        self
    }

    /// Whether the SELECT this scope backs produces the statement's result
    /// rows.
    pub fn is_statement_output(&self) -> bool {
        self.statement_output
    }

    /// This scope with sequence accessors allowed: the expression it resolves
    /// is a SELECT-list item the Control Plane evaluates per output row.
    pub fn allowing_cp_functions(mut self) -> Self {
        self.cp_functions = true;
        self
    }

    /// Whether a sequence accessor resolves here despite rows being in scope.
    pub fn allows_cp_functions(&self) -> bool {
        self.cp_functions
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

        self.resolve_bare(&col).map(|_| ())
    }

    /// Where an unqualified column reference resolves.
    ///
    /// A SELECT output name wins. Otherwise the relations a bare name reaches
    /// are tried:
    /// - One relation declares the column: it resolves there, even beside an
    ///   open-schema relation.
    /// - Two or more declare it: refused as ambiguous.
    /// - None declares it and exactly one accepts any name (an open schema,
    ///   an implicit column): it resolves there.
    /// - None declares it and two or more accept any name: refused as
    ///   ambiguous. Picking one would read NULL for a field the other holds.
    /// - None accepts it: the enclosing query's scope is tried, else the
    ///   column is unknown.
    pub fn resolve_bare(&self, column: &str) -> Result<BareColumn<'_>> {
        let col = column.to_lowercase();
        if self.output_names.iter().any(|n| n == &col) {
            return Ok(BareColumn::Output);
        }
        let bare: Vec<&ResolvedTable> = self
            .order
            .iter()
            .filter(|key| !self.qualified_only.contains(*key))
            .map(|key| &self.tables[key])
            .collect();
        let declared: Vec<&ResolvedTable> = bare
            .iter()
            .copied()
            .filter(|table| table.info.columns.iter().any(|c| c.name == col))
            .collect();
        match declared.as_slice() {
            &[table] => return Ok(BareColumn::Relation(table)),
            [] => {}
            _ => return Err(SqlError::AmbiguousColumn { column: col }),
        }
        let accepting: Vec<&ResolvedTable> = bare
            .iter()
            .copied()
            .filter(|table| self.column_exists(table, &col))
            .collect();
        match accepting.as_slice() {
            &[table] => Ok(BareColumn::Relation(table)),
            [] => match &self.outer {
                Some(outer) => outer.resolve_bare(&col).map(|_| BareColumn::Outer),
                None => Err(SqlError::UnknownColumn {
                    table: self
                        .order
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "<unknown>".into()),
                    column: col,
                }),
            },
            _ => Err(SqlError::AmbiguousColumn { column: col }),
        }
    }

    /// This scope with column references named for `naming`'s rows.
    pub fn with_column_naming(mut self, naming: ColumnNaming) -> Self {
        self.column_naming = naming;
        self
    }

    /// How this scope's column references name their fields.
    pub fn column_naming(&self) -> ColumnNaming {
        self.column_naming
    }

    /// The relation added first. A join write adds its target first.
    pub(super) fn first_relation(&self) -> Option<&ResolvedTable> {
        self.order.first().and_then(|key| self.tables.get(key))
    }

    /// Reject a `USING (column)` key that `right_ref`'s relation or every
    /// other relation lacks. Both sides of the join must carry it.
    pub fn check_using_column(&self, right_ref: Option<&str>, column: &str) -> Result<()> {
        let col = column.to_lowercase();
        let right_key = right_ref.map(str::to_lowercase);
        let right = right_key.as_deref().and_then(|key| self.tables.get(key));
        let carriers = self
            .order
            .iter()
            .filter(|key| !self.qualified_only.contains(*key))
            .filter(|key| self.column_exists(&self.tables[*key], &col));
        let has_both = match (right, right_key.as_deref()) {
            (Some(right), Some(key)) => {
                self.column_exists(right, &col) && carriers.into_iter().any(|k| k != key)
            }
            _ => carriers.count() >= 2,
        };
        if has_both {
            Ok(())
        } else {
            Err(SqlError::UnknownColumn {
                table: right_key.unwrap_or_else(|| "<unknown>".into()),
                column: col,
            })
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
    ///
    /// A derived-subquery factor is planned here so its relation carries the
    /// search cells its plan produces; the planner context (`functions`,
    /// `temporal`) is required for that and for no other arm.
    pub fn resolve_from(
        catalog: &dyn SqlCatalog,
        functions: &FunctionRegistry,
        temporal: TemporalScope,
        from: &[sqlparser::ast::TableWithJoins],
    ) -> Result<Self> {
        let mut scope = Self::new();
        let from_names = crate::resolver::derived::from_relation_names(from);
        let factors = from.iter().flat_map(|twj| {
            std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation))
        });
        for factor in factors {
            scope
                .resolve_table_factor(catalog, functions, temporal, factor)
                .map_err(|e| {
                    crate::resolver::derived::refuse_correlated_derived(e, factor, &from_names)
                })?;
        }
        Ok(scope)
    }

    fn resolve_table_factor(
        &mut self,
        catalog: &dyn SqlCatalog,
        functions: &FunctionRegistry,
        temporal: TemporalScope,
        factor: &sqlparser::ast::TableFactor,
    ) -> Result<()> {
        // ARRAY_*(...) table-valued function: synthesize a ResolvedTable
        // from the columns the call's rows carry so equi-join keys against
        // the TVF's output rows resolve.
        if let Some(resolved) =
            crate::resolver::array_tvf::resolve_array_tvf(catalog, factor, temporal)?
        {
            self.add(resolved)?;
            return Ok(());
        }
        // Derived subquery, LATERAL or not: register the alias as the relation
        // its projection list exposes — a column reference on the alias
        // resolves without a catalog lookup. A non-LATERAL subquery is planned
        // so its relation carries the search cells the plan produces; a
        // correlated LATERAL one cannot be planned at scope time, and the
        // lateral planner routes its shapes without those cells.
        if let sqlparser::ast::TableFactor::Derived {
            lateral,
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
            let plan = if *lateral {
                None
            } else {
                Some(crate::planner::select::plan_query(
                    subquery, catalog, functions, temporal,
                )?)
            };
            let info = crate::resolver::derived::infer_subquery_relation(
                catalog,
                &alias_str,
                subquery,
                plan.as_ref(),
                functions,
                temporal,
            )?;
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

    fn open_collection(name: &str) -> CollectionInfo {
        CollectionInfo {
            engine: EngineType::DocumentSchemaless,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
            ..strict_collection(name, Vec::new())
        }
    }

    fn array_relation(name: &str, columns: Vec<&str>) -> CollectionInfo {
        CollectionInfo {
            engine: EngineType::Array,
            open_schema: false,
            ..strict_collection(name, columns)
        }
    }

    /// A join scope over `(alias, info)` relations, in FROM order.
    fn join_scope(relations: Vec<(&str, CollectionInfo)>) -> TableScope {
        let mut scope = TableScope::new();
        for (alias, info) in relations {
            scope
                .add(ResolvedTable {
                    name: info.name.clone(),
                    alias: Some(alias.into()),
                    info,
                })
                .expect("add failed");
        }
        scope.with_column_naming(ColumnNaming::JoinRow)
    }

    fn owner<'a>(scope: &'a TableScope, column: &str) -> Option<&'a str> {
        match scope.resolve_bare(column).expect("resolves") {
            BareColumn::Relation(table) => Some(table.ref_name()),
            BareColumn::Output | BareColumn::Outer => None,
        }
    }

    #[test]
    fn column_declared_by_one_relation_resolves_there() {
        let scope = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("o", strict_collection("orders", vec!["id", "amount"])),
        ]);
        assert_eq!(owner(&scope, "title"), Some("a"));
        assert_eq!(owner(&scope, "amount"), Some("o"));
    }

    #[test]
    fn declared_relation_wins_over_a_schemaless_one() {
        let scope = join_scope(vec![
            ("e", open_collection("events")),
            ("a", strict_collection("articles", vec!["id", "title"])),
        ]);
        assert_eq!(owner(&scope, "title"), Some("a"));
    }

    #[test]
    fn column_declared_by_two_relations_is_ambiguous() {
        let scope = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("b", strict_collection("blogs", vec!["id", "title"])),
        ]);
        let err = scope.resolve_bare("title").expect_err("ambiguous");
        assert!(
            matches!(err, SqlError::AmbiguousColumn { ref column } if column == "title"),
            "{err:?}"
        );
    }

    #[test]
    fn undeclared_column_resolves_to_the_only_schemaless_relation() {
        let scope = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("e", open_collection("events")),
        ]);
        assert_eq!(owner(&scope, "payload"), Some("e"));
    }

    #[test]
    fn undeclared_column_over_two_schemaless_relations_is_ambiguous() {
        let scope = join_scope(vec![
            ("e", open_collection("events")),
            ("d", open_collection("devices")),
        ]);
        let err = scope.resolve_bare("region").expect_err("ambiguous");
        assert!(
            matches!(err, SqlError::AmbiguousColumn { ref column } if column == "region"),
            "{err:?}"
        );
    }

    #[test]
    fn column_no_relation_accepts_is_unknown() {
        let scope = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("o", strict_collection("orders", vec!["id", "amount"])),
        ]);
        let err = scope.resolve_bare("ghost").expect_err("unknown");
        assert!(matches!(err, SqlError::UnknownColumn { .. }), "{err:?}");
    }

    #[test]
    fn output_name_resolves_before_relations() {
        let scope = join_scope(vec![
            ("e", open_collection("events")),
            ("d", open_collection("devices")),
        ])
        .with_output_names(["total".to_string()]);
        assert!(matches!(
            scope.resolve_bare("total").expect("resolves"),
            BareColumn::Output
        ));
        assert_eq!(
            scope.bare_column_qualifier("total").expect("resolves"),
            None
        );
    }

    #[test]
    fn join_row_naming_qualifies_a_bare_column_with_its_owner() {
        let scope = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("e", open_collection("events")),
        ]);
        assert_eq!(
            scope.bare_column_qualifier("title").expect("resolves"),
            Some("a".to_string())
        );
        assert_eq!(
            scope.bare_column_qualifier("payload").expect("resolves"),
            Some("e".to_string())
        );
    }

    #[test]
    fn as_written_naming_checks_but_keeps_a_bare_column_bare() {
        let scope = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("o", strict_collection("orders", vec!["id", "amount"])),
        ])
        .with_column_naming(ColumnNaming::AsWritten);
        assert_eq!(
            scope.bare_column_qualifier("title").expect("resolves"),
            None
        );
        assert!(
            scope.bare_column_qualifier("id").is_err(),
            "still ambiguous"
        );
    }

    #[test]
    fn array_relation_columns_carry_no_qualifier() {
        let scope = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("g", array_relation("genome", vec!["chrom", "qual"])),
        ]);
        assert_eq!(scope.bare_column_qualifier("qual").expect("resolves"), None);
        assert_eq!(scope.qualified_column_qualifier("g"), None);
        assert_eq!(scope.qualified_column_qualifier("a"), Some("a".to_string()));
    }

    #[test]
    fn output_row_naming_drops_relation_qualifiers() {
        let scope = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("o", strict_collection("orders", vec!["id", "amount"])),
        ])
        .with_column_naming(ColumnNaming::OutputRow);
        assert_eq!(scope.qualified_column_qualifier("a"), None);
        assert_eq!(
            scope.bare_column_qualifier("title").expect("resolves"),
            None
        );
        assert_eq!(
            scope.qualified_column_qualifier("outer_ref"),
            Some("outer_ref".to_string()),
            "a qualifier naming no relation here is kept"
        );
    }

    #[test]
    fn nested_scope_does_not_inherit_join_row_naming() {
        let outer = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("o", strict_collection("orders", vec!["id", "amount"])),
        ]);
        let inner = scope_with(strict_collection("tags", vec!["tag"])).nested_in(outer);
        assert_eq!(inner.column_naming(), ColumnNaming::AsWritten);
        assert!(matches!(
            inner.resolve_bare("amount").expect("resolves"),
            BareColumn::Outer
        ));
    }

    #[test]
    fn using_column_must_exist_on_both_sides() {
        let scope = join_scope(vec![
            ("a", strict_collection("articles", vec!["id", "title"])),
            ("o", strict_collection("orders", vec!["id", "amount"])),
        ]);
        scope
            .check_using_column(Some("o"), "id")
            .expect("both sides carry id");
        assert!(scope.check_using_column(Some("o"), "title").is_err());
        assert!(scope.check_using_column(Some("o"), "amount").is_err());
    }

    #[test]
    fn using_column_over_two_schemaless_relations_is_not_ambiguous() {
        let scope = join_scope(vec![
            ("e", open_collection("events")),
            ("d", open_collection("devices")),
        ]);
        scope
            .check_using_column(Some("d"), "device_id")
            .expect("USING names the column on both sides");
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
