//! Column and table resolution against the catalog.

use std::collections::HashMap;

use crate::error::{Result, SqlError};
use crate::parser::normalize::{normalize_ident, normalize_object_name, table_name_from_factor};
use crate::types::{
    ArrayCatalogView, CollectionInfo, ColumnInfo, EngineType, SqlCatalog, SqlDataType,
};
use crate::types_array::{ArrayAttrType, ArrayDimType};

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
#[derive(Debug, Default)]
pub struct TableScope {
    /// Tables by reference name (alias or table name).
    pub tables: HashMap<String, ResolvedTable>,
    /// Insertion order for unambiguous column resolution.
    order: Vec<String>,
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

    /// Resolve a column name, optionally qualified with a table reference.
    ///
    /// For schemaless collections, any column is accepted (dynamic fields).
    /// For typed collections, the column must exist in the schema.
    pub fn resolve_column(
        &self,
        table_ref: Option<&str>,
        column: &str,
    ) -> Result<(String, String)> {
        let col = column.to_lowercase();

        if let Some(tref) = table_ref {
            let tref_lower = tref.to_lowercase();
            let table = self
                .tables
                .get(&tref_lower)
                .ok_or_else(|| SqlError::UnknownTable {
                    name: tref_lower.clone(),
                })?;
            self.validate_column(table, &col)?;
            return Ok((table.name.clone(), col));
        }

        // Unqualified: search all tables.
        let mut matches = Vec::new();
        for key in &self.order {
            let table = &self.tables[key];
            if self.column_exists(table, &col) {
                matches.push(table.name.clone());
            }
        }

        match matches.len() {
            0 => {
                // For single-table queries with schemaless, accept anything.
                if self.tables.len() == 1 {
                    let table = self.tables.values().next().unwrap();
                    if table.info.engine == EngineType::DocumentSchemaless {
                        return Ok((table.name.clone(), col));
                    }
                }
                Err(SqlError::UnknownColumn {
                    table: self
                        .order
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "<unknown>".into()),
                    column: col,
                })
            }
            1 => Ok((matches.into_iter().next().unwrap(), col)),
            _ => Err(SqlError::AmbiguousColumn { column: col }),
        }
    }

    fn column_exists(&self, table: &ResolvedTable, column: &str) -> bool {
        // Schemaless accepts any column.
        if table.info.engine == EngineType::DocumentSchemaless {
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
        // NDARRAY_*(...) table-valued function: synthesize a ResolvedTable
        // from the array's dim+attr schema so equi-join keys against the
        // TVF's output rows resolve.
        if let Some(resolved) = resolve_array_tvf(catalog, factor)? {
            self.add(resolved)?;
            return Ok(());
        }
        if let Some((name, alias)) = table_name_from_factor(factor) {
            let info = catalog
                .get_collection(&name)?
                .ok_or_else(|| SqlError::UnknownTable { name: name.clone() })?;
            self.add(ResolvedTable { name, alias, info })?;
        }
        Ok(())
    }
}

/// If `factor` is `NDARRAY_*(name, ...)`, look up the array via the
/// catalog and build a `ResolvedTable` whose columns mirror the array's
/// dims + attrs. Returns `Ok(None)` for any non-array-TVF factor.
fn resolve_array_tvf(
    catalog: &dyn SqlCatalog,
    factor: &sqlparser::ast::TableFactor,
) -> Result<Option<ResolvedTable>> {
    let (fn_name, args, alias) = match factor {
        sqlparser::ast::TableFactor::Table {
            name,
            args: Some(args),
            alias,
            ..
        } => (
            normalize_object_name(name),
            args,
            alias.as_ref().map(|a| normalize_ident(&a.name)),
        ),
        _ => return Ok(None),
    };
    if !matches!(
        fn_name.as_str(),
        "ndarray_slice" | "ndarray_project" | "ndarray_agg" | "ndarray_elementwise"
    ) {
        return Ok(None);
    }

    // First positional arg is the array name as a string literal.
    let first = args.args.first().ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: missing array-name argument"),
    })?;
    let array_name = extract_string_literal_arg(first).ok_or_else(|| SqlError::Unsupported {
        detail: format!("{fn_name}: array-name argument must be a string literal"),
    })?;
    let view = catalog
        .lookup_array(&array_name)
        .ok_or_else(|| SqlError::UnknownTable {
            name: array_name.clone(),
        })?;

    let info = CollectionInfo {
        name: view.name.clone(),
        engine: EngineType::Array,
        columns: array_columns(&view),
        primary_key: None,
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
    };
    Ok(Some(ResolvedTable {
        name: view.name,
        alias,
        info,
    }))
}

fn array_columns(view: &ArrayCatalogView) -> Vec<ColumnInfo> {
    let mut cols = Vec::with_capacity(view.dims.len() + view.attrs.len());
    for d in &view.dims {
        cols.push(ColumnInfo {
            name: d.name.clone(),
            data_type: dim_type_to_sql(d.dtype),
            nullable: false,
            is_primary_key: false,
            default: None,
        });
    }
    for a in &view.attrs {
        cols.push(ColumnInfo {
            name: a.name.clone(),
            data_type: attr_type_to_sql(a.dtype),
            nullable: a.nullable,
            is_primary_key: false,
            default: None,
        });
    }
    cols
}

fn dim_type_to_sql(t: ArrayDimType) -> SqlDataType {
    match t {
        ArrayDimType::Int64 => SqlDataType::Int64,
        ArrayDimType::Float64 => SqlDataType::Float64,
        ArrayDimType::TimestampMs => SqlDataType::Timestamp,
        ArrayDimType::String => SqlDataType::String,
    }
}

fn attr_type_to_sql(t: ArrayAttrType) -> SqlDataType {
    match t {
        ArrayAttrType::Int64 => SqlDataType::Int64,
        ArrayAttrType::Float64 => SqlDataType::Float64,
        ArrayAttrType::String => SqlDataType::String,
        ArrayAttrType::Bytes => SqlDataType::Bytes,
    }
}

fn extract_string_literal_arg(arg: &sqlparser::ast::FunctionArg) -> Option<String> {
    use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, Value};
    let expr = match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(e),
            ..
        } => e,
        _ => return None,
    };
    match expr {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    }
}
