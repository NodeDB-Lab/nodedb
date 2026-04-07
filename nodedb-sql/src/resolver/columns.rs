//! Column and table resolution against the catalog.

use std::collections::HashMap;

use crate::error::{Result, SqlError};
use crate::parser::normalize::table_name_from_factor;
use crate::types::{CollectionInfo, EngineType, SqlCatalog};

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
        if let Some((name, alias)) = table_name_from_factor(factor) {
            let info = catalog
                .get_collection(&name)
                .ok_or_else(|| SqlError::UnknownTable { name: name.clone() })?;
            self.add(ResolvedTable { name, alias, info })?;
        }
        Ok(())
    }
}
