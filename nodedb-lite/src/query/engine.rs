//! Lite query engine: full SQL via DataFusion over Loro documents.
//!
//! Manages a DataFusion `SessionContext` with collections registered
//! as table providers backed by the CRDT engine.

use std::sync::{Arc, Mutex};

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::crdt::CrdtEngine;
use crate::engine::strict::StrictEngine;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::strict_provider::StrictTableProvider;
use super::table_provider::LiteTableProvider;

/// Lite-side query engine wrapping DataFusion.
///
/// Registered collections appear as tables. SQL queries execute
/// entirely in-process against the Loro CRDT state or strict Binary Tuple store.
pub struct LiteQueryEngine<S: StorageEngine> {
    ctx: SessionContext,
    crdt: Arc<Mutex<CrdtEngine>>,
    strict: Arc<Mutex<StrictEngine<S>>>,
    storage: Arc<S>,
}

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Create a new query engine.
    pub fn new(
        crdt: Arc<Mutex<CrdtEngine>>,
        strict: Arc<Mutex<StrictEngine<S>>>,
        storage: Arc<S>,
    ) -> Self {
        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("nodedb", "public");

        let ctx = SessionContext::new_with_config(config);
        super::spatial_udf::register_spatial_udfs(&ctx);
        Self {
            ctx,
            crdt,
            strict,
            storage,
        }
    }

    /// Register a collection as a queryable table.
    ///
    /// Call this before executing SQL that references the collection.
    /// For auto-registration, call `register_all_collections()`.
    pub fn register_collection(&self, name: &str) {
        let provider = LiteTableProvider::new(name.to_string(), Arc::clone(&self.crdt));
        // Register directly via the session context.
        let _ = self.ctx.register_table(name, Arc::new(provider));
    }

    /// Register a strict collection as a queryable table.
    pub fn register_strict_collection(&self, name: &str) {
        let strict = match self.strict.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        if let Some(schema) = strict.schema(name) {
            let provider =
                StrictTableProvider::new(name.to_string(), schema, Arc::clone(&self.storage));
            let _ = self.ctx.register_table(name, Arc::new(provider));
        }
    }

    /// Register all existing collections as tables (both CRDT and strict).
    pub fn register_all_collections(&self) {
        // Register CRDT (schemaless) collections.
        let crdt = match self.crdt.lock() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };
        let crdt_collections = crdt.collection_names();
        drop(crdt);

        for name in &crdt_collections {
            if name.starts_with("__") {
                continue;
            }
            self.register_collection(name);
        }

        // Register strict document collections.
        let strict = match self.strict.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        let strict_names: Vec<String> = strict
            .collection_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        drop(strict);

        for name in &strict_names {
            self.register_strict_collection(name);
        }
    }

    /// Execute a SQL query and return results.
    ///
    /// DDL statements (CREATE/DROP COLLECTION) are intercepted and handled
    /// directly. All other statements are passed to DataFusion.
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResult, LiteError> {
        // Intercept DDL before DataFusion.
        if let Some(result) = self.try_handle_ddl(sql).await {
            return result;
        }

        // Auto-register collections mentioned in the query.
        self.register_all_collections();

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| LiteError::Query(format!("SQL parse/plan: {e}")))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| LiteError::Query(format!("SQL execute: {e}")))?;

        // Convert Arrow RecordBatches to QueryResult.
        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        for batch in &batches {
            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
            }

            let num_rows = batch.num_rows();
            for row_idx in 0..num_rows {
                let mut row = Vec::with_capacity(columns.len());
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let value = arrow_value_at(col, row_idx)?;
                    row.push(value);
                }
                rows.push(row);
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
        })
    }

    /// Intercept DDL statements before passing to DataFusion.
    ///
    /// Returns `Some(result)` if the statement was handled, `None` if it should
    /// be passed to DataFusion.
    async fn try_handle_ddl(&self, sql: &str) -> Option<Result<QueryResult, LiteError>> {
        let upper = sql.trim().to_uppercase();

        // CREATE COLLECTION ... WITH storage = 'strict'
        if upper.starts_with("CREATE COLLECTION ")
            && upper.contains("STORAGE")
            && upper.contains("STRICT")
        {
            return Some(self.handle_create_strict(sql).await);
        }

        // DROP COLLECTION <name> — check if it's strict, handle accordingly.
        if upper.starts_with("DROP COLLECTION ") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() >= 3
                && parts[0].eq_ignore_ascii_case("DROP")
                && parts[1].eq_ignore_ascii_case("COLLECTION")
            {
                let name = &parts[2];
                let name_lower = name.to_lowercase();
                let is_strict = {
                    let strict = match self.strict.lock() {
                        Ok(s) => s,
                        Err(p) => p.into_inner(),
                    };
                    strict.schema(&name_lower).is_some()
                }; // Guard dropped here, before await.
                if is_strict {
                    return Some(self.handle_drop_strict(&name_lower).await);
                }
            }
        }

        // DESCRIBE <name> — show strict schema if applicable.
        if upper.starts_with("DESCRIBE ") || upper.starts_with("\\D ") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if let Some(name) = parts.get(1) {
                let name_lower = name.to_lowercase();
                let schema_clone = {
                    let strict = match self.strict.lock() {
                        Ok(s) => s,
                        Err(p) => p.into_inner(),
                    };
                    strict.schema(&name_lower).cloned()
                }; // Guard dropped here.
                if let Some(schema) = schema_clone {
                    return Some(Ok(describe_strict_collection(&name_lower, &schema)));
                }
            }
        }

        None
    }

    /// Handle: CREATE COLLECTION <name> (<col_defs>) WITH storage = 'strict'
    async fn handle_create_strict(&self, sql: &str) -> Result<QueryResult, LiteError> {
        let (name, schema) = parse_strict_create_sql(sql)?;

        // StrictEngine::create_collection is async (uses storage), so we must
        // not hold the std::sync::MutexGuard across the await. Instead, clone
        // the Arc and acquire inside a block_in_place or use a scoped approach.
        // Since StrictEngine methods take &mut self, we need the guard — but we
        // can use block_in_place to avoid Send requirements.
        {
            let mut strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            // create_collection calls storage.batch_write which is async.
            // Use tokio::task::block_in_place + Handle::block_on to call it
            // while holding the sync MutexGuard.
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(strict.create_collection(&name, schema))
            })?;
        }

        // Register the new collection in the query engine.
        self.register_strict_collection(&name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "strict collection '{name}' created"
            ))]],
            rows_affected: 0,
        })
    }

    /// Handle: DROP COLLECTION <name> (for strict collections).
    async fn handle_drop_strict(&self, name: &str) -> Result<QueryResult, LiteError> {
        {
            let mut strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(strict.drop_collection(name))
            })?;
        }

        // Deregister from DataFusion.
        let _ = self.ctx.deregister_table(name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "strict collection '{name}' dropped"
            ))]],
            rows_affected: 0,
        })
    }
}

/// Parse `CREATE COLLECTION <name> (<col_defs>) WITH storage = 'strict'`.
///
/// Column definitions: `name TYPE [NOT NULL] [PRIMARY KEY] [DEFAULT expr], ...`
fn parse_strict_create_sql(
    sql: &str,
) -> Result<(String, nodedb_types::columnar::StrictSchema), LiteError> {
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    // Extract collection name: word after "CREATE COLLECTION".
    let upper = sql.to_uppercase();
    let after_create = sql
        .get(
            upper
                .find("COLLECTION")
                .ok_or(LiteError::Query("expected COLLECTION keyword".into()))?
                + 10..,
        )
        .ok_or(LiteError::Query("unexpected end of SQL".into()))?
        .trim();

    let name_end = after_create
        .find(|c: char| c == '(' || c.is_whitespace())
        .unwrap_or(after_create.len());
    let name = after_create[..name_end].trim().to_lowercase();

    if name.is_empty() {
        return Err(LiteError::Query("missing collection name".into()));
    }

    // Extract column definitions between parentheses.
    let paren_start = sql.find('(').ok_or(LiteError::Query(
        "expected column definitions in parentheses".into(),
    ))?;

    // Find the matching closing paren (handle nested parens for VECTOR(dim)).
    let sql_bytes = sql.as_bytes();
    let mut depth = 0;
    let mut paren_end = None;
    for (i, &b) in sql_bytes.iter().enumerate().skip(paren_start) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    paren_end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let paren_end = paren_end.ok_or(LiteError::Query("unmatched parenthesis".into()))?;

    let col_defs_str = &sql[paren_start + 1..paren_end];

    // Split by comma, but respect parentheses inside type names like VECTOR(768).
    let col_parts = split_top_level_commas(col_defs_str);

    let mut columns = Vec::new();
    for part in &col_parts {
        let col = parse_column_def(part.trim())?;
        columns.push(col);
    }

    if columns.is_empty() {
        return Err(LiteError::Query("at least one column required".into()));
    }

    // Auto-generate _rowid PK if no PK column specified.
    if !columns.iter().any(|c| c.primary_key) {
        columns.insert(
            0,
            ColumnDef::required("_rowid", ColumnType::Int64).with_primary_key(),
        );
    }

    let schema = StrictSchema::new(columns).map_err(|e| LiteError::Query(e.to_string()))?;
    Ok((name, schema))
}

/// Split a string by commas at the top level (not inside parentheses).
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        parts.push(&s[start..]);
    }
    parts
}

/// Parse a single column definition: `name TYPE [NOT NULL] [PRIMARY KEY] [DEFAULT expr]`
fn parse_column_def(s: &str) -> Result<nodedb_types::columnar::ColumnDef, LiteError> {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    let upper = s.to_uppercase();
    let tokens: Vec<&str> = s.split_whitespace().collect();

    if tokens.len() < 2 {
        return Err(LiteError::Query(format!(
            "column definition requires at least name and type, got: '{s}'"
        )));
    }

    let name = tokens[0].to_lowercase();

    // Find the type — may span multiple tokens for VECTOR(dim).
    // Rejoin everything after the name until we hit a keyword.
    let after_name = s.split_whitespace().skip(1).collect::<Vec<_>>().join(" ");
    let type_end = find_keyword_start(&after_name);
    let type_str = after_name[..type_end].trim();

    let column_type: ColumnType =
        type_str
            .parse()
            .map_err(|e: nodedb_types::columnar::ColumnTypeParseError| {
                LiteError::Query(e.to_string())
            })?;

    let is_not_null = upper.contains("NOT NULL");
    let is_pk = upper.contains("PRIMARY KEY");
    let nullable = !is_not_null && !is_pk;

    // Parse DEFAULT value.
    let default = if let Some(pos) = upper.find("DEFAULT ") {
        let after_default = s[pos + 8..].trim();
        // Take until next keyword or end.
        let end = find_keyword_start(after_default);
        let expr = after_default[..end].trim();
        if expr.is_empty() {
            None
        } else {
            Some(expr.to_string())
        }
    } else {
        None
    };

    let mut col = if nullable {
        ColumnDef::nullable(name, column_type)
    } else {
        ColumnDef::required(name, column_type)
    };

    if is_pk {
        col = col.with_primary_key();
    }
    if let Some(d) = default {
        col = col.with_default(d);
    }

    Ok(col)
}

/// Find the start position of the first SQL keyword in a column definition suffix.
/// Matches at word boundaries (start of string or preceded by whitespace).
fn find_keyword_start(s: &str) -> usize {
    let upper = s.to_uppercase();
    let keywords = ["NOT", "NULL", "PRIMARY", "DEFAULT"];
    let mut earliest = s.len();
    for kw in &keywords {
        if let Some(pos) = upper.find(kw) {
            // Ensure word boundary: at start or preceded by whitespace.
            let at_boundary = pos == 0
                || upper
                    .as_bytes()
                    .get(pos - 1)
                    .is_some_and(|b| b.is_ascii_whitespace());
            if at_boundary && pos < earliest {
                earliest = pos;
            }
        }
    }
    earliest
}

/// Build a DESCRIBE result for a strict collection.
fn describe_strict_collection(
    name: &str,
    schema: &nodedb_types::columnar::StrictSchema,
) -> QueryResult {
    use nodedb_types::columnar::SchemaOps;

    let mut rows = Vec::with_capacity(schema.len() + 2);

    // Collection name and storage mode info.
    rows.push(vec![
        Value::String("__collection".into()),
        Value::String(name.to_string()),
        Value::String(String::new()),
        Value::String(String::new()),
        Value::String(String::new()),
    ]);
    rows.push(vec![
        Value::String("__storage".into()),
        Value::String("document".into()),
        Value::String("strict".into()),
        Value::String(String::new()),
        Value::String(format!("v{}", schema.version)),
    ]);

    for col in &schema.columns {
        rows.push(vec![
            Value::String(col.name.clone()),
            Value::String(col.column_type.to_string()),
            Value::String(if col.nullable { "YES" } else { "NO" }.into()),
            Value::String(if col.primary_key { "YES" } else { "NO" }.into()),
            Value::String(col.default.clone().unwrap_or_default()),
        ]);
    }

    QueryResult {
        columns: vec![
            "field".into(),
            "type".into(),
            "nullable".into(),
            "primary_key".into(),
            "default".into(),
        ],
        rows,
        rows_affected: 0,
    }
}

/// Extract a single value from an Arrow array at the given row index.
///
/// Returns `Err` if the Arrow array type doesn't match the expected downcast.
fn arrow_value_at(col: &dyn Array, row: usize) -> Result<Value, crate::error::LiteError> {
    use datafusion::arrow::array::*;

    if col.is_null(row) {
        return Ok(Value::Null);
    }

    /// Downcast helper that returns a proper error instead of panicking.
    macro_rules! downcast {
        ($col:expr, $arr_type:ty, $type_name:expr) => {
            $col.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                crate::error::LiteError::ArrowTypeConversion {
                    expected: $type_name.into(),
                    got: format!("{:?}", $col.data_type()),
                }
            })?
        };
    }

    match col.data_type() {
        DataType::Utf8 => Ok(Value::String(
            downcast!(col, StringArray, "StringArray")
                .value(row)
                .to_string(),
        )),
        DataType::LargeUtf8 => Ok(Value::String(
            downcast!(col, LargeStringArray, "LargeStringArray")
                .value(row)
                .to_string(),
        )),
        DataType::Int8 => Ok(Value::Integer(
            downcast!(col, Int8Array, "Int8Array").value(row) as i64,
        )),
        DataType::Int16 => Ok(Value::Integer(
            downcast!(col, Int16Array, "Int16Array").value(row) as i64,
        )),
        DataType::Int32 => Ok(Value::Integer(
            downcast!(col, Int32Array, "Int32Array").value(row) as i64,
        )),
        DataType::Int64 => Ok(Value::Integer(
            downcast!(col, Int64Array, "Int64Array").value(row),
        )),
        DataType::UInt8 => Ok(Value::Integer(
            downcast!(col, UInt8Array, "UInt8Array").value(row) as i64,
        )),
        DataType::UInt16 => Ok(Value::Integer(
            downcast!(col, UInt16Array, "UInt16Array").value(row) as i64,
        )),
        DataType::UInt32 => Ok(Value::Integer(
            downcast!(col, UInt32Array, "UInt32Array").value(row) as i64,
        )),
        DataType::UInt64 => Ok(Value::Integer(
            downcast!(col, UInt64Array, "UInt64Array").value(row) as i64,
        )),
        DataType::Float32 => Ok(Value::Float(
            downcast!(col, Float32Array, "Float32Array").value(row) as f64,
        )),
        DataType::Float64 => Ok(Value::Float(
            downcast!(col, Float64Array, "Float64Array").value(row),
        )),
        DataType::Boolean => Ok(Value::Bool(
            downcast!(col, BooleanArray, "BooleanArray").value(row),
        )),
        _ => {
            let formatter = datafusion::arrow::util::display::ArrayFormatter::try_new(
                col,
                &datafusion::arrow::util::display::FormatOptions::default(),
            );
            match formatter {
                Ok(fmt) => Ok(Value::String(fmt.value(row).to_string())),
                Err(_) => Ok(Value::Null),
            }
        }
    }
}
