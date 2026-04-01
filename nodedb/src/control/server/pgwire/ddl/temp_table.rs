//! `CREATE TEMPORARY TABLE` / `CREATE TEMP TABLE` DDL handler.
//!
//! Temporary tables are session-local DataFusion MemTables. They shadow
//! permanent tables with the same name for the creating session. Auto-dropped
//! on session disconnect.

use std::sync::Arc;

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::session::SessionStore;
use crate::control::server::pgwire::session::temp_tables::{OnCommitAction, TempTableMeta};

/// Handle `CREATE TEMPORARY TABLE name (col1 type1, ...) [ON COMMIT ...]`.
///
/// Also handles `CREATE TEMP TABLE` (alias).
pub fn create_temp_table(
    sessions: &SessionStore,
    identity: &AuthenticatedIdentity,
    addr: &std::net::SocketAddr,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let upper = sql.to_uppercase();

    // Parse table name.
    // CREATE TEMPORARY TABLE name ... or CREATE TEMP TABLE name ...
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let name_idx = if upper.starts_with("CREATE TEMPORARY TABLE ")
        || upper.starts_with("CREATE TEMP TABLE ")
    {
        3
    } else {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "syntax: CREATE TEMPORARY TABLE <name> (columns...)".to_owned(),
        ))));
    };

    let name = parts
        .get(name_idx)
        .ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "CREATE TEMPORARY TABLE requires a name".to_owned(),
            )))
        })?
        .to_lowercase();

    // Check if already exists in this session.
    if sessions.has_temp_table(addr, &name) {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42P07".to_owned(),
            format!("temporary table \"{name}\" already exists in this session"),
        ))));
    }

    // Parse ON COMMIT behavior.
    let on_commit = if upper.contains("ON COMMIT DROP") {
        OnCommitAction::Drop
    } else if upper.contains("ON COMMIT DELETE ROWS") {
        OnCommitAction::DeleteRows
    } else {
        OnCommitAction::PreserveRows
    };

    // Parse column definitions from parenthesized list.
    let schema = parse_temp_table_schema(sql)?;

    // Register in session metadata.
    sessions.register_temp_table(
        addr,
        name.clone(),
        TempTableMeta {
            schema: Arc::new(schema),
            on_commit,
        },
    );

    tracing::info!(
        table = %name,
        user = %identity.username,
        "created temporary table"
    );

    Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))])
}

/// Handle `DROP TABLE name` for temp tables.
///
/// Returns `Some(response)` if the table was a temp table, `None` otherwise
/// (caller should fall through to permanent table drop).
pub fn drop_temp_table_if_exists(
    sessions: &SessionStore,
    addr: &std::net::SocketAddr,
    name: &str,
) -> Option<PgWireResult<Vec<Response>>> {
    if sessions.has_temp_table(addr, name) {
        sessions.remove_temp_table(addr, name);
        Some(Ok(vec![Response::Execution(Tag::new("DROP TABLE"))]))
    } else {
        None
    }
}

/// Parse column definitions from `CREATE TEMPORARY TABLE name (col1 type1, col2 type2, ...)`.
fn parse_temp_table_schema(sql: &str) -> PgWireResult<datafusion::arrow::datatypes::Schema> {
    use datafusion::arrow::datatypes::{Field, Schema};

    let paren_start = sql.find('(').ok_or_else(|| {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "CREATE TEMPORARY TABLE requires column definitions in parentheses".to_owned(),
        )))
    })?;

    // Find matching close paren (skip nested parens for DEFAULT expressions).
    let inner = &sql[paren_start + 1..];
    let paren_end = inner.rfind(')').unwrap_or(inner.len());
    let cols_str = &inner[..paren_end];

    let mut fields = Vec::new();
    for col_def in cols_str.split(',') {
        let col_def = col_def.trim();
        if col_def.is_empty() {
            continue;
        }
        let mut tokens = col_def.split_whitespace();
        let col_name = tokens.next().ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                "column name expected in column definition".to_owned(),
            )))
        })?;
        let type_str = tokens
            .next()
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    format!("column type expected for column \"{col_name}\""),
                )))
            })?
            .to_uppercase();

        let data_type = sql_type_to_arrow(&type_str);
        let nullable = !col_def.to_uppercase().contains("NOT NULL");
        fields.push(Field::new(col_name, data_type, nullable));
    }

    if fields.is_empty() {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "42601".to_owned(),
            "temporary table must have at least one column".to_owned(),
        ))));
    }

    Ok(Schema::new(fields))
}

/// Map SQL type name to Arrow DataType.
fn sql_type_to_arrow(type_str: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;
    match type_str {
        "INT" | "INT4" | "INTEGER" => DataType::Int32,
        "BIGINT" | "INT8" => DataType::Int64,
        "SMALLINT" | "INT2" => DataType::Int16,
        "FLOAT" | "FLOAT4" | "REAL" => DataType::Float32,
        "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => DataType::Float64,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TEXT" | "VARCHAR" | "STRING" => DataType::Utf8,
        "BYTEA" | "BYTES" => DataType::Binary,
        "TIMESTAMP" | "TIMESTAMPTZ" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None)
        }
        "DATE" => DataType::Date32,
        _ => DataType::Utf8, // fallback to text
    }
}
