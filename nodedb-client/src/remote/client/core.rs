// SPDX-License-Identifier: Apache-2.0

//! `NodeDbRemote` struct definition and connection/raw-query helpers.

use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;

use crate::remote_parse::pg_value_to_value;

/// Remote NodeDB client. Connects to an Origin instance over pgwire and
/// translates `NodeDb` trait calls into SQL/DSL queries.
pub struct NodeDbRemote {
    pub(super) client: Arc<Mutex<Client>>,
}

/// Extract a useful detail string from a `tokio_postgres::Error`.
///
/// Without this, `Display` returns the literal `"db error"` and the
/// SQLSTATE + server message are dropped — every failure surfaces as the
/// same opaque string and is impossible to diagnose without a debug
/// rebuild. Mirrors the harness's `pg_error_detail` so client and test
/// reports look identical.
pub(super) fn pg_error_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        format!(
            "{}: {} (SQLSTATE {})",
            db_err.severity(),
            db_err.message(),
            db_err.code().code()
        )
    } else {
        format!("{e}")
    }
}

/// Whether `sql` is a statement the server plans and describes at Parse: a
/// `SELECT`, `WITH`, `VALUES` or `TABLE` query, or an `INSERT`, `UPDATE` or
/// `DELETE` with a `RETURNING` clause. Leading parentheses and whitespace are
/// skipped. Any other statement may be one Parse does not plan, so it stays
/// on the simple-query protocol.
pub(super) fn is_row_query(sql: &str) -> bool {
    let body = sql.trim_start_matches(|c: char| c.is_whitespace() || c == '(');
    let first = body
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match first.as_str() {
        "SELECT" | "WITH" | "VALUES" | "TABLE" => true,
        "INSERT" | "UPDATE" | "DELETE" => body
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .any(|word| word.eq_ignore_ascii_case("RETURNING")),
        _ => false,
    }
}

/// Each row's cells, decoded by their column types.
fn typed_rows(rows: &[tokio_postgres::Row]) -> NodeDbResult<Vec<Vec<Value>>> {
    rows.iter()
        .map(|row| {
            row.columns()
                .iter()
                .enumerate()
                .map(|(idx, column)| pg_value_to_value(row, idx, column))
                .collect()
        })
        .collect()
}

impl NodeDbRemote {
    /// Connect to a NodeDB Origin instance.
    ///
    /// `config` is a standard PostgreSQL connection string:
    /// `"host=localhost port=5432 user=app dbname=mydb"`
    pub async fn connect(config: &str) -> NodeDbResult<Self> {
        let (client, connection) = tokio_postgres::connect(config, NoTls)
            .await
            .map_err(|e| NodeDbError::sync_connection_failed(e.to_string()))?;

        // Spawn the connection handler — it runs in the background.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("pgwire connection error: {e}");
            }
        });

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    /// Execute a raw SQL string and return rows as `Vec<Vec<Value>>`.
    pub(super) async fn query_raw(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> NodeDbResult<(Vec<String>, Vec<Vec<Value>>)> {
        let client = self.client.lock().await;
        let rows = client.query(sql, params).await.map_err(|e| {
            NodeDbError::storage(format!("pgwire query failed: {}", pg_error_detail(&e)))
        })?;

        if rows.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();

        Ok((columns, typed_rows(&rows)?))
    }

    /// Prepare `sql` with the declared parameter `types`, run it with
    /// `params`, and return its rows typed by column.
    pub(super) async fn query_typed_params(
        &self,
        sql: &str,
        types: &[tokio_postgres::types::Type],
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> NodeDbResult<(Vec<String>, Vec<Vec<Value>>)> {
        let client = self.client.lock().await;
        let statement = client.prepare_typed(sql, types).await.map_err(|e| {
            NodeDbError::storage(format!("pgwire prepare failed: {}", pg_error_detail(&e)))
        })?;
        let rows = client.query(&statement, params).await.map_err(|e| {
            NodeDbError::storage(format!("pgwire query failed: {}", pg_error_detail(&e)))
        })?;
        if rows.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        Ok((columns, typed_rows(&rows)?))
    }

    /// Run a parameterless statement, with each cell typed by its column
    /// type when the statement's result shape is known at Parse.
    ///
    /// The simple-query protocol carries no column types to the client:
    /// `tokio_postgres` keeps only the column names of its RowDescription, so
    /// every cell would arrive as text. A statement whose result columns the
    /// server describes at Parse (a planned `SELECT`, a `RETURNING` write)
    /// runs through the extended protocol instead, and each cell decodes by
    /// its RowDescription type: a `numeric` column is a `Decimal`, a `uuid`
    /// a `Uuid`, a `timestamp` an instant.
    ///
    /// Only a row query ([`is_row_query`]) is prepared. Every other
    /// statement (DDL, a DSL statement, SET, transaction control) runs
    /// through the simple-query protocol, as does a row query the server
    /// describes no columns for. Parse executes nothing, so no statement
    /// runs twice. A row query Parse refuses fails with the server's error:
    /// it plans at Parse exactly as the simple query would plan it, so the
    /// error is the one the simple query reports.
    pub(super) async fn query_typed_or_simple(
        &self,
        sql: &str,
    ) -> NodeDbResult<(Vec<String>, Vec<Vec<Value>>)> {
        if !is_row_query(sql) {
            return self.simple_query_raw(sql).await;
        }
        let client = self.client.lock().await;
        let statement = client.prepare(sql).await.map_err(|e| {
            NodeDbError::storage(format!("pgwire prepare failed: {}", pg_error_detail(&e)))
        })?;
        if statement.columns().is_empty() {
            drop(client);
            return self.simple_query_raw(sql).await;
        }
        let rows = client.query(&statement, &[]).await.map_err(|e| {
            NodeDbError::storage(format!("pgwire query failed: {}", pg_error_detail(&e)))
        })?;
        let columns = statement
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();
        Ok((columns, typed_rows(&rows)?))
    }

    /// Execute a statement that doesn't return rows (INSERT/UPDATE/DELETE).
    pub(super) async fn execute_raw(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> NodeDbResult<u64> {
        let client = self.client.lock().await;
        client.execute(sql, params).await.map_err(|e| {
            NodeDbError::storage(format!("pgwire execute failed: {}", pg_error_detail(&e)))
        })
    }

    /// Execute a parameterless statement via the simple-query protocol
    /// (single `Query` message — no `Parse`/`Bind`/`Describe` round-trip).
    ///
    /// Required for DDL statements that don't fit the extended-query
    /// row-description shape that `Client::query` expects.
    /// `simple_query` doesn't support bound parameters, so callers with
    /// non-empty params must continue to use `query_raw`.
    ///
    /// All values come back as strings from the simple-query protocol;
    /// we wrap them as `Value::String` and let downstream consumers
    /// coerce as needed.
    pub(super) async fn simple_query_raw(
        &self,
        sql: &str,
    ) -> NodeDbResult<(Vec<String>, Vec<Vec<Value>>)> {
        use tokio_postgres::SimpleQueryMessage;

        let client = self.client.lock().await;
        let messages = client.simple_query(sql).await.map_err(|e| {
            NodeDbError::storage(format!(
                "pgwire simple_query failed: {}",
                pg_error_detail(&e)
            ))
        })?;

        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        for msg in messages {
            match msg {
                SimpleQueryMessage::RowDescription(fields) => {
                    columns = fields.iter().map(|f| f.name().to_string()).collect();
                }
                SimpleQueryMessage::Row(row) => {
                    let mut vals = Vec::with_capacity(row.len());
                    for i in 0..row.len() {
                        match row.get(i) {
                            Some(s) => vals.push(Value::String(s.to_string())),
                            None => vals.push(Value::Null),
                        }
                    }
                    rows.push(vals);
                }
                SimpleQueryMessage::CommandComplete(_) => {
                    // DDL / DML completion — no rows.
                }
                _ => {}
            }
        }
        Ok((columns, rows))
    }

    /// Run a parameterless statement via the simple-query protocol and
    /// return the row count of its first command tag. A response with no
    /// command tag is an error, never a zero count.
    pub(super) async fn simple_command_count(&self, sql: &str) -> NodeDbResult<u64> {
        use tokio_postgres::SimpleQueryMessage;

        let client = self.client.lock().await;
        let messages = client.simple_query(sql).await.map_err(|e| {
            NodeDbError::storage(format!(
                "pgwire simple_query failed: {}",
                pg_error_detail(&e)
            ))
        })?;
        messages
            .into_iter()
            .find_map(|message| match message {
                SimpleQueryMessage::CommandComplete(count) => Some(count),
                _ => None,
            })
            .ok_or_else(|| NodeDbError::internal("the server answered with no command tag"))
    }
}

#[cfg(test)]
mod tests {
    use super::{NodeDbRemote, is_row_query};
    use crate::traits::NodeDb;

    /// A row query is prepared for typed cells. Any statement Parse might not
    /// plan stays on the simple-query protocol, as does a write without
    /// `RETURNING`.
    #[test]
    fn row_queries_are_prepared_and_other_statements_are_not() {
        for sql in [
            "SELECT 1.50::numeric",
            "  (select * from t)",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "VALUES (1)",
            "INSERT INTO t (a) VALUES (1) RETURNING a",
            "delete from t where a = 1 returning *",
        ] {
            assert!(is_row_query(sql), "{sql}");
        }
        for sql in [
            "CREATE COLLECTION t",
            "SET nodedb.x = 1",
            "BEGIN",
            "INSERT INTO t (a) VALUES (1)",
            "UPDATE t SET returning_flag = 1",
            "SHOW COLLECTIONS",
            "DESCRIBE t",
            "",
        ] {
            assert!(!is_row_query(sql), "{sql}");
        }
    }

    /// Compile-time check: `NodeDbRemote` implements `NodeDb`.
    #[test]
    fn remote_is_nodedb() {
        fn _accepts_dyn(_db: &dyn NodeDb) {}
        let _ = std::marker::PhantomData::<NodeDbRemote>;
    }
}
