// SPDX-License-Identifier: BUSL-1.1

//! Query / exec convenience methods on [`TestServer`], a second-connection
//! helper, and the `with_database` constructor.

use super::types::TestServer;

#[allow(dead_code)]
impl TestServer {
    /// Execute a SQL statement, returning the text of each row's first column.
    pub async fn query_text(&self, sql: &str) -> Result<Vec<String>, String> {
        let client = self.client.as_ref();
        match client.simple_query(sql).await {
            Ok(msgs) => {
                let mut rows = Vec::new();
                for msg in msgs {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                        rows.push(row.get(0).unwrap_or("").to_string());
                    }
                }
                Ok(rows)
            }
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Execute a SQL statement, returning each row's columns joined by tab.
    ///
    /// Useful for `SELECT *` assertions like `rows[0].contains(value)`
    /// where the value may live in any column.  Single-column SELECTs
    /// degrade to the column value directly (no separator emitted).
    pub async fn query_text_joined(&self, sql: &str) -> Result<Vec<String>, String> {
        let client = self.client.as_ref();
        match client.simple_query(sql).await {
            Ok(msgs) => {
                let mut rows = Vec::new();
                for msg in msgs {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                        let n = row.len();
                        let mut joined = String::new();
                        for i in 0..n {
                            if i > 0 {
                                joined.push('\t');
                            }
                            joined.push_str(row.get(i).unwrap_or(""));
                        }
                        rows.push(joined);
                    }
                }
                Ok(rows)
            }
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Execute a SQL statement, returning every row as a `HashMap` keyed by
    /// the column name reported in the row description. Useful for tests
    /// that need to assert on specific projected columns regardless of
    /// projection order. NULL columns are stored as the empty string.
    pub async fn query_named_rows(
        &self,
        sql: &str,
    ) -> Result<Vec<std::collections::HashMap<String, String>>, String> {
        let client = self.client.as_ref();
        match client.simple_query(sql).await {
            Ok(msgs) => {
                let mut rows: Vec<std::collections::HashMap<String, String>> = Vec::new();
                for msg in msgs {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                        // `SimpleColumn` is not `Clone`; collect names by
                        // borrowing the column slice directly.
                        let names: Vec<String> =
                            row.columns().iter().map(|c| c.name().to_string()).collect();
                        let mut map = std::collections::HashMap::with_capacity(names.len());
                        for (i, name) in names.into_iter().enumerate() {
                            map.insert(name, row.get(i).unwrap_or("").to_string());
                        }
                        rows.push(map);
                    }
                }
                Ok(rows)
            }
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Execute a SQL statement, returning every row as a Vec of its column
    /// values (in projection order). Column count is taken from the first
    /// row received.
    pub async fn query_rows(&self, sql: &str) -> Result<Vec<Vec<String>>, String> {
        let client = self.client.as_ref();
        match client.simple_query(sql).await {
            Ok(msgs) => {
                let mut rows: Vec<Vec<String>> = Vec::new();
                for msg in msgs {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                        let n = row.len();
                        let mut cols: Vec<String> = Vec::with_capacity(n);
                        for i in 0..n {
                            cols.push(row.get(i).unwrap_or("").to_string());
                        }
                        rows.push(cols);
                    }
                }
                Ok(rows)
            }
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Execute a SQL statement expecting success (no result needed).
    pub async fn exec(&self, sql: &str) -> Result<(), String> {
        let client = self.client.as_ref();
        match client.simple_query(sql).await {
            Ok(_) => Ok(()),
            Err(e) => Err(pg_error_detail(&e)),
        }
    }

    /// Open a second pgwire connection on the same listener under a different
    /// username. Returns a client and its background connection task handle.
    pub async fn connect_as(
        &self,
        user: &str,
        password: &str,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), String> {
        self.connect_as_database(user, password, "nodedb").await
    }

    /// Open a second pgwire connection under a user-selected database.
    pub async fn connect_as_database(
        &self,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), String> {
        let conn_str = format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            self.pg_port, user, password, database
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|error| pg_error_detail(&error))?;
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok((client, handle))
    }

    /// Spawn a server and connect to a named database.
    ///
    /// The database is created inside the running server after startup. A
    /// UUID suffix is appended to `name` to guarantee uniqueness across
    /// parallel test runs (e.g. `emp_prod_<uuid>`). The returned name is
    /// the full suffixed name so callers can reference it in subsequent
    /// queries.
    ///
    /// Existing tests that do not call this method pin implicitly to the
    /// built-in `default` database — no behavior change.
    pub async fn with_database(name: &str) -> (Self, String) {
        let server = Self::start().await;
        let unique_name = format!("{}_{}", name, uuid_v4_hex());
        server
            .client
            .simple_query(&format!("CREATE DATABASE {unique_name}"))
            .await
            .unwrap_or_else(|e| panic!("with_database: CREATE DATABASE {unique_name} failed: {e}"));
        server
            .client
            .simple_query(&format!("USE DATABASE {unique_name}"))
            .await
            .unwrap_or_else(|e| panic!("with_database: USE DATABASE {unique_name} failed: {e}"));
        (server, unique_name)
    }

    /// Execute a SQL statement expecting an error containing the given substring.
    pub async fn expect_error(&self, sql: &str, expected_substring: &str) {
        let client = self.client.as_ref();
        match client.simple_query(sql).await {
            Ok(_) => panic!("expected error containing '{expected_substring}', got success"),
            Err(e) => {
                let msg = pg_error_detail(&e);
                assert!(
                    msg.to_lowercase()
                        .contains(&expected_substring.to_lowercase()),
                    "expected error containing '{expected_substring}', got: {msg}"
                );
            }
        }
    }
}

/// Extract a detailed error message from a tokio-postgres error.
///
/// tokio-postgres `Error::to_string()` just returns "db error" — useless for
/// debugging. This pulls the actual server message out of the `DbError` when
/// available.
fn pg_error_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db_err) = e.as_db_error() {
        format!(
            "{}: {} (SQLSTATE {})",
            db_err.severity(),
            db_err.message(),
            db_err.code().code()
        )
    } else {
        format!("{e:?}")
    }
}

/// Generate a short hex string suitable for unique test name suffixes.
fn uuid_v4_hex() -> String {
    let id = uuid::Uuid::new_v4();
    let bytes = id.as_bytes();
    // Use the first 8 bytes (16 hex chars) — enough entropy for test isolation.
    format!(
        "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]
    )
}
