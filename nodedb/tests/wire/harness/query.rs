// SPDX-License-Identifier: BUSL-1.1

//! Query / exec convenience methods on [`TestServer`].

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
    /// where the value may live in any column. Single-column SELECTs
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

    /// POST `sql` to the HTTP query route. Returns the status code and the body.
    pub async fn http_query(&self, sql: &str) -> (u16, String) {
        let response = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{}/v1/query", self.http_port))
            .json(&serde_json::json!({ "sql": sql }))
            .send()
            .await
            .unwrap_or_else(|e| panic!("POST /v1/query: {e}"));
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        (status, body)
    }

    /// [`Self::http_query`] expecting a success status.
    pub async fn http_exec(&self, sql: &str) {
        let (status, body) = self.http_query(sql).await;
        assert!(
            (200..300).contains(&status),
            "the request answered {status}: {body}"
        );
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
pub(super) fn pg_error_detail(e: &tokio_postgres::Error) -> String {
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
