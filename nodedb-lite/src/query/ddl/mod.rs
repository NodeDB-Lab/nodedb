//! DDL statement interception and dispatch for the Lite query engine.

pub mod alter;
pub mod columnar;
pub mod convert;
pub mod htap;
pub mod kv;
pub mod parser;
pub mod strict;
#[cfg(test)]
mod tests;
pub mod timeseries;

pub(crate) use parser::describe_strict_collection;

use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Intercept DDL statements before passing to DataFusion.
    ///
    /// Returns `Some(result)` if the statement was handled, `None` if it should
    /// be passed to DataFusion.
    pub(in crate::query) async fn try_handle_ddl(
        &self,
        sql: &str,
    ) -> Option<Result<QueryResult, LiteError>> {
        let upper = sql.trim().to_uppercase();

        // CREATE MATERIALIZED VIEW <target> FROM <source> ...
        if upper.starts_with("CREATE MATERIALIZED VIEW ") {
            return Some(self.handle_create_materialized_view(sql).await);
        }

        // DROP MATERIALIZED VIEW <target>
        if upper.starts_with("DROP MATERIALIZED VIEW ") {
            return Some(self.handle_drop_materialized_view(sql).await);
        }

        // CREATE TIMESERIES [COLLECTION] <name> ...
        if upper.starts_with("CREATE TIMESERIES ") {
            return Some(self.handle_create_timeseries(sql).await);
        }

        // CREATE COLLECTION ... WITH storage = 'strict'
        if upper.starts_with("CREATE COLLECTION ")
            && upper.contains("STORAGE")
            && upper.contains("STRICT")
        {
            return Some(self.handle_create_strict(sql).await);
        }

        // CREATE COLLECTION ... WITH storage = 'columnar'
        if upper.starts_with("CREATE COLLECTION ")
            && upper.contains("STORAGE")
            && upper.contains("COLUMNAR")
        {
            return Some(self.handle_create_columnar(sql).await);
        }

        // CREATE COLLECTION ... WITH storage = 'kv'
        if upper.starts_with("CREATE COLLECTION ")
            && upper.contains("STORAGE")
            && kv::is_kv_storage_mode(&upper)
        {
            return Some(self.handle_create_kv(sql).await);
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

                // Check if it's a columnar collection.
                let is_columnar = {
                    let columnar = match self.columnar.lock() {
                        Ok(c) => c,
                        Err(p) => p.into_inner(),
                    };
                    columnar.schema(&name_lower).is_some()
                };
                if is_columnar {
                    return Some(self.handle_drop_columnar(&name_lower).await);
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

        // CONVERT COLLECTION <name> TO strict|columnar|document
        if upper.starts_with("CONVERT COLLECTION ") || upper.starts_with("CONVERT ") {
            if upper.contains(" TO STRICT") {
                return Some(self.handle_convert_to_strict(sql).await);
            }
            if upper.contains(" TO COLUMNAR") {
                return Some(self.handle_convert_to_columnar(sql).await);
            }
            if upper.contains(" TO DOCUMENT") || upper.contains(" TO DOC") {
                return Some(self.handle_convert_to_document(sql).await);
            }
        }

        // ALTER TABLE <name> ADD COLUMN <col_def>
        if upper.starts_with("ALTER TABLE ")
            && (upper.contains("ADD COLUMN") || upper.contains("ADD "))
        {
            return Some(self.handle_alter_add_column(sql).await);
        }

        None
    }
}
