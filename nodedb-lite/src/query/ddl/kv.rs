//! DDL handlers for KV collection operations in Lite.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::parser::parse_strict_create_sql;

// Re-export for use from DDL dispatch.
pub(super) use nodedb_types::kv_parsing::is_kv_storage_mode;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Handle: `CREATE COLLECTION <name> (<col_defs>) WITH storage = 'kv' [, ttl = ...]`
    pub(in crate::query) async fn handle_create_kv(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let (name, schema) = parse_strict_create_sql(sql)?;

        // Validate: exactly one PRIMARY KEY column required.
        let pk_columns: Vec<_> = schema.columns.iter().filter(|c| c.primary_key).collect();
        if pk_columns.is_empty() {
            return Err(LiteError::Query(
                "KV collections require a PRIMARY KEY column (the hash key)".into(),
            ));
        }
        if pk_columns.len() > 1 {
            return Err(LiteError::Query(
                "KV collections support exactly one PRIMARY KEY column".into(),
            ));
        }

        // Validate: PRIMARY KEY column type must be hashable.
        let pk = pk_columns[0];
        if !nodedb_types::is_valid_kv_key_type(&pk.column_type) {
            return Err(LiteError::Query(format!(
                "KV PRIMARY KEY type '{}' is not supported; \
                 use TEXT, UUID, INT, BIGINT, BYTES, or TIMESTAMP",
                pk.column_type
            )));
        }

        // Parse optional TTL from the WITH clause.
        let ttl = parse_lite_kv_ttl(sql, &schema)?;

        // Parse optional capacity hint.
        let capacity_hint = parse_lite_kv_capacity(sql);

        let config = nodedb_types::KvConfig {
            schema,
            ttl,
            capacity_hint,
            inline_threshold: nodedb_types::KV_DEFAULT_INLINE_THRESHOLD,
        };

        // Store KV collection metadata via NodeDbLite.
        let db = self.storage.clone();
        let config_clone = config.clone();
        let name_clone = name.clone();
        tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async {
                // Use the low-level storage to write metadata, same pattern as
                // other Lite DDL handlers.
                let meta = crate::nodedb::collection::ddl::CollectionMeta {
                    name: name_clone.clone(),
                    collection_type: "kv".to_string(),
                    created_at_ms: crate::nodedb::collection::ddl::now_ms(),
                    fields: config_clone
                        .schema
                        .columns
                        .iter()
                        .map(|c| (c.name.clone(), c.column_type.to_string()))
                        .collect(),
                    config_json: serde_json::to_string(&config_clone).ok(),
                };
                let key = format!("collection:{name_clone}");
                let bytes = serde_json::to_vec(&meta)
                    .map_err(|e| LiteError::Query(format!("serialize: {e}")))?;
                db.put(nodedb_types::Namespace::Meta, key.as_bytes(), &bytes)
                    .await
                    .map_err(|e| LiteError::Query(format!("storage: {e}")))?;
                Ok::<(), LiteError>(())
            })
        })?;

        let ttl_info = match &config.ttl {
            Some(nodedb_types::KvTtlPolicy::FixedDuration { duration_ms }) => {
                format!(" (ttl: {duration_ms}ms)")
            }
            Some(nodedb_types::KvTtlPolicy::FieldBased { field, offset_ms }) => {
                format!(" (ttl: {field} + {offset_ms}ms)")
            }
            None => String::new(),
        };

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "kv collection '{name}' created{ttl_info}"
            ))]],
            rows_affected: 0,
        })
    }
}

// ---------------------------------------------------------------------------
// TTL / capacity parsing (Lite-specific — wraps shared helpers with LiteError)
// ---------------------------------------------------------------------------

/// Parse TTL from the WITH clause in a Lite KV DDL statement.
fn parse_lite_kv_ttl(
    sql: &str,
    schema: &nodedb_types::StrictSchema,
) -> Result<Option<nodedb_types::KvTtlPolicy>, LiteError> {
    let upper = sql.to_uppercase();
    let ttl_pos = match nodedb_types::kv_parsing::find_with_option(&upper, "TTL") {
        Some(pos) => pos,
        None => return Ok(None),
    };

    let after_ttl = &sql[ttl_pos..];
    let after_eq = after_ttl
        .find('=')
        .map(|p| &after_ttl[p + 1..])
        .unwrap_or(after_ttl)
        .trim();

    let expr_end = nodedb_types::kv_parsing::find_with_option_end(after_eq);
    let expr = after_eq[..expr_end].trim();

    if expr.is_empty() {
        return Err(LiteError::Query("TTL expression is empty".into()));
    }

    // Field-based: <field_name> + INTERVAL '...'
    if let Some(plus_pos) = expr.find('+') {
        let field_name = expr[..plus_pos].trim().to_lowercase();
        let interval_part = expr[plus_pos + 1..].trim();

        if !schema.columns.iter().any(|c| c.name == field_name) {
            return Err(LiteError::Query(format!(
                "TTL field '{field_name}' not found in schema"
            )));
        }

        let offset_ms = nodedb_types::kv_parsing::parse_interval_to_ms(interval_part)
            .map_err(|e| LiteError::Query(e.to_string()))?;
        return Ok(Some(nodedb_types::KvTtlPolicy::FieldBased {
            field: field_name,
            offset_ms,
        }));
    }

    // Fixed duration: INTERVAL '...'
    if expr.to_uppercase().contains("INTERVAL") {
        let duration_ms = nodedb_types::kv_parsing::parse_interval_to_ms(expr)
            .map_err(|e| LiteError::Query(e.to_string()))?;
        return Ok(Some(nodedb_types::KvTtlPolicy::FixedDuration {
            duration_ms,
        }));
    }

    Err(LiteError::Query(format!(
        "invalid TTL expression: '{expr}'; \
         expected INTERVAL '...' or <field> + INTERVAL '...'"
    )))
}

/// Parse optional `capacity = N` from WITH clause.
fn parse_lite_kv_capacity(sql: &str) -> u32 {
    let upper = sql.to_uppercase();
    if let Some(pos) = nodedb_types::kv_parsing::find_with_option(&upper, "CAPACITY") {
        let after = &upper[pos + 8..];
        let after_eq = after
            .find('=')
            .map(|p| &after[p + 1..])
            .unwrap_or(after)
            .trim();
        let end = after_eq
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after_eq.len());
        after_eq[..end].trim().parse().unwrap_or(0)
    } else {
        0
    }
}
