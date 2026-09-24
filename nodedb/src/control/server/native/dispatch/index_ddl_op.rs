// SPDX-License-Identifier: BUSL-1.1

//! Native index-DDL opcodes run the SQL DDL statements they name.
//!
//! | opcode                  | SQL                                              |
//! |-------------------------|--------------------------------------------------|
//! | `KvRegisterSortedIndex` | `CREATE SORTED INDEX`                            |
//! | `KvDropSortedIndex`     | `DROP SORTED INDEX`                              |
//! | `VectorSetParams`       | `ALTER VECTOR INDEX`, or `CREATE VECTOR INDEX`   |
//! | `KvRegisterIndex`       | `CREATE INDEX IF NOT EXISTS ON c (field)`        |
//! | `KvDropIndex`           | `DROP INDEX` of the index on the field           |
//! | `DocumentDropIndex`     | `DROP INDEX` of the index on the field           |
//! | `DocumentRegister`      | `CREATE COLLECTION IF NOT EXISTS`, then one `CREATE INDEX IF NOT EXISTS` per index path |
//!
//! The opcode then reaches the same catalog entries, engine steps and
//! per-connection transaction buffer as the SQL statements. The index is in
//! the catalog, survives a restart, and is listed by `SHOW INDEXES`. Inside
//! an explicit transaction it is visible to later statements, applies at
//! COMMIT and is discarded on ROLLBACK.
//!
//! `DocumentRegister` runs its statements in order and stops at the first
//! error. Each statement is `IF NOT EXISTS`, so a retry after a partial run
//! completes it.
//!
//! The opcode answers like any other opcode: `Ok` with no status row, count
//! or verb on success, the first failing statement's error on failure.
//!
//! Every rendered identifier passes the SQL identifier rules and is a plain
//! token of ASCII letters, digits and underscores, so no field value can
//! change the statement's shape.

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use super::{DispatchCtx, error_to_native_with_sqlstate, handle_sql};

/// Run the index-DDL opcode `op` as the SQL statement it names.
pub(crate) async fn handle_index_ddl_op(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    op: OpCode,
    fields: &TextFields,
) -> NativeResponse {
    let collection = fields
        .collection
        .as_deref()
        .unwrap_or("default")
        .to_lowercase();
    let statements = match index_ddl_statements(ctx, op, fields, &collection) {
        Ok(statements) => statements,
        Err(error) => return error_to_native_with_sqlstate(seq, "42601", &error),
    };
    for sql in &statements {
        let response = handle_sql(ctx, seq, sql, None).await;
        // A failure keeps the statement's error.
        if response.status != nodedb_types::protocol::ResponseStatus::Ok {
            return response;
        }
    }
    // An opcode answers as every opcode does: success carries no DDL status
    // row, no count and no verb.
    NativeResponse::ok(seq)
}

/// The DDL statements opcode `op` names, in the order they run.
fn index_ddl_statements(
    ctx: &DispatchCtx<'_>,
    op: OpCode,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<Vec<String>> {
    match op {
        OpCode::KvRegisterSortedIndex => Ok(vec![create_sorted_index(fields, collection)?]),
        OpCode::KvDropSortedIndex => {
            let name = ident(required(fields.index_name.as_deref(), "index_name")?)?;
            Ok(vec![format!("DROP SORTED INDEX {name}")])
        }
        OpCode::VectorSetParams => Ok(vec![vector_set_params(ctx, fields, collection)?]),
        OpCode::KvRegisterIndex => Ok(vec![kv_register_index(fields, collection)?]),
        OpCode::KvDropIndex | OpCode::DocumentDropIndex => {
            Ok(vec![drop_index_on_field(ctx, fields, collection)?])
        }
        OpCode::DocumentRegister => document_register(fields, collection),
        other => Err(bad_request(format!(
            "opcode {other:?} is not an index DDL opcode"
        ))),
    }
}

/// `KvRegisterIndex` indexes one field of the collection, backfilled from
/// every row it holds.
///
/// `backfill = false` is refused: a catalog index answers lookups for every
/// row, and an index that skipped the existing rows would miss them.
fn kv_register_index(fields: &TextFields, collection: &str) -> crate::Result<String> {
    if fields.backfill == Some(false) {
        return Err(bad_request(
            "KvRegisterIndex with backfill = false is not supported: an index covers every \
             row, so it is built from the rows the collection already holds",
        ));
    }
    let collection = ident(collection)?;
    let field = index_field(required(fields.field.as_deref(), "field")?)?;
    Ok(format!(
        "CREATE INDEX IF NOT EXISTS ON {collection} ({field})"
    ))
}

/// `DocumentRegister` makes sure the document collection exists, then
/// indexes each of its index paths.
fn document_register(fields: &TextFields, collection: &str) -> crate::Result<Vec<String>> {
    let collection = ident(collection)?;
    let mut statements = vec![format!(
        "CREATE COLLECTION IF NOT EXISTS {collection} WITH (engine='document_schemaless')"
    )];
    for path in fields.index_paths.as_deref().unwrap_or_default() {
        let field = index_field(path)?;
        statements.push(format!(
            "CREATE INDEX IF NOT EXISTS ON {collection} ({field})"
        ));
    }
    Ok(statements)
}

/// A top-level field named bare or as `$.field`.
fn index_field(raw: &str) -> crate::Result<String> {
    ident(raw.strip_prefix("$.").unwrap_or(raw))
}

fn create_sorted_index(fields: &TextFields, collection: &str) -> crate::Result<String> {
    let name = ident(required(fields.index_name.as_deref(), "index_name")?)?;
    let collection = ident(collection)?;
    let columns = fields
        .sort_columns
        .as_deref()
        .filter(|columns| !columns.is_empty())
        .ok_or_else(|| bad_request("missing 'sort_columns'"))?
        .iter()
        .map(|(column, direction)| {
            let direction = direction.to_ascii_uppercase();
            if direction != "ASC" && direction != "DESC" {
                return Err(bad_request(format!(
                    "invalid sort direction '{direction}', expected ASC or DESC"
                )));
            }
            Ok(format!("{} {direction}", ident(column)?))
        })
        .collect::<crate::Result<Vec<_>>>()?
        .join(", ");
    let key = ident(required(fields.key_column.as_deref(), "key_column")?)?;
    let mut sql = format!("CREATE SORTED INDEX {name} ON {collection} ({columns}) KEY {key}");
    match fields.window_type.as_deref().map(str::to_ascii_lowercase) {
        None => {}
        Some(window) if window == "none" => {}
        Some(window) if matches!(window.as_str(), "daily" | "weekly" | "monthly") => {
            sql.push_str(&format!(" WINDOW {}", window.to_ascii_uppercase()));
            if let Some(column) = fields.window_timestamp_column.as_deref() {
                sql.push_str(&format!(" ON {}", ident(column)?));
            }
        }
        Some(window) if window == "custom" => {
            sql.push_str(&format!(
                " WINDOW CUSTOM START {} END {}",
                fields.window_start_ms.unwrap_or(0),
                fields.window_end_ms.unwrap_or(0)
            ));
            if let Some(column) = fields.window_timestamp_column.as_deref() {
                sql.push_str(&format!(" ON {}", ident(column)?));
            }
        }
        Some(window) => {
            return Err(bad_request(format!(
                "invalid window type '{window}', expected none, daily, weekly, monthly or \
                 custom"
            )));
        }
    }
    Ok(sql)
}

/// `VectorSetParams` alters the vector index the column already carries, or
/// creates one when it has none.
fn vector_set_params(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<String> {
    let collection = ident(collection)?;
    let column = match fields.field_name.as_deref().filter(|f| !f.is_empty()) {
        Some(column) => Some(ident(column)?),
        None => None,
    };
    let index_type = fields.index_type.as_deref().map(plain_token).transpose()?;
    let metric = fields.metric.as_deref().map(plain_token).transpose()?;
    let existing = ctx.state.credentials.catalog().get_vector_index_params(
        ctx.database_id().as_u64(),
        ctx.tenant_id().as_u64(),
        &collection,
        column.as_deref().unwrap_or(""),
    )?;

    if let Some(existing) = existing {
        if metric.as_deref().is_some_and(|m| m != existing.metric) {
            return Err(bad_request(format!(
                "the vector index on '{collection}' uses metric '{}'; a metric change needs \
                 a new index",
                existing.metric
            )));
        }
        let mut set = Vec::new();
        if let Some(m) = fields.m {
            set.push(format!("m = {m}"));
        }
        if let Some(ef) = fields.ef_construction {
            set.push(format!("ef_construction = {ef}"));
        }
        if let Some(index_type) = &index_type {
            set.push(format!("index_type = {index_type}"));
        }
        if set.is_empty() {
            return Err(bad_request(
                "VectorSetParams on an existing index needs m, ef_construction or index_type",
            ));
        }
        let target = match &column {
            Some(column) => format!("{collection}.{column}"),
            None => collection.clone(),
        };
        return Ok(format!(
            "ALTER VECTOR INDEX ON {target} SET ({})",
            set.join(", ")
        ));
    }

    let name = match fields.index_name.as_deref() {
        Some(name) => ident(name)?,
        None => match &column {
            Some(column) => ident(&format!("vec_{collection}_{column}"))?,
            None => ident(&format!("vec_{collection}"))?,
        },
    };
    let mut sql = format!("CREATE VECTOR INDEX {name} ON {collection}");
    if let Some(column) = &column {
        sql.push_str(&format!(" ({column})"));
    }
    sql.push_str(&format!(" DIM {}", fields.vector_dim.unwrap_or(0)));
    if let Some(metric) = &metric {
        sql.push_str(&format!(" METRIC {metric}"));
    }
    if let Some(m) = fields.m {
        sql.push_str(&format!(" M {m}"));
    }
    if let Some(ef) = fields.ef_construction {
        sql.push_str(&format!(" EF_CONSTRUCTION {ef}"));
    }
    if let Some(index_type) = &index_type {
        sql.push_str(&format!(" INDEX_TYPE {index_type}"));
    }
    Ok(sql)
}

/// `KvDropIndex` and `DocumentDropIndex` name a field; the SQL statement
/// names the index on it.
fn drop_index_on_field(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<String> {
    let field = required(fields.field.as_deref(), "field")?;
    let path = if field.starts_with('$') {
        field.to_string()
    } else {
        format!("$.{field}")
    };
    let stored = ctx
        .state
        .credentials
        .catalog()
        .get_collection(ctx.database_id(), ctx.tenant_id().as_u64(), collection)?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id: ctx.tenant_id(),
            collection: collection.to_string(),
        })?;
    let index = stored
        .indexes
        .iter()
        .find(|index| index.field == path)
        .ok_or_else(|| bad_request(format!("no index on field '{field}' of '{collection}'")))?;
    Ok(format!("DROP INDEX {}", ident(&index.name)?))
}

fn required<'a>(value: Option<&'a str>, name: &str) -> crate::Result<&'a str> {
    value
        .filter(|v| !v.is_empty())
        .ok_or_else(|| bad_request(format!("missing '{name}'")))
}

/// A SQL identifier that renders without quoting.
fn ident(raw: &str) -> crate::Result<String> {
    let normalized =
        nodedb_sql::reserved::check_identifier(raw).map_err(|e| bad_request(e.to_string()))?;
    plain_token(&normalized)
}

/// A token of ASCII letters, digits and underscores.
fn plain_token(raw: &str) -> crate::Result<String> {
    let plain = !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if plain {
        Ok(raw.to_string())
    } else {
        Err(bad_request(format!(
            "'{raw}' must be letters, digits and underscores"
        )))
    }
}

fn bad_request(detail: impl Into<String>) -> crate::Error {
    crate::Error::BadRequest {
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sorted_index_opcode_renders_its_statement() {
        let fields = TextFields {
            index_name: Some("lb".into()),
            sort_columns: Some(vec![("score".into(), "desc".into())]),
            key_column: Some("player".into()),
            window_type: Some("daily".into()),
            window_timestamp_column: Some("ts".into()),
            ..TextFields::default()
        };
        assert_eq!(
            create_sorted_index(&fields, "scores").expect("renders"),
            "CREATE SORTED INDEX lb ON scores (score DESC) KEY player WINDOW DAILY ON ts"
        );
    }

    #[test]
    fn a_kv_index_opcode_renders_an_idempotent_create() {
        let fields = TextFields {
            field: Some("$.region".into()),
            ..TextFields::default()
        };
        assert_eq!(
            kv_register_index(&fields, "sessions").expect("renders"),
            "CREATE INDEX IF NOT EXISTS ON sessions (region)"
        );
        let skip_backfill = TextFields {
            field: Some("region".into()),
            backfill: Some(false),
            ..TextFields::default()
        };
        assert!(kv_register_index(&skip_backfill, "sessions").is_err());
    }

    #[test]
    fn a_register_opcode_creates_the_collection_then_each_index() {
        let fields = TextFields {
            index_paths: Some(vec!["$.region".into(), "status".into()]),
            ..TextFields::default()
        };
        assert_eq!(
            document_register(&fields, "orders").expect("renders"),
            vec![
                "CREATE COLLECTION IF NOT EXISTS orders WITH (engine='document_schemaless')"
                    .to_string(),
                "CREATE INDEX IF NOT EXISTS ON orders (region)".to_string(),
                "CREATE INDEX IF NOT EXISTS ON orders (status)".to_string(),
            ]
        );
        let nested = TextFields {
            index_paths: Some(vec!["$.a.b".into()]),
            ..TextFields::default()
        };
        assert!(document_register(&nested, "orders").is_err());
    }

    #[test]
    fn a_field_that_would_change_the_statement_is_refused() {
        let fields = TextFields {
            index_name: Some("lb; DROP".into()),
            sort_columns: Some(vec![("score".into(), "DESC".into())]),
            key_column: Some("player".into()),
            ..TextFields::default()
        };
        assert!(create_sorted_index(&fields, "scores").is_err());
        assert!(plain_token("cosine) KEY x").is_err());
    }
}
