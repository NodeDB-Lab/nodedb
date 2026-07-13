// SPDX-License-Identifier: BUSL-1.1

//! Execute a prepared statement from an extended query portal.
//!
//! Binds parameter values from the portal into the SQL, then executes
//! through the same `execute_sql` path as SimpleQuery — preserving
//! all DDL dispatch, transaction handling, and permission checks.

use std::fmt::Debug;

use bytes::Bytes;
use futures::sink::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::control::server::response_shape::schema::{OutputColumn, OutputSchema};

use super::super::core::NodeDbPgHandler;
use super::super::routing::result_shaping::ResultShaping;
use super::result_format::{pg_type_to_ddl_col_type, resolve_result_formats};
use super::statement::ParsedStatement;

impl NodeDbPgHandler {
    /// Execute a prepared statement from a portal.
    ///
    /// Called by the `ExtendedQueryHandler::do_query` implementation.
    /// Binds parameters at the AST level (not SQL text substitution), then
    /// plans and dispatches through the standard pipeline.
    pub(crate) async fn execute_prepared<C>(
        &self,
        client: &mut C,
        portal: &Portal<ParsedStatement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let addr = client.socket_addr();
        let identity = self.resolve_identity(client, &addr)?;
        self.authorize_session_database(&identity, &addr)?;
        let stmt = &portal.statement.statement;
        let tenant_id = identity.tenant_id;

        // J.4: mirror `do_query`'s audit scope. The extended-query
        // path also triggers DDL (a prepared `CREATE COLLECTION`
        // binds parameters then dispatches), so audit context must
        // be installed here too or followers receive a plain
        // `CatalogDdl` with no SQL trail.
        let _audit_scope = crate::control::server::shared::session::audit_context::AuditScope::new(
            crate::control::server::shared::session::audit_context::AuditCtx {
                auth_user_id: identity.user_id.to_string(),
                auth_user_name: identity.username.clone(),
                sql_text: stmt.sql.clone(),
            },
        );

        // Wire-streaming COPY shapes for backup/restore. Recognised before
        // sqlparser-based execution because the shapes aren't standard COPY
        // grammar. See `control::backup::detect`.
        if let Some(intent) = crate::control::backup::detect(&stmt.sql) {
            return self.intent_to_response(&identity, addr, intent).await;
        }

        // Convert pgwire binary parameters to typed ParamValues for AST/DSL
        // binding. Done once, used by both the DSL path and the planned-SQL
        // path below.
        let params = convert_portal_params(
            &portal.parameters,
            &stmt.param_types,
            &portal.parameter_format,
        )?;

        // DSL passthroughs (SEARCH, GRAPH, MATCH, UPSERT INTO, etc.) cannot be
        // handled by the planned-SQL path because sqlparser doesn't parse the
        // DSL grammar. Before dispatching, substitute `$N` placeholders in the
        // SQL text via sqlparser's tokenizer (string/identifier/comment-aware).
        // `BoundDslSql` is a newtype — the compiler refuses to pass a raw
        // `&str` to a DSL execution path, so forgetting binding on a future
        // DSL is a compile error, not a runtime silent-drop.
        if stmt.is_dsl {
            let bound = nodedb_sql::dsl_bind::bind_dsl(&stmt.sql, &params).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".into(),
                    "42601".into(),
                    format!("DSL parameter bind: {e}"),
                )))
            })?;
            let mut results = self.execute_sql(&identity, &addr, bound.as_str()).await?;
            return Ok(results.pop().unwrap_or(Response::EmptyQuery));
        }

        // When the statement declared typed result columns via Describe, the
        // client expects DataRow messages with one field per declared column
        // (the RowDescription was already sent by Describe). Build a neutral
        // projection from the declared result fields — lookup_key == display_name
        // == field name, exactly matching the prior post-hoc reproject — so the
        // SELECT-read producer shapes and projects the response in one pass.
        // When no result columns were declared, no projection is applied.
        //
        // DML RETURNING rows are shaped as multi-column `RowsPayload` by the
        // `ReturningRows` producer (which ignores projection), so they stay
        // correct without any guard.
        // Resolve the client's requested per-column result formats (from the
        // Bind message), downgrading any column whose binary encoding is
        // feature-blocked back to text. Parallel to `stmt.result_fields`.
        let result_formats =
            resolve_result_formats(&stmt.result_fields, &portal.result_column_format);

        let projection: Option<OutputSchema> = if stmt.result_fields.is_empty() {
            None
        } else {
            Some(OutputSchema {
                columns: stmt
                    .result_fields
                    .iter()
                    .map(|f| OutputColumn {
                        display_name: f.name().into(),
                        lookup_key: f.name().into(),
                        // Carry each column's real catalog type (from the
                        // Describe-phase field) so the encoder can render the
                        // matching PostgreSQL text form and, for binary
                        // columns, extract the correctly-typed scalar.
                        ty: pg_type_to_ddl_col_type(f.datatype()),
                    })
                    .collect(),
                is_star: false,
            })
        };

        // Execute through the planned SQL path with AST-level parameter binding.
        let mut results = self
            .execute_planned_sql_with_params(
                &identity,
                &stmt.sql,
                tenant_id,
                &addr,
                &params,
                ResultShaping {
                    projection: projection.as_ref(),
                    formats: &result_formats,
                },
            )
            .await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}

/// Convert pgwire portal parameters to typed `ParamValue` for AST-level binding.
///
/// Uses per-parameter format codes from the pgwire 0.38 `Format` API to determine
/// whether each parameter was sent in text or binary format.
///
/// Binary-format NUMERIC, TIMESTAMP, and TIMESTAMPTZ parameters are explicitly
/// rejected with SQLSTATE 0A000 — their binary encodings are client-library-specific
/// structs that would produce corrupt values if decoded naively. Clients must use
/// text format for these types.
fn convert_portal_params(
    params: &[Option<Bytes>],
    param_types: &[Option<Type>],
    param_format: &pgwire::api::portal::Format,
) -> PgWireResult<Vec<nodedb_sql::ParamValue>> {
    let mut result = Vec::with_capacity(params.len());
    for (i, param) in params.iter().enumerate() {
        let pg_type = param_types
            .get(i)
            .and_then(|t| t.as_ref())
            .unwrap_or(&Type::UNKNOWN);

        let pv = match param {
            None => nodedb_sql::ParamValue::Null,
            Some(bytes) => {
                // Reject binary format for types whose binary encoding is
                // client-library-specific and cannot be decoded portably.
                if param_format.is_binary(i) {
                    let type_name = if *pg_type == Type::NUMERIC {
                        Some("NUMERIC")
                    } else if *pg_type == Type::TIMESTAMP {
                        Some("TIMESTAMP")
                    } else if *pg_type == Type::TIMESTAMPTZ {
                        Some("TIMESTAMPTZ")
                    } else {
                        None
                    };
                    if let Some(name) = type_name {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "0A000".to_owned(),
                            format!(
                                "binary {name} parameter format is not supported for \
                                 parameter ${n}; use text format",
                                n = i + 1
                            ),
                        ))));
                    }
                }

                let text = std::str::from_utf8(bytes).map_err(|_| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "22021".to_owned(),
                        format!("invalid UTF-8 in parameter ${}", i + 1),
                    )))
                })?;

                pgwire_text_to_param(text, pg_type)
            }
        };
        result.push(pv);
    }
    Ok(result)
}

/// Convert a pgwire text parameter + declared type to a typed
/// `ParamValue` for AST/DSL binding.
///
/// # Type coverage
///
/// Natively decoded: `BOOL`, `INT2`/`INT4`/`INT8`, `FLOAT4`/`FLOAT8`/
/// `NUMERIC`, `TIMESTAMP`, `TIMESTAMPTZ`, `TEXT`/`VARCHAR` (implicit via
/// fall-through), and `UNKNOWN` (the untyped-driver path).
///
/// # TIMESTAMP / TIMESTAMPTZ
///
/// Text-format TIMESTAMP and TIMESTAMPTZ parameters are parsed directly to
/// `ParamValue::Timestamp` / `ParamValue::Timestamptz`. This produces the
/// correct typed `SqlValue` variant (Timestamp vs Timestamptz) through the
/// resolver, ensuring the planner and engine see the right column type rather
/// than a generic string that must be coerced.
///
/// If parsing fails the text is passed through as `ParamValue::Text` so the
/// engine's string-coercion path can attempt a best-effort conversion — the
/// same as all other text-passthrough types.
///
/// # Fallback policy (catch-all arm)
///
/// Types the bind layer does not decode natively — `DATE`, `TIME`, `BYTEA`,
/// `UUID`, `JSON`, `JSONB`, `INTERVAL`, array types, and user-defined types —
/// fall through to `ParamValue::Text(text)`. The pgwire text representation of
/// these types is well-defined and the AST bind emits it as a
/// `SingleQuotedString`. Downstream, the planner/engine type-coerces the text
/// via the same path used for literal strings in simple-query SQL.
///
/// Binary-format parameters are handled at a layer above this function
/// (see `convert_portal_params`); they never reach this function.
///
/// # Why not error on unknown types
///
/// Postgres itself accepts text representations of every built-in type through
/// the extended-query protocol; refusing here would break drivers that
/// legitimately send dates/UUIDs/etc. as text.
fn pgwire_text_to_param(text: &str, pg_type: &Type) -> nodedb_sql::ParamValue {
    match *pg_type {
        Type::BOOL => {
            let lower = text.to_lowercase();
            if lower == "t" || lower == "true" || lower == "1" {
                return nodedb_sql::ParamValue::Bool(true);
            }
            if lower == "f" || lower == "false" || lower == "0" {
                return nodedb_sql::ParamValue::Bool(false);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::INT2 | Type::INT4 | Type::INT8 => {
            if let Ok(n) = text.parse::<i64>() {
                return nodedb_sql::ParamValue::Int64(n);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::FLOAT4 | Type::FLOAT8 => {
            if let Ok(f) = text.parse::<f64>() {
                return nodedb_sql::ParamValue::Float64(f);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::NUMERIC => {
            // Parse NUMERIC as exact Decimal, not lossy f64.
            if let Ok(d) = rust_decimal::Decimal::from_str_exact(text) {
                return nodedb_sql::ParamValue::Decimal(d);
            }
            // If parsing fails, return typed error — do not fall back to Float
            // since that would silently lose precision.
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::TIMESTAMP => {
            // Parse ISO 8601 / PostgreSQL timestamp text to a typed NaiveDateTime.
            if let Some(dt) = nodedb_types::datetime::NdbDateTime::parse(text) {
                return nodedb_sql::ParamValue::Timestamp(dt);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        Type::TIMESTAMPTZ => {
            // Parse ISO 8601 / PostgreSQL timestamptz text to a typed DateTime (UTC).
            if let Some(dt) = nodedb_types::datetime::NdbDateTime::parse(text) {
                return nodedb_sql::ParamValue::Timestamptz(dt);
            }
            nodedb_sql::ParamValue::Text(text.to_string())
        }
        // Text-passthrough types: wire-format text is already the
        // canonical representation. Engine performs type coercion.
        _ => nodedb_sql::ParamValue::Text(text.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use pgwire::api::portal::Format;

    use super::*;

    fn text_format() -> Format {
        Format::UnifiedText
    }

    fn binary_format() -> Format {
        Format::UnifiedBinary
    }

    #[test]
    fn convert_null_param() {
        let params = vec![None];
        let types = vec![Some(Type::INT8)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], nodedb_sql::ParamValue::Null));
    }

    #[test]
    fn convert_typed_params() {
        let params = vec![
            Some(Bytes::from_static(b"42")),
            Some(Bytes::from_static(b"hello")),
            Some(Bytes::from_static(b"true")),
        ];
        let types = vec![Some(Type::INT8), Some(Type::TEXT), Some(Type::BOOL)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(matches!(result[0], nodedb_sql::ParamValue::Int64(42)));
        assert!(matches!(&result[1], nodedb_sql::ParamValue::Text(s) if s == "hello"));
        assert!(matches!(result[2], nodedb_sql::ParamValue::Bool(true)));
    }

    #[test]
    fn convert_float_param() {
        let params = vec![Some(Bytes::from_static(b"2.78"))];
        let types = vec![Some(Type::FLOAT8)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(
            matches!(result[0], nodedb_sql::ParamValue::Float64(f) if (f - 2.78).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn convert_numeric_text_to_decimal() {
        let params = vec![Some(Bytes::from_static(b"123.45"))];
        let types = vec![Some(Type::NUMERIC)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        match &result[0] {
            nodedb_sql::ParamValue::Decimal(decimal) => assert_eq!(decimal.to_string(), "123.45"),
            other => panic!("expected Decimal, got {other:?}"),
        }
    }

    fn assert_binary_type_rejected(ty: Type, bytes: &'static [u8], name: &str) {
        let params = vec![Some(Bytes::from_static(bytes))];
        let types = vec![Some(ty)];
        let error = convert_portal_params(&params, &types, &binary_format()).unwrap_err();
        let message = error.to_string();
        assert!(message.contains(name) || message.contains("0A000"));
    }

    #[test]
    fn convert_numeric_binary_returns_error() {
        assert_binary_type_rejected(Type::NUMERIC, &[0x00, 0x03, 0x00, 0x02], "NUMERIC");
    }

    #[test]
    fn convert_timestamp_binary_returns_error() {
        assert_binary_type_rejected(Type::TIMESTAMP, &[0; 8], "TIMESTAMP");
    }

    #[test]
    fn convert_timestamptz_binary_returns_error() {
        assert_binary_type_rejected(Type::TIMESTAMPTZ, &[0; 8], "TIMESTAMPTZ");
    }

    fn assert_text_param(
        input: &'static [u8],
        ty: Type,
        expected: fn(&nodedb_sql::ParamValue) -> bool,
    ) {
        let params = vec![Some(Bytes::from_static(input))];
        let types = vec![Some(ty)];
        let result = convert_portal_params(&params, &types, &text_format()).unwrap();
        assert!(expected(&result[0]));
    }

    #[test]
    fn convert_timestamp_text_to_typed() {
        assert_text_param(b"2024-01-01 00:00:00", Type::TIMESTAMP, |value| {
            matches!(value, nodedb_sql::ParamValue::Timestamp(_))
        });
    }

    #[test]
    fn convert_timestamptz_text_to_typed() {
        assert_text_param(b"2024-01-01 00:00:00+00", Type::TIMESTAMPTZ, |value| {
            matches!(value, nodedb_sql::ParamValue::Timestamptz(_))
        });
    }

    #[test]
    fn convert_bool_variants() {
        for (input, expected) in [("t", true), ("f", false), ("1", true), ("0", false)] {
            let params = vec![Some(Bytes::from(input))];
            let types = vec![Some(Type::BOOL)];
            let result = convert_portal_params(&params, &types, &text_format()).unwrap();
            assert!(matches!(result[0], nodedb_sql::ParamValue::Bool(value) if value == expected));
        }
    }

    #[test]
    fn passthrough_date_text() {
        let value = pgwire_text_to_param("2026-04-19", &Type::DATE);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "2026-04-19"));
    }

    #[test]
    fn timestamp_text_parses_to_typed() {
        let value = pgwire_text_to_param("2026-04-19 12:00:00", &Type::TIMESTAMP);
        assert!(matches!(value, nodedb_sql::ParamValue::Timestamp(_)));
    }

    #[test]
    fn timestamptz_text_parses_to_typed() {
        let value = pgwire_text_to_param("2026-04-19 12:00:00+00", &Type::TIMESTAMPTZ);
        assert!(matches!(value, nodedb_sql::ParamValue::Timestamptz(_)));
    }

    #[test]
    fn passthrough_uuid_text() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let value = pgwire_text_to_param(uuid, &Type::UUID);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == uuid));
    }

    #[test]
    fn passthrough_jsonb_text() {
        let json = r#"{"a":1}"#;
        let value = pgwire_text_to_param(json, &Type::JSONB);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == json));
    }

    #[test]
    fn passthrough_bytea_hex_text() {
        let value = pgwire_text_to_param("\\xDEADBEEF", &Type::BYTEA);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "\\xDEADBEEF"));
    }

    #[test]
    fn int_parse_failure_falls_back_to_text() {
        let value = pgwire_text_to_param("abc", &Type::INT8);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "abc"));
    }

    #[test]
    fn unknown_type_routes_to_text() {
        let value = pgwire_text_to_param("42", &Type::UNKNOWN);
        assert!(matches!(&value, nodedb_sql::ParamValue::Text(text) if text == "42"));
    }
}
