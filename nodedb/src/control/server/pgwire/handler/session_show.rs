// SPDX-License-Identifier: BUSL-1.1

//! SHOW and SHOW ALL command handlers for session parameters.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{PgWireError, PgWireResult};

use crate::control::security::identity::AuthenticatedIdentity;

use super::super::types::text_field;
use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Handle SHOW commands: return session parameter values.
    pub(super) fn handle_show(
        &self,
        identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        use crate::control::server::shared::session::parse_show_command;
        use pgwire::error::ErrorInfo;

        let param = match parse_show_command(sql) {
            Some(p) => p,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    "syntax error: SHOW <parameter> or SHOW ALL".to_owned(),
                ))));
            }
        };

        if param == "all" {
            return self.handle_show_all(addr);
        }

        // `SHOW TENANT` (singular) reports the session's *effective* tenant —
        // the override installed via `SET TENANT = ...` if any, otherwise the
        // identity-bound tenant. Returns a single row with `tenant_id` and
        // `tenant_name` so a session that switched can confirm where its
        // writes will land. `SHOW TENANTS` (plural) is a separate DDL.
        if param == "tenant" {
            let effective = self
                .sessions
                .get_effective_tenant_id(addr)
                .unwrap_or(identity.tenant_id);
            let name = self
                .state
                .credentials
                .catalog()
                .load_all_tenants()
                .ok()
                .and_then(|tenants| {
                    tenants
                        .into_iter()
                        .find(|t| t.tenant_id == effective.as_u64())
                        .map(|t| t.name)
                })
                .unwrap_or_default();
            let schema = Arc::new(vec![text_field("tenant_id"), text_field("tenant_name")]);
            let mut encoder = DataRowEncoder::new(schema.clone());
            encoder.encode_field(&effective.as_u64().to_string())?;
            encoder.encode_field(&name)?;
            let row = encoder.take_row();
            return Ok(vec![Response::Query(QueryResponse::new(
                schema,
                futures::stream::iter(vec![Ok(row)]),
            ))]);
        }

        let value = self.resolve_guc(addr, &param)?;

        let schema = Arc::new(vec![text_field(&param)]);
        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder.encode_field(&value)?;
        let row = encoder.take_row();
        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(vec![Ok(row)]),
        ))])
    }

    /// Resolve a runtime parameter (GUC) value the same way `SHOW <param>`
    /// does: built-in PG-compat constants first, then a value explicitly
    /// set by `SET` in this session. If neither matches and the parameter
    /// is not on the known-parameter allowlist, return `42704`
    /// (`undefined_object`) — the same SQLSTATE PostgreSQL uses when a
    /// client requests an unrecognised runtime parameter. This prevents
    /// administrative commands like `SHOW DATABASES`, `SHOW ROLES`,
    /// `SHOW STATS`, `SHOW METRICS`, `SHOW MEMORY` from being silently
    /// swallowed as if they were unset session parameters; those commands
    /// are routed through the DDL / AST router before this handler is
    /// reached.
    pub(super) fn resolve_guc(
        &self,
        addr: &std::net::SocketAddr,
        param: &str,
    ) -> PgWireResult<String> {
        use crate::control::server::shared::session::is_known_pg_runtime_parameter;
        use pgwire::error::ErrorInfo;

        let builtin = match param {
            "server_version" => Some(nodedb_types::pg_compat::server_version_string(
                crate::version::VERSION,
            )),
            "server_version_num" => Some(nodedb_types::pg_compat::PG_COMPAT_VERSION_NUM.to_owned()),
            "server_encoding" => Some("UTF8".into()),
            _ => None,
        };
        let session_value = self.sessions.get_parameter(addr, param);

        match (builtin, session_value) {
            (Some(v), _) => Ok(v),
            (None, Some(v)) => Ok(v),
            (None, None) => {
                if !is_known_pg_runtime_parameter(param) {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42704".to_owned(),
                        format!("unrecognized configuration parameter \"{param}\""),
                    ))));
                }
                Ok(String::new())
            }
        }
    }

    /// SHOW ALL — return all session parameters.
    pub(super) fn handle_show_all(
        &self,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        let schema = Arc::new(vec![text_field("name"), text_field("setting")]);

        let params = self.sessions.all_parameters(addr);
        let mut rows = Vec::with_capacity(params.len());
        let mut encoder = DataRowEncoder::new(schema.clone());

        for (key, session_value) in &params {
            let value = match key.as_str() {
                "server_version" | "server_version_num" | "server_encoding" => {
                    self.resolve_guc(addr, key)?
                }
                _ => session_value.clone(),
            };
            encoder.encode_field(key)?;
            encoder.encode_field(&value)?;
            rows.push(Ok(encoder.take_row()));
        }

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(rows),
        ))])
    }
}
