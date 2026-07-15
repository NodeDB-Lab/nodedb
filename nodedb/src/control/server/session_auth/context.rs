// SPDX-License-Identifier: BUSL-1.1

//! `AuthContext` construction, scope enrichment, and per-query `ON DENY`
//! extraction.

use nodedb_sql::parser::preprocess::lex::rfind_ascii_case_insensitive;

use crate::control::security::auth_context::{AuthContext, generate_session_id};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::security::util::base64_url_decode;

/// Build an `AuthContext` from an `AuthenticatedIdentity`.
///
/// This is the centralized factory used by all auth flows (password,
/// API key, certificate, trust). JWT flows can use `AuthContext::from_jwt()`
/// directly when JWT claims are available for richer context.
pub fn build_auth_context(identity: &AuthenticatedIdentity) -> AuthContext {
    let mut ctx = AuthContext::from_identity(identity, generate_session_id());
    // Stamp the per-user default database so `$auth.database_id` is available
    // for RLS predicates even before a `USE DATABASE` command.
    ctx.database_id = identity.default_database;
    ctx
}

/// Enrich AuthContext with scope status data from the scope grant store.
///
/// Populates metadata entries for `scope_status.<name>` and `scope_expires_at.<name>`
/// so RLS predicates can reference `$auth.metadata.scope_status.pro:all`.
pub fn enrich_auth_context_with_scopes(
    ctx: &mut AuthContext,
    scope_grants: &crate::control::security::scope::grant::ScopeGrantStore,
    org_ids: &[String],
) {
    let effective = scope_grants.effective_scopes(&ctx.id, org_ids);
    for scope_name in &effective {
        let status = scope_grants.scope_status(scope_name, "user", &ctx.id);
        ctx.metadata
            .insert(format!("scope_status.{scope_name}"), status.to_string());
        let expires_at = scope_grants.scope_expires_at(scope_name, "user", &ctx.id);
        if expires_at > 0 {
            ctx.metadata.insert(
                format!("scope_expires_at.{scope_name}"),
                expires_at.to_string(),
            );
        }
    }
    // Also set a comma-separated list of effective scopes.
    let scope_list: Vec<String> = effective.into_iter().collect();
    if !scope_list.is_empty() {
        ctx.metadata.insert("scopes".into(), scope_list.join(","));
    }
}

/// Build an `AuthContext` with pgwire session overrides applied.
///
/// Reads `nodedb.on_deny`, `nodedb.auth_token`, and `nodedb.auth_session`
/// from session parameters. Per-transaction JWT (`nodedb.auth_token`) takes
/// precedence — it creates a full AuthContext from the token's claims,
/// replacing the connection-level identity for RLS purposes.
pub fn build_auth_context_with_session(
    identity: &AuthenticatedIdentity,
    sessions: &crate::control::server::shared::session::SessionStore,
    addr: &std::net::SocketAddr,
) -> AuthContext {
    // Per-transaction JWT: SET LOCAL nodedb.auth_token = 'eyJ...'
    // Validates the JWT and builds AuthContext from its claims.
    if let Some(token) = sessions.get_parameter(addr, "nodedb.auth_token") {
        // Validate JWT structure (3 parts) and decode claims.
        if token.matches('.').count() == 2 {
            // Decode claims without signature verification (the token was
            // already validated when the session handle or original auth
            // was established — this is a per-transaction override).
            if let Some(payload_b64) = token.split('.').nth(1)
                && let Some(payload_bytes) = base64_url_decode(payload_b64)
                && let Ok(claims) =
                    sonic_rs::from_slice::<crate::control::security::jwt::JwtClaims>(&payload_bytes)
            {
                let mut ctx = AuthContext::from_jwt(&claims, generate_session_id());
                // Still apply ON DENY override.
                if let Some(on_deny_val) = sessions.get_parameter(addr, "nodedb.on_deny")
                    && let Ok(mode) = crate::control::security::deny::parse_on_deny(&[&on_deny_val])
                {
                    ctx.on_deny_override = Some(mode);
                }
                return ctx;
            }
        }
    }

    let mut ctx = build_auth_context(identity);

    // Read ON DENY override from SET LOCAL nodedb.on_deny = '...'.
    if let Some(on_deny_val) = sessions.get_parameter(addr, "nodedb.on_deny")
        && let Ok(mode) = crate::control::security::deny::parse_on_deny(&[&on_deny_val])
    {
        ctx.on_deny_override = Some(mode);
    }

    // The active session database overrides the per-user default so that
    // `$auth.database_id` tracks `USE DATABASE` commands within a session.
    if let Some(db) = sessions.get_current_database(addr) {
        ctx.database_id = Some(db);
    }

    ctx
}

/// Extract a per-query `ON DENY` clause from SQL and apply it to the auth context.
///
/// Parses: `SELECT ... ON DENY ERROR 'CODE' MESSAGE '...'`
/// Strips the `ON DENY` clause from the SQL and sets `auth_ctx.on_deny_override`.
/// Returns the cleaned SQL.
pub fn extract_and_apply_on_deny(
    sql: &str,
    auth_ctx: &mut crate::control::security::auth_context::AuthContext,
) -> String {
    let Some(idx) = rfind_ascii_case_insensitive(sql, "on deny ") else {
        return sql.to_string();
    };

    // Only strip ON DENY from SELECT/WITH queries (not CREATE RLS POLICY).
    let trimmed = sql.trim_start();
    if !trimmed
        .get(.."select".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select"))
        && !trimmed
            .get(.."with".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("with"))
    {
        return sql.to_string();
    }

    let on_deny_part = &sql[idx + "on deny ".len()..];
    let parts: Vec<&str> = on_deny_part.split_whitespace().collect();
    match crate::control::security::deny::parse_on_deny(&parts) {
        Ok(mode) => {
            auth_ctx.on_deny_override = Some(mode);
            sql[..idx].trim_end().to_string()
        }
        Err(_) => sql.to_string(),
    }
}
