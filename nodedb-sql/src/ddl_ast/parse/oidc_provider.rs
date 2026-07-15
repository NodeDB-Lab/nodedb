// SPDX-License-Identifier: Apache-2.0

//! Parse OIDC provider DDL statements.
//!
//! Syntax:
//!
//! ```sql
//! CREATE OIDC PROVIDER <name>
//!     ISSUER '<iss>'
//!     JWKS_URI '<uri>'
//!     TENANT <u64>
//!     [AUDIENCE '<aud>']
//!     [CLAIM MAPPING WHEN <claim_name> = '<value>'
//!         [SET DEFAULT_DATABASE = <id>]
//!         [ADD DATABASES [<id>, ...]]
//!         [ADD ROLES ['<role>', ...]]
//!     ...]
//!
//! ALTER OIDC PROVIDER <name>
//!     SET CLAIM MAPPING WHEN <claim_name> = '<value>'
//!         [SET DEFAULT_DATABASE = <id>]
//!         [ADD DATABASES [<id>, ...]]
//!         [ADD ROLES ['<role>', ...]]
//!     [...]
//!
//! DROP OIDC PROVIDER [IF EXISTS] <name>
//!
//! SHOW OIDC PROVIDERS
//! ```

use crate::ddl_ast::statement::{AuthStmt, NodedbStatement, OidcClaimMappingClause};
use crate::error::SqlError;
use crate::parser::preprocess::lex::find_ascii_case_insensitive_from;

pub(super) fn try_parse(
    _upper: &str,
    _parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    let statement = strip_trailing_terminator(trimmed);
    let upper = statement.to_uppercase();
    let parts: Vec<&str> = statement.split_whitespace().collect();
    if upper.starts_with("CREATE OIDC PROVIDER ") {
        return Some(parse_create(&parts, statement));
    }
    if upper.starts_with("ALTER OIDC PROVIDER ") {
        return Some(parse_alter(&parts, statement));
    }
    if upper.starts_with("DROP OIDC PROVIDER ") {
        return Some(parse_drop(&parts));
    }
    if upper == "SHOW OIDC PROVIDERS" {
        return Some(Ok(NodedbStatement::Auth(AuthStmt::ShowOidcProviders)));
    }
    None
}

// ── CREATE ─────────────────────────────────────────────────────────────────

fn parse_create(parts: &[&str], trimmed: &str) -> Result<NodedbStatement, SqlError> {
    let trimmed = strip_trailing_terminator(trimmed);
    // parts: CREATE OIDC PROVIDER <name> ISSUER '<iss>' JWKS_URI '<uri>' TENANT <u64> ...
    let name = parts
        .get(3)
        .ok_or_else(|| SqlError::Parse {
            detail: "syntax: CREATE OIDC PROVIDER <name> ISSUER '<iss>' JWKS_URI '<uri>'"
                .to_string(),
        })?
        .to_string();

    // Provider clauses end at CLAIM MAPPING, so claim names cannot satisfy or
    // overwrite the provider's top-level configuration.
    let clause_start = after_token(trimmed, 4);
    let mappings_start = claim_mapping_start(trimmed, clause_start);
    let provider_clauses = &trimmed[clause_start..mappings_start];
    let issuer =
        extract_keyword_value(provider_clauses, "ISSUER")?.ok_or_else(|| SqlError::Parse {
            detail: "CREATE OIDC PROVIDER: ISSUER '<url>' is required".to_string(),
        })?;

    let jwks_uri =
        extract_keyword_value(provider_clauses, "JWKS_URI")?.ok_or_else(|| SqlError::Parse {
            detail: "CREATE OIDC PROVIDER: JWKS_URI '<url>' is required".to_string(),
        })?;

    let tenant_id = parse_required_u64_keyword(provider_clauses, "TENANT", "CREATE OIDC PROVIDER")?;

    let audience = extract_keyword_value(provider_clauses, "AUDIENCE")?;

    let claim_mappings = parse_claim_mappings(&trimmed[mappings_start..])?;

    Ok(NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
        name,
        issuer,
        jwks_uri,
        tenant_id,
        audience,
        claim_mappings,
    }))
}

// ── ALTER ──────────────────────────────────────────────────────────────────

fn parse_alter(parts: &[&str], trimmed: &str) -> Result<NodedbStatement, SqlError> {
    // ALTER OIDC PROVIDER <name> SET CLAIM MAPPING WHEN ...
    let name = parts
        .get(3)
        .ok_or_else(|| SqlError::Parse {
            detail: "syntax: ALTER OIDC PROVIDER <name> SET CLAIM MAPPING WHEN ...".to_string(),
        })?
        .to_string();

    let mappings_start = claim_mapping_start(trimmed, after_token(trimmed, 4));
    let claim_mappings = parse_claim_mappings(&trimmed[mappings_start..])?;

    Ok(NodedbStatement::Auth(
        AuthStmt::AlterOidcProviderClaimMapping {
            name,
            claim_mappings,
        },
    ))
}

// ── DROP ───────────────────────────────────────────────────────────────────

fn parse_drop(parts: &[&str]) -> Result<NodedbStatement, SqlError> {
    // DROP OIDC PROVIDER [IF EXISTS] <name>
    // parts: DROP OIDC PROVIDER ...
    let (if_exists, name_idx) = if parts.get(3).map(|s| s.eq_ignore_ascii_case("IF")) == Some(true)
    {
        (true, 5) // IF EXISTS <name>
    } else {
        (false, 3)
    };

    let name = parts
        .get(name_idx)
        .ok_or_else(|| SqlError::Parse {
            detail: "syntax: DROP OIDC PROVIDER [IF EXISTS] <name>".to_string(),
        })?
        .to_string();

    Ok(NodedbStatement::Auth(AuthStmt::DropOidcProvider {
        name,
        if_exists,
    }))
}

// ── Claim-mapping parser ────────────────────────────────────────────────────

/// Parse zero or more `WHEN <claim_name> = '<value>' ...` clauses.
///
/// Each clause is delimited by the next `WHEN` (case-insensitive) or end-of-input.
fn parse_claim_mappings(trimmed: &str) -> Result<Vec<OidcClaimMappingClause>, SqlError> {
    // Find all WHEN keyword positions (case-insensitive, whole-word).
    let mut clauses = Vec::new();

    // Split the input into segments starting at each "WHEN".
    let mut positions: Vec<usize> = Vec::new();
    let mut search_from = 0;
    while let Some(pos) = find_keyword(trimmed, "WHEN", search_from) {
        positions.push(pos);
        search_from = pos + 4;
    }

    for (i, &start) in positions.iter().enumerate() {
        let end = positions.get(i + 1).copied().unwrap_or(trimmed.len());
        let segment = &trimmed[start..end];
        let clause = parse_when_clause(segment)?;
        clauses.push(clause);
    }

    Ok(clauses)
}

/// Parse a single `WHEN <claim_name> = '<value>' [SET DEFAULT_DATABASE = <id>]
/// [ADD DATABASES [...]] [ADD ROLES [...]]` segment.
fn parse_when_clause(segment: &str) -> Result<OidcClaimMappingClause, SqlError> {
    let after_when = segment["WHEN".len()..].trim_start();
    let claim_name_end = after_when
        .find(char::is_whitespace)
        .ok_or_else(|| SqlError::Parse {
            detail: "CLAIM MAPPING: missing claim name after WHEN".to_string(),
        })?;
    let claim_name = after_when[..claim_name_end].to_lowercase();
    let after_claim_name = after_when[claim_name_end..].trim_start();
    let after_equals = after_claim_name
        .strip_prefix('=')
        .ok_or_else(|| SqlError::Parse {
            detail: "CLAIM MAPPING: syntax is WHEN <claim> = '<value>'".to_string(),
        })?
        .trim_start();
    let claim_value = parse_quoted_value(
        after_equals,
        "CLAIM MAPPING: syntax is WHEN <claim> = '<value>'",
    )?;

    // Action sequences are found outside quoted claim values and only when
    // every keyword is a complete word. This prevents claim names and values
    // such as `roles` or `'ADD DATABASES'` from being mistaken for actions.
    let default_database = find_action_end(segment, &["SET", "DEFAULT_DATABASE"])
        .and_then(|end| extract_u64_after_eq(&segment[end..]));
    let add_databases = find_action_end(segment, &["ADD", "DATABASES"])
        .map(|end| extract_u64_list(&segment[end..], "DATABASES"))
        .transpose()?
        .unwrap_or_default();
    let add_roles = find_action_end(segment, &["ADD", "ROLES"])
        .map(|end| extract_string_list(&segment[end..], "ROLES"))
        .transpose()?
        .unwrap_or_default();

    Ok(OidcClaimMappingClause {
        claim_name,
        claim_value,
        default_database,
        add_databases,
        add_roles,
    })
}

// ── Token extraction helpers ────────────────────────────────────────────────

/// Extract the quoted string value that follows `KEYWORD` in provider clauses.
///
/// Both single- and double-quoted strings are accepted, but bare tokens and
/// malformed strings are rejected.
fn extract_keyword_value(trimmed: &str, keyword: &str) -> Result<Option<String>, SqlError> {
    let Some(pos) = find_keyword(trimmed, keyword, 0) else {
        return Ok(None);
    };
    let detail = format!("CREATE OIDC PROVIDER: {keyword} must be a quoted string");
    parse_quoted_value(trimmed[pos + keyword.len()..].trim_start(), &detail).map(Some)
}

/// Parse one SQL-style single- or double-quoted string with doubled delimiters.
///
/// The value must end before whitespace or input end so tokens cannot be
/// silently accepted after its closing quote.
fn parse_quoted_value(input: &str, detail: &str) -> Result<String, SqlError> {
    let Some(quote) = input.chars().next().filter(|ch| matches!(ch, '\'' | '"')) else {
        return Err(SqlError::Parse {
            detail: detail.to_string(),
        });
    };

    let value = &input[quote.len_utf8()..];
    let mut parsed = String::new();
    let mut index = 0;
    while index < value.len() {
        let Some(ch) = value[index..].chars().next() else {
            break;
        };
        if ch != quote {
            parsed.push(ch);
            index += ch.len_utf8();
            continue;
        }

        let after_quote = index + quote.len_utf8();
        if value[after_quote..].starts_with(quote) {
            parsed.push(quote);
            index = after_quote + quote.len_utf8();
            continue;
        }
        if value[after_quote..]
            .chars()
            .next()
            .is_some_and(|ch| !ch.is_whitespace())
        {
            return Err(SqlError::Parse {
                detail: detail.to_string(),
            });
        }
        return Ok(parsed);
    }

    Err(SqlError::Parse {
        detail: detail.to_string(),
    })
}

/// Remove one optional statement terminator from the end of an OIDC statement.
fn strip_trailing_terminator(input: &str) -> &str {
    input.strip_suffix(';').unwrap_or(input)
}

/// Extract the required unsigned integer immediately following `KEYWORD`.
fn parse_required_u64_keyword(
    original: &str,
    keyword: &str,
    statement: &str,
) -> Result<u64, SqlError> {
    let Some(pos) = find_keyword(original, keyword, 0) else {
        return Err(SqlError::Parse {
            detail: format!("{statement}: {keyword} <u64> is required"),
        });
    };
    let raw_value = original[pos + keyword.len()..]
        .split_whitespace()
        .next()
        .ok_or_else(|| SqlError::Parse {
            detail: format!("{statement}: {keyword} must be a u64"),
        })?;
    if !raw_value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SqlError::Parse {
            detail: format!("{statement}: {keyword} must be a u64"),
        });
    }
    raw_value.parse::<u64>().map_err(|_| SqlError::Parse {
        detail: format!("{statement}: {keyword} must be a u64"),
    })
}

/// Return the start of the first top-level `CLAIM MAPPING` clause, or input end.
fn claim_mapping_start(input: &str, search_from: usize) -> usize {
    let mut search_from = search_from;
    while let Some(pos) = find_keyword(input, "CLAIM", search_from) {
        let after_claim = &input[pos + "CLAIM".len()..];
        let whitespace = after_claim.len() - after_claim.trim_start().len();
        let mapping_start = pos + "CLAIM".len() + whitespace;
        if find_keyword(input, "MAPPING", mapping_start) == Some(mapping_start) {
            return pos;
        }
        search_from = pos + "CLAIM".len();
    }
    input.len()
}

/// Return the byte offset immediately after the first `token_count`
/// whitespace-delimited tokens.
fn after_token(input: &str, token_count: usize) -> usize {
    let mut tokens_seen = 0;
    let mut in_token = false;
    for (index, ch) in input.char_indices() {
        if ch.is_whitespace() {
            if in_token {
                tokens_seen += 1;
                if tokens_seen == token_count {
                    return index;
                }
                in_token = false;
            }
        } else {
            in_token = true;
        }
    }
    input.len()
}

/// Extract a `u64` value following an action's `=`, if present.
fn extract_u64_after_eq(after_action: &str) -> Option<u64> {
    let after_eq = after_action.trim_start().strip_prefix('=')?.trim_start();
    let tok = after_eq.split_whitespace().next()?;
    tok.parse::<u64>().ok()
}

/// Extract `[<id>, <id>, ...]` immediately following an `ADD DATABASES` action.
fn extract_u64_list(after_action: &str, keyword: &str) -> Result<Vec<u64>, SqlError> {
    let after = after_action.trim_start();
    if !after.starts_with('[') {
        // Possibly `ADD DATABASES` without a list — treat as empty.
        return Ok(Vec::new());
    }
    let close = after.find(']').ok_or_else(|| SqlError::Parse {
        detail: format!("ADD {keyword}: missing closing ']'"),
    })?;
    let inner = &after[1..close];
    inner
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<u64>().map_err(|_| SqlError::Parse {
                detail: format!("ADD {keyword}: invalid database id '{s}'"),
            })
        })
        .collect()
}

/// Extract `['<role>', ...]` immediately following an `ADD ROLES` action.
fn extract_string_list(after_action: &str, keyword: &str) -> Result<Vec<String>, SqlError> {
    let after = after_action.trim_start();
    if !after.starts_with('[') {
        return Ok(Vec::new());
    }
    let close = after.find(']').ok_or_else(|| SqlError::Parse {
        detail: format!("ADD {keyword}: missing closing ']'"),
    })?;
    let inner = &after[1..close];
    Ok(inner
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

/// Find the byte offset of a quote-aware whole-word ASCII keyword match.
fn find_keyword(haystack: &str, keyword: &str, from: usize) -> Option<usize> {
    let bytes = haystack.as_bytes();
    let keyword_len = keyword.len();
    let mut index = from;
    let mut quote = None;
    while index + keyword_len <= bytes.len() {
        if let Some(delimiter) = quote {
            if bytes[index] == delimiter {
                if bytes.get(index + 1) == Some(&delimiter) {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            index += 1;
            continue;
        }
        if matches!(bytes[index], b'\'' | b'"') {
            quote = Some(bytes[index]);
            index += 1;
            continue;
        }
        let before_ok = index == 0 || !is_word_byte(bytes[index - 1]);
        let after = index + keyword_len;
        let after_ok = after == bytes.len() || !is_word_byte(bytes[after]);
        if before_ok
            && after_ok
            && find_ascii_case_insensitive_from(haystack, keyword, index) == Some(index)
        {
            return Some(index);
        }
        index += 1;
    }
    None
}

/// Find the end of an exact, quote-aware, whitespace-delimited action sequence.
fn find_action_end(input: &str, words: &[&str]) -> Option<usize> {
    let mut search_from = 0;
    while let Some(start) = find_keyword(input, words[0], search_from) {
        let mut end = start + words[0].len();
        let mut matched = true;
        for word in &words[1..] {
            let whitespace = input[end..].len() - input[end..].trim_start().len();
            if whitespace == 0 {
                matched = false;
                break;
            }
            end += whitespace;
            if find_ascii_case_insensitive_from(input, word, end) != Some(end)
                || input
                    .as_bytes()
                    .get(end + word.len())
                    .is_some_and(|byte| is_word_byte(*byte))
            {
                matched = false;
                break;
            }
            end += word.len();
        }
        if matched {
            return Some(end);
        }
        search_from = start + words[0].len();
    }
    None
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::statement::NodedbStatement;

    fn ok(sql: &str) -> NodedbStatement {
        let upper = sql.to_uppercase();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        try_parse(&upper, &parts, sql)
            .expect("expected Some")
            .expect("expected Ok")
    }

    #[test]
    fn quoted_provider_value_after_unicode_text_preserves_original_offsets() {
        assert_eq!(
            extract_keyword_value("prefixﬀﬀ ISSUER 'https://idp.example'", "ISSUER").unwrap(),
            Some("https://idp.example".to_string())
        );
    }

    #[test]
    fn required_numeric_value_after_unicode_text_preserves_original_offsets() {
        assert_eq!(
            parse_required_u64_keyword("prefixﬀﬀ TENANT 42", "TENANT", "provider").unwrap(),
            42
        );
    }

    #[test]
    fn claim_mapping_boundaries_after_unicode_values_preserve_original_offsets() {
        let mappings = parse_claim_mappings(
            "WHEN sub = 'ﬀﬀ' ADD ROLES ['readonly'] WHEN aud = 'api' ADD DATABASES [7]",
        )
        .unwrap();
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].add_roles, vec!["readonly"]);
        assert_eq!(mappings[1].add_databases, vec![7]);
    }

    #[test]
    fn show_oidc_providers() {
        assert_eq!(
            ok("SHOW OIDC PROVIDERS"),
            NodedbStatement::Auth(AuthStmt::ShowOidcProviders)
        );
    }

    #[test]
    fn oidc_statements_accept_one_trailing_terminator() {
        assert_eq!(
            ok("SHOW OIDC PROVIDERS;"),
            NodedbStatement::Auth(AuthStmt::ShowOidcProviders)
        );
        assert_eq!(
            ok("DROP OIDC PROVIDER IF EXISTS myidp;"),
            NodedbStatement::Auth(AuthStmt::DropOidcProvider {
                name: "myidp".to_string(),
                if_exists: true,
            })
        );
        match ok(
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1;",
        ) {
            NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
                claim_mappings,
                ..
            }) => {
                assert_eq!(claim_mappings[0].default_database, Some(1));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn drop_oidc_provider() {
        let stmt = ok("DROP OIDC PROVIDER myidp");
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::DropOidcProvider {
                name: "myidp".to_string(),
                if_exists: false,
            })
        );
    }

    #[test]
    fn drop_oidc_provider_if_exists() {
        let stmt = ok("DROP OIDC PROVIDER IF EXISTS myidp");
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::DropOidcProvider {
                name: "myidp".to_string(),
                if_exists: true,
            })
        );
    }

    #[test]
    fn create_oidc_provider_requires_numeric_tenant() {
        for sql in [
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://auth0.example.com' JWKS_URI 'https://auth0.example.com/jwks'",
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://auth0.example.com' JWKS_URI 'https://auth0.example.com/jwks' TENANT acme",
        ] {
            let upper = sql.to_uppercase();
            let parts: Vec<&str> = sql.split_whitespace().collect();
            assert!(
                matches!(
                    try_parse(&upper, &parts, sql),
                    Some(Err(SqlError::Parse { .. }))
                ),
                "CREATE OIDC PROVIDER must require a numeric TENANT: {sql}"
            );
        }
    }

    #[test]
    fn create_oidc_provider_with_tenant_terminator() {
        let sql = "CREATE OIDC PROVIDER auth0 ISSUER 'https://auth0.example.com' JWKS_URI 'https://auth0.example.com/.well-known/jwks.json' TENANT 42;";
        let stmt = ok(sql);
        assert_eq!(
            stmt,
            NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
                name: "auth0".to_string(),
                issuer: "https://auth0.example.com".to_string(),
                jwks_uri: "https://auth0.example.com/.well-known/jwks.json".to_string(),
                tenant_id: 42,
                audience: None,
                claim_mappings: vec![],
            })
        );
        assert!(
            format!("{stmt:?}").contains("tenant_id: 42"),
            "the parsed provider must retain its tenant binding: {stmt:?}"
        );
    }

    #[test]
    fn provider_name_clause_keywords_do_not_supply_clauses() {
        for name in ["ISSUER", "JWKS_URI", "TENANT", "AUDIENCE"] {
            let stmt = ok(&format!(
                "CREATE OIDC PROVIDER {name} ISSUER 'https://idp.example.com' \
                 JWKS_URI 'https://idp.example.com/jwks' AUDIENCE 'nodedb-api' TENANT 42 \
                 CLAIM MAPPING WHEN sub = '*' SET DEFAULT_DATABASE = 1"
            ));
            match stmt {
                NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
                    name: parsed_name,
                    issuer,
                    jwks_uri,
                    tenant_id,
                    audience,
                    claim_mappings,
                }) => {
                    assert_eq!(parsed_name, name);
                    assert_eq!(issuer, "https://idp.example.com");
                    assert_eq!(jwks_uri, "https://idp.example.com/jwks");
                    assert_eq!(tenant_id, 42);
                    assert_eq!(audience.as_deref(), Some("nodedb-api"));
                    assert_eq!(claim_mappings.len(), 1);
                }
                other => panic!("unexpected: {other:?}"),
            }
        }
    }

    #[test]
    fn create_oidc_provider_with_audience_terminator() {
        let sql = "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' JWKS_URI 'https://idp.example.com/jwks' TENANT 42 AUDIENCE 'nodedb-api';";
        let stmt = ok(sql);
        assert!(
            format!("{stmt:?}").contains("tenant_id: 42"),
            "the parsed provider must retain its tenant binding: {stmt:?}"
        );
        match stmt {
            NodedbStatement::Auth(AuthStmt::CreateOidcProvider { audience, .. }) => {
                assert_eq!(audience, Some("nodedb-api".to_string()));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn claim_mappings_cannot_supply_provider_clauses() {
        let missing_issuer = "CREATE OIDC PROVIDER auth0 JWKS_URI 'https://idp.example.com/jwks' TENANT 42 \
            CLAIM MAPPING WHEN issuer = 'https://idp.example.com'";
        let missing_jwks = "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' TENANT 42 \
            CLAIM MAPPING WHEN jwks_uri = 'https://idp.example.com/jwks'";
        let missing_tenant = "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' \
            JWKS_URI 'https://idp.example.com/jwks' CLAIM MAPPING WHEN tenant = '42'";
        for sql in [missing_issuer, missing_jwks, missing_tenant] {
            let upper = sql.to_uppercase();
            let parts: Vec<&str> = sql.split_whitespace().collect();
            assert!(matches!(
                try_parse(&upper, &parts, sql),
                Some(Err(SqlError::Parse { .. }))
            ));
        }
    }

    #[test]
    fn claim_audience_does_not_set_provider_audience() {
        let stmt = ok(
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' \
             JWKS_URI 'https://idp.example.com/jwks' TENANT 42 \
             CLAIM MAPPING WHEN audience = 'nodedb-api'",
        );
        match stmt {
            NodedbStatement::Auth(AuthStmt::CreateOidcProvider { audience, .. }) => {
                assert_eq!(audience, None);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn create_oidc_provider_requires_quoted_provider_strings() {
        for sql in [
            "CREATE OIDC PROVIDER auth0 ISSUER = 'https://idp.example.com' JWKS_URI 'https://idp.example.com/jwks' TENANT 42",
            "CREATE OIDC PROVIDER auth0 ISSUER https://idp.example.com JWKS_URI 'https://idp.example.com/jwks' TENANT 42",
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' JWKS_URI = 'https://idp.example.com/jwks' TENANT 42",
            "CREATE OIDC PROVIDER auth0 ISSUER 'https://idp.example.com' JWKS_URI 'https://idp.example.com/jwks' AUDIENCE = 'nodedb-api' TENANT 42",
        ] {
            let upper = sql.to_uppercase();
            let parts: Vec<&str> = sql.split_whitespace().collect();
            assert!(matches!(
                try_parse(&upper, &parts, sql),
                Some(Err(SqlError::Parse { .. }))
            ));
        }
    }

    #[test]
    fn create_oidc_provider_with_claim_mapping() {
        let sql = "CREATE OIDC PROVIDER corp ISSUER 'https://sso.corp.com' JWKS_URI 'https://sso.corp.com/jwks' \
             AUDIENCE 'nodedb' TENANT 42 \
             CLAIM MAPPING WHEN org_id = 'acme' SET DEFAULT_DATABASE = 42 ADD DATABASES [43, 44] ADD ROLES ['readwrite'];";
        let stmt = ok(sql);
        assert!(
            format!("{stmt:?}").contains("tenant_id: 42"),
            "the parsed provider must retain its tenant binding: {stmt:?}"
        );
        match stmt {
            NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
                name,
                claim_mappings,
                ..
            }) => {
                assert_eq!(name, "corp");
                assert_eq!(claim_mappings.len(), 1);
                let cm = &claim_mappings[0];
                assert_eq!(cm.claim_name, "org_id");
                assert_eq!(cm.claim_value, "acme");
                assert_eq!(cm.default_database, Some(42));
                assert_eq!(cm.add_databases, vec![43, 44]);
                assert_eq!(cm.add_roles, vec!["readwrite"]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn alter_oidc_provider_claim_mapping() {
        let sql = "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN sub = '*' ADD ROLES ['admin']";
        let stmt = ok(sql);
        match stmt {
            NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
                name,
                claim_mappings,
            }) => {
                assert_eq!(name, "auth0");
                assert_eq!(claim_mappings.len(), 1);
                assert_eq!(claim_mappings[0].claim_value, "*");
                assert_eq!(claim_mappings[0].add_roles, vec!["admin"]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn claim_mapping_values_preserve_spaces_and_doubled_quotes() {
        let single_quoted = ok(
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN department = 'platform engineering' \
             ADD ROLES ['reader']",
        );
        let double_quoted = ok(
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN title = \"Director of \"\"Data\"\"\" \
             SET DEFAULT_DATABASE = 7",
        );

        let NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
            claim_mappings, ..
        }) = single_quoted
        else {
            panic!("expected ALTER OIDC PROVIDER claim mapping");
        };
        assert_eq!(claim_mappings[0].claim_value, "platform engineering");
        assert_eq!(claim_mappings[0].add_roles, vec!["reader"]);

        let NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
            claim_mappings, ..
        }) = double_quoted
        else {
            panic!("expected ALTER OIDC PROVIDER claim mapping");
        };
        assert_eq!(claim_mappings[0].claim_value, "Director of \"Data\"");
        assert_eq!(claim_mappings[0].default_database, Some(7));
    }

    #[test]
    fn claim_mapping_rejects_unquoted_and_malformed_values() {
        for sql in [
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN department = engineering",
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN department = 'platform engineering",
            "ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN department = 'engineering'junk",
        ] {
            let upper = sql.to_uppercase();
            let parts: Vec<&str> = sql.split_whitespace().collect();
            assert!(matches!(
                try_parse(&upper, &parts, sql),
                Some(Err(SqlError::Parse { .. }))
            ));
        }
    }

    #[test]
    fn claim_mapping_actions_ignore_keyword_like_claim_names_and_values() {
        let absent = ok("ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN roles = \
             'SET DEFAULT_DATABASE ADD DATABASES ADD ROLES'");
        let actual = ok("ALTER OIDC PROVIDER auth0 SET CLAIM MAPPING WHEN add = \
             'DEFAULT_DATABASE DATABASES ROLES SET ADD it''s ADD ROLES' \
             SET DEFAULT_DATABASE = 7 ADD DATABASES [8, 9] ADD ROLES ['reader']");

        for statement in [absent, actual] {
            let NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
                claim_mappings,
                ..
            }) = statement
            else {
                panic!("expected ALTER OIDC PROVIDER claim mapping");
            };
            let mapping = &claim_mappings[0];
            if mapping.claim_name == "roles" {
                assert_eq!(mapping.default_database, None);
                assert!(mapping.add_databases.is_empty());
                assert!(mapping.add_roles.is_empty());
            } else {
                assert_eq!(mapping.claim_name, "add");
                assert_eq!(mapping.default_database, Some(7));
                assert_eq!(mapping.add_databases, vec![8, 9]);
                assert_eq!(mapping.add_roles, vec!["reader"]);
            }
        }
    }
}
