// SPDX-License-Identifier: BUSL-1.1

//! Parse and validate the `WITH (engine='...')` option from raw SQL, plus
//! engine-name suggestions for the deprecated `TYPE <keyword>` axis.
//!
//! `parse_engine_option` has no production caller today (the typed-AST path
//! uses `validate_engine_name` instead) — it is kept, with its test suite,
//! exactly as it was on the pgwire side; only the error envelope changed from
//! `PgWireResult` to `Result<_, DdlError>`.

use nodedb_sql::parser::preprocess::lex::find_ascii_case_insensitive;

use super::super::super::super::super::result::DdlError;
use super::validate::canonical_list;

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Parse and validate the `WITH (engine='...')` option from `sql`.
///
/// Returns:
/// - `Ok(Some(name))` — one of the 7 canonical names (static str).
/// - `Ok(None)`       — no `engine=` key present; caller applies its default.
/// - `Err(_)`         — hard rejection (Axis A / B / C / unknown engine).
///
/// All rejection codes follow the spec:
/// - `0A000` — deprecated axis or unsupported feature syntax.
/// - `42601` — unknown engine name.
pub fn parse_engine_option(sql: &str, upper: &str) -> Result<Option<&'static str>, DdlError> {
    // ── Axis A: TYPE <keyword> ──────────────────────────────────────────────
    // Detect any `TYPE <keyword>` token sequence in the uppercased SQL.
    if upper.contains("TYPE")
        && let Some(suggestion) = axis_a_suggestion(upper)
    {
        return Err(err(
            "0A000",
            format!(
                "NodeDB has canonicalized engine selection; the 'TYPE ...' syntax is no \
                     longer accepted. Use: CREATE COLLECTION foo (...) WITH ({suggestion})"
            ),
        ));
    }

    // ── Axis B: WITH (profile='...') ────────────────────────────────────────
    // Reject profile= whether or not engine= is also present.
    if let Some(profile_val) = extract_with_value(sql, "profile") {
        let profile_up = profile_val.to_uppercase();
        let suggestion = match profile_up.as_str() {
            "TIMESERIES" => "engine='timeseries'",
            "SPATIAL" => "engine='spatial'",
            other => {
                return Err(err(
                    "0A000",
                    format!(
                        "NodeDB has canonicalized engine selection; 'WITH (profile='{other}')' \
                         is no longer accepted. Use WITH (engine='timeseries') or \
                         WITH (engine='spatial')"
                    ),
                ));
            }
        };
        return Err(err(
            "0A000",
            format!(
                "NodeDB has canonicalized engine selection; 'WITH (profile=...)' is no longer \
                 accepted. Use: CREATE COLLECTION foo (...) WITH ({suggestion})"
            ),
        ));
    }

    // ── Resolve engine= ─────────────────────────────────────────────────────
    let engine_val = match extract_with_value(sql, "engine") {
        Some(v) => v,
        None => {
            // ── Axis C: vector_field= without engine= ──────────────────────
            if extract_with_value(sql, "vector_field").is_some() {
                return Err(err(
                    "0A000",
                    "NodeDB has canonicalized engine selection; 'WITH (vector_field=...)' without \
                     'engine=...' is no longer accepted. Use: CREATE COLLECTION foo (...) WITH \
                     (engine='vector', vector_field='embedding')"
                        .to_string(),
                ));
            }
            return Ok(None);
        }
    };

    let engine_lower = engine_val.to_lowercase();

    // Map to canonical static str, or reject.
    let canonical = match engine_lower.as_str() {
        "document_schemaless" => "document_schemaless",
        "document_strict" => "document_strict",
        "kv" => "kv",
        "columnar" => "columnar",
        "timeseries" => "timeseries",
        "spatial" => "spatial",
        "vector" => "vector",

        // Helpful alias rejections.
        "strict" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'strict'; did you mean 'document_strict'? Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        "document" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'document'; did you mean 'document_schemaless' or \
                     'document_strict'? Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        "key_value" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'key_value'; did you mean 'kv'? Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        "fts" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'fts'; FTS uses CREATE FULLTEXT INDEX (separate DDL); \
                     graph operations work against existing collections via MATCH. \
                     Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        "graph" => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine 'graph'; graph operations work against existing collections \
                     via MATCH / GRAPH INSERT/DELETE — there is no engine='graph'. \
                     Supported engines: {}",
                    canonical_list()
                ),
            ));
        }
        other => {
            return Err(err(
                "42601",
                format!(
                    "unknown engine '{other}'; supported: {}. \
                     (FTS uses CREATE FULLTEXT INDEX; graph operations work against existing \
                     collections via MATCH.)",
                    canonical_list()
                ),
            ));
        }
    };

    Ok(Some(canonical))
}

/// Extract a value from a `WITH` clause: `key = 'value'` or `key = "value"`.
///
/// Duplicated from `collection::helpers` to avoid `pub(super)` visibility reach.
fn extract_with_value(sql: &str, key: &str) -> Option<String> {
    let pos = find_ascii_case_insensitive(sql, key)?;
    let after = sql[pos + key.len()..].trim_start();
    let after = after.strip_prefix('=')?;
    let after = after.trim_start();
    let val = after.trim_start_matches('\'').trim_start_matches('"');
    let end = val
        .find('\'')
        .or_else(|| val.find('"'))
        .or_else(|| val.find(','))
        .or_else(|| val.find(')'))
        .unwrap_or(val.len());
    let result = val[..end].trim().to_string();
    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

/// Map a `TYPE <keyword>` token pair to the corrective `WITH (engine=...)` suggestion.
///
/// Returns `None` when the SQL contains TYPE but we cannot identify a known
/// keyword following it (i.e., not one of our legacy axes — let the caller
/// fall through to normal parsing, which will surface a different error).
fn axis_a_suggestion(upper: &str) -> Option<&'static str> {
    // Walk the uppercased token stream looking for TYPE followed by a known keyword.
    let tokens: Vec<&str> = upper.split_whitespace().collect();
    for window in tokens.windows(2) {
        if window[0] == "TYPE" {
            return match window[1] {
                "STRICT" => Some("engine='document_strict'"),
                "KEY_VALUE" => Some("engine='kv'"),
                "COLUMNAR" => Some("engine='columnar'"),
                "VECTOR" => Some("engine='vector', vector_field='...'"),
                _ => None,
            };
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_engine(sql: &str, upper: &str) -> Option<&'static str> {
        parse_engine_option(sql, upper).expect("should succeed")
    }

    fn err_code(sql: &str, upper: &str) -> String {
        match parse_engine_option(sql, upper) {
            Ok(_) => panic!("expected error"),
            Err(e) => format!("{}: {}", e.sqlstate, e.message),
        }
    }

    // ── Axis A ────────────────────────────────────────────────────────────

    #[test]
    fn axis_a_type_strict() {
        let sql = "CREATE COLLECTION foo TYPE STRICT (id BIGINT)";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(
            e.contains("0A000") || e.contains("engine='document_strict'"),
            "{e}"
        );
    }

    #[test]
    fn axis_a_type_key_value() {
        let sql = "CREATE COLLECTION foo TYPE KEY_VALUE (id TEXT)";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("engine='kv'"), "{e}");
    }

    #[test]
    fn axis_a_type_columnar() {
        let sql = "CREATE COLLECTION foo TYPE COLUMNAR (ts TIMESTAMP)";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("engine='columnar'"), "{e}");
    }

    #[test]
    fn axis_a_type_vector() {
        let sql = "CREATE COLLECTION foo TYPE VECTOR (emb VECTOR(128))";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("engine='vector'"), "{e}");
    }

    // ── Axis B ────────────────────────────────────────────────────────────

    #[test]
    fn axis_b_profile_timeseries() {
        let sql = "CREATE COLLECTION foo (ts TIMESTAMP) WITH (profile='timeseries')";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("engine='timeseries'"), "{e}");
    }

    #[test]
    fn axis_b_profile_spatial() {
        let sql = "CREATE COLLECTION foo (ts TIMESTAMP) WITH (profile='spatial')";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("engine='spatial'"), "{e}");
    }

    #[test]
    fn axis_b_engine_columnar_with_profile() {
        let sql =
            "CREATE COLLECTION foo (ts TIMESTAMP) WITH (engine='columnar', profile='timeseries')";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("engine='timeseries'"), "{e}");
    }

    // ── Axis C ────────────────────────────────────────────────────────────

    #[test]
    fn axis_c_vector_field_without_engine() {
        let sql = "CREATE COLLECTION foo (emb VECTOR(128)) WITH (vector_field='emb')";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("engine='vector'"), "{e}");
    }

    // ── Unknown engine 42601 ──────────────────────────────────────────────

    #[test]
    fn unknown_engine_fts() {
        let sql = "CREATE COLLECTION foo WITH (engine='fts')";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("42601") || e.contains("fts"), "{e}");
        assert!(e.contains("FULLTEXT") || e.contains("fts"), "{e}");
    }

    #[test]
    fn unknown_engine_graph() {
        let sql = "CREATE COLLECTION foo WITH (engine='graph')";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("graph"), "{e}");
        assert!(e.contains("MATCH"), "{e}");
    }

    #[test]
    fn unknown_engine_strict_alias() {
        let sql = "CREATE COLLECTION foo WITH (engine='strict')";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("document_strict"), "{e}");
    }

    #[test]
    fn unknown_engine_nonsense() {
        let sql = "CREATE COLLECTION foo WITH (engine='nonsense')";
        let upper = sql.to_uppercase();
        let e = err_code(sql, &upper);
        assert!(e.contains("nonsense"), "{e}");
        assert!(e.contains("document_schemaless"), "{e}");
    }

    // ── Canonical success ─────────────────────────────────────────────────

    #[test]
    fn canonical_document_schemaless() {
        let sql = "CREATE COLLECTION foo WITH (engine='document_schemaless')";
        let upper = sql.to_uppercase();
        assert_eq!(ok_engine(sql, &upper), Some("document_schemaless"));
    }

    #[test]
    fn canonical_document_strict() {
        let sql = "CREATE COLLECTION foo WITH (engine='document_strict')";
        let upper = sql.to_uppercase();
        assert_eq!(ok_engine(sql, &upper), Some("document_strict"));
    }

    #[test]
    fn canonical_kv() {
        let sql = "CREATE COLLECTION foo WITH (engine='kv')";
        let upper = sql.to_uppercase();
        assert_eq!(ok_engine(sql, &upper), Some("kv"));
    }

    #[test]
    fn canonical_columnar() {
        let sql = "CREATE COLLECTION foo WITH (engine='columnar')";
        let upper = sql.to_uppercase();
        assert_eq!(ok_engine(sql, &upper), Some("columnar"));
    }

    #[test]
    fn canonical_timeseries() {
        let sql = "CREATE COLLECTION foo WITH (engine='timeseries')";
        let upper = sql.to_uppercase();
        assert_eq!(ok_engine(sql, &upper), Some("timeseries"));
    }

    #[test]
    fn canonical_spatial() {
        let sql = "CREATE COLLECTION foo WITH (engine='spatial')";
        let upper = sql.to_uppercase();
        assert_eq!(ok_engine(sql, &upper), Some("spatial"));
    }

    #[test]
    fn canonical_vector() {
        let sql = "CREATE COLLECTION foo WITH (engine='vector', vector_field='emb')";
        let upper = sql.to_uppercase();
        assert_eq!(ok_engine(sql, &upper), Some("vector"));
    }

    #[test]
    fn no_engine_returns_none() {
        let sql = "CREATE COLLECTION foo";
        let upper = sql.to_uppercase();
        assert_eq!(ok_engine(sql, &upper), None);
    }

    #[test]
    fn no_engine_with_columns_returns_none() {
        let sql = "CREATE COLLECTION foo (id INT)";
        let upper = sql.to_uppercase();
        assert_eq!(ok_engine(sql, &upper), None);
    }
}
