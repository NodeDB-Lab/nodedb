// SPDX-License-Identifier: Apache-2.0

//! Parse the body of `CREATE COLLECTION` / `CREATE TABLE` after the name.
//!
//! The body is read token by token from the cursor. A clause keyword
//! (`WITH`, `BALANCED ON`, `ENGINE`, a flag) matches only as a whole word at
//! parenthesis depth zero, so the same word inside a literal, a quoted
//! identifier, a column definition, or another clause's parentheses is
//! never read as that clause. `TYPE <kind>` and a leading `STRICT` select
//! an engine (see `type_clause`). Other words no clause claims are skipped.

use super::column_list::read_column_list;
use super::engine_suffix::read_engine_suffix;
use super::flags::{FLAGS, flag_index, flag_names, is_truthy};
use super::type_clause::{read_leading_kind, read_type_kind};
use super::with_clause::{read_balanced, read_with_options};
use crate::ddl_ast::parse::clause::ClauseCursor;
use crate::error::SqlError;

/// Parsed body of a `CREATE COLLECTION` / `CREATE TABLE` statement.
///
/// Tuple shape: `(engine, columns, options, flags, balanced_raw)`:
/// - `engine`: value of `engine=` from the WITH clause, or of an
///   `ENGINE = <name>` clause (lowercased), if present. Both forms feed the
///   same field so downstream validation (`validate_engine_name`) treats
///   them identically. If both are given they must name the same engine
///   (case-insensitively) or parsing fails with `SqlError::ConflictingEngineClause`.
/// - `columns`: `(name, type)` pairs from the parenthesised column list.
/// - `options`: remaining WITH clause `key=value` pairs (excluding `engine`).
/// - `flags`: set modifier flags: `APPEND_ONLY`, `HASH_CHAIN`, `BITEMPORAL`,
///   `SIGNED_DELTAS`.
/// - `balanced_raw`: raw interior of the `BALANCED ON (...)` clause, or `None`.
pub(super) type CollectionBody = (
    Option<String>,
    Vec<(String, String)>,
    Vec<(String, String)>,
    Vec<String>,
    Option<String>,
);

/// Read the body from the cursor, positioned right after the collection
/// name, to the end of the statement. `src` is the cursor's source text.
pub(super) fn parse_collection_body(
    c: &mut ClauseCursor<'_, '_>,
    src: &str,
) -> Result<CollectionBody, SqlError> {
    let mut type_engine = read_leading_kind(c)?;
    let (columns, column_list_flags) = read_column_list(c, src)?;
    let mut with_engine: Option<String> = None;
    let mut options: Vec<(String, String)> = Vec::new();
    let mut suffix_engine: Option<String> = None;
    let mut balanced_raw: Option<String> = None;
    let mut flags = column_list_flags;

    while !c.at_end() {
        if c.eat_keyword("WITH") {
            // `WITH BALANCED ON (...)` introduces the BALANCED clause.
            if c.peek_keyword("BALANCED") {
                continue;
            }
            c.clause("WITH")?;
            for (key, value) in read_with_options(c, src)? {
                if let Some(i) = flag_index(&key) {
                    flags[i] |= is_truthy(&value);
                }
                if key == "engine" {
                    with_engine = Some(value.to_lowercase());
                } else {
                    options.push((key, value));
                }
            }
        } else if c.eat_keyword("BALANCED") {
            c.clause("BALANCED ON")?;
            balanced_raw = Some(read_balanced(c)?);
        } else if c.eat_keyword("ENGINE") {
            c.clause("ENGINE")?;
            suffix_engine = Some(read_engine_suffix(c)?);
        } else if c.eat_keyword("TYPE") {
            c.clause("TYPE")?;
            type_engine = Some(read_type_kind(c)?);
        } else if let Some(i) = c.peek_word_upper().and_then(|w| flag_index(&w)) {
            c.skip(1);
            flags[i] |= if c.eat_punct('=') {
                let value = match c.string("flag value") {
                    Ok(v) => v,
                    Err(_) => c.name("flag value")?,
                };
                is_truthy(&value)
            } else {
                true
            };
        } else if c.peek_punct('(') {
            c.raw_paren("CREATE COLLECTION clause")?;
        } else {
            c.skip(1);
        }
    }

    let engine = match (with_engine, suffix_engine) {
        (Some(w), Some(s)) => {
            if w.eq_ignore_ascii_case(&s) {
                Some(w)
            } else {
                return Err(SqlError::ConflictingEngineClause {
                    with_engine: w,
                    suffix_engine: s,
                });
            }
        }
        (Some(w), None) => Some(w),
        (None, Some(s)) => Some(s.to_lowercase()),
        (None, None) => None,
    };
    // A `TYPE` clause selects its engine. Beside another engine clause it
    // must name the same engine.
    let engine = match (engine, type_engine) {
        (Some(named), Some(kind)) if !named.eq_ignore_ascii_case(kind) => {
            return Err(c.error(format!(
                "TYPE selects engine '{kind}' but the statement also names engine '{named}'; \
                 give one engine-selection clause"
            )));
        }
        (Some(named), _) => Some(named),
        (None, Some(kind)) => Some(kind.to_string()),
        (None, None) => None,
    };

    Ok((engine, columns, options, flag_names(flags), balanced_raw))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::clause::tokenize;

    fn parse(sql: &str) -> Result<CollectionBody, SqlError> {
        let toks = tokenize(sql);
        let mut c = ClauseCursor::new(sql, &toks, "CREATE COLLECTION");
        c.expect_keyword("CREATE")?;
        if !c.eat_keyword("COLLECTION") {
            c.expect_keyword("TABLE")?;
        }
        c.eat_keywords(&["IF", "NOT", "EXISTS"]);
        c.ident("collection name")?;
        parse_collection_body(&mut c, sql)
    }

    fn ok(sql: &str) -> CollectionBody {
        parse(sql).expect("collection body should parse")
    }

    #[test]
    fn column_list_bitemporal_and_append_only_flags_match_trailing_flags() {
        let (_, _, _, flags, _) = ok(
            "CREATE COLLECTION u2 (id TEXT PRIMARY KEY, n INT, BITEMPORAL) \
             WITH (engine='document_strict')",
        );
        assert_eq!(flags, vec!["BITEMPORAL"]);
        let (_, _, _, flags, _) = ok(
            "CREATE COLLECTION u4 (id TEXT PRIMARY KEY, n INT, APPEND_ONLY) \
             WITH (engine='document_strict')",
        );
        assert_eq!(flags, vec!["APPEND_ONLY"]);
    }

    #[test]
    fn trailing_hash_chain_flags_parse_with_their_engine_and_columns() {
        let (engine, columns, _, flags, _) = ok("CREATE COLLECTION ledger \
             (id STRING PRIMARY KEY, amount INT) \
             WITH (engine='document_schemaless') APPEND_ONLY HASH_CHAIN");
        assert_eq!(engine, Some("document_schemaless".to_string()));
        assert_eq!(columns.len(), 2);
        assert_eq!(flags, vec!["APPEND_ONLY", "HASH_CHAIN"]);
    }

    /// A column whose name matches a flag declares a column, not the flag.
    #[test]
    fn a_column_named_like_a_flag_sets_no_flag() {
        let (_, columns, _, flags, _) =
            ok("CREATE COLLECTION t (id INT PRIMARY KEY, append_only BOOL) WITH (engine='kv')");
        assert_eq!(columns.len(), 2);
        assert!(flags.is_empty(), "got flags {flags:?}");
    }

    #[test]
    fn a_flag_inside_an_identifier_literal_or_quoted_name_sets_nothing() {
        for sql in [
            "CREATE COLLECTION t WITH (engine='kv') not_append_only_view",
            "CREATE COLLECTION t WITH (note='HASH_CHAIN') \"BITEMPORAL\"",
            "CREATE COLLECTION t WITH (note='it''s APPEND_ONLY')",
        ] {
            assert!(ok(sql).3.is_empty(), "{sql}");
        }
    }

    #[test]
    fn an_assigned_flag_follows_its_value() {
        assert_eq!(
            ok("CREATE COLLECTION t WITH (bitemporal=true)").3,
            ["BITEMPORAL"]
        );
        assert_eq!(
            ok("CREATE COLLECTION t BITEMPORAL = 'TRUE'").3,
            ["BITEMPORAL"]
        );
        assert!(
            ok("CREATE COLLECTION t WITH (bitemporal=false)")
                .3
                .is_empty()
        );
        assert!(ok("CREATE COLLECTION t APPEND_ONLY = 'no'").3.is_empty());
    }

    /// The options clause without parentheses means what it says.
    #[test]
    fn bare_with_clause_yields_its_options() {
        let (_, _, options, ..) =
            ok("CREATE COLLECTION t (id INT PRIMARY KEY) WITH storage = 'kv'");
        assert_eq!(options, vec![("storage".to_string(), "kv".to_string())]);
    }

    #[test]
    fn bare_with_clause_yields_every_option() {
        let (_, _, options, ..) =
            ok("CREATE COLLECTION t (id INT PRIMARY KEY) WITH storage = 'kv', ttl = INTERVAL '1h'");
        assert_eq!(
            options,
            vec![
                ("storage".to_string(), "kv".to_string()),
                ("ttl".to_string(), "INTERVAL '1h'".to_string()),
            ]
        );
    }

    #[test]
    fn bare_with_clause_yields_the_engine() {
        let (engine, ..) =
            ok("CREATE COLLECTION t (id INT PRIMARY KEY) WITH engine = 'timeseries'");
        assert_eq!(engine, Some("timeseries".to_string()));
    }

    /// An `ENGINE = <name>` clause ends the bare options clause rather than
    /// being swallowed as an option value.
    #[test]
    fn bare_with_clause_stops_at_an_engine_suffix() {
        let (engine, _, options, ..) =
            ok("CREATE COLLECTION t (id INT PRIMARY KEY) WITH storage = 'kv' ENGINE = kv");
        assert_eq!(engine, Some("kv".to_string()));
        assert_eq!(options, vec![("storage".to_string(), "kv".to_string())]);
    }

    #[test]
    fn engine_word_inside_a_literal_is_not_the_engine_clause() {
        // The old byte scanner read `ENGINE x` inside the literal as the
        // suffix and then refused the statement as a conflicting engine.
        let (engine, _, options, ..) =
            ok("CREATE COLLECTION t (id INT) WITH note = 'ENGINE timeseries', engine = 'kv'");
        assert_eq!(engine, Some("kv".to_string()));
        assert_eq!(
            options,
            vec![("note".to_string(), "ENGINE timeseries".to_string())]
        );
    }

    #[test]
    fn balanced_word_inside_a_literal_is_not_the_balanced_clause() {
        let (.., balanced) = ok("CREATE COLLECTION t (note TEXT DEFAULT 'BALANCED ON (x)')");
        assert_eq!(balanced, None);
        let (.., balanced) =
            ok("CREATE COLLECTION ledger WITH BALANCED ON (group_key = journal_id)");
        assert_eq!(balanced.as_deref(), Some("group_key = journal_id"));
    }

    #[test]
    fn engine_suffix_without_a_column_list_is_read() {
        assert_eq!(
            ok("CREATE COLLECTION t ENGINE = kv").0,
            Some("kv".to_string())
        );
    }

    #[test]
    fn duplicate_clauses_are_refused() {
        assert!(parse("CREATE COLLECTION t WITH (a=1) WITH (b=2)").is_err());
        assert!(parse("CREATE COLLECTION t ENGINE = kv ENGINE = kv").is_err());
    }

    #[test]
    fn collection_body_after_unicode_comment_preserves_original_offsets() {
        let (engine, columns, ..) = ok("CREATE /*İ*/ COLLECTION items (id INT) WITH (engine='kv')");
        assert_eq!(engine, Some("kv".to_string()));
        assert_eq!(columns, vec![("id".to_string(), "INT".to_string())]);
    }

    #[test]
    fn unicode_collection_name_preserves_body_boundary() {
        let (engine, columns, ..) = ok("CREATE COLLECTION Åx (id INT) WITH (engine='kv')");
        assert_eq!(engine, Some("kv".to_string()));
        assert_eq!(columns, vec![("id".to_string(), "INT".to_string())]);
    }

    #[test]
    fn engine_suffix_matches_with_clause_result() {
        let (engine_suffix, ..) =
            ok("CREATE COLLECTION t (id INT PRIMARY KEY) ENGINE = timeseries");
        let (engine_with, ..) =
            ok("CREATE COLLECTION t (id INT PRIMARY KEY) WITH (engine='timeseries')");
        assert_eq!(engine_suffix, Some("timeseries".to_string()));
        assert_eq!(engine_suffix, engine_with);
    }

    #[test]
    fn engine_suffix_no_spaces() {
        let (engine, ..) = ok("CREATE COLLECTION t (id INT PRIMARY KEY) ENGINE=columnar");
        assert_eq!(engine, Some("columnar".to_string()));
    }

    #[test]
    fn engine_suffix_survives_nested_paren_column_types() {
        let (engine, columns, ..) = ok("CREATE COLLECTION t \
             (id INT PRIMARY KEY, v VECTOR(3), amt NUMERIC(10,2)) ENGINE = kv");
        assert_eq!(engine, Some("kv".to_string()));
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[1], ("v".to_string(), "VECTOR(3)".to_string()));
        assert_eq!(columns[2], ("amt".to_string(), "NUMERIC(10,2)".to_string()));
    }

    #[test]
    fn conflicting_with_and_suffix_engine_errors() {
        let err = parse(
            "CREATE COLLECTION t (id INT PRIMARY KEY) WITH (engine='kv') ENGINE = timeseries",
        )
        .expect_err("must reject conflicting engine clauses");
        match err {
            SqlError::ConflictingEngineClause {
                with_engine,
                suffix_engine,
            } => {
                assert_eq!(with_engine, "kv");
                assert_eq!(suffix_engine, "timeseries");
            }
            other => panic!("expected ConflictingEngineClause, got {other:?}"),
        }
    }

    #[test]
    fn agreeing_with_and_suffix_engine_is_accepted() {
        let (engine, ..) =
            ok("CREATE COLLECTION t (id INT PRIMARY KEY) WITH (engine='kv') ENGINE = kv");
        assert_eq!(engine, Some("kv".to_string()));
    }

    #[test]
    fn signed_deltas_with_option_is_preserved_as_a_collection_flag() {
        let (_, _, _, flags, _) = ok("CREATE COLLECTION t (id INT PRIMARY KEY) \
             WITH (engine='document', crdt=true, signed_deltas=true)");
        assert!(flags.iter().any(|flag| flag == "SIGNED_DELTAS"));
    }

    /// Every `TYPE` spelling selects the engine it names, with or without a
    /// column list.
    #[test]
    fn type_spellings_select_their_engine() {
        for (sql, engine) in [
            ("CREATE COLLECTION t TYPE document", "document_schemaless"),
            (
                "CREATE COLLECTION t TYPE DOCUMENT SCHEMALESS",
                "document_schemaless",
            ),
            ("CREATE COLLECTION t TYPE schemaless", "document_schemaless"),
            (
                "CREATE COLLECTION t TYPE document WITH sync = true",
                "document_schemaless",
            ),
            (
                "CREATE COLLECTION t TYPE strict (id STRING PRIMARY KEY)",
                "document_strict",
            ),
            (
                "CREATE COLLECTION t TYPE DOCUMENT STRICT (id STRING PRIMARY KEY)",
                "document_strict",
            ),
            (
                "CREATE COLLECTION t STRICT (id STRING PRIMARY KEY)",
                "document_strict",
            ),
            ("CREATE COLLECTION t TYPE kv (k TEXT PRIMARY KEY)", "kv"),
            (
                "CREATE COLLECTION t TYPE KEY_VALUE (k TEXT PRIMARY KEY)",
                "kv",
            ),
            (
                "CREATE COLLECTION t TYPE columnar (id INT PRIMARY KEY)",
                "columnar",
            ),
            (
                "CREATE COLLECTION t TYPE timeseries (ts BIGINT TIME_KEY)",
                "timeseries",
            ),
            ("CREATE COLLECTION t TYPE spatial (loc GEOMETRY)", "spatial"),
            (
                "CREATE COLLECTION t TYPE vector WITH (vector_field='emb')",
                "vector",
            ),
            (
                "CREATE COLLECTION t (id STRING PRIMARY KEY) TYPE strict",
                "document_strict",
            ),
        ] {
            assert_eq!(ok(sql).0.as_deref(), Some(engine), "{sql}");
        }
    }

    #[test]
    fn type_spelling_keeps_its_column_list() {
        let (engine, columns, ..) =
            ok("CREATE COLLECTION t TYPE DOCUMENT STRICT (id STRING PRIMARY KEY, v INT)");
        assert_eq!(engine.as_deref(), Some("document_strict"));
        assert_eq!(
            columns.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            ["id", "v"]
        );
    }

    #[test]
    fn unknown_type_kind_is_refused() {
        for sql in [
            "CREATE COLLECTION t TYPE graph (id TEXT)",
            "CREATE COLLECTION t TYPE (id TEXT)",
            "CREATE COLLECTION t TYPE",
        ] {
            let err = parse(sql).expect_err(sql);
            assert!(matches!(err, SqlError::Parse { .. }), "{sql}: {err:?}");
            assert!(err.to_string().contains("engine='<name>'"), "{sql}: {err}");
        }
    }

    #[test]
    fn type_and_engine_clause_must_agree() {
        assert!(
            parse("CREATE COLLECTION t TYPE strict (id STRING PRIMARY KEY) WITH (engine='kv')")
                .is_err()
        );
        assert!(parse("CREATE COLLECTION t TYPE kv ENGINE = columnar").is_err());
        assert_eq!(
            ok("CREATE COLLECTION t TYPE kv (k TEXT PRIMARY KEY) WITH (engine='kv')").0,
            Some("kv".to_string())
        );
        assert!(
            parse("CREATE COLLECTION t TYPE kv TYPE kv").is_err(),
            "TYPE given twice"
        );
    }

    #[test]
    fn column_list_keywords_select_no_engine() {
        assert_eq!(ok("CREATE COLLECTION t COLUMNS (id TEXT)").0, None);
        assert_eq!(ok("CREATE COLLECTION t FIELDS (id TEXT)").0, None);
        assert_eq!(ok("CREATE COLLECTION t (id TEXT)").0, None);
    }

    #[test]
    fn malformed_engine_suffix_is_a_parse_error() {
        let err = parse("CREATE COLLECTION t (id INT PRIMARY KEY) ENGINE =")
            .expect_err("empty engine name must error");
        assert!(matches!(err, SqlError::Parse { .. }));
    }
}
