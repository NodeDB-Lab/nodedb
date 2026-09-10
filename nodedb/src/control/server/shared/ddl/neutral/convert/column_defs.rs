// SPDX-License-Identifier: BUSL-1.1

//! CONVERT COLLECTION statement text parsed into a target type and columns.
//!
//! A column definition is `name TYPE [NOT NULL] [PRIMARY KEY] [DEFAULT expr]`.
//! The `DEFAULT` clause is validated here and stored on the created column, so
//! a later insert that omits the column takes its value.

use nodedb_sql::ddl_ast::collection_type::parse_column_type_str_full;
use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from,
};

use crate::control::server::shared::ddl::sql_parse::{parse_ident_token, split_values};

use super::super::super::result::DdlError;
use super::super::column_default::validate_column_default;
use super::support::err;
use super::type_map::sql_type_to_column_type;

/// Parse CONVERT COLLECTION SQL.
///
/// Returns `(collection_name, target_type, explicit_columns)`.
/// `explicit_columns` is `None` for `TO document` or `TO strict` without parens.
pub(super) fn parse_convert_sql(
    sql: &str,
) -> Result<
    (
        String,
        String,
        Option<Vec<nodedb_types::columnar::ColumnDef>>,
    ),
    DdlError,
> {
    // Extract collection name: CONVERT COLLECTION <name> TO ...
    let coll_pos = find_ascii_case_insensitive(sql, "COLLECTION ")
        .ok_or_else(|| err("42601", "expected COLLECTION keyword"))?;
    let after_coll = sql[coll_pos + 11..].trim_start();
    let collection = parse_ident_token(
        after_coll
            .split_whitespace()
            .next()
            .ok_or_else(|| err("42601", "missing collection name"))?,
    )?;

    // Extract target type: TO <type>
    let to_pos = find_ascii_case_insensitive_from(sql, " TO ", coll_pos + 11)
        .ok_or_else(|| err("42601", "expected TO <type> clause"))?;
    let after_to = sql[to_pos + 4..].trim_start();
    let target_type = after_to
        .split_whitespace()
        .next()
        .ok_or_else(|| err("42601", "missing target type after TO"))?
        .to_lowercase()
        .trim_matches('(')
        .to_string();

    match target_type.as_str() {
        "document_schemaless" => Ok((collection, "document_schemaless".into(), None)),
        "document_strict" | "kv" => {
            if after_to.contains('(') {
                let cols = parse_column_defs(after_to)?;
                Ok((collection, target_type, Some(cols)))
            } else {
                Ok((collection, target_type, None))
            }
        }
        "document" | "doc" => Err(err(
            "42601",
            "deprecated target type 'document'; use 'document_schemaless'",
        )),
        "strict" => Err(err(
            "42601",
            "deprecated target type 'strict'; use 'document_strict'",
        )),
        "key_value" | "keyvalue" => Err(err("42601", "deprecated target type; use 'kv'")),
        other => Err(err(
            "42601",
            format!(
                "unsupported target type: '{other}' \
                 (expected document_schemaless, document_strict, kv)"
            ),
        )),
    }
}

/// Words that end a type spelling in a CONVERT column definition.
const COLUMN_MODIFIER_KEYWORDS: [&str; 6] = ["NOT", "NULL", "NOTNULL", "PRIMARY", "KEY", "DEFAULT"];

/// Parse `(col1 TYPE, col2 TYPE, ...)` into `Vec<ColumnDef>`.
fn parse_column_defs(s: &str) -> Result<Vec<nodedb_types::columnar::ColumnDef>, DdlError> {
    use nodedb_types::columnar::ColumnDef;

    let open = s
        .find('(')
        .ok_or_else(|| err("42601", "expected (column definitions) after type"))?;
    let close = s
        .rfind(')')
        .ok_or_else(|| err("42601", "missing closing parenthesis"))?;
    if close <= open {
        return Err(err("42601", "empty column definitions"));
    }

    let inner = &s[open + 1..close];
    let mut columns: Vec<ColumnDef> = Vec::new();

    // The list splits on top-level commas, so a parameter list keeps its own
    // comma and `amount DECIMAL(10, 2)` stays one column definition.
    for part in split_values(inner) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let name_token = part.split_whitespace().next().ok_or_else(|| {
            err(
                "42601",
                format!("expected 'name TYPE' in column def: {part}"),
            )
        })?;
        let (head, tail, default_expr) = split_column_default(part, name_token)?;
        // The modifier scan reads the definition without its DEFAULT
        // expression, so `NOT NULL` written after the clause still lands.
        let declaration = format!("{head} {tail}");
        let tokens: Vec<&str> = declaration.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(err(
                "42601",
                format!("expected 'name TYPE' in column def: {part}"),
            ));
        }
        let col_name = parse_ident_token(tokens[0])?;
        // The type spelling runs from the second token to the first modifier
        // word, so `TIMESTAMP WITH TIME ZONE` resolves whole.
        let type_end = tokens[1..]
            .iter()
            .position(|token| {
                COLUMN_MODIFIER_KEYWORDS
                    .iter()
                    .any(|keyword| token.eq_ignore_ascii_case(keyword))
            })
            .map(|offset| offset + 1)
            .unwrap_or(tokens.len());
        if type_end < 2 {
            return Err(err(
                "42601",
                format!("expected 'name TYPE' in column def: {part}"),
            ));
        }
        let col_type = tokens[1..type_end].join(" ").to_uppercase();
        let nullable = !tokens
            .windows(2)
            .any(|w| w[0].eq_ignore_ascii_case("NOT") && w[1].eq_ignore_ascii_case("NULL"))
            && !tokens.iter().any(|t| t.eq_ignore_ascii_case("NOTNULL"));
        let primary_key = tokens
            .windows(2)
            .any(|w| w[0].eq_ignore_ascii_case("PRIMARY") && w[1].eq_ignore_ascii_case("KEY"));

        let ct = sql_type_to_column_type(&col_type)?;
        let mut col = if nullable {
            ColumnDef::nullable(col_name, ct)
        } else {
            ColumnDef::required(col_name, ct)
        };
        if primary_key {
            col = col.with_primary_key();
        }
        if let Some(expr) = default_expr {
            validate_column_default(&col.name, &expr)?;
            col = col.with_default(expr);
        }
        columns.push(col);
    }

    if columns.is_empty() {
        return Err(err("42601", "at least one column required"));
    }

    Ok(columns)
}

/// Split a column definition around its `DEFAULT` clause.
///
/// Returns the text before the keyword, the text after the expression, and
/// the expression itself. The clause is read by
/// `parse_column_type_str_full`, the parser `CREATE COLLECTION` reads a
/// column `DEFAULT` with, so both statements accept one syntax.
fn split_column_default<'a>(
    part: &'a str,
    name_token: &str,
) -> Result<(&'a str, &'a str, Option<String>), DdlError> {
    let after_name = &part[name_token.len()..];
    let Some(offset) = find_ascii_case_insensitive(after_name, "DEFAULT") else {
        return Ok((part, "", None));
    };
    let (_, _, _, default_expr) = parse_column_type_str_full(after_name);
    let expr = default_expr.ok_or_else(|| {
        err(
            "42601",
            format!("DEFAULT needs an expression in column def: {part}"),
        )
    })?;
    let after_keyword = after_name[offset + "DEFAULT".len()..].trim();
    let Some(tail) = after_keyword.strip_prefix(expr.as_str()) else {
        return Err(err(
            "42601",
            format!("DEFAULT clause is malformed in column def: {part}"),
        ));
    };
    Ok((&part[..name_token.len() + offset], tail, Some(expr)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::columnar::ColumnType;

    /// A CONVERT column definition resolves a multi-word type spelling whole,
    /// so the declared zone survives.
    #[test]
    fn convert_column_defs_resolve_a_multi_word_type_spelling() {
        let sql = "CONVERT COLLECTION events TO document_strict \
                   (id TEXT PRIMARY KEY, at TIMESTAMP WITH TIME ZONE)";
        let (_, _, cols) = parse_convert_sql(sql).expect("multi-word column type must parse");
        let cols = cols.expect("explicit column defs must be present");
        assert_eq!(cols[1].name, "at");
        assert_eq!(cols[1].column_type, ColumnType::Timestamptz);
    }

    /// A CONVERT column definition naming no known type stops the statement.
    #[test]
    fn convert_column_defs_refuse_an_unknown_type() {
        let sql = "CONVERT COLLECTION events TO document_strict (id TEXT, payload WIDGET)";
        let error = parse_convert_sql(sql).expect_err("an unknown column type must refuse");
        assert_eq!(error.sqlstate, "42601");
    }

    /// A parameter list keeps its own comma, so the space after it changes
    /// nothing about how the column list splits.
    #[test]
    fn convert_column_defs_split_around_a_parameter_list_comma() {
        for sql in [
            "CONVERT COLLECTION sales TO document_strict (id TEXT, amount DECIMAL(10, 2))",
            "CONVERT COLLECTION sales TO document_strict (id TEXT, amount DECIMAL(10,2))",
        ] {
            let (_, _, cols) = parse_convert_sql(sql).expect("a parameter list must parse");
            let cols = cols.expect("explicit column defs must be present");
            assert_eq!(
                cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
                ["id", "amount"],
                "column list must split on the top-level comma only: {sql}"
            );
            assert_eq!(
                cols[1].column_type,
                ColumnType::Decimal {
                    precision: 10,
                    scale: 2
                }
            );
        }
    }

    /// A `DEFAULT` clause reaches the created column instead of being dropped.
    #[test]
    fn convert_column_defs_carry_a_default_clause() {
        let sql = "CONVERT COLLECTION orders TO document_strict \
                   (id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')";
        let (_, _, cols) = parse_convert_sql(sql).expect("a DEFAULT clause must parse");
        let cols = cols.expect("explicit column defs must be present");
        assert_eq!(cols[1].name, "status");
        assert_eq!(cols[1].column_type, ColumnType::String);
        assert!(
            !cols[1].nullable,
            "NOT NULL must survive the DEFAULT clause"
        );
        assert_eq!(cols[1].default.as_deref(), Some("'pending'"));
    }

    /// `NOT NULL` written after the `DEFAULT` clause still reaches the column.
    #[test]
    fn convert_column_defs_read_a_modifier_after_the_default_clause() {
        let sql = "CONVERT COLLECTION orders TO document_strict \
                   (id TEXT, status TEXT DEFAULT 'pending' NOT NULL)";
        let (_, _, cols) = parse_convert_sql(sql).expect("a trailing modifier must parse");
        let cols = cols.expect("explicit column defs must be present");
        assert_eq!(cols[1].column_type, ColumnType::String);
        assert!(!cols[1].nullable, "NOT NULL after DEFAULT must land");
        assert_eq!(cols[1].default.as_deref(), Some("'pending'"));
    }

    /// A `DEFAULT` naming a function the server cannot evaluate stops CONVERT
    /// with the SQLSTATE `CREATE COLLECTION` raises for the same clause.
    #[test]
    fn convert_column_defs_refuse_an_unevaluable_default() {
        let sql = "CONVERT COLLECTION orders TO document_strict \
                   (id TEXT, status TEXT DEFAULT no_such_function())";
        let error = parse_convert_sql(sql).expect_err("an unevaluable DEFAULT must refuse");
        assert_eq!(error.sqlstate, "42883");
    }

    /// A `DEFAULT` with no expression is a syntax error, not a dropped clause.
    #[test]
    fn convert_column_defs_refuse_an_empty_default() {
        let sql = "CONVERT COLLECTION orders TO document_strict (id TEXT, status TEXT DEFAULT)";
        let error = parse_convert_sql(sql).expect_err("an empty DEFAULT must refuse");
        assert_eq!(error.sqlstate, "42601");
    }

    #[test]
    fn parse_convert_to_document_schemaless() {
        let (coll, target, cols) =
            parse_convert_sql("CONVERT COLLECTION users TO document_schemaless").unwrap();
        assert_eq!(coll, "users");
        assert_eq!(target, "document_schemaless");
        assert!(cols.is_none());
    }

    #[test]
    fn parse_convert_deprecated_document_rejected() {
        assert!(parse_convert_sql("CONVERT COLLECTION users TO document").is_err());
    }

    #[test]
    fn parse_convert_to_document_strict() {
        let sql =
            "CONVERT COLLECTION users TO document_strict (name VARCHAR, age INT, active BOOLEAN)";
        let (coll, target, cols) = parse_convert_sql(sql).unwrap();
        assert_eq!(coll, "users");
        assert_eq!(target, "document_strict");

        let cols = cols.unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "name");
        assert!(matches!(cols[0].column_type, ColumnType::String));
        assert_eq!(cols[1].name, "age");
        assert!(matches!(cols[1].column_type, ColumnType::Int64));
        assert_eq!(cols[2].name, "active");
        assert!(matches!(cols[2].column_type, ColumnType::Bool));
    }

    #[test]
    fn parse_convert_deprecated_strict_rejected() {
        let sql = "CONVERT COLLECTION users TO strict (name VARCHAR)";
        assert!(parse_convert_sql(sql).is_err());
    }

    #[test]
    fn parse_convert_to_kv() {
        let sql = "CONVERT COLLECTION cache TO kv (key VARCHAR, value BLOB)";
        let (coll, target, cols) = parse_convert_sql(sql).unwrap();
        assert_eq!(coll, "cache");
        assert_eq!(target, "kv");
        assert!(cols.is_some());
    }

    #[test]
    fn parse_convert_not_null_constraint() {
        let sql = "CONVERT COLLECTION users TO document_strict (id INT NOT NULL, name VARCHAR)";
        let (_, _, cols) = parse_convert_sql(sql).unwrap();
        let cols = cols.unwrap();
        assert!(!cols[0].nullable);
        assert!(cols[1].nullable);
    }

    #[test]
    fn parse_convert_missing_to_errors() {
        assert!(parse_convert_sql("CONVERT COLLECTION users").is_err());
    }

    #[test]
    fn parse_convert_unknown_type_errors() {
        assert!(parse_convert_sql("CONVERT COLLECTION users TO graph").is_err());
    }
}
