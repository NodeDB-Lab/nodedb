// SPDX-License-Identifier: BUSL-1.1

//! Shared parsing helpers used across collection DDL sub-modules.
//!
//! Relocated verbatim from the pgwire `ddl::collection::helpers` module (now
//! deleted): its sole external caller was already `neutral::collection::alter::add_column`.

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from,
};
use sonic_rs;

/// Parse a single column definition: `name TYPE [NOT NULL] [PRIMARY KEY] [DEFAULT expr]`
pub(crate) fn parse_origin_column_def(s: &str) -> crate::Result<nodedb_types::columnar::ColumnDef> {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    let upper = s.to_uppercase();
    let tokens: Vec<&str> = s.split_whitespace().collect();
    if tokens.len() < 2 {
        return Err(crate::Error::BadRequest {
            detail: format!("column definition requires name and type, got: '{s}'"),
        });
    }

    let name = tokens[0].to_lowercase();

    // Find the type string (may span tokens for VECTOR(dim)).
    let keywords = [
        " NOT ",
        " NULL",
        " PRIMARY ",
        " DEFAULT ",
        " GENERATED ",
        " TIME_KEY",
        " SPATIAL_INDEX",
    ];
    let type_start = tokens[0].len();
    let type_end = keywords
        .iter()
        .filter_map(|kw| find_ascii_case_insensitive_from(s, kw, type_start))
        .min()
        .unwrap_or(s.len());
    let type_str = s[type_start..type_end].trim();

    let column_type: ColumnType =
        type_str
            .parse()
            .map_err(|e: nodedb_types::columnar::ColumnTypeParseError| {
                crate::Error::BadRequest {
                    detail: e.to_string(),
                }
            })?;

    let is_not_null = upper.contains("NOT NULL");
    let is_pk = upper.contains("PRIMARY KEY");
    let nullable = !is_not_null && !is_pk;

    let default = if let Some(pos) = find_ascii_case_insensitive(s, "DEFAULT ") {
        let after_default = s[pos + 8..].trim();
        let end = keywords
            .iter()
            .filter_map(|kw| find_ascii_case_insensitive(after_default, kw))
            .min()
            .unwrap_or(after_default.len());
        let expr = after_default[..end].trim();
        if expr.is_empty() {
            None
        } else {
            Some(expr.to_string())
        }
    } else {
        None
    };

    let mut col = if nullable {
        ColumnDef::nullable(name, column_type)
    } else {
        ColumnDef::required(name, column_type)
    };
    if is_pk {
        col = col.with_primary_key();
    }
    if let Some(d) = default {
        col = col.with_default(d);
    }

    // GENERATED ALWAYS AS (expr): stored computed column.
    if upper.contains("GENERATED ALWAYS AS") || upper.contains("GENERATED AS") {
        let gen_kw = if upper.contains("GENERATED ALWAYS AS") {
            "GENERATED ALWAYS AS"
        } else {
            "GENERATED AS"
        };
        if let Some(gen_pos) = find_ascii_case_insensitive(s, gen_kw) {
            let after_gen = s[gen_pos + gen_kw.len()..].trim();
            // Extract parenthesized expression.
            if after_gen.starts_with('(') {
                let mut depth = 0;
                let mut end = 0;
                for (i, ch) in after_gen.char_indices() {
                    match ch {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                end = i;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                if end > 1 {
                    let expr_text = &after_gen[1..end];
                    let (parsed_expr, deps) =
                        nodedb_query::expr_parse::parse_generated_expr(expr_text).map_err(|e| {
                            crate::Error::BadRequest {
                                detail: format!("invalid GENERATED expression: {e}"),
                            }
                        })?;
                    let expr_json = sonic_rs::to_string(&parsed_expr).map_err(|e| {
                        crate::Error::BadRequest {
                            detail: format!("failed to serialize expression: {e}"),
                        }
                    })?;
                    col.generated_expr = Some(expr_json);
                    col.generated_deps = deps;
                    // Generated columns are nullable (computed value may be null).
                    col.nullable = true;
                }
            }
        }
    }

    // Column modifiers: TIME_KEY, SPATIAL_INDEX.
    if upper.contains("TIME_KEY") {
        col.modifiers
            .push(nodedb_types::columnar::ColumnModifier::TimeKey);
    }
    if upper.contains("SPATIAL_INDEX") {
        col.modifiers
            .push(nodedb_types::columnar::ColumnModifier::SpatialIndex);
    }

    Ok(col)
}
