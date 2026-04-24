//! Shared parsing helpers used across collection DDL sub-modules.

use sonic_rs;

/// Reconstruct uppercase SQL from split parts for keyword detection.
pub(super) fn sql_upper_from_parts(parts: &[&str]) -> String {
    parts.join(" ").to_uppercase()
}

/// Extract a value from a WITH clause: `key = 'value'`.
///
/// Searches the SQL for `key = 'value'` or `key = "value"` patterns.
pub(super) fn extract_with_value(sql: &str, key: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let key_upper = key.to_uppercase();
    let pos = upper.find(&key_upper)?;
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

/// Parse column definitions from a CREATE COLLECTION SQL statement into a StrictSchema.
///
/// Extracts the parenthesized column list: `(id BIGINT NOT NULL PRIMARY KEY, name TEXT, ...)`.
/// Auto-generates `_rowid` PK if no PK column is declared.
pub(in crate::control::server::pgwire::ddl) fn parse_typed_schema(
    sql: &str,
) -> Result<nodedb_types::columnar::StrictSchema, String> {
    parse_typed_schema_with_flags(sql, false)
}

/// Variant of `parse_typed_schema` that additionally flags the schema
/// as bitemporal, prepending the three reserved fixed-Int64 slots
/// (`__system_from_ms`, `__valid_from_ms`, `__valid_until_ms`).
pub(in crate::control::server::pgwire::ddl) fn parse_typed_schema_with_flags(
    sql: &str,
    bitemporal: bool,
) -> Result<nodedb_types::columnar::StrictSchema, String> {
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    // Find parenthesized column definitions.
    let paren_start = sql
        .find('(')
        .ok_or("expected column definitions in parentheses")?;

    // Find matching close paren (handle nested parens for VECTOR(dim)).
    let mut depth = 0;
    let mut paren_end = None;
    for (i, b) in sql.bytes().enumerate().skip(paren_start) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    paren_end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let paren_end = paren_end.ok_or("unmatched parenthesis")?;
    let col_defs_str = &sql[paren_start + 1..paren_end];

    // Split by top-level commas.
    let mut columns = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in col_defs_str.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                let part = col_defs_str[start..i].trim();
                if !part.is_empty() {
                    columns.push(parse_origin_column_def(part)?);
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = col_defs_str[start..].trim();
    if !last.is_empty() {
        columns.push(parse_origin_column_def(last)?);
    }

    if columns.is_empty() {
        return Err("at least one column required".into());
    }

    // Auto-generate _rowid PK if none declared.
    if !columns.iter().any(|c| c.primary_key) {
        columns.insert(
            0,
            ColumnDef::required("_rowid", ColumnType::Int64).with_primary_key(),
        );
    }

    if bitemporal {
        StrictSchema::new_bitemporal(columns).map_err(|e| e.to_string())
    } else {
        StrictSchema::new(columns).map_err(|e| e.to_string())
    }
}

/// Parse a single column definition: `name TYPE [NOT NULL] [PRIMARY KEY] [DEFAULT expr]`
pub(super) fn parse_origin_column_def(
    s: &str,
) -> Result<nodedb_types::columnar::ColumnDef, String> {
    use nodedb_types::columnar::{ColumnDef, ColumnType};

    let upper = s.to_uppercase();
    let tokens: Vec<&str> = s.split_whitespace().collect();
    if tokens.len() < 2 {
        return Err(format!(
            "column definition requires name and type, got: '{s}'"
        ));
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
    let type_end = keywords
        .iter()
        .filter_map(|kw| upper[name.len()..].find(kw))
        .min()
        .map(|p| p + name.len())
        .unwrap_or(s.len());
    let type_str = s[name.len()..type_end].trim();

    let column_type: ColumnType = type_str
        .parse()
        .map_err(|e: nodedb_types::columnar::ColumnTypeParseError| e.to_string())?;

    let is_not_null = upper.contains("NOT NULL");
    let is_pk = upper.contains("PRIMARY KEY");
    let nullable = !is_not_null && !is_pk;

    let default = if let Some(pos) = upper.find("DEFAULT ") {
        let after_default = s[pos + 8..].trim();
        let end = keywords
            .iter()
            .filter_map(|kw| after_default.to_uppercase().find(kw))
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
        if let Some(gen_pos) = upper.find(gen_kw) {
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
                        nodedb_query::expr_parse::parse_generated_expr(expr_text)
                            .map_err(|e| format!("invalid GENERATED expression: {e}"))?;
                    let expr_json = sonic_rs::to_string(&parsed_expr)
                        .map_err(|e| format!("failed to serialize expression: {e}"))?;
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
