// SPDX-License-Identifier: BUSL-1.1

//! Schema parsing and type validation helpers for collection DDL.

/// Expand a `SERIAL` / `BIGSERIAL` type token into its backing type and DEFAULT.
///
/// `SERIAL` yields `INT DEFAULT nextval('{collection}_{field}_seq')` and
/// `BIGSERIAL` yields `BIGINT DEFAULT nextval(...)`. The sequence name matches
/// the one `create_serial_sequences` materializes, so the column reads the
/// sequence its own declaration created. Returns `None` for every other type.
///
/// Trailing modifiers such as `PRIMARY KEY` follow the DEFAULT clause, where
/// `parse_column_type_str_full` expects them.
fn expand_serial_type(collection: &str, field: &str, type_str: &str) -> Option<String> {
    let trimmed = type_str.trim();
    let (head, tail) = match trimmed.find(char::is_whitespace) {
        Some(index) => (&trimmed[..index], trimmed[index..].trim()),
        None => (trimmed, ""),
    };
    let base = if head.eq_ignore_ascii_case("SERIAL") {
        "INT"
    } else if head.eq_ignore_ascii_case("BIGSERIAL") {
        "BIGINT"
    } else {
        return None;
    };
    let expanded = format!("{base} DEFAULT nextval('{collection}_{field}_seq')");
    Some(if tail.is_empty() {
        expanded
    } else {
        format!("{expanded} {tail}")
    })
}

/// Parse FIELDS clause from CREATE COLLECTION parts.
///
/// Syntax: `CREATE COLLECTION name FIELDS (field1 type1, field2 type2, ...)`
/// Returns empty vec if no FIELDS clause.
///
/// SERIAL and BIGSERIAL expand to their backing type plus a `nextval` DEFAULT
/// naming the sequence the caller creates for that column.
///
/// The second return value lists field names that had SERIAL/BIGSERIAL types,
/// so the caller can create the implicit sequences.
pub(crate) fn parse_fields_clause(
    collection: &str,
    parts: &[&str],
) -> (Vec<(String, String)>, Vec<String>) {
    let fields_idx = parts.iter().position(|p| p.eq_ignore_ascii_case("FIELDS"));
    let fields_idx = match fields_idx {
        Some(i) => i,
        None => return (Vec::new(), Vec::new()),
    };

    let rest = parts[fields_idx + 1..].join(" ");
    let rest = rest.trim();
    let inner = if rest.starts_with('(') && rest.ends_with(')') {
        &rest[1..rest.len() - 1]
    } else {
        rest
    };

    let mut fields = Vec::new();
    let mut serial_fields = Vec::new();

    for pair in inner.split(',') {
        let pair = pair.trim();
        let mut tokens = pair.split_whitespace();
        let Some(name) = tokens.next() else {
            continue;
        };
        let name = name.to_string();
        let type_name = tokens.next().unwrap_or("text").to_uppercase();

        // Expand SERIAL/BIGSERIAL shorthand into type + nextval DEFAULT.
        let actual_type = match expand_serial_type(collection, &name, &type_name) {
            Some(expanded) => {
                serial_fields.push(name.clone());
                expanded
            }
            None => type_name,
        };

        fields.push((name, actual_type));
    }

    (fields, serial_fields)
}

/// Expand pre-parsed `(name, type)` column pairs, handling SERIAL/BIGSERIAL.
///
/// Mirrors `parse_fields_clause` but operates on already-tokenised pairs
/// instead of raw SQL parts. Used by the typed-AST collection handlers.
///
/// Returns `(fields, serial_fields)` where `serial_fields` holds the names
/// of columns whose type was expanded from SERIAL / BIGSERIAL so the caller
/// can create implicit sequences.
pub(crate) fn parse_fields_clause_from_pairs(
    collection: &str,
    columns: &[(String, String)],
) -> (Vec<(String, String)>, Vec<String>) {
    let mut fields: Vec<(String, String)> = Vec::new();
    let mut serial_fields: Vec<String> = Vec::new();

    for (name, type_str) in columns {
        let actual_type = match expand_serial_type(collection, name, type_str) {
            Some(expanded) => {
                serial_fields.push(name.clone());
                expanded
            }
            None => type_str.clone(),
        };
        fields.push((name.clone(), actual_type));
    }

    (fields, serial_fields)
}

/// Validate a JSON document against a collection's declared schema.
///
/// Returns Ok(()) if valid, or Err with a descriptive message.
/// Empty fields = schemaless (always valid).
pub fn validate_document_schema(
    fields: &[(String, String)],
    doc: &serde_json::Value,
) -> crate::Result<()> {
    if fields.is_empty() {
        return Ok(());
    }

    let obj = match doc.as_object() {
        Some(o) => o,
        None => {
            return Err(crate::Error::BadRequest {
                detail: "document must be a JSON object".into(),
            });
        }
    };

    for (field_name, type_name) in fields {
        if let Some(val) = obj.get(field_name)
            && !val.is_null()
            && !type_matches(type_name, val)
        {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "field '{}' expected type {}, got {}",
                    field_name,
                    type_name,
                    json_type_name(val)
                ),
            });
        }
    }

    Ok(())
}

/// Parse a VECTOR(dim, metric) type declaration.
///
/// Returns `(dimension, metric)` if the type is a vector type.
/// Supports: `VECTOR(384)`, `VECTOR(384, cosine)`, `VECTOR(768, l2)`.
pub fn parse_vector_type(type_str: &str) -> Option<(usize, String)> {
    if !nodedb_types::starts_with_ascii_case_insensitive(type_str, "VECTOR") {
        return None;
    }
    // Extract parenthesized args.
    let paren_start = type_str.find('(')?;
    let paren_end = type_str.rfind(')')?;
    if paren_start >= paren_end {
        return None;
    }
    let inner = &type_str[paren_start + 1..paren_end];
    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    let dim: usize = parts.first()?.parse().ok()?;
    let metric = parts
        .get(1)
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| "cosine".to_string());
    Some((dim, metric))
}

/// Extract vector field declarations from a collection's fields.
///
/// Returns `(field_name, dimension, metric)` for each VECTOR-typed field.
pub fn extract_vector_fields(fields: &[(String, String)]) -> Vec<(String, usize, String)> {
    fields
        .iter()
        .filter_map(|(name, type_str)| {
            let (dim, metric) = parse_vector_type(type_str)?;
            Some((name.clone(), dim, metric))
        })
        .collect()
}

fn type_matches(type_name: &str, val: &serde_json::Value) -> bool {
    match type_name {
        "VARCHAR" | "TEXT" | "STRING" => val.is_string(),
        "INT" | "INT4" | "INTEGER" | "INT2" | "SMALLINT" | "INT8" | "BIGINT" => {
            val.is_i64() || val.is_u64()
        }
        "FLOAT" | "FLOAT4" | "REAL" | "FLOAT8" | "DOUBLE" => val.is_f64() || val.is_i64(),
        "BOOL" | "BOOLEAN" => val.is_boolean(),
        "JSON" | "JSONB" => val.is_object() || val.is_array(),
        "BYTEA" | "BYTES" => val.is_string(),
        "TIMESTAMP" | "TIMESTAMPTZ" => val.is_string(),
        _ if type_name.starts_with("VECTOR") => true, // Vector fields don't appear in JSON docs.
        _ => true,
    }
}

fn json_type_name(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}
