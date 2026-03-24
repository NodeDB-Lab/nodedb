//! Schema parsing and type validation helpers for collection DDL.
//!
//! Extracted from `collection.rs` to keep that file under the 500-line limit.

/// Parse FIELDS clause from CREATE COLLECTION parts.
///
/// Syntax: `CREATE COLLECTION name FIELDS (field1 type1, field2 type2, ...)`
/// Returns empty vec if no FIELDS clause.
pub(super) fn parse_fields_clause(parts: &[&str]) -> Vec<(String, String)> {
    let fields_idx = parts.iter().position(|p| p.eq_ignore_ascii_case("FIELDS"));
    let fields_idx = match fields_idx {
        Some(i) => i,
        None => return Vec::new(),
    };

    let rest = parts[fields_idx + 1..].join(" ");
    let rest = rest.trim();
    let inner = if rest.starts_with('(') && rest.ends_with(')') {
        &rest[1..rest.len() - 1]
    } else {
        rest
    };

    inner
        .split(',')
        .filter_map(|pair| {
            let pair = pair.trim();
            let mut tokens = pair.split_whitespace();
            let name = tokens.next()?.to_string();
            let type_name = tokens.next().unwrap_or("text").to_uppercase();
            Some((name, type_name))
        })
        .collect()
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
    let upper = type_str.to_uppercase();
    if !upper.starts_with("VECTOR") {
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
