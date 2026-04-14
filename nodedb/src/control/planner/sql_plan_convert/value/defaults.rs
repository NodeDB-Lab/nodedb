//! Column DEFAULT expression evaluation at insert time.
//!
//! Supports ID generation functions (UUIDv4/v7, ULID, CUID2, NANOID), `NOW()`,
//! and literal values. More complex defaults (arbitrary expressions) go
//! through the shared SqlExpr evaluator path.

pub(crate) fn evaluate_default_expr(expr: &str) -> Option<nodedb_types::Value> {
    let upper = expr.trim().to_uppercase();
    match upper.as_str() {
        "UUID_V7" | "UUIDV7" | "GEN_UUID_V7()" | "UUID_V7()" => {
            Some(nodedb_types::Value::String(nodedb_types::id_gen::uuid_v7()))
        }
        "UUID_V4" | "UUIDV4" | "UUID" | "GEN_UUID_V4()" | "UUID_V4()" => {
            Some(nodedb_types::Value::String(nodedb_types::id_gen::uuid_v4()))
        }
        "ULID" | "GEN_ULID()" | "ULID()" => {
            Some(nodedb_types::Value::String(nodedb_types::id_gen::ulid()))
        }
        "CUID2" | "CUID2()" => Some(nodedb_types::Value::String(nodedb_types::id_gen::cuid2())),
        "NANOID" | "NANOID()" => Some(nodedb_types::Value::String(nodedb_types::id_gen::nanoid())),
        "NOW()" => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            Some(nodedb_types::Value::String(
                chrono::DateTime::from_timestamp_millis(now.as_millis() as i64)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| now.as_millis().to_string()),
            ))
        }
        _ => parse_parametric_or_literal(expr, &upper),
    }
}

fn parse_parametric_or_literal(expr: &str, upper: &str) -> Option<nodedb_types::Value> {
    // NANOID(N) — custom length.
    if upper.starts_with("NANOID(") && upper.ends_with(')') {
        let len_str = &upper[7..upper.len() - 1];
        if let Ok(len) = len_str.parse::<usize>() {
            return Some(nodedb_types::Value::String(
                nodedb_types::id_gen::nanoid_with_length(len),
            ));
        }
    }
    // CUID2(N) — custom length.
    if upper.starts_with("CUID2(") && upper.ends_with(')') {
        let len_str = &upper[6..upper.len() - 1];
        if let Ok(len) = len_str.parse::<usize>() {
            return Some(nodedb_types::Value::String(
                nodedb_types::id_gen::cuid2_with_length(len),
            ));
        }
    }
    // Numeric literal.
    if let Ok(i) = expr.trim().parse::<i64>() {
        return Some(nodedb_types::Value::Integer(i));
    }
    if let Ok(f) = expr.trim().parse::<f64>() {
        return Some(nodedb_types::Value::Float(f));
    }
    // Quoted string literal.
    let trimmed = expr.trim();
    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        return Some(nodedb_types::Value::String(
            trimmed[1..trimmed.len() - 1].to_string(),
        ));
    }
    None
}
