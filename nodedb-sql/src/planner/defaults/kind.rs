// SPDX-License-Identifier: Apache-2.0

//! The compiled forms a column DEFAULT classifies into.

use crate::error::SqlError;
use crate::types::SqlExpr;

/// What a DEFAULT expression compiles to.
///
/// Classification happens once per statement. Nothing here holds the original
/// text, so evaluation cannot fall back to parsing it again.
#[derive(Debug, Clone)]
pub(super) enum DefaultKind {
    /// A generator that produces a fresh value on every call.
    Generator(Generator),
    /// A constant the declaration spells out.
    Literal(nodedb_types::Value),
    /// A parsed expression: a sequence accessor, or a const-folder target.
    Expr(SqlExpr),
}

/// A DEFAULT spelled as a value generator rather than a registered call.
///
/// The catalog accepts both the bare form (`UUID_V7`) and the call form
/// (`UUID_V7()`), so both spellings classify to the same variant.
#[derive(Debug, Clone, Copy)]
pub(super) enum Generator {
    UuidV7,
    UuidV4,
    Ulid,
    Cuid2,
    Cuid2Len(usize),
    Nanoid,
    NanoidLen(usize),
    Now,
}

impl Generator {
    /// Produce one fresh value.
    pub(super) fn generate(self) -> crate::Result<nodedb_types::Value> {
        let value = match self {
            Self::UuidV7 => nodedb_types::Value::String(nodedb_types::id_gen::uuid_v7()),
            Self::UuidV4 => nodedb_types::Value::String(nodedb_types::id_gen::uuid_v4()),
            Self::Ulid => nodedb_types::Value::String(nodedb_types::id_gen::ulid()),
            Self::Cuid2 => nodedb_types::Value::String(nodedb_types::id_gen::cuid2()),
            Self::Cuid2Len(len) => nodedb_types::Value::String(cuid2_with_length(len)?),
            Self::Nanoid => nodedb_types::Value::String(nodedb_types::id_gen::nanoid()),
            Self::NanoidLen(len) => {
                nodedb_types::Value::String(nodedb_types::id_gen::nanoid_with_length(len))
            }
            Self::Now => nodedb_types::Value::String(now_rfc3339()),
        };
        Ok(value)
    }
}

/// Generate a CUID2 of `len` characters, mapping a rejected length to a
/// planning error.
pub(super) fn cuid2_with_length(len: usize) -> crate::Result<String> {
    nodedb_types::id_gen::cuid2_with_length(len).map_err(|e| SqlError::Parse {
        detail: format!("CUID2({len}) default expression is invalid: {e}"),
    })
}

/// Render the current wall-clock instant the way `DEFAULT NOW()` stores it.
fn now_rfc3339() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    chrono::DateTime::from_timestamp_millis(now.as_millis() as i64)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| now.as_millis().to_string())
}

/// Classify the keyword-spelled defaults: the ID generators and `NOW()`.
///
/// Returns `None` for every other expression. This is the one list of keyword
/// forms; the DDL gate classifies through it rather than repeating it.
pub(super) fn keyword_generator(upper: &str) -> Option<Generator> {
    let generator = match upper {
        "UUID_V7" | "UUIDV7" | "GEN_UUID_V7()" | "UUID_V7()" => Generator::UuidV7,
        "UUID_V4" | "UUIDV4" | "UUID" | "GEN_UUID_V4()" | "UUID_V4()" => Generator::UuidV4,
        "ULID" | "GEN_ULID()" | "ULID()" => Generator::Ulid,
        "CUID2" | "CUID2()" => Generator::Cuid2,
        "NANOID" | "NANOID()" => Generator::Nanoid,
        "NOW()" => Generator::Now,
        _ => return None,
    };
    Some(generator)
}

/// Classify the parametric ID generators and the bare literals.
///
/// Returns `Ok(None)` when `expr` is none of them, leaving it to the parser.
/// This is the one list of literal forms; the DDL gate reuses it.
///
/// `CUID2(N)` validates its length here, so a rejected length raises at
/// declaration rather than at the first insert.
pub(super) fn parametric_or_literal(expr: &str, upper: &str) -> crate::Result<Option<DefaultKind>> {
    // NANOID(N) — custom length.
    if upper.starts_with("NANOID(") && upper.ends_with(')') {
        let len_str = &upper[7..upper.len() - 1];
        if let Ok(len) = len_str.parse::<usize>() {
            return Ok(Some(DefaultKind::Generator(Generator::NanoidLen(len))));
        }
    }
    // CUID2(N) — custom length; validates length range and surfaces planning errors.
    if upper.starts_with("CUID2(") && upper.ends_with(')') {
        let len_str = &upper[6..upper.len() - 1];
        if let Ok(len) = len_str.parse::<usize>() {
            // One generation checks the length range against the one authority
            // for it. The value is discarded; each row generates its own.
            cuid2_with_length(len)?;
            return Ok(Some(DefaultKind::Generator(Generator::Cuid2Len(len))));
        }
    }
    // Numeric literal.
    if let Ok(i) = expr.trim().parse::<i64>() {
        return Ok(Some(DefaultKind::Literal(nodedb_types::Value::Integer(i))));
    }
    if let Ok(f) = expr.trim().parse::<f64>() {
        return Ok(Some(DefaultKind::Literal(nodedb_types::Value::Float(f))));
    }
    // Quoted string literal.
    // A length of two is the shortest quoted literal, the empty string. One
    // lone quote character opens a literal nothing closes, so it is not one.
    let trimmed = expr.trim();
    if trimmed.len() >= 2
        && ((trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"')))
    {
        return Ok(Some(DefaultKind::Literal(nodedb_types::Value::String(
            trimmed[1..trimmed.len() - 1].to_string(),
        ))));
    }

    Ok(None)
}
