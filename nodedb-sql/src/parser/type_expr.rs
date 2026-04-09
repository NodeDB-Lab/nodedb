//! Parser and validator for type expression strings used by the typeguard system.
//!
//! Parses strings like `"STRING"`, `"INT|NULL"`, `"ARRAY<STRING>"` into a
//! [`TypeExpr`] that can be used to validate [`nodedb_types::Value`] instances
//! at write time.

use nodedb_types::Value;

/// A parsed type expression that can validate Values.
#[derive(Debug, Clone, PartialEq)]
pub enum TypeExpr {
    /// Matches `Value::Null` / absent.
    Null,
    /// Matches a specific Value variant.
    Simple(SimpleType),
    /// Typed array: every element must match inner.
    TypedArray(Box<TypeExpr>),
    /// Typed set: every element must match inner.
    TypedSet(Box<TypeExpr>),
    /// Union: value must match at least one variant.
    Union(Vec<TypeExpr>),
}

/// Leaf type variants that map to a single Value discriminant.
#[derive(Debug, Clone, PartialEq)]
pub enum SimpleType {
    Int,
    Float,
    String,
    Bool,
    Bytes,
    Timestamp,
    Decimal,
    Uuid,
    Ulid,
    Geometry,
    Duration,
    /// Untyped array (any element type).
    Array,
    Object,
    Json,
    /// Untyped set (any element type).
    Set,
    Regex,
    Range,
    Record,
    /// Fixed-dimension float32 vector.
    Vector(u32),
}

// ── Parser ───────────────────────────────────────────────────────────────────

/// Parse a type expression string into a [`TypeExpr`].
///
/// # Examples
///
/// ```
/// use nodedb_sql::parser::type_expr::{parse_type_expr, TypeExpr, SimpleType};
///
/// assert_eq!(parse_type_expr("STRING").unwrap(), TypeExpr::Simple(SimpleType::String));
/// assert_eq!(
///     parse_type_expr("STRING|NULL").unwrap(),
///     TypeExpr::Union(vec![TypeExpr::Simple(SimpleType::String), TypeExpr::Null]),
/// );
/// ```
pub fn parse_type_expr(s: &str) -> Result<TypeExpr, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty type expression".to_string());
    }
    let mut pos = 0usize;
    let chars: Vec<char> = s.chars().collect();
    parse_union(&chars, &mut pos, false)
}

/// Parse `single_type ('|' single_type)*`.
///
/// When `stop_at_gt` is true the parser stops before a `>` character (used
/// when parsing inside `ARRAY<...>` / `SET<...>`).
fn parse_union(chars: &[char], pos: &mut usize, stop_at_gt: bool) -> Result<TypeExpr, String> {
    let mut variants: Vec<TypeExpr> = Vec::new();
    variants.push(parse_single(chars, pos, stop_at_gt)?);

    loop {
        skip_ws(chars, pos);
        if *pos >= chars.len() {
            break;
        }
        if stop_at_gt && chars[*pos] == '>' {
            break;
        }
        if chars[*pos] != '|' {
            break;
        }
        *pos += 1; // consume '|'
        skip_ws(chars, pos);
        variants.push(parse_single(chars, pos, stop_at_gt)?);
    }

    if variants.len() == 1 {
        Ok(variants.remove(0))
    } else {
        Ok(TypeExpr::Union(variants))
    }
}

/// Parse a single type token (keyword, ARRAY<...>, SET<...>, VECTOR(N)).
fn parse_single(chars: &[char], pos: &mut usize, stop_at_gt: bool) -> Result<TypeExpr, String> {
    skip_ws(chars, pos);
    let keyword = read_keyword(chars, pos);
    if keyword.is_empty() {
        return Err(format!(
            "expected type keyword at position {pos}, found: {:?}",
            chars.get(*pos)
        ));
    }

    match keyword.as_str() {
        "NULL" => Ok(TypeExpr::Null),

        "INT" | "INTEGER" | "BIGINT" | "INT64" => Ok(TypeExpr::Simple(SimpleType::Int)),
        "FLOAT" | "DOUBLE" | "REAL" | "FLOAT64" => Ok(TypeExpr::Simple(SimpleType::Float)),
        "STRING" | "TEXT" | "VARCHAR" => Ok(TypeExpr::Simple(SimpleType::String)),
        "BOOL" | "BOOLEAN" => Ok(TypeExpr::Simple(SimpleType::Bool)),
        "BYTES" | "BYTEA" | "BLOB" => Ok(TypeExpr::Simple(SimpleType::Bytes)),
        "TIMESTAMP" | "TIMESTAMPTZ" => Ok(TypeExpr::Simple(SimpleType::Timestamp)),
        "DECIMAL" | "NUMERIC" => Ok(TypeExpr::Simple(SimpleType::Decimal)),
        "UUID" => Ok(TypeExpr::Simple(SimpleType::Uuid)),
        "ULID" => Ok(TypeExpr::Simple(SimpleType::Ulid)),
        "GEOMETRY" => Ok(TypeExpr::Simple(SimpleType::Geometry)),
        "DURATION" => Ok(TypeExpr::Simple(SimpleType::Duration)),
        "OBJECT" => Ok(TypeExpr::Simple(SimpleType::Object)),
        "JSON" => Ok(TypeExpr::Simple(SimpleType::Json)),
        "REGEX" => Ok(TypeExpr::Simple(SimpleType::Regex)),
        "RANGE" => Ok(TypeExpr::Simple(SimpleType::Range)),
        "RECORD" => Ok(TypeExpr::Simple(SimpleType::Record)),

        "VECTOR" => {
            // Expect '(' digits ')'.
            skip_ws(chars, pos);
            if *pos >= chars.len() || chars[*pos] != '(' {
                return Err(format!("expected '(' after VECTOR at position {pos}"));
            }
            *pos += 1; // consume '('
            skip_ws(chars, pos);
            let digits = read_digits(chars, pos);
            if digits.is_empty() {
                return Err("expected dimension digits inside VECTOR(...)".to_string());
            }
            let dim: u32 = digits
                .parse()
                .map_err(|_| format!("invalid VECTOR dimension: '{digits}'"))?;
            if dim == 0 {
                return Err("VECTOR dimension must be > 0".to_string());
            }
            skip_ws(chars, pos);
            if *pos >= chars.len() || chars[*pos] != ')' {
                return Err(format!(
                    "expected ')' to close VECTOR({dim} at position {pos}"
                ));
            }
            *pos += 1; // consume ')'
            Ok(TypeExpr::Simple(SimpleType::Vector(dim)))
        }

        "ARRAY" => {
            // Optional typed variant: ARRAY<inner>
            skip_ws(chars, pos);
            if *pos < chars.len() && chars[*pos] == '<' {
                *pos += 1; // consume '<'
                skip_ws(chars, pos);
                let inner = parse_union(chars, pos, true)?;
                skip_ws(chars, pos);
                if *pos >= chars.len() || chars[*pos] != '>' {
                    return Err(format!(
                        "expected '>' to close ARRAY<...> at position {pos}"
                    ));
                }
                *pos += 1; // consume '>'
                Ok(TypeExpr::TypedArray(Box::new(inner)))
            } else {
                Ok(TypeExpr::Simple(SimpleType::Array))
            }
        }

        "SET" => {
            // Optional typed variant: SET<inner>
            skip_ws(chars, pos);
            if *pos < chars.len() && chars[*pos] == '<' && !stop_at_gt {
                *pos += 1; // consume '<'
                skip_ws(chars, pos);
                let inner = parse_union(chars, pos, true)?;
                skip_ws(chars, pos);
                if *pos >= chars.len() || chars[*pos] != '>' {
                    return Err(format!("expected '>' to close SET<...> at position {pos}"));
                }
                *pos += 1; // consume '>'
                Ok(TypeExpr::TypedSet(Box::new(inner)))
            } else if *pos < chars.len() && chars[*pos] == '<' {
                // Inside a nested context — consume the '<' and inner normally.
                *pos += 1;
                skip_ws(chars, pos);
                let inner = parse_union(chars, pos, true)?;
                skip_ws(chars, pos);
                if *pos >= chars.len() || chars[*pos] != '>' {
                    return Err(format!("expected '>' to close SET<...> at position {pos}"));
                }
                *pos += 1;
                Ok(TypeExpr::TypedSet(Box::new(inner)))
            } else {
                Ok(TypeExpr::Simple(SimpleType::Set))
            }
        }

        other => Err(format!("unknown type keyword: '{other}'")),
    }
}

fn skip_ws(chars: &[char], pos: &mut usize) {
    while *pos < chars.len() && chars[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

/// Read a contiguous alphabetic/digit/underscore token (uppercased).
fn read_keyword(chars: &[char], pos: &mut usize) -> String {
    let mut s = String::new();
    while *pos < chars.len() {
        let c = chars[*pos];
        if c.is_ascii_alphanumeric() || c == '_' {
            s.push(c.to_ascii_uppercase());
            *pos += 1;
        } else {
            break;
        }
    }
    s
}

/// Read contiguous ASCII digits.
fn read_digits(chars: &[char], pos: &mut usize) -> String {
    let mut s = String::new();
    while *pos < chars.len() && chars[*pos].is_ascii_digit() {
        s.push(chars[*pos]);
        *pos += 1;
    }
    s
}

// ── Validator ────────────────────────────────────────────────────────────────

/// Check if a [`Value`] matches a [`TypeExpr`].
///
/// Coercion rules:
/// - `Simple(Float)` accepts `Value::Integer` (int→float widening).
/// - `Simple(Timestamp)` accepts `Value::DateTime`, `Value::Integer`, and
///   `Value::String` (same rules as `ColumnType::Timestamp.accepts()`).
/// - `Simple(Decimal)` accepts `Value::Decimal`, `Value::Float`, `Value::Integer`,
///   and `Value::String`.
/// - `Simple(Uuid)` accepts `Value::Uuid` and `Value::String`.
/// - `Simple(Geometry)` accepts `Value::Geometry` and `Value::String`.
/// - `TypedArray(inner)` matches `Value::Array` where every element matches `inner`.
/// - `TypedSet(inner)` matches `Value::Set` where every element matches `inner`.
/// - `Union(variants)` matches if any variant matches.
pub fn value_matches_type(value: &Value, expr: &TypeExpr) -> bool {
    match expr {
        TypeExpr::Null => matches!(value, Value::Null),

        TypeExpr::Simple(simple) => value_matches_simple(value, simple),

        TypeExpr::TypedArray(inner) => match value {
            Value::Array(items) => items.iter().all(|item| value_matches_type(item, inner)),
            _ => false,
        },

        TypeExpr::TypedSet(inner) => match value {
            Value::Set(items) => items.iter().all(|item| value_matches_type(item, inner)),
            _ => false,
        },

        TypeExpr::Union(variants) => variants.iter().any(|v| value_matches_type(value, v)),
    }
}

fn value_matches_simple(value: &Value, simple: &SimpleType) -> bool {
    match simple {
        SimpleType::Int => matches!(value, Value::Integer(_)),
        SimpleType::Float => matches!(value, Value::Float(_) | Value::Integer(_)),
        SimpleType::String => matches!(value, Value::String(_)),
        SimpleType::Bool => matches!(value, Value::Bool(_)),
        SimpleType::Bytes => matches!(value, Value::Bytes(_)),
        SimpleType::Timestamp => matches!(
            value,
            Value::DateTime(_) | Value::Integer(_) | Value::String(_)
        ),
        SimpleType::Decimal => matches!(
            value,
            Value::Decimal(_) | Value::Float(_) | Value::Integer(_) | Value::String(_)
        ),
        SimpleType::Uuid => matches!(value, Value::Uuid(_) | Value::String(_)),
        SimpleType::Ulid => matches!(value, Value::Ulid(_) | Value::String(_)),
        SimpleType::Geometry => matches!(value, Value::Geometry(_) | Value::String(_)),
        SimpleType::Duration => matches!(value, Value::Duration(_)),
        SimpleType::Array => matches!(value, Value::Array(_)),
        SimpleType::Object => matches!(value, Value::Object(_)),
        SimpleType::Json => true, // Json accepts any value (same as ColumnType::Json)
        SimpleType::Set => matches!(value, Value::Set(_)),
        SimpleType::Regex => matches!(value, Value::Regex(_)),
        SimpleType::Range => matches!(value, Value::Range { .. }),
        SimpleType::Record => matches!(value, Value::Record { .. }),
        SimpleType::Vector(_) => matches!(value, Value::Array(_) | Value::Bytes(_)),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Parsing ──────────────────────────────────────────────────────────────

    #[test]
    fn parse_simple() {
        assert_eq!(
            parse_type_expr("STRING").unwrap(),
            TypeExpr::Simple(SimpleType::String)
        );
    }

    #[test]
    fn parse_union() {
        assert_eq!(
            parse_type_expr("STRING|NULL").unwrap(),
            TypeExpr::Union(vec![TypeExpr::Simple(SimpleType::String), TypeExpr::Null])
        );
    }

    #[test]
    fn parse_typed_array() {
        assert_eq!(
            parse_type_expr("ARRAY<STRING>").unwrap(),
            TypeExpr::TypedArray(Box::new(TypeExpr::Simple(SimpleType::String)))
        );
    }

    #[test]
    fn parse_typed_array_union() {
        assert_eq!(
            parse_type_expr("ARRAY<INT|FLOAT>").unwrap(),
            TypeExpr::TypedArray(Box::new(TypeExpr::Union(vec![
                TypeExpr::Simple(SimpleType::Int),
                TypeExpr::Simple(SimpleType::Float),
            ])))
        );
    }

    #[test]
    fn parse_vector() {
        assert_eq!(
            parse_type_expr("VECTOR(384)").unwrap(),
            TypeExpr::Simple(SimpleType::Vector(384))
        );
    }

    #[test]
    fn parse_case_insensitive() {
        assert_eq!(
            parse_type_expr("string|null").unwrap(),
            TypeExpr::Union(vec![TypeExpr::Simple(SimpleType::String), TypeExpr::Null])
        );
    }

    #[test]
    fn parse_aliases() {
        // INT aliases
        assert_eq!(
            parse_type_expr("INTEGER").unwrap(),
            TypeExpr::Simple(SimpleType::Int)
        );
        assert_eq!(
            parse_type_expr("BIGINT").unwrap(),
            TypeExpr::Simple(SimpleType::Int)
        );
        assert_eq!(
            parse_type_expr("INT64").unwrap(),
            TypeExpr::Simple(SimpleType::Int)
        );
        // TEXT alias
        assert_eq!(
            parse_type_expr("TEXT").unwrap(),
            TypeExpr::Simple(SimpleType::String)
        );
        // BOOLEAN alias
        assert_eq!(
            parse_type_expr("BOOLEAN").unwrap(),
            TypeExpr::Simple(SimpleType::Bool)
        );
        // BYTEA alias
        assert_eq!(
            parse_type_expr("BYTEA").unwrap(),
            TypeExpr::Simple(SimpleType::Bytes)
        );
    }

    #[test]
    fn parse_typed_set() {
        assert_eq!(
            parse_type_expr("SET<INT>").unwrap(),
            TypeExpr::TypedSet(Box::new(TypeExpr::Simple(SimpleType::Int)))
        );
    }

    #[test]
    fn parse_untyped_array() {
        assert_eq!(
            parse_type_expr("ARRAY").unwrap(),
            TypeExpr::Simple(SimpleType::Array)
        );
    }

    #[test]
    fn parse_untyped_set() {
        assert_eq!(
            parse_type_expr("SET").unwrap(),
            TypeExpr::Simple(SimpleType::Set)
        );
    }

    #[test]
    fn parse_error_unknown_keyword() {
        assert!(parse_type_expr("FOOBAR").is_err());
    }

    #[test]
    fn parse_error_empty() {
        assert!(parse_type_expr("").is_err());
        assert!(parse_type_expr("  ").is_err());
    }

    #[test]
    fn parse_error_vector_zero_dim() {
        assert!(parse_type_expr("VECTOR(0)").is_err());
    }

    // ── Matching ─────────────────────────────────────────────────────────────

    #[test]
    fn match_string() {
        let expr = parse_type_expr("STRING").unwrap();
        assert!(value_matches_type(&Value::String("hello".into()), &expr));
        assert!(!value_matches_type(&Value::Integer(1), &expr));
    }

    #[test]
    fn match_null_union() {
        let expr = parse_type_expr("STRING|NULL").unwrap();
        assert!(value_matches_type(&Value::Null, &expr));
        assert!(value_matches_type(&Value::String("x".into()), &expr));
        assert!(!value_matches_type(&Value::Integer(1), &expr));
    }

    #[test]
    fn match_typed_array() {
        let expr = parse_type_expr("ARRAY<INT>").unwrap();
        assert!(value_matches_type(
            &Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
            &expr
        ));
    }

    #[test]
    fn match_typed_array_fail() {
        let expr = parse_type_expr("ARRAY<INT>").unwrap();
        // Mixed-type array should fail.
        assert!(!value_matches_type(
            &Value::Array(vec![Value::Integer(1), Value::String("x".into())]),
            &expr
        ));
    }

    #[test]
    fn match_int_coercion() {
        // Integer should match Float (widening coercion).
        let expr = parse_type_expr("FLOAT").unwrap();
        assert!(value_matches_type(&Value::Integer(42), &expr));
    }

    #[test]
    fn no_match() {
        let expr = parse_type_expr("STRING").unwrap();
        assert!(!value_matches_type(&Value::Integer(99), &expr));
    }

    #[test]
    fn match_timestamp_coercions() {
        let expr = parse_type_expr("TIMESTAMP").unwrap();
        assert!(value_matches_type(
            &Value::String("2024-01-01".into()),
            &expr
        ));
        assert!(value_matches_type(&Value::Integer(1_700_000_000), &expr));
    }

    #[test]
    fn match_null_expr() {
        let expr = TypeExpr::Null;
        assert!(value_matches_type(&Value::Null, &expr));
        assert!(!value_matches_type(&Value::Integer(0), &expr));
    }

    #[test]
    fn match_json_accepts_any() {
        let expr = TypeExpr::Simple(SimpleType::Json);
        assert!(value_matches_type(&Value::Null, &expr));
        assert!(value_matches_type(&Value::Integer(1), &expr));
        assert!(value_matches_type(&Value::String("x".into()), &expr));
        assert!(value_matches_type(&Value::Bool(true), &expr));
    }

    #[test]
    fn match_vector_type() {
        let expr = parse_type_expr("VECTOR(128)").unwrap();
        // Accepts Array (float list) or Bytes (packed floats).
        assert!(value_matches_type(
            &Value::Array(vec![Value::Float(0.1)]),
            &expr
        ));
        assert!(value_matches_type(&Value::Bytes(vec![0u8; 512]), &expr));
        assert!(!value_matches_type(&Value::Integer(1), &expr));
    }
}
