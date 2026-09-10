// SPDX-License-Identifier: Apache-2.0

//! Parser and validator for type expression strings used by the typeguard system.
//!
//! Parses strings like `"STRING"`, `"INT|NULL"`, `"ARRAY<STRING>"` into a
//! [`TypeExpr`] that can be used to validate [`nodedb_types::Value`] instances
//! at write time.

use nodedb_types::Value;
use nodedb_types::columnar::ColumnType;

use crate::error::SqlError;

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
///
/// Every variant except [`SimpleType::Object`] names one
/// [`ColumnType`](nodedb_types::columnar::ColumnType), so a spelling resolves
/// to the same type here and in the CONVERT path.
#[derive(Debug, Clone, PartialEq)]
pub enum SimpleType {
    Int,
    Float,
    String,
    Bool,
    Bytes,
    Timestamp,
    Timestamptz,
    /// Engine-assigned instant. Matching mirrors
    /// `ColumnType::SystemTimestamp`: a client writes an instant, never text.
    SystemTimestamp,
    /// Declared precision and scale carry the spelling the author wrote.
    /// Matching is variant-level, as it is for [`SimpleType::Vector`].
    Decimal {
        precision: u8,
        scale: u8,
    },
    Uuid,
    Ulid,
    Geometry,
    Duration,
    /// Untyped array (any element type).
    Array,
    /// Typeguard-only leaf: a field holding a nested map. No declared DDL
    /// spelling names it, and CONVERT maps such a field to `ColumnType::Json`.
    Object,
    Json,
    /// Untyped set (any element type).
    Set,
    Regex,
    Range,
    Record,
    /// Fixed-dimension float32 vector.
    Vector(u32),
    /// Dimensionless sparse vector — a `'{id: weight}'` string literal. Unlike
    /// `Vector(u32)` it takes no `(N)`; the sparse index carries no dimension.
    SparseVector,
}

// ── Parser ───────────────────────────────────────────────────────────────────

/// Parse a type expression string into a [`TypeExpr`].
///
/// Each leaf spelling resolves through
/// [`ColumnType::from_str`](std::str::FromStr), the parser the CONVERT path
/// resolves the same typeguard text through. A leaf the shared parser does not
/// know is an error, so a trailing word such as `TIMESTAMP GARBAGE` is
/// rejected rather than read as `TIMESTAMP`.
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
pub fn parse_type_expr(s: &str) -> Result<TypeExpr, SqlError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(SqlError::Parse {
            detail: "empty type expression".to_string(),
        });
    }
    let mut pos = 0usize;
    let chars: Vec<char> = s.chars().collect();
    let expr = parse_union(&chars, &mut pos, false)?;
    skip_ws(&chars, &mut pos);
    if pos < chars.len() {
        let rest: String = chars[pos..].iter().collect();
        return Err(SqlError::Parse {
            detail: format!("unexpected trailing input in type expression: '{rest}'"),
        });
    }
    Ok(expr)
}

/// Parse `single_type ('|' single_type)*`.
///
/// When `stop_at_gt` is true the parser stops before a `>` character (used
/// when parsing inside `ARRAY<...>` / `SET<...>`).
fn parse_union(chars: &[char], pos: &mut usize, stop_at_gt: bool) -> Result<TypeExpr, SqlError> {
    let mut variants: Vec<TypeExpr> = Vec::new();
    variants.push(parse_single(chars, pos)?);

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
        variants.push(parse_single(chars, pos)?);
    }

    if variants.len() == 1 {
        Ok(variants.remove(0))
    } else {
        Ok(TypeExpr::Union(variants))
    }
}

/// Parse a single type token: a keyword, `ARRAY<...>`, or `SET<...>`.
fn parse_single(chars: &[char], pos: &mut usize) -> Result<TypeExpr, SqlError> {
    skip_ws(chars, pos);
    let keyword = read_keyword(chars, pos);
    if keyword.is_empty() {
        return Err(SqlError::Parse {
            detail: format!(
                "expected type keyword at position {pos}, found: {:?}",
                chars.get(*pos)
            ),
        });
    }

    match keyword.as_str() {
        // Union sentinel. No declared DDL spelling names it.
        "NULL" => Ok(TypeExpr::Null),

        // Typeguard-only keyword: a field holding a nested map. The shared
        // declared-type parser has no `OBJECT` spelling, and the CONVERT path
        // maps such a field to `ColumnType::Json`.
        "OBJECT" => Ok(TypeExpr::Simple(SimpleType::Object)),

        // Typed generics: the typeguard grammar types the element, which no
        // declared DDL spelling does. Bare `ARRAY` / `SET` resolve as leaves.
        "ARRAY" | "SET" => {
            skip_ws(chars, pos);
            if *pos < chars.len() && chars[*pos] == '<' {
                *pos += 1; // consume '<'
                skip_ws(chars, pos);
                let inner = Box::new(parse_union(chars, pos, true)?);
                skip_ws(chars, pos);
                if *pos >= chars.len() || chars[*pos] != '>' {
                    return Err(SqlError::Parse {
                        detail: format!("expected '>' to close {keyword}<...> at position {pos}"),
                    });
                }
                *pos += 1; // consume '>'
                return Ok(if keyword == "ARRAY" {
                    TypeExpr::TypedArray(inner)
                } else {
                    TypeExpr::TypedSet(inner)
                });
            }
            parse_leaf(chars, pos, &keyword)
        }

        _ => parse_leaf(chars, pos, &keyword),
    }
}

/// Resolve one leaf spelling through the shared declared-type parser.
///
/// `ColumnType` answers which type a declared spelling names for both planes,
/// so routing here is what keeps typeguard enforcement and typeguard CONVERT
/// from reading one spelling two ways.
fn parse_leaf(chars: &[char], pos: &mut usize, keyword: &str) -> Result<TypeExpr, SqlError> {
    let spelling = read_spelling(chars, pos, keyword)?;
    let column_type: ColumnType = spelling.parse().map_err(|e| SqlError::Parse {
        detail: format!("type '{spelling}': {e}"),
    })?;
    Ok(TypeExpr::Simple(simple_from_column_type(
        column_type,
        &spelling,
    )?))
}

/// Read one declared type spelling: the leading keyword, the words that
/// continue it, and an attached parameter list.
///
/// `TIMESTAMP WITH TIME ZONE` is one type name, so continuation words join
/// with single spaces and the shared parser resolves the whole spelling. A
/// word that continues no known spelling makes the spelling unknown, which is
/// how a trailing garbage word is rejected instead of ignored.
///
/// A parameter list attaches to its keyword with no space, as
/// `DECIMAL(10, 2)` does. A `(` behind whitespace stays unread and the caller
/// reports it as trailing input.
fn read_spelling(chars: &[char], pos: &mut usize, keyword: &str) -> Result<String, SqlError> {
    let mut spelling = keyword.to_string();
    loop {
        if *pos < chars.len() && chars[*pos] == '(' {
            spelling.push_str(&read_paren_group(chars, pos)?);
            continue;
        }
        let mut probe = *pos;
        skip_ws(chars, &mut probe);
        if probe == *pos {
            break;
        }
        let word = read_keyword(chars, &mut probe);
        if word.is_empty() {
            break;
        }
        spelling.push(' ');
        spelling.push_str(&word);
        *pos = probe;
    }
    Ok(spelling)
}

/// Read a balanced parenthesized group, both parentheses included.
fn read_paren_group(chars: &[char], pos: &mut usize) -> Result<String, SqlError> {
    let start = *pos;
    let mut depth = 0usize;
    let mut group = String::new();
    while *pos < chars.len() {
        let ch = chars[*pos];
        group.push(ch);
        *pos += 1;
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Ok(group);
                }
            }
            _ => {}
        }
    }
    Err(SqlError::Parse {
        detail: format!("unclosed '(' at position {start}"),
    })
}

/// Map a resolved column type onto the typeguard leaf that validates it.
///
/// `ColumnType` is `#[non_exhaustive]`: a variant added later reaches the
/// typed error arm until a leaf is written for it.
fn simple_from_column_type(
    column_type: ColumnType,
    spelling: &str,
) -> Result<SimpleType, SqlError> {
    Ok(match column_type {
        ColumnType::Int64 => SimpleType::Int,
        ColumnType::Float64 => SimpleType::Float,
        ColumnType::String => SimpleType::String,
        ColumnType::Bool => SimpleType::Bool,
        ColumnType::Bytes => SimpleType::Bytes,
        ColumnType::Timestamp => SimpleType::Timestamp,
        ColumnType::Timestamptz => SimpleType::Timestamptz,
        ColumnType::SystemTimestamp => SimpleType::SystemTimestamp,
        ColumnType::Decimal { precision, scale } => SimpleType::Decimal { precision, scale },
        ColumnType::Geometry => SimpleType::Geometry,
        ColumnType::Vector(dim) => SimpleType::Vector(dim),
        ColumnType::SparseVector => SimpleType::SparseVector,
        ColumnType::Uuid => SimpleType::Uuid,
        ColumnType::Json => SimpleType::Json,
        ColumnType::Ulid => SimpleType::Ulid,
        ColumnType::Duration => SimpleType::Duration,
        ColumnType::Array => SimpleType::Array,
        ColumnType::Set => SimpleType::Set,
        ColumnType::Regex => SimpleType::Regex,
        ColumnType::Range => SimpleType::Range,
        ColumnType::Record => SimpleType::Record,
        other => {
            return Err(SqlError::Parse {
                detail: format!(
                    "type '{spelling}' resolves to {other}, which no typeguard validates"
                ),
            });
        }
    })
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

// ── Validator ────────────────────────────────────────────────────────────────

/// Check if a [`Value`] matches a [`TypeExpr`].
///
/// Coercion rules:
/// - `Simple(Float)` accepts `Value::Integer` (int→float widening).
/// - `Simple(Timestamp)` accepts `Value::DateTime`, `Value::Integer`, and
///   `Value::String` (same rules as `ColumnType::Timestamp.accepts()`).
/// - `Simple(Decimal)` accepts `Value::Decimal`, `Value::Float`, `Value::Integer`,
///   and `Value::String`. Declared precision and scale do not narrow it.
/// - `Simple(SystemTimestamp)` accepts `Value::DateTime` and `Value::Integer`.
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
            Value::NaiveDateTime(_) | Value::Integer(_) | Value::String(_)
        ),
        SimpleType::Timestamptz => matches!(
            value,
            Value::DateTime(_) | Value::Integer(_) | Value::String(_)
        ),
        // Mirrors `ColumnType::SystemTimestamp.accepts`: an engine-assigned
        // instant takes no text form.
        SimpleType::SystemTimestamp => matches!(value, Value::DateTime(_) | Value::Integer(_)),
        SimpleType::Decimal { .. } => matches!(
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
        // Sparse vector arrives as a `'{id: weight}'` string literal (raw bytes
        // are also accepted, mirroring `ColumnType::SparseVector::accepts`).
        SimpleType::SparseVector => matches!(value, Value::String(_) | Value::Bytes(_)),
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
    fn parse_sparsevector() {
        // Dimensionless keyword parses to the SparseVector leaf type.
        assert_eq!(
            parse_type_expr("SPARSEVECTOR").unwrap(),
            TypeExpr::Simple(SimpleType::SparseVector)
        );
        assert_eq!(
            parse_type_expr("sparsevector").unwrap(),
            TypeExpr::Simple(SimpleType::SparseVector)
        );
    }

    #[test]
    fn match_sparsevector_accepts_string_literal() {
        let expr = parse_type_expr("SPARSEVECTOR").unwrap();
        assert!(value_matches_type(&Value::String("{3:0.5}".into()), &expr));
        assert!(value_matches_type(&Value::Bytes(vec![1, 2, 3]), &expr));
        assert!(!value_matches_type(&Value::Integer(1), &expr));
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

    /// Both zone spellings are one type name each, so the parser reads the
    /// zone the author wrote instead of the leading word alone.
    #[test]
    fn parse_timestamp_zone_spellings() {
        assert_eq!(
            parse_type_expr("TIMESTAMP WITH TIME ZONE").unwrap(),
            TypeExpr::Simple(SimpleType::Timestamptz)
        );
        assert_eq!(
            parse_type_expr("timestamp without time zone").unwrap(),
            TypeExpr::Simple(SimpleType::Timestamp)
        );
    }

    /// A word that continues no known spelling is an error. Reading it as the
    /// leading keyword is what let one declaration mean two types.
    #[test]
    fn parse_error_trailing_words() {
        assert!(parse_type_expr("TIMESTAMP GARBAGE WORDS").is_err());
        assert!(parse_type_expr("STRING FOO").is_err());
        assert!(parse_type_expr("TIMESTAMP WITH").is_err());
        assert!(parse_type_expr("INT )").is_err());
        assert!(parse_type_expr("VECTOR(3) EXTRA").is_err());
    }

    /// Every integer and float spelling the shared parser knows parses here.
    /// A keyword added to either list extends this test, so a declaration the
    /// rest of the database resolves cannot fail typeguard enforcement.
    #[test]
    fn parse_every_declared_numeric_keyword() {
        use nodedb_types::columnar::{DECLARED_FLOAT_KEYWORDS, DECLARED_INT_KEYWORDS};
        for keyword in DECLARED_INT_KEYWORDS {
            assert_eq!(
                parse_type_expr(keyword).unwrap(),
                TypeExpr::Simple(SimpleType::Int),
                "{keyword} must parse as an integer leaf"
            );
        }
        for keyword in DECLARED_FLOAT_KEYWORDS {
            assert_eq!(
                parse_type_expr(keyword).unwrap(),
                TypeExpr::Simple(SimpleType::Float),
                "{keyword} must parse as a float leaf"
            );
        }
    }

    #[test]
    fn parse_jsonb() {
        assert_eq!(
            parse_type_expr("JSONB").unwrap(),
            TypeExpr::Simple(SimpleType::Json)
        );
    }

    #[test]
    fn parse_decimal_carries_declared_params() {
        assert_eq!(
            parse_type_expr("DECIMAL(10, 2)").unwrap(),
            TypeExpr::Simple(SimpleType::Decimal {
                precision: 10,
                scale: 2
            })
        );
        assert_eq!(
            parse_type_expr("NUMERIC").unwrap(),
            TypeExpr::Simple(SimpleType::Decimal {
                precision: 38,
                scale: 10
            })
        );
        assert!(parse_type_expr("DECIMAL(0)").is_err());
    }

    /// A declared character length resolves to the same leaf bare `VARCHAR`
    /// gives, and matching stays a string check.
    #[test]
    fn parse_varchar_length() {
        let expr = parse_type_expr("VARCHAR(255)").unwrap();
        assert_eq!(expr, TypeExpr::Simple(SimpleType::String));
        assert!(value_matches_type(&Value::String("x".into()), &expr));
        assert!(parse_type_expr("VARCHAR(0)").is_err());
    }

    /// `SYSTEM_TIMESTAMP` is engine-assigned, so it takes an instant and no
    /// text — the rule `ColumnType::SystemTimestamp` enforces.
    #[test]
    fn parse_and_match_system_timestamp() {
        let expr = parse_type_expr("SYSTEM_TIMESTAMP").unwrap();
        assert_eq!(expr, TypeExpr::Simple(SimpleType::SystemTimestamp));
        let dt = nodedb_types::NdbDateTime::from_micros(1_700_000_000_000_000);
        assert!(value_matches_type(&Value::DateTime(dt), &expr));
        assert!(value_matches_type(&Value::Integer(1_700_000_000), &expr));
        assert!(!value_matches_type(
            &Value::String("2024-01-01".into()),
            &expr
        ));
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
    fn parse_timestamptz() {
        assert_eq!(
            parse_type_expr("TIMESTAMPTZ").unwrap(),
            TypeExpr::Simple(SimpleType::Timestamptz)
        );
    }

    #[test]
    fn match_timestamptz_coercions() {
        let expr = parse_type_expr("TIMESTAMPTZ").unwrap();
        let dt = nodedb_types::NdbDateTime::from_micros(1_700_000_000_000_000);
        assert!(value_matches_type(&Value::DateTime(dt), &expr));
        assert!(value_matches_type(
            &Value::String("2024-01-01T00:00:00Z".into()),
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
