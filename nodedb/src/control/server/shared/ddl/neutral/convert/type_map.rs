// SPDX-License-Identifier: BUSL-1.1

//! Declared type spellings resolved to a `ColumnType` for CONVERT COLLECTION.
//!
//! Two mappers share one resolver: the column-definition mapper reads the
//! spelling an author wrote in the statement, the typeguard mapper reads the
//! spelling a guard declared. Both answer through
//! `nodedb_sql::parser::type_expr::parse_type_expr`'s resolver, so one
//! spelling resolves to one type on every path.

use super::super::super::result::DdlError;
use super::support::err;

/// Resolve a declared type spelling to a `ColumnType`.
///
/// The whole spelling resolves, so `TIMESTAMP WITH TIME ZONE` keeps the zone
/// the author wrote and `TIMESTAMP GARBAGE` names no type at all.
///
/// This is the parser `nodedb_sql::parser::type_expr::parse_type_expr`
/// resolves the same text through, so a spelling added there reaches both
/// CONVERT mappers with no edit here.
fn resolve_declared_type(
    declared: &str,
) -> Result<nodedb_types::columnar::ColumnType, nodedb_types::columnar::ColumnTypeParseError> {
    declared.trim().parse()
}

/// Map a declared SQL type spelling to a `ColumnType`.
///
/// The spelling resolves through [`resolve_declared_type`].
///
/// `BINARY` resolves ahead of that call. It is a CONVERT-only spelling for
/// `ColumnType::Bytes` that the shared parser rejects.
///
/// A spelling the shared parser rejects raises `42601`. The author names the
/// stored column type, so an unresolvable spelling must never become
/// `ColumnType::String`.
pub(super) fn sql_type_to_column_type(
    sql_type: &str,
) -> Result<nodedb_types::columnar::ColumnType, DdlError> {
    use nodedb_types::columnar::ColumnType;

    let declared = sql_type.trim();
    if declared.eq_ignore_ascii_case("BINARY") {
        return Ok(ColumnType::Bytes);
    }
    resolve_declared_type(declared)
        .map_err(|e| err("42601", format!("column type '{declared}': {e}")))
}

/// Map a typeguard type expression string to a `ColumnType`.
///
/// Union and generic wrappers reduce to their leaf spelling first. The leaf
/// then resolves through [`resolve_declared_type`], apart from the one
/// typeguard-only spelling named below.
///
/// A leaf the shared parser rejects raises `42601`. The guard declares what
/// the column holds, so an unresolvable leaf must never become
/// `ColumnType::String`.
pub(super) fn typeguard_type_to_column_type(
    type_expr: &str,
) -> Result<nodedb_types::columnar::ColumnType, DdlError> {
    use nodedb_types::columnar::ColumnType;

    // Strip union types — take the first non-NULL type.
    let base = type_expr
        .split('|')
        .map(|s| s.trim())
        .find(|s| !s.eq_ignore_ascii_case("NULL"))
        .unwrap_or(type_expr)
        .to_uppercase();

    // Strip generic parameters: ARRAY<STRING> → ARRAY, SET<INT> → SET.
    let base = base.split('<').next().unwrap_or(&base).trim();

    // `OBJECT` is a typeguard-only keyword. It names a field holding a nested
    // map, has no declared-DDL spelling, and stores as inline MessagePack.
    if base.eq_ignore_ascii_case("OBJECT") {
        return Ok(ColumnType::Json);
    }

    // Every other spelling resolves through the one shared parser, so a
    // spelling added there reaches typeguard conversion with no edit here.
    resolve_declared_type(base).map_err(|e| err("42601", format!("type guard type '{base}': {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::columnar::{ColumnType, DECLARED_FLOAT_KEYWORDS, DECLARED_INT_KEYWORDS};

    /// Both CONVERT type mappers must answer with the shared parser for every
    /// integer spelling it knows. A keyword added to `DECLARED_INT_KEYWORDS`
    /// extends this test, so neither mapper can fall to its `String` default
    /// for a spelling the rest of the database resolves to `Int64`.
    #[test]
    fn convert_mappers_resolve_every_declared_int_keyword() {
        for keyword in DECLARED_INT_KEYWORDS {
            assert_eq!(
                sql_type_to_column_type(keyword)
                    .expect("the column def mapper resolves a declared int/float keyword"),
                ColumnType::Int64,
                "column def mapper must resolve {keyword}"
            );
            assert_eq!(
                typeguard_type_to_column_type(keyword)
                    .expect("the typeguard mapper resolves a declared int/float keyword"),
                ColumnType::Int64,
                "typeguard mapper must resolve {keyword}"
            );
        }
    }

    /// The float counterpart of
    /// [`convert_mappers_resolve_every_declared_int_keyword`].
    #[test]
    fn convert_mappers_resolve_every_declared_float_keyword() {
        for keyword in DECLARED_FLOAT_KEYWORDS {
            assert_eq!(
                sql_type_to_column_type(keyword)
                    .expect("the column def mapper resolves a declared int/float keyword"),
                ColumnType::Float64,
                "column def mapper must resolve {keyword}"
            );
            assert_eq!(
                typeguard_type_to_column_type(keyword)
                    .expect("the typeguard mapper resolves a declared int/float keyword"),
                ColumnType::Float64,
                "typeguard mapper must resolve {keyword}"
            );
        }
    }

    /// Both mappers delegate to `ColumnType::from_declared_type` outside their
    /// documented exceptions. This pins the delegation itself, so a mapper
    /// cannot regrow a private keyword table that answers differently.
    #[test]
    fn convert_mappers_delegate_to_the_shared_parser() {
        for declared in [
            "TEXT",
            "VARCHAR",
            "STRING",
            "BOOL",
            "BOOLEAN",
            "BYTES",
            "BYTEA",
            "BLOB",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "NUMERIC",
            "DECIMAL",
            "UUID",
            "ULID",
            "JSON",
            "JSONB",
            "GEOMETRY",
            "SPARSEVECTOR",
            "VECTOR(384)",
            "DURATION",
            "ARRAY",
            "SET",
            "REGEX",
            "RANGE",
            "RECORD",
            "SYSTEM_TIMESTAMP",
        ] {
            let shared = ColumnType::from_declared_type(declared)
                .expect("test input names a type the shared parser knows");
            assert_eq!(
                sql_type_to_column_type(declared)
                    .expect("the column def mapper resolves a shared-parser spelling"),
                shared,
                "column def mapper must match the shared parser on {declared}"
            );
            assert_eq!(
                typeguard_type_to_column_type(declared)
                    .expect("the typeguard mapper resolves a shared-parser spelling"),
                shared,
                "typeguard mapper must match the shared parser on {declared}"
            );
        }
    }

    /// A spelling the shared parser rejects raises `42601` on both mappers.
    ///
    /// A silent `String` answer would store a column type the author never
    /// declared, so the refusal is the whole contract here. `TIMESTAMP
    /// GARBAGE` covers the trailing-word case, which resolving the leading
    /// token alone would read as `TIMESTAMP`.
    #[test]
    fn convert_mappers_refuse_a_spelling_the_shared_parser_rejects() {
        for declared in ["WIDGET", "TIMESTAMP GARBAGE"] {
            let column_def = sql_type_to_column_type(declared)
                .expect_err("the column def mapper must refuse an unresolvable spelling");
            assert_eq!(
                column_def.sqlstate, "42601",
                "column def mapper must refuse {declared} with 42601"
            );
            let typeguard = typeguard_type_to_column_type(declared)
                .expect_err("the typeguard mapper must refuse an unresolvable spelling");
            assert_eq!(
                typeguard.sqlstate, "42601",
                "typeguard mapper must refuse {declared} with 42601"
            );
        }
    }

    /// `BINARY` is a CONVERT-only spelling for `Bytes`; the shared parser
    /// rejects it, so the column def mapper resolves it itself.
    #[test]
    fn binary_stays_a_convert_only_bytes_spelling() {
        assert_eq!(
            sql_type_to_column_type("BINARY").expect("BINARY resolves"),
            ColumnType::Bytes
        );
        assert_eq!(ColumnType::from_declared_type("BINARY"), None);
    }

    /// `OBJECT` is a typeguard-only keyword the shared parser rejects, and a
    /// multi-word timestamp spelling keeps the zone the author wrote.
    #[test]
    fn typeguard_only_spellings_keep_their_own_answer() {
        assert_eq!(
            typeguard_type_to_column_type("OBJECT").expect("OBJECT resolves"),
            ColumnType::Json
        );
        assert_eq!(ColumnType::from_declared_type("OBJECT"), None);
        assert_eq!(
            typeguard_type_to_column_type("TIMESTAMP WITH TIME ZONE")
                .expect("the zone spelling resolves"),
            ColumnType::Timestamptz
        );
        assert_eq!(
            typeguard_type_to_column_type("TIMESTAMP WITHOUT TIME ZONE")
                .expect("the no-zone spelling resolves"),
            ColumnType::Timestamp
        );
    }

    /// Union and generic wrappers resolve from their inner leaf type.
    #[test]
    fn typeguard_unions_and_generics_resolve_to_the_leaf() {
        assert_eq!(
            typeguard_type_to_column_type("INT|NULL").expect("INT|NULL resolves"),
            ColumnType::Int64
        );
        assert_eq!(
            typeguard_type_to_column_type("NULL|STRING").expect("NULL|STRING resolves"),
            ColumnType::String
        );
        assert_eq!(
            typeguard_type_to_column_type("ARRAY<STRING>").expect("ARRAY<STRING> resolves"),
            ColumnType::Array
        );
        assert_eq!(
            typeguard_type_to_column_type("SET<INT>").expect("SET<INT> resolves"),
            ColumnType::Set
        );
    }

    /// One spelling, one answer. The typeguard ENFORCEMENT parser
    /// (`nodedb_sql::parser::type_expr::parse_type_expr`, which decides
    /// whether a written value is valid) and this CONVERT mapper (which
    /// decides the column type the same guard becomes) must resolve every
    /// declared spelling to the same type.
    ///
    /// A spelling one path reads as `Timestamp` and the other as
    /// `Timestamptz` is a document validated under one rule and stored under
    /// another. Adding a spelling to either parser extends this list, and a
    /// leaf added to `SimpleType` fails the mapping below until it names its
    /// column type.
    #[test]
    fn enforcement_and_convert_agree_on_every_typeguard_spelling() {
        use nodedb_sql::parser::type_expr::{SimpleType, TypeExpr, parse_type_expr};

        // The column type each typeguard leaf denotes. `Object` is the
        // typeguard-only leaf; every other leaf names its own `ColumnType`.
        fn leaf_column_type(leaf: &SimpleType) -> ColumnType {
            match leaf {
                SimpleType::Int => ColumnType::Int64,
                SimpleType::Float => ColumnType::Float64,
                SimpleType::String => ColumnType::String,
                SimpleType::Bool => ColumnType::Bool,
                SimpleType::Bytes => ColumnType::Bytes,
                SimpleType::Timestamp => ColumnType::Timestamp,
                SimpleType::Timestamptz => ColumnType::Timestamptz,
                SimpleType::SystemTimestamp => ColumnType::SystemTimestamp,
                SimpleType::Decimal { precision, scale } => ColumnType::Decimal {
                    precision: *precision,
                    scale: *scale,
                },
                SimpleType::Uuid => ColumnType::Uuid,
                SimpleType::Ulid => ColumnType::Ulid,
                SimpleType::Geometry => ColumnType::Geometry,
                SimpleType::Duration => ColumnType::Duration,
                SimpleType::Array => ColumnType::Array,
                SimpleType::Object => ColumnType::Json,
                SimpleType::Json => ColumnType::Json,
                SimpleType::Set => ColumnType::Set,
                SimpleType::Regex => ColumnType::Regex,
                SimpleType::Range => ColumnType::Range,
                SimpleType::Record => ColumnType::Record,
                SimpleType::Vector(dim) => ColumnType::Vector(*dim),
                SimpleType::SparseVector => ColumnType::SparseVector,
            }
        }

        let mut spellings: Vec<String> = [
            "TEXT",
            "VARCHAR",
            "VARCHAR(255)",
            "STRING",
            "BOOL",
            "BOOLEAN",
            "BYTES",
            "BYTEA",
            "BLOB",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMP WITHOUT TIME ZONE",
            "SYSTEM_TIMESTAMP",
            "DECIMAL",
            "NUMERIC",
            "DECIMAL(10, 2)",
            "UUID",
            "ULID",
            "GEOMETRY",
            "DURATION",
            "JSON",
            "JSONB",
            "OBJECT",
            "ARRAY",
            "SET",
            "REGEX",
            "RANGE",
            "RECORD",
            "SPARSEVECTOR",
            "VECTOR(384)",
        ]
        .iter()
        .map(|spelling| spelling.to_string())
        .collect();
        spellings.extend(
            DECLARED_INT_KEYWORDS
                .iter()
                .chain(DECLARED_FLOAT_KEYWORDS.iter())
                .map(|keyword| keyword.to_string()),
        );

        for spelling in spellings {
            let parsed = parse_type_expr(&spelling)
                .unwrap_or_else(|e| panic!("enforcement must parse {spelling}: {e}"));
            let TypeExpr::Simple(leaf) = parsed else {
                panic!("{spelling} must parse to a leaf type, got {parsed:?}")
            };
            assert_eq!(
                leaf_column_type(&leaf),
                typeguard_type_to_column_type(&spelling)
                    .unwrap_or_else(|e| panic!("CONVERT must resolve {spelling}: {}", e.message)),
                "enforcement and CONVERT must agree on {spelling}"
            );
        }
    }

    #[test]
    fn sql_type_to_column_type_distinguishes_timestamp_from_timestamptz() {
        for (spelling, expected) in [
            ("TIMESTAMP", ColumnType::Timestamp),
            ("TIMESTAMP WITHOUT TIME ZONE", ColumnType::Timestamp),
            ("TIMESTAMPTZ", ColumnType::Timestamptz),
            ("TIMESTAMP WITH TIME ZONE", ColumnType::Timestamptz),
        ] {
            assert_eq!(
                sql_type_to_column_type(spelling).expect("the spelling resolves"),
                expected,
                "column def mapper on {spelling}"
            );
        }
    }

    #[test]
    fn typeguard_type_to_column_type_distinguishes_timestamp_from_timestamptz() {
        for (spelling, expected) in [
            ("TIMESTAMP", ColumnType::Timestamp),
            ("TIMESTAMP WITHOUT TIME ZONE", ColumnType::Timestamp),
            ("TIMESTAMPTZ", ColumnType::Timestamptz),
            ("TIMESTAMP WITH TIME ZONE", ColumnType::Timestamptz),
        ] {
            assert_eq!(
                typeguard_type_to_column_type(spelling).expect("the spelling resolves"),
                expected,
                "typeguard mapper on {spelling}"
            );
        }
    }
}
