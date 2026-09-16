// SPDX-License-Identifier: BUSL-1.1

//! Resolve per-column result formats (text vs binary) for the extended query
//! protocol.
//!
//! A Bind message carries the client's requested result-column format codes
//! (via `portal.result_column_format`). NodeDB honors a binary request only
//! for the scalar types the cell encoder (`shape_encode::encode_cell`) has a
//! binary arm for: the integers, floats, `bool`, the string types, and the
//! timestamps. Every other column (`Bytea`/`Json`/`Jsonb`, the arrays, and
//! anything mapping to no dedicated scalar wire type) stays in text format
//! even when binary was requested — this is protocol-legal (the
//! RowDescription advertises text, the client decodes text).

use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{FieldFormat, FieldInfo};

use crate::control::server::response_shape::types::DdlColType;

/// Map a pgwire `Type` (as carried on a Describe-phase `FieldInfo`) back to the
/// protocol-neutral [`DdlColType`]. Types with no dedicated neutral scalar
/// (arrays aside) fall back to `Text`, matching the all-text default.
///
/// This is the inverse of the `DdlColType` -> `Type` mapping used when the
/// parser builds result fields, restricted to the variants that round-trip.
pub(super) fn pg_type_to_ddl_col_type(t: &Type) -> DdlColType {
    if *t == Type::INT8 {
        DdlColType::Int8
    } else if *t == Type::INT4 {
        DdlColType::Int4
    } else if *t == Type::INT2 {
        DdlColType::Int2
    } else if *t == Type::FLOAT8 {
        DdlColType::Float8
    } else if *t == Type::FLOAT4 {
        DdlColType::Float4
    } else if *t == Type::BOOL {
        DdlColType::Bool
    } else if *t == Type::BYTEA {
        DdlColType::Bytea
    } else if *t == Type::VARCHAR {
        DdlColType::Varchar
    } else if *t == Type::JSON {
        DdlColType::Json
    } else if *t == Type::JSONB {
        DdlColType::Jsonb
    } else if *t == Type::TIMESTAMP {
        DdlColType::Timestamp
    } else if *t == Type::TIMESTAMPTZ {
        DdlColType::Timestamptz
    } else if *t == Type::FLOAT4_ARRAY {
        DdlColType::Float4Array
    } else if *t == Type::FLOAT8_ARRAY {
        DdlColType::Float8Array
    } else {
        DdlColType::Text
    }
}

/// Whether a column of this neutral type has a binary arm in the cell
/// encoder. Bytea/Json/Jsonb and the array types have none and stay text
/// even when binary is requested.
pub(super) fn binary_supported(ct: DdlColType) -> bool {
    matches!(
        ct,
        DdlColType::Int8
            | DdlColType::Int4
            | DdlColType::Int2
            | DdlColType::Float8
            | DdlColType::Float4
            | DdlColType::Bool
            | DdlColType::Text
            | DdlColType::Varchar
            | DdlColType::Timestamp
            | DdlColType::Timestamptz
    )
}

/// Safe per-index format lookup that never panics.
///
/// `Format::format_for` indexes an `Individual(Vec<i16>)` unchecked; a Bind
/// with fewer result-format codes than result columns would otherwise panic.
/// Missing entries default to text.
fn requested_format(fmt: &Format, idx: usize) -> FieldFormat {
    match fmt {
        Format::Individual(codes) => codes
            .get(idx)
            .map(|c| FieldFormat::from(*c))
            .unwrap_or(FieldFormat::Text),
        // Unified variants apply to any index.
        _ => fmt.format_for(idx),
    }
}

/// Resolve the effective per-column result format for `fields`, honoring the
/// client's requested `fmt` but downgrading to `Text` for any column whose
/// type cannot be binary-encoded under the current feature set.
///
/// The returned vector is parallel to `fields`.
pub(super) fn resolve_result_formats(fields: &[FieldInfo], fmt: &Format) -> Vec<FieldFormat> {
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let ct = pg_type_to_ddl_col_type(f.datatype());
            if requested_format(fmt, i) == FieldFormat::Binary && binary_supported(ct) {
                FieldFormat::Binary
            } else {
                FieldFormat::Text
            }
        })
        .collect()
}

/// Rebuild `fields` with each column's format replaced by the resolved
/// `formats[i]` (defaulting to Text when short), preserving name and datatype.
///
/// Used by the portal Describe path so its RowDescription advertises the same
/// formats the Execute path will actually encode.
pub(super) fn stamp_formats(fields: &[FieldInfo], formats: &[FieldFormat]) -> Vec<FieldInfo> {
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let format = formats.get(i).copied().unwrap_or(FieldFormat::Text);
            FieldInfo::new(
                f.name().to_owned(),
                None,
                None,
                f.datatype().clone(),
                format,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_scalar_pg_types() {
        assert_eq!(pg_type_to_ddl_col_type(&Type::INT8), DdlColType::Int8);
        assert_eq!(pg_type_to_ddl_col_type(&Type::FLOAT8), DdlColType::Float8);
        assert_eq!(pg_type_to_ddl_col_type(&Type::BOOL), DdlColType::Bool);
        assert_eq!(pg_type_to_ddl_col_type(&Type::BYTEA), DdlColType::Bytea);
        assert_eq!(
            pg_type_to_ddl_col_type(&Type::TIMESTAMP),
            DdlColType::Timestamp
        );
        // Unmapped type falls back to Text.
        assert_eq!(pg_type_to_ddl_col_type(&Type::UUID), DdlColType::Text);
    }

    #[test]
    fn binary_supported_excludes_types_without_a_binary_arm() {
        assert!(binary_supported(DdlColType::Int8));
        assert!(binary_supported(DdlColType::Bool));
        assert!(binary_supported(DdlColType::Text));
        assert!(binary_supported(DdlColType::Timestamp));
        assert!(binary_supported(DdlColType::Timestamptz));
        // bytea has no binary arm (a byte cell renders as base64 text): it
        // downgrades to text-format like json and the arrays.
        assert!(!binary_supported(DdlColType::Bytea));
        assert!(!binary_supported(DdlColType::Json));
        assert!(!binary_supported(DdlColType::Float8Array));
    }

    #[test]
    fn unified_binary_downgrades_blocked_types() {
        let fields = vec![
            FieldInfo::new("a".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("b".into(), None, None, Type::JSON, FieldFormat::Text),
            FieldInfo::new("c".into(), None, None, Type::TIMESTAMP, FieldFormat::Text),
        ];
        let formats = resolve_result_formats(&fields, &Format::UnifiedBinary);
        assert_eq!(formats[0], FieldFormat::Binary);
        // JSON has no binary arm -> stays text even under UnifiedBinary.
        assert_eq!(formats[1], FieldFormat::Text);
        // TIMESTAMP has one -> honoured.
        assert_eq!(formats[2], FieldFormat::Binary);
    }

    #[test]
    fn individual_shorter_than_columns_defaults_text() {
        let fields = vec![
            FieldInfo::new("a".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("b".into(), None, None, Type::INT8, FieldFormat::Text),
        ];
        // Only one code provided for two columns; the second must default to
        // text rather than panic.
        let formats = resolve_result_formats(&fields, &Format::Individual(vec![1]));
        assert_eq!(formats[0], FieldFormat::Binary);
        assert_eq!(formats[1], FieldFormat::Text);
    }

    #[test]
    fn unified_text_is_all_text() {
        let fields = vec![FieldInfo::new(
            "a".into(),
            None,
            None,
            Type::INT8,
            FieldFormat::Text,
        )];
        let formats = resolve_result_formats(&fields, &Format::UnifiedText);
        assert_eq!(formats[0], FieldFormat::Text);
    }
}
