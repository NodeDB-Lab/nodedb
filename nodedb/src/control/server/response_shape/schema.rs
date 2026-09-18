// SPDX-License-Identifier: BUSL-1.1

//! Planner-authoritative output schema types, plus the type mapping from the
//! planner's `SqlDataType` to the response shaper's wire-facing `DdlColType`.
//!
//! Nothing in this module is consumed by existing call sites yet; it is a
//! purely additive foundation for later threading the planner's resolved
//! output schema into response shaping (replacing the SQL-string re-parse
//! path).

/// One output column of a resolved query, as known by the planner.
///
/// `display_name` is the client-facing column label; `lookup_key` is the key
/// used to find the value in the flat row object emitted by the Data Plane
/// (for qualified `table.column` refs this is the full dot-joined form the
/// join executor prefixes). `ty` is the column's resolved type.
#[derive(Clone, Debug)]
pub struct OutputColumn {
    pub display_name: String,
    pub lookup_key: String,
    pub ty: super::types::DdlColType,
}

/// A SELECT-list column the Control Plane computes per output row after
/// the Data Plane returns the rows. `expr` is the bridge form; sequence
/// accessor calls inside it are resolved against the session's registry.
#[derive(Debug, Clone)]
pub struct CpComputedColumn {
    /// The output name the evaluated value is written under.
    pub alias: String,
    /// The expression to evaluate against the flat row.
    pub expr: crate::bridge::expr_eval::SqlExpr,
}

/// The authoritative output schema of a query, resolved by the planner.
///
/// `columns` is the ordered projected column list. `is_star` marks a
/// `SELECT *` whose concrete columns are only known from the returned rows
/// (id-first union derivation still applies for that case). `cp_computed`
/// lists the columns the response shaper evaluates on the Control Plane
/// before the projection runs; each one also appears in `columns` under its
/// alias.
#[derive(Clone, Debug, Default)]
pub struct OutputSchema {
    pub columns: Vec<OutputColumn>,
    pub is_star: bool,
    pub cp_computed: Vec<CpComputedColumn>,
}

/// Maps the planner's resolved SQL column type to the response shaper's
/// protocol-neutral wire type.
///
/// Variants with no dedicated wire type yet (`Decimal`, `Uuid`, `Vector`,
/// `Geometry`) fall back to `DdlColType::Text`, preserving today's
/// all-TEXT behavior for those types until a dedicated wire type exists.
pub fn sql_data_type_to_ddl_col_type(
    ty: &nodedb_sql::types_expr::SqlDataType,
) -> super::types::DdlColType {
    use super::types::DdlColType;
    use nodedb_sql::types_expr::SqlDataType;

    match ty {
        SqlDataType::Int64 => DdlColType::Int8,
        SqlDataType::Float64 => DdlColType::Float8,
        SqlDataType::String => DdlColType::Text,
        SqlDataType::Bool => DdlColType::Bool,
        SqlDataType::Bytes => DdlColType::Bytea,
        SqlDataType::Timestamp => DdlColType::Timestamp,
        SqlDataType::Timestamptz => DdlColType::Timestamptz,
        // No dedicated wire type yet; falls back to Text (no regression).
        SqlDataType::Decimal => DdlColType::Text,
        // No dedicated wire type yet; falls back to Text (no regression).
        SqlDataType::Uuid => DdlColType::Text,
        // No dedicated wire type yet; falls back to Text (no regression).
        SqlDataType::Vector(_) => DdlColType::Text,
        // No dedicated wire type yet; falls back to Text (no regression).
        SqlDataType::Geometry => DdlColType::Text,
        // No declared type at all; the column resolves by name only.
        SqlDataType::Unknown => DdlColType::Text,
    }
}

/// Like [`sql_data_type_to_ddl_col_type`], but narrows the result to the
/// column's declared numeric width — `Int8` to the declared
/// [`IntWidth`](nodedb_types::columnar::IntWidth), `Float8` to the declared
/// [`FloatWidth`](nodedb_types::columnar::FloatWidth) — when the catalog
/// recorded one.
///
/// # Why only `Int8` and `Float8` are narrowed
///
/// `SqlDataType::Int64` is the single planner-facing type for every integer
/// width — `SMALLINT`/`INT2`, `INTEGER`/`INT4`, and `BIGINT`/`INT8` all resolve
/// to it — and `SqlDataType::Float64` is likewise the single type for every
/// float width, with `REAL`/`FLOAT4` and `DOUBLE`/`FLOAT8`/`FLOAT` all
/// resolving to it (see `catalog_adapter::type_convert::parse_type_str` and
/// `nodedb_types::columnar::ColumnType::from_str`). nodedb's storage —
/// columnar, strict, and kv alike — always keeps integers as a full `i64` and
/// floats as a full `f64`. The declared width therefore carries no storage
/// meaning; it is authoritative for the wire contract: a client that declared
/// `SMALLINT` expects OID 21 and, in binary format, exactly two bytes, and one
/// that declared `REAL` expects OID 700 and exactly four. Silently widening
/// every integer to `BIGINT`'s OID 20, or every float to `double precision`'s
/// OID 701, breaks ORMs and typed client libraries that trust the advertised
/// OID. Every other `SqlDataType` variant passes through
/// unchanged with both widths ignored — there is no other wire-ambiguous case
/// to resolve.
///
/// # Why this takes resolved widths rather than a type string
///
/// One function resolves both numeric families so no call site can thread a
/// declared width for one and forget the other. Both widths are resolved once
/// at the catalog boundary by `catalog_adapter::type_convert` from the same
/// `fields` entries that catalog introspection (`\d`) reads, so the two
/// descriptions of a column's type cannot disagree.
///
/// For integers the resolved width is *also* enforced on the write path
/// (`nodedb_sql::planner::dml_helpers::check_declared_int_ranges`), because
/// narrowing an out-of-range `i64` wraps. Floats have no write-path
/// counterpart: narrowing rounds rather than wraps, and PostgreSQL itself
/// accepts-and-rounds a `double` literal into a `real` column. The one float
/// failure mode — a finite `f64` beyond `f32`'s range overflowing to infinity
/// — is caught at encode time by the pgwire shape encoder.
///
/// A `None` width means the catalog has no record of the declared type (for
/// example a planner-synthesized column) and leaves the base `Int8` / `Float8`
/// — the widest, and so the only lossless, fallback.
pub fn sql_data_type_to_ddl_col_type_with_width(
    ty: &nodedb_sql::types_expr::SqlDataType,
    int_width: Option<nodedb_types::columnar::IntWidth>,
    float_width: Option<nodedb_types::columnar::FloatWidth>,
) -> super::types::DdlColType {
    use super::types::DdlColType;
    use nodedb_types::columnar::{FloatWidth, IntWidth};

    let base = sql_data_type_to_ddl_col_type(ty);

    if let (DdlColType::Int8, Some(width)) = (base, int_width) {
        return match width {
            IntWidth::I16 => DdlColType::Int2,
            IntWidth::I32 => DdlColType::Int4,
            IntWidth::I64 => DdlColType::Int8,
        };
    }

    if let (DdlColType::Float8, Some(width)) = (base, float_width) {
        return match width {
            FloatWidth::F32 => DdlColType::Float4,
            FloatWidth::F64 => DdlColType::Float8,
        };
    }

    base
}

#[cfg(test)]
mod tests {
    use super::super::types::DdlColType;
    use super::*;
    use nodedb_sql::types_expr::SqlDataType;
    use nodedb_types::columnar::{FloatWidth, IntWidth};

    /// Each declared width narrows the `Int8` base to its own wire type, and
    /// the mapping matches the OIDs `IntWidth` itself advertises — locking the
    /// two representations of "how wide is this column" against drift.
    #[test]
    fn narrows_int8_to_each_declared_width() {
        let cases: &[(IntWidth, DdlColType, u32)] = &[
            (IntWidth::I16, DdlColType::Int2, 21),
            (IntWidth::I32, DdlColType::Int4, 23),
            (IntWidth::I64, DdlColType::Int8, 20),
        ];
        for (width, expected, expected_oid) in cases {
            assert_eq!(
                sql_data_type_to_ddl_col_type_with_width(&SqlDataType::Int64, Some(*width), None),
                *expected,
                "declared width {width:?} must narrow to {expected:?}"
            );
            assert_eq!(
                width.pg_oid(),
                *expected_oid,
                "declared width {width:?} must advertise OID {expected_oid}"
            );
        }
    }

    /// The float analogue: each declared float width narrows the `Float8` base
    /// to its own wire type, and the mapping matches the OIDs `FloatWidth`
    /// advertises. A column declared `REAL` must reach the client as float4
    /// (700), not float8 (701).
    #[test]
    fn narrows_float8_to_each_declared_width() {
        let cases: &[(FloatWidth, DdlColType, u32)] = &[
            (FloatWidth::F32, DdlColType::Float4, 700),
            (FloatWidth::F64, DdlColType::Float8, 701),
        ];
        for (width, expected, expected_oid) in cases {
            assert_eq!(
                sql_data_type_to_ddl_col_type_with_width(&SqlDataType::Float64, None, Some(*width)),
                *expected,
                "declared width {width:?} must narrow to {expected:?}"
            );
            assert_eq!(
                width.pg_oid(),
                *expected_oid,
                "declared width {width:?} must advertise OID {expected_oid}"
            );
        }
    }

    /// `width = None` — a planner-synthesized column, or one whose declared
    /// type the catalog never recorded — stays at the base `Int8` / `Float8`.
    /// `BIGINT` and `double precision` are the widest wire types of their
    /// families, so they are the only fallbacks that cannot lose a stored
    /// value.
    #[test]
    fn no_declared_width_stays_at_the_wider_base() {
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(&SqlDataType::Int64, None, None),
            DdlColType::Int8
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(&SqlDataType::Float64, None, None),
            DdlColType::Float8
        );
    }

    /// The two width families are independent: an integer width never narrows
    /// a float column and a float width never narrows an integer one, even
    /// when both are supplied. Neither combination is reachable from the
    /// catalog (a declared type resolves to at most one family), but a single
    /// function taking both must not cross-apply them.
    #[test]
    fn widths_do_not_cross_between_numeric_families() {
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(
                &SqlDataType::Float64,
                Some(IntWidth::I16),
                None
            ),
            DdlColType::Float8
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(
                &SqlDataType::Int64,
                None,
                Some(FloatWidth::F32)
            ),
            DdlColType::Int8
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(
                &SqlDataType::Int64,
                Some(IntWidth::I32),
                Some(FloatWidth::F32)
            ),
            DdlColType::Int4
        );
    }

    /// Only an `Int8` / `Float8` base is eligible for narrowing — every other
    /// `SqlDataType` passes through `sql_data_type_to_ddl_col_type` exactly,
    /// with both widths ignored even when supplied. A width can never disagree
    /// with the planner's resolved `SqlDataType` in practice, but this proves
    /// narrowing is never misapplied to a non-numeric column.
    #[test]
    fn passes_through_non_numeric_types_untouched() {
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(
                &SqlDataType::String,
                Some(IntWidth::I16),
                Some(FloatWidth::F32)
            ),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type_with_width(&SqlDataType::Bool, None, None),
            DdlColType::Bool
        );
    }

    #[test]
    fn maps_every_sql_data_type_variant() {
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Int64),
            DdlColType::Int8
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Float64),
            DdlColType::Float8
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::String),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Bool),
            DdlColType::Bool
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Bytes),
            DdlColType::Bytea
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Timestamp),
            DdlColType::Timestamp
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Timestamptz),
            DdlColType::Timestamptz
        );
        // Fallback variants: no dedicated wire type, all map to Text.
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Decimal),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Uuid),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Vector(3)),
            DdlColType::Text
        );
        assert_eq!(
            sql_data_type_to_ddl_col_type(&SqlDataType::Geometry),
            DdlColType::Text
        );
    }
}
