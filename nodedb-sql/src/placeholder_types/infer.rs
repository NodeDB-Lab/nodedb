// SPDX-License-Identifier: Apache-2.0

//! The public entry point: parse once, run both inference passes, report one
//! slot per placeholder position.

use sqlparser::ast::{Statement, Visit};

use super::column_backed;
use super::slots::{InferenceContext, InferredParamType};
use crate::catalog::SqlCatalog;
use crate::parser::array_stmt::try_parse_array_statement;
use crate::parser::preprocess;
use crate::parser::statement::parse_sql;

/// Best-effort, catalog-aware inference of `$N` placeholder types.
///
/// Returns one slot per placeholder, indexed by `N - 1`. `None` means the
/// position is not one this pass can resolve unambiguously.
///
/// Under-inference is always safe: a PostgreSQL client that receives an
/// unknown parameter type sends the value in text format, which this server
/// already handles. Over-inference is NOT safe — reporting a concrete OID
/// makes the client commit to a binary encoding for that type, so any
/// position whose type is ambiguous MUST stay `None` rather than be guessed.
///
/// The pass never errors and never panics: unparseable SQL yields an empty
/// result, and a catalog lookup that fails leaves the positions behind it
/// unresolved.
///
/// # Resolved forms
///
/// Catalog-free:
///
/// * `LIMIT $N` / `OFFSET $N` (including the `UPDATE` / `DELETE` limit forms)
///   → [`crate::types_expr::SqlDataType::Int64`].
/// * `$N::<type>` and `CAST($N AS <type>)` → the named type.
///
/// Catalog-backed, each carrying the column's declared
/// [`nodedb_types::columnar::IntWidth`]:
///
/// * `col <cmp> $N` and `$N <cmp> col` in `WHERE` / `HAVING`.
/// * `col IN ($N, ...)` and `col BETWEEN $N AND $N`.
/// * `UPDATE t SET col = $N`.
/// * `INSERT INTO t (cols...) VALUES ($N, ...)`, including multi-row
///   `VALUES` and the positional (no column list) form.
pub fn infer_placeholder_types(
    sql: &str,
    catalog: &dyn SqlCatalog,
) -> Vec<Option<InferredParamType>> {
    let Some(statements) = parse_best_effort(sql) else {
        return Vec::new();
    };
    let mut ctx = InferenceContext::default();
    for statement in &statements {
        // `Visit` only breaks when the visitor asks it to; this one never
        // does. It sizes the slot table and resolves the catalog-free forms.
        let _ = statement.visit(&mut ctx);
        column_backed::infer_from_statement(&mut ctx, catalog, statement);
    }
    ctx.finish()
}

/// Parse `sql` the same way `plan_sql` does, but swallowing every failure.
///
/// Mirrors `plan_sql`'s front end so a statement that plans successfully is
/// also one this pass sees: NodeDB's `ARRAY` DDL/DML family bypasses
/// sqlparser entirely (and carries no placeholders), and everything else goes
/// through the preprocessor before `parse_sql`. If the preprocessor rejects
/// the input we still try the raw text, since a preprocessor-only failure
/// (e.g. an unsupported NodeDB extension) does not imply the placeholders are
/// unreadable.
fn parse_best_effort(sql: &str) -> Option<Vec<Statement>> {
    // Array statements accept no bound parameters.
    if let Ok(Some(_)) = try_parse_array_statement(sql) {
        return None;
    }
    let preprocessed = preprocess::preprocess(sql).ok().flatten();
    let effective_sql = preprocessed.as_ref().map_or(sql, |p| p.sql.as_str());
    parse_sql(effective_sql).ok()
}

/// Unit tests for `$N` placeholder type inference.
#[cfg(test)]
mod tests {
    use nodedb_types::DatabaseId;
    use nodedb_types::columnar::{FloatWidth, IntWidth};

    use super::infer_placeholder_types;
    use crate::catalog::{SqlCatalog, SqlCatalogError};
    use crate::placeholder_types::slots::{InferredParamType, parse_placeholder_body};
    use crate::types::{CollectionInfo, ColumnInfo, EngineType};
    use crate::types_expr::SqlDataType;

    // ---------------------------------------------------------------------------
    // Catalog double
    // ---------------------------------------------------------------------------

    fn col(name: &str, data_type: SqlDataType, int_width: Option<IntWidth>) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width,
            float_width: None,
        }
    }

    /// A float column with a declared width — the float analogue of `col`'s
    /// `int_width` argument. Separate because the two families never co-occur on
    /// one column, so widening `col` with a fourth argument would make every
    /// integer and string call site carry a `None` it can never set.
    fn float_col(name: &str, float_width: FloatWidth) -> ColumnInfo {
        ColumnInfo {
            float_width: Some(float_width),
            ..col(name, SqlDataType::Float64, None)
        }
    }

    fn collection(name: &str, columns: Vec<ColumnInfo>) -> CollectionInfo {
        CollectionInfo {
            name: name.into(),
            engine: EngineType::DocumentStrict,
            columns,
            primary_key: Some("id".into()),
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
        }
    }

    /// Minimal catalog mirroring the shape used by the crate's other planner
    /// tests (`planner::select::tests`, `tests/positional_insert_column_binding.rs`),
    /// extended with the declared column widths this pass has to preserve.
    struct TestCatalog;

    impl SqlCatalog for TestCatalog {
        fn get_collection(
            &self,
            _: DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            let info = match name {
                "t" => Some(collection(
                    "t",
                    vec![
                        col("id", SqlDataType::String, None),
                        col("n", SqlDataType::Int64, Some(IntWidth::I32)),
                        col("big", SqlDataType::Int64, Some(IntWidth::I64)),
                        col("label", SqlDataType::String, None),
                        col("flag", SqlDataType::Bool, None),
                        float_col("ratio", FloatWidth::F32),
                        float_col("wide", FloatWidth::F64),
                    ],
                )),
                // Two relations sharing a column name: the ambiguity lock.
                "lefty" => Some(collection(
                    "lefty",
                    vec![
                        col("shared", SqlDataType::String, None),
                        col("only_left", SqlDataType::Int64, Some(IntWidth::I32)),
                    ],
                )),
                "righty" => Some(collection(
                    "righty",
                    vec![
                        col("shared", SqlDataType::Int64, Some(IntWidth::I64)),
                        col("only_right", SqlDataType::Bool, None),
                    ],
                )),
                // Schemaless: exists, but declares no column order.
                "loose" => Some(collection("loose", Vec::new())),
                _ => None,
            };
            Ok(info)
        }
    }

    // ---------------------------------------------------------------------------
    // Assertion helpers
    // ---------------------------------------------------------------------------

    fn infer(sql: &str) -> Vec<Option<InferredParamType>> {
        infer_placeholder_types(sql, &TestCatalog)
    }

    /// A type the SQL text named on its own — no catalog column, no width.
    fn from_sql(data_type: SqlDataType) -> Option<InferredParamType> {
        Some(InferredParamType {
            data_type,
            int_width: None,
            float_width: None,
        })
    }

    /// A type taken from a catalog column, declared integer width included.
    fn from_col(data_type: SqlDataType, int_width: Option<IntWidth>) -> Option<InferredParamType> {
        Some(InferredParamType {
            data_type,
            int_width,
            float_width: None,
        })
    }

    /// A type taken from a catalog float column, declared float width included.
    fn from_float_col(float_width: FloatWidth) -> Option<InferredParamType> {
        Some(InferredParamType {
            data_type: SqlDataType::Float64,
            int_width: None,
            float_width: Some(float_width),
        })
    }

    // ---------------------------------------------------------------------------
    // Catalog-free forms
    // ---------------------------------------------------------------------------

    #[test]
    fn limit_placeholder_is_int64() {
        assert_eq!(
            infer("SELECT id FROM t LIMIT $1"),
            vec![from_sql(SqlDataType::Int64)]
        );
    }

    #[test]
    fn offset_placeholder_is_int64() {
        assert_eq!(
            infer("SELECT id FROM t LIMIT 10 OFFSET $1"),
            vec![from_sql(SqlDataType::Int64)]
        );
    }

    #[test]
    fn limit_and_offset_placeholders_are_both_int64() {
        assert_eq!(
            infer("SELECT id FROM t LIMIT $1 OFFSET $2"),
            vec![from_sql(SqlDataType::Int64), from_sql(SqlDataType::Int64)]
        );
    }

    #[test]
    fn double_colon_cast_resolves_target_type() {
        // `INT` is `int4`, as the cast evaluates it and PostgreSQL describes it.
        assert_eq!(
            infer("SELECT $1::INT"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I32))]
        );
        assert_eq!(
            infer("SELECT $1::INT4"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I32))]
        );
        assert_eq!(
            infer("SELECT $1::SMALLINT"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I16))]
        );
        assert_eq!(
            infer("SELECT $1::BIGINT"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I64))]
        );
    }

    #[test]
    fn cast_as_syntax_resolves_target_type() {
        assert_eq!(
            infer("SELECT CAST($1 AS TEXT)"),
            vec![from_sql(SqlDataType::String)]
        );
    }

    #[test]
    fn cast_target_types_cover_each_mapped_family() {
        let cases: &[(&str, SqlDataType)] = &[
            ("VARCHAR(10)", SqlDataType::String),
            ("BOOLEAN", SqlDataType::Bool),
            ("BYTEA", SqlDataType::Bytes),
            ("TIMESTAMP", SqlDataType::Timestamp),
            ("TIMESTAMPTZ", SqlDataType::Timestamptz),
        ];
        for (name, expected) in cases {
            assert_eq!(
                infer(&format!("SELECT CAST($1 AS {name})")),
                vec![from_sql(expected.clone())],
                "cast to {name} must resolve to {expected:?}"
            );
        }
    }

    /// Types with no faithful wire representation on the pgwire side stay
    /// unresolved rather than being narrowed to something a client cannot
    /// round-trip.
    /// A float cast target carries its width: `REAL` is `float4`, `DOUBLE
    /// PRECISION` `float8`.
    #[test]
    fn float_cast_targets_carry_their_width() {
        assert_eq!(
            infer("SELECT CAST($1 AS REAL)"),
            vec![from_float_col(FloatWidth::F32)]
        );
        assert_eq!(
            infer("SELECT CAST($1 AS DOUBLE PRECISION)"),
            vec![from_float_col(FloatWidth::F64)]
        );
    }

    #[test]
    fn unmapped_cast_target_stays_none() {
        assert_eq!(infer("SELECT $1::NUMERIC"), vec![None]);
        assert_eq!(infer("SELECT $1::UUID"), vec![None]);
    }

    /// Slot assignment follows the placeholder index, not the order the
    /// positions appear in the statement.
    #[test]
    fn out_of_order_indices_land_in_correct_slots() {
        assert_eq!(
            infer("SELECT id FROM t WHERE unknown_col = $2 LIMIT $1"),
            vec![from_sql(SqlDataType::Int64), None]
        );
    }

    /// A repeated index typed once by a resolvable position keeps that type.
    #[test]
    fn repeated_index_resolved_once_keeps_its_type() {
        assert_eq!(
            infer("SELECT id FROM t WHERE unknown_col = $1 LIMIT $1"),
            vec![from_sql(SqlDataType::Int64)]
        );
    }

    /// Two resolvable positions that disagree about the same index leave it
    /// unknown — reporting either would over-infer.
    #[test]
    fn conflicting_types_for_one_index_stay_none() {
        assert_eq!(infer("SELECT $1::TEXT FROM t LIMIT $1"), vec![None]);
    }

    /// The width is part of the identity a conflict is judged on: `LIMIT` types
    /// its position as a width-less int8, a `WHERE n = $1` on an `INT` column as
    /// an int4. Advertising either for a position that is both would be wrong.
    #[test]
    fn same_logical_type_but_different_width_conflicts() {
        assert_eq!(infer("SELECT id FROM t WHERE n = $1 LIMIT $1"), vec![None]);
    }

    /// Sizing follows the highest index seen, so unmentioned lower indices
    /// still get a slot.
    #[test]
    fn result_is_sized_to_highest_index() {
        let inferred = infer("SELECT id FROM t WHERE unknown_col = $3 LIMIT $1");
        assert_eq!(inferred.len(), 3);
        assert_eq!(inferred[0], from_sql(SqlDataType::Int64));
        assert_eq!(inferred[1], None);
        assert_eq!(inferred[2], None);
    }

    #[test]
    fn statement_without_placeholders_is_empty() {
        assert!(infer("SELECT id FROM t").is_empty());
    }

    #[test]
    fn unparseable_sql_returns_empty() {
        assert!(infer("this is not sql at all $1").is_empty());
        assert!(infer("").is_empty());
        assert!(infer("SELECT FROM WHERE $1 $2").is_empty());
    }

    /// A non-`$N` placeholder spelling must neither panic nor claim a slot.
    #[test]
    fn malformed_placeholder_bodies_claim_no_slot() {
        assert!(parse_placeholder_body("$").is_none());
        assert!(parse_placeholder_body("$0").is_none());
        assert!(parse_placeholder_body("$abc").is_none());
        assert!(parse_placeholder_body("?").is_none());
        assert!(parse_placeholder_body("").is_none());
        assert_eq!(parse_placeholder_body("$7"), Some(7));
    }

    /// `?` placeholders carry no position, so nothing is reported for them.
    #[test]
    fn positionless_placeholder_reports_nothing() {
        assert!(infer("SELECT id FROM t WHERE n = ?").is_empty());
    }

    #[test]
    fn update_limit_placeholder_is_int64() {
        assert_eq!(
            infer("UPDATE t SET label = 'x' WHERE id > '0' LIMIT $1"),
            vec![from_sql(SqlDataType::Int64)]
        );
    }

    /// A derived table's columns cannot be enumerated, so the outer scope is
    /// opaque and `s.id` stays unresolved — while the inner `LIMIT` still types.
    #[test]
    fn subquery_limit_placeholder_is_int64() {
        assert_eq!(
            infer("SELECT * FROM (SELECT id FROM t LIMIT $1) s WHERE s.id = $2"),
            vec![from_sql(SqlDataType::Int64), None]
        );
    }

    #[test]
    fn cte_cast_placeholder_resolves() {
        assert_eq!(
            infer("WITH x AS (SELECT $1::BIGINT AS v) SELECT v FROM x"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I64))]
        );
    }

    // ---------------------------------------------------------------------------
    // Catalog-backed forms
    // ---------------------------------------------------------------------------

    /// The headline form. An `INT` column must carry its declared `I32` width so
    /// the caller can advertise oid 23 rather than collapsing to int8.
    #[test]
    fn where_comparison_resolves_column_type_and_width() {
        assert_eq!(
            infer("SELECT id FROM t WHERE n = $1"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I32))]
        );
        assert_eq!(
            infer("SELECT id FROM t WHERE big = $1"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I64))]
        );
        assert_eq!(
            infer("SELECT id FROM t WHERE label = $1"),
            vec![from_col(SqlDataType::String, None)]
        );
        assert_eq!(
            infer("SELECT id FROM t WHERE flag = $1"),
            vec![from_col(SqlDataType::Bool, None)]
        );
    }

    /// The float analogue: a `REAL` column must carry its declared `F32` width so
    /// the caller advertises oid 700 and the client encodes four bytes, while a
    /// `DOUBLE` column stays `F64` (oid 701).
    #[test]
    fn where_comparison_resolves_declared_float_width() {
        assert_eq!(
            infer("SELECT id FROM t WHERE ratio = $1"),
            vec![from_float_col(FloatWidth::F32)]
        );
        assert_eq!(
            infer("SELECT id FROM t WHERE wide = $1"),
            vec![from_float_col(FloatWidth::F64)]
        );
    }

    /// `$1 = col` is the same form with the operands swapped.
    #[test]
    fn reversed_operand_order_resolves() {
        assert_eq!(
            infer("SELECT id FROM t WHERE $1 = n"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I32))]
        );
    }

    #[test]
    fn every_comparison_operator_resolves() {
        for op in ["=", "<>", "!=", "<", "<=", ">", ">="] {
            assert_eq!(
                infer(&format!("SELECT id FROM t WHERE n {op} $1")),
                vec![from_col(SqlDataType::Int64, Some(IntWidth::I32))],
                "`n {op} $1` must resolve to the column's type"
            );
        }
    }

    #[test]
    fn conjunction_resolves_each_side_independently() {
        assert_eq!(
            infer("SELECT id FROM t WHERE n = $1 AND label = $2"),
            vec![
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
                from_col(SqlDataType::String, None),
            ]
        );
    }

    #[test]
    fn in_list_types_every_placeholder() {
        assert_eq!(
            infer("SELECT id FROM t WHERE n IN ($1, $2)"),
            vec![
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
            ]
        );
    }

    #[test]
    fn between_types_both_bounds() {
        assert_eq!(
            infer("SELECT id FROM t WHERE n BETWEEN $1 AND $2"),
            vec![
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
            ]
        );
    }

    #[test]
    fn having_clause_resolves_against_the_same_scope() {
        assert_eq!(
            infer("SELECT label FROM t GROUP BY label HAVING label = $1"),
            vec![from_col(SqlDataType::String, None)]
        );
    }

    #[test]
    fn update_set_resolves_target_column_type() {
        assert_eq!(
            infer("UPDATE t SET n = $1"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I32))]
        );
        assert_eq!(
            infer("UPDATE t SET label = $1 WHERE n = $2"),
            vec![
                from_col(SqlDataType::String, None),
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
            ]
        );
    }

    #[test]
    fn delete_predicate_resolves() {
        assert_eq!(
            infer("DELETE FROM t WHERE n = $1"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I32))]
        );
    }

    #[test]
    fn insert_values_map_positionally_to_the_column_list() {
        assert_eq!(
            infer("INSERT INTO t (id, n) VALUES ($1, $2)"),
            vec![
                from_col(SqlDataType::String, None),
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
            ]
        );
    }

    #[test]
    fn insert_multi_row_values_map_each_row() {
        assert_eq!(
            infer("INSERT INTO t (id, n) VALUES ($1, $2), ($3, $4)"),
            vec![
                from_col(SqlDataType::String, None),
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
                from_col(SqlDataType::String, None),
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
            ]
        );
    }

    /// No explicit column list: positional against the declared column order,
    /// which is only meaningful when the arity matches it exactly. `t` declares
    /// seven columns, so seven values are required for the mapping to apply —
    /// the two trailing float columns also pin that declared float width rides
    /// along the same positional path as integer width.
    #[test]
    fn insert_without_column_list_maps_against_declared_order() {
        assert_eq!(
            infer("INSERT INTO t VALUES ($1, $2, $3, $4, $5, $6, $7)"),
            vec![
                from_col(SqlDataType::String, None),
                from_col(SqlDataType::Int64, Some(IntWidth::I32)),
                from_col(SqlDataType::Int64, Some(IntWidth::I64)),
                from_col(SqlDataType::String, None),
                from_col(SqlDataType::Bool, None),
                from_float_col(FloatWidth::F32),
                from_float_col(FloatWidth::F64),
            ]
        );
    }

    /// Fewer values than declared columns: the positional mapping the planner
    /// will use is not knowable from the SQL alone, so nothing is typed.
    #[test]
    fn insert_without_column_list_and_mismatched_arity_stays_none() {
        assert_eq!(infer("INSERT INTO t VALUES ($1, $2)"), vec![None, None]);
    }

    /// A collection that declares no columns has no order to be positional
    /// against.
    #[test]
    fn insert_positional_into_schemaless_collection_stays_none() {
        assert_eq!(infer("INSERT INTO loose VALUES ($1, $2)"), vec![None, None]);
    }

    // ---------------------------------------------------------------------------
    // Ambiguity locks — the safety contract
    // ---------------------------------------------------------------------------

    /// A bare column name declared by two relations in scope must never be
    /// resolved to one of them. `shared` is TEXT in `lefty` and BIGINT in
    /// `righty`; picking either would be a coin flip the client pays for.
    #[test]
    fn bare_column_in_two_joined_relations_stays_none() {
        assert_eq!(
            infer(
                "SELECT only_left FROM lefty JOIN righty ON lefty.only_left = righty.only_right \
                 WHERE shared = $1"
            ),
            vec![None]
        );
    }

    /// A column unique across the joined relations is still unambiguous.
    #[test]
    fn bare_column_unique_across_join_resolves() {
        assert_eq!(
            infer(
                "SELECT only_left FROM lefty JOIN righty ON lefty.only_left = righty.only_right \
                 WHERE only_right = $1"
            ),
            vec![from_col(SqlDataType::Bool, None)]
        );
    }

    /// Qualifying the ambiguous name names one relation outright.
    #[test]
    fn qualified_column_resolves_in_a_join() {
        assert_eq!(
            infer(
                "SELECT only_left FROM lefty JOIN righty ON lefty.only_left = righty.only_right \
                 WHERE righty.shared = $1"
            ),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I64))]
        );
    }

    /// An alias is the qualifier once declared.
    #[test]
    fn table_alias_is_the_qualifier() {
        assert_eq!(
            infer("SELECT id FROM t AS x WHERE x.n = $1"),
            vec![from_col(SqlDataType::Int64, Some(IntWidth::I32))]
        );
    }

    #[test]
    fn column_absent_from_the_catalog_stays_none() {
        assert_eq!(infer("SELECT id FROM t WHERE nope = $1"), vec![None]);
    }

    /// An unresolvable relation may declare any column at all, so nothing bare
    /// in its scope may resolve.
    #[test]
    fn unknown_table_stays_none() {
        assert_eq!(infer("SELECT id FROM missing WHERE n = $1"), vec![None]);
        assert_eq!(
            infer("SELECT id FROM t JOIN missing ON t.id = missing.id WHERE n = $1"),
            vec![None]
        );
    }

    /// The other operand must be a *bare* column reference: an arithmetic
    /// expression, a function call or a subquery does not carry the column's
    /// type.
    #[test]
    fn computed_operand_stays_none() {
        assert_eq!(infer("SELECT id FROM t WHERE n + 1 = $1"), vec![None]);
        assert_eq!(infer("SELECT id FROM t WHERE abs(n) = $1"), vec![None]);
    }

    /// Forms outside the resolved set stay unresolved even when a column is
    /// plainly nearby.
    #[test]
    fn unlisted_forms_stay_none() {
        // Projection position.
        assert_eq!(infer("SELECT $1 FROM t"), vec![None]);
        // Function argument.
        assert_eq!(infer("SELECT id FROM t WHERE abs($1) = n"), vec![None]);
    }
}
