// SPDX-License-Identifier: Apache-2.0

//! Integration tests: `INSERT ... ON CONFLICT (...) DO UPDATE SET col =
//! <literal>` must range-check the assignment literals against the
//! collection's declared column widths, exactly like the row path does.
//!
//! The non-KV `SqlPlan::Upsert` path coerces `on_conflict_updates` to the
//! declared types but, before this fix, never ran the range check on the
//! coerced values — so an out-of-declared-width literal written only
//! through `ON CONFLICT DO UPDATE` bypassed the width constraint entirely,
//! while the same literal in the inserted row itself was correctly
//! rejected. This is the only reachable surface for that path (it is a
//! private planner function, wired only through `plan_sql`), so it is
//! covered here end-to-end rather than as a planner-internal unit test.
//!
//! The equivalent KV `SqlPlan::KvInsert` path has the same fix, exercised
//! as planner-internal unit tests in
//! `nodedb-sql/src/planner/dml_helpers.rs` (`kv_on_conflict_range_tests`),
//! which call `build_kv_insert_plan` directly — mirroring the existing
//! `float_range_tests` style in that file.

use nodedb_sql::types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType};
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlError, plan_sql};
use nodedb_types::DatabaseId;

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        let info = match name {
            // Strict document collection: declared INT/REAL widths, so the
            // non-KV `ON CONFLICT DO UPDATE` path is exercised.
            "probe" => Some(CollectionInfo {
                name: "probe".into(),
                engine: EngineType::DocumentStrict,
                columns: vec![
                    ColumnInfo {
                        name: "id".into(),
                        data_type: SqlDataType::Int64,
                        nullable: false,
                        is_primary_key: true,
                        default: None,
                        raw_type: Some("INT".into()),
                        int_width: Some(nodedb_types::columnar::IntWidth::I32),
                        float_width: None,
                    },
                    ColumnInfo {
                        name: "n".into(),
                        data_type: SqlDataType::Int64,
                        nullable: true,
                        is_primary_key: false,
                        default: None,
                        raw_type: Some("INT".into()),
                        int_width: Some(nodedb_types::columnar::IntWidth::I32),
                        float_width: None,
                    },
                    ColumnInfo {
                        name: "r".into(),
                        data_type: SqlDataType::Float64,
                        nullable: true,
                        is_primary_key: false,
                        default: None,
                        raw_type: Some("REAL".into()),
                        int_width: None,
                        float_width: Some(nodedb_types::columnar::FloatWidth::F32),
                    },
                ],
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
            }),
            _ => None,
        };
        Ok(info)
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

// ---- non-KV (SqlPlan::Upsert) path ----

#[test]
fn upsert_on_conflict_int_beyond_declared_width_is_rejected() {
    let err = plan_sql(
        "INSERT INTO probe (id, n) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET n = 9876543210",
        &Catalog,
    )
    .expect_err("n is declared INT; 9876543210 overflows i32");
    assert!(
        matches!(err, SqlError::IntegerOutOfRange { .. }),
        "expected IntegerOutOfRange, got {err:?}"
    );
}

#[test]
fn upsert_on_conflict_float_beyond_f32_range_is_rejected() {
    let err = plan_sql(
        "INSERT INTO probe (id, r) VALUES (1, 1.0) ON CONFLICT (id) DO UPDATE SET r = 1e300",
        &Catalog,
    )
    .expect_err("r is declared REAL; 1e300 overflows f32");
    assert!(
        matches!(err, SqlError::FloatOutOfRange { .. }),
        "expected FloatOutOfRange, got {err:?}"
    );
}

#[test]
fn upsert_on_conflict_float_rounding_is_accepted() {
    // Pinned: narrowing to f32 rounds, it is not an error, and a future
    // change must not tighten this into a rejection.
    plan_sql(
        "INSERT INTO probe (id, r) VALUES (1, 1.0) ON CONFLICT (id) DO UPDATE SET r = 1.1",
        &Catalog,
    )
    .expect("rounding into f32 must not be rejected");
}

#[test]
fn upsert_on_conflict_in_range_values_are_accepted() {
    plan_sql(
        "INSERT INTO probe (id, n) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET n = 42",
        &Catalog,
    )
    .expect("in-range i32 literal must be accepted");
}
