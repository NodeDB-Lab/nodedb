// SPDX-License-Identifier: BUSL-1.1

//! Integration tests: a positional `INSERT`/`UPSERT` `VALUES` clause (no
//! explicit column list) must bind each value to the collection's
//! DECLARED column names, not synthetic `col0`, `col1`, ... placeholders.
//!
//! The defect: `INSERT INTO probe VALUES (42, 'stored?')` on
//! `probe (id INT PRIMARY KEY, note TEXT)` stored the row under `col0`/
//! `col1` instead of `id`/`note`. `SELECT *` still looked correct (it
//! doesn't care about names), but any named projection or `WHERE`
//! predicate on `id`/`note` silently saw nothing — the row was there,
//! just unaddressable under its real column names.
//!
//! All tests use `plan_sql()` with a minimal catalog (mirrors
//! `point_get_operand_order.rs` / `schema_qualified_rejection.rs`).

use nodedb_sql::types::{
    CollectionInfo, ColumnInfo, EngineType, InsertPlan, SqlDataType, UpsertPlan,
};
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlError, SqlPlan, SqlValue, plan_sql};
use nodedb_types::DatabaseId;

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        let info = match name {
            // Strict document collection with a declared 2-column schema —
            // the exact shape from the issue repro.
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
                        name: "note".into(),
                        data_type: SqlDataType::String,
                        nullable: true,
                        is_primary_key: false,
                        default: None,
                        raw_type: Some("TEXT".into()),
                        int_width: None,
                        float_width: None,
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
            // Schemaless collection: no declared column order exists to
            // bind positionally to. The pre-existing `col{i}` fallback is
            // the correct, unchanged behaviour here.
            "loose" => Some(CollectionInfo {
                name: "loose".into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
            }),
            // KV collection: key/value are separated by the KV insert path
            // matching column names against the "key"/"ttl" sentinels, not
            // by declared column order. Pinned so a future refactor doesn't
            // accidentally route KV through the new positional-binding path.
            "kv_probe" => Some(CollectionInfo {
                name: "kv_probe".into(),
                engine: EngineType::KeyValue,
                columns: Vec::new(),
                primary_key: Some("key".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: CollectionInfo::open_schema_for(EngineType::KeyValue),
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

fn plan_one(sql: &str) -> SqlPlan {
    let mut plans = plan_sql(sql, &Catalog).expect("planning must succeed");
    assert_eq!(plans.len(), 1, "expected exactly one plan for: {sql}");
    plans.pop().unwrap()
}

fn insert_rows(plan: SqlPlan) -> Vec<Vec<(String, SqlValue)>> {
    match plan {
        SqlPlan::Insert(InsertPlan { rows, .. }) => rows,
        SqlPlan::Upsert(UpsertPlan { rows, .. }) => rows,
        other => panic!("expected an Insert or Upsert plan, got {other:?}"),
    }
}

/// The core regression: a positional INSERT must bind its
/// values to the DECLARED column names, not `col0`/`col1`.
#[test]
fn positional_insert_binds_declared_column_names() {
    let plan = plan_one("INSERT INTO probe VALUES (42, 'stored?')");
    let rows = insert_rows(plan);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![
            ("id".to_string(), SqlValue::Int(42)),
            ("note".to_string(), SqlValue::String("stored?".into())),
        ],
        "positional INSERT must bind values to the declared column names \
         (`id`, `note`), not synthetic `col0`/`col1`"
    );
}

/// Control: a named insert must keep binding exactly as before — this fix
/// must not perturb the named-column path.
#[test]
fn named_insert_still_binds_by_name() {
    let plan = plan_one("INSERT INTO probe (id, note) VALUES (43, 'named')");
    let rows = insert_rows(plan);
    assert_eq!(
        rows[0],
        vec![
            ("id".to_string(), SqlValue::Int(43)),
            ("note".to_string(), SqlValue::String("named".into())),
        ]
    );
}

/// A VALUES row with MORE values than the collection has declared columns
/// must be rejected outright — inventing a `col2` slot for the overflow
/// would reproduce the exact unaddressable-column bug this fix closes.
#[test]
fn positional_insert_arity_overflow_is_rejected() {
    let err = plan_sql("INSERT INTO probe VALUES (1, 'a', 'extra')", &Catalog)
        .expect_err("row has 3 values but `probe` declares only 2 columns");
    assert!(
        matches!(
            err,
            SqlError::InsertColumnArityMismatch {
                given: 3,
                declared: 2,
                ..
            }
        ),
        "expected InsertColumnArityMismatch {{ given: 3, declared: 2, .. }}, got {err:?}"
    );
}

/// Schemaless collections have no declared column order to bind
/// positionally to — the pre-existing `col{i}` fallback remains the
/// correct, unchanged behaviour there.
#[test]
fn positional_insert_on_schemaless_collection_keeps_col_i_fallback() {
    let plan = plan_one("INSERT INTO loose VALUES (1, 'a')");
    let rows = insert_rows(plan);
    assert_eq!(
        rows[0],
        vec![
            ("col0".to_string(), SqlValue::Int(1)),
            ("col1".to_string(), SqlValue::String("a".into())),
        ],
        "schemaless collections have no declared columns to bind to; the \
         `col{{i}}` fallback must remain the last resort"
    );
}

/// Same defect, UPSERT path: `UPSERT INTO t VALUES (...)` is pre-processed
/// to the same `plan_upsert` planner path and must bind identically.
#[test]
fn positional_upsert_binds_declared_column_names() {
    let plan = plan_one("UPSERT INTO probe VALUES (44, 'upserted')");
    let rows = insert_rows(plan);
    assert_eq!(
        rows[0],
        vec![
            ("id".to_string(), SqlValue::Int(44)),
            ("note".to_string(), SqlValue::String("upserted".into())),
        ]
    );
}

/// KV collections split key/value by matching column *names* against
/// `pk_col`/`"key"`/`"ttl"`, not by declared column order. A positional
/// insert supplies no column list, so there is no key to bind to —
/// `build_kv_insert_plan` would otherwise emit an empty-keyed, empty-valued
/// entry (every such row colliding, all data silently dropped). It is
/// rejected outright, mirroring the arity-overflow decision.
#[test]
fn positional_insert_on_kv_collection_is_rejected() {
    let err = plan_sql("INSERT INTO kv_probe VALUES ('k1', 'v1')", &Catalog)
        .expect_err("positional KV insert has no key/value column names to bind to");
    assert!(
        matches!(err, SqlError::PositionalKvInsertUnsupported { .. }),
        "expected PositionalKvInsertUnsupported, got {err:?}"
    );
}
