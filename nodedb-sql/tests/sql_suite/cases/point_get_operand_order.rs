// SPDX-License-Identifier: Apache-2.0

//! Primary-key equality must be rewritten to a point-lookup regardless of
//! operand order.
//!
//! `WHERE id = 'x'` and `WHERE 'x' = id` are logically identical. The
//! point-get optimizer must produce the same `SqlPlan::PointGet` for both.
//! If it rewrites only the column-first spelling and leaves the literal-first
//! spelling as a `Scan`, the same predicate is answered by two different
//! physical operators — an index point-lookup vs a sequential scan — which
//! disagree whenever the primary-key index and the stored tuples diverge
//! (the operand-direction asymmetry the ghost-tuple report observed:
//! `id = 'x'` returned no row while `'x' = id` returned one).

use nodedb_sql::types::{CollectionInfo, EngineType};
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlPlan, plan_sql};
use nodedb_types::DatabaseId;

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        let info = match name {
            "articles" => Some(CollectionInfo {
                name: "articles".into(),
                engine: EngineType::DocumentStrict,
                columns: vec![nodedb_sql::types::ColumnInfo {
                    name: "id".into(),
                    data_type: nodedb_sql::types::SqlDataType::String,
                    nullable: false,
                    is_primary_key: true,
                    default: None,
                    raw_type: None,
                    int_width: None,
                    float_width: None,
                }],
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

fn plan_one(sql: &str) -> SqlPlan {
    let mut plans = plan_sql(sql, &Catalog).expect("planning must succeed");
    assert_eq!(plans.len(), 1, "expected exactly one plan for: {sql}");
    plans.pop().unwrap()
}

/// Control: column-first PK equality is rewritten to a point-lookup.
#[test]
fn column_first_pk_equality_is_point_get() {
    let plan = plan_one("SELECT id FROM articles WHERE id = 'x'");
    assert!(
        matches!(plan, SqlPlan::PointGet { .. }),
        "`id = 'x'` on the primary key must plan as a point-lookup, got {plan:?}"
    );
}

/// The defect: literal-first PK equality must ALSO be rewritten to a
/// point-lookup. `'x' = id` is logically identical to `id = 'x'`; leaving it a
/// `Scan` routes the same predicate to a different physical operator.
#[test]
fn literal_first_pk_equality_is_point_get() {
    let plan = plan_one("SELECT id FROM articles WHERE 'x' = id");
    assert!(
        matches!(plan, SqlPlan::PointGet { .. }),
        "`'x' = id` on the primary key must plan as the same point-lookup as \
         `id = 'x'`, but operand order left it as {plan:?}"
    );
}
