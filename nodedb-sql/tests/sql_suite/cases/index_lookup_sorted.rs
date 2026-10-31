// SPDX-License-Identifier: BUSL-1.1

//! An indexed equality with an ORDER BY, or beside an unindexed equality.
//!
//! The index lookup carries the sort keys, so the equality still plans the
//! index and the handler orders the matched rows. A window function is the
//! one shape the index lookup does not evaluate, so it plans a scan.

use nodedb_sql::types::{
    CollectionInfo, DocumentIndexLookupPlan, EngineType, IndexSpec, IndexState, SqlPlan,
};
use nodedb_sql::{SqlCatalog, SqlCatalogError, plan_sql};
use nodedb_types::DatabaseId;

fn column(name: &str, is_primary_key: bool) -> nodedb_sql::types::ColumnInfo {
    nodedb_sql::types::ColumnInfo {
        name: name.into(),
        data_type: nodedb_sql::types::SqlDataType::String,
        nullable: !is_primary_key,
        is_primary_key,
        default: None,
        raw_type: None,
        int_width: None,
        float_width: None,
    }
}

/// A strict `turns` with a ready index on `session_id`.
struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok((name == "turns").then(|| CollectionInfo {
            name: name.into(),
            engine: EngineType::DocumentStrict,
            columns: vec![
                column("id", true),
                column("agent_id", false),
                column("session_id", false),
                column("created_at", false),
            ],
            primary_key: Some("id".into()),
            has_auto_tier: false,
            indexes: vec![IndexSpec {
                name: "turns_session".into(),
                field: "$.session_id".into(),
                unique: false,
                case_insensitive: false,
                state: IndexState::Ready,
                predicate: None,
            }],
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
        }))
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
    assert_eq!(plans.len(), 1, "one plan for {sql}");
    plans.remove(0)
}

/// The index lookup under the wrapper that trims a sort column the SELECT
/// list does not name.
fn lookup_of(plan: SqlPlan) -> Option<DocumentIndexLookupPlan> {
    match plan {
        SqlPlan::DocumentIndexLookup(lookup) => Some(lookup),
        SqlPlan::Subquery { input, .. } => lookup_of(*input),
        _ => None,
    }
}

#[test]
fn order_by_with_limit_and_offset_plans_the_bare_index_lookup() {
    let plan = plan_one(
        "SELECT id FROM turns WHERE session_id = 's1' ORDER BY created_at DESC LIMIT 3 OFFSET 2",
    );
    // No tail above the lookup: a second sort and cut would apply the OFFSET twice.
    let SqlPlan::DocumentIndexLookup(lookup) = plan else {
        panic!("expected the bare index lookup, got {plan:?}");
    };
    assert_eq!(lookup.field, "$.session_id");
    assert_eq!(
        lookup.sort_keys.len(),
        1,
        "the lookup carries the ORDER BY term"
    );
    assert!(!lookup.sort_keys[0].ascending, "DESC survives planning");
    assert_eq!(lookup.limit, Some(3));
    assert_eq!(lookup.offset, 2);
}

#[test]
fn a_residual_filter_stays_on_the_sorted_index_lookup() {
    let plan = plan_one(
        "SELECT id FROM turns WHERE session_id = 's1' AND created_at > 'a' ORDER BY created_at",
    );
    let lookup = lookup_of(plan).expect("an index lookup");
    assert_eq!(
        lookup.filters.len(),
        1,
        "the non-indexed conjunct is the residual"
    );
    assert_eq!(lookup.sort_keys.len(), 1);
}

#[test]
fn a_leading_unindexed_equality_does_not_hide_the_indexed_one() {
    let plan = plan_one(
        "SELECT id FROM turns WHERE agent_id = 'a1' AND session_id = 's1' \
         ORDER BY created_at DESC LIMIT 10",
    );
    let lookup = lookup_of(plan).expect("an index lookup");
    assert_eq!(
        lookup.field, "$.session_id",
        "the indexed column drives the lookup"
    );
    assert_eq!(format!("{:?}", lookup.value), "String(\"s1\")");
    assert_eq!(
        lookup.filters.len(),
        1,
        "the unindexed equality is the residual"
    );
}

#[test]
fn an_equality_with_no_index_on_any_column_plans_a_scan() {
    let plan = plan_one("SELECT id FROM turns WHERE agent_id = 'a1' AND created_at = 'x'");
    assert!(
        lookup_of(plan).is_none(),
        "no ready index covers either column"
    );
}

#[test]
fn a_computed_item_with_an_order_by_plans_a_scan() {
    let plan =
        plan_one("SELECT id, created_at AS ts FROM turns WHERE session_id = 's1' ORDER BY ts DESC");
    assert!(
        lookup_of(plan).is_none(),
        "a sort key can name a value only the computed tail holds"
    );
}

#[test]
fn a_window_function_still_plans_a_scan() {
    let plan = plan_one(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) AS n FROM turns WHERE session_id = 's1'",
    );
    assert!(
        lookup_of(plan).is_none(),
        "the index lookup evaluates no window function"
    );
}
