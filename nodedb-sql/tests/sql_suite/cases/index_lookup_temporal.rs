// SPDX-License-Identifier: Apache-2.0

//! An indexed equality under a time scope.
//!
//! The versioned index answers the current state and a system-time cutoff,
//! so `AS OF SYSTEM TIME <ms>` plans the index lookup and carries the scope
//! to the physical read. The index holds no valid time and lists no history,
//! so a valid-time read and `AS OF SYSTEM TIME NULL` plan a scan.

use nodedb_sql::types::{
    CollectionInfo, DocumentIndexLookupPlan, EngineType, IndexSpec, IndexState, SqlPlan,
};
use nodedb_sql::{SqlCatalog, SqlCatalogError, plan_sql};
use nodedb_types::{DatabaseId, SystemTimeScope};

fn text_column(name: &str, is_primary_key: bool) -> nodedb_sql::types::ColumnInfo {
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

/// A bitemporal strict `ledger` with a ready index on `kind`.
struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok((name == "ledger").then(|| CollectionInfo {
            name: name.into(),
            engine: EngineType::DocumentStrict,
            columns: vec![text_column("id", true), text_column("kind", false)],
            primary_key: Some("id".into()),
            has_auto_tier: false,
            indexes: vec![IndexSpec {
                name: "ledger_kind".into(),
                field: "$.kind".into(),
                unique: false,
                case_insensitive: false,
                state: IndexState::Ready,
                predicate: None,
            }],
            bitemporal: true,
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

/// An equi-correlated LATERAL with no LIMIT lowers to a join, and both sides
/// read the statement's valid time, in either clause form.
#[test]
fn both_sides_of_a_lowered_lateral_carry_the_valid_time() {
    for sql in [
        "SELECT l.id FROM ledger l JOIN LATERAL \
         (SELECT i.id FROM ledger i WHERE i.kind = l.kind) x ON true AS OF VALID TIME 5",
        "SELECT l.id FROM ledger l FOR VALID_TIME CONTAINS 5 JOIN LATERAL \
         (SELECT i.id FROM ledger i WHERE i.kind = l.kind) x ON true",
    ] {
        let SqlPlan::Join { left, right, .. } = plan_one(sql) else {
            panic!("expected a join for {sql}");
        };
        for side in [&left, &right] {
            let SqlPlan::Scan { temporal, .. } = side.as_ref() else {
                panic!("expected a scan side for {sql}, got {side:?}");
            };
            assert_eq!(temporal.valid_time, nodedb_sql::ValidTime::At(5), "{sql}");
        }
    }
}

#[test]
fn as_of_indexed_equality_plans_the_index_lookup_with_its_cutoff() {
    let plan = plan_one("SELECT id, kind FROM ledger AS OF SYSTEM TIME 1000 WHERE kind = 'x'");
    match plan {
        SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan {
            field, temporal, ..
        }) => {
            assert_eq!(field, "$.kind");
            assert_eq!(temporal.system_time, SystemTimeScope::AsOf(1000));
        }
        other => panic!("expected an index lookup, got {other:?}"),
    }
}

#[test]
fn current_indexed_equality_plans_the_index_lookup() {
    let plan = plan_one("SELECT id, kind FROM ledger WHERE kind = 'x'");
    assert!(
        matches!(
            plan,
            SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan { ref temporal, .. })
                if temporal.system_time == SystemTimeScope::Current
        ),
        "got {plan:?}"
    );
}

#[test]
fn all_versions_indexed_equality_plans_a_scan() {
    let plan = plan_one("SELECT id, kind FROM ledger AS OF SYSTEM TIME NULL WHERE kind = 'x'");
    assert!(matches!(plan, SqlPlan::Scan { .. }), "got {plan:?}");
}
