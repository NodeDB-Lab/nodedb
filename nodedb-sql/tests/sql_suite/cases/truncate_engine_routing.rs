// SPDX-License-Identifier: BUSL-1.1

//! `TRUNCATE <coll>` resolves the collection through the catalog and routes
//! through `EngineRules::plan_truncate`: every table engine lowers to
//! `SqlPlan::Truncate` tagged with its own `EngineType`, a vector-primary
//! collection lowers to `SqlPlan::VectorPrimaryTruncate`, an array refuses
//! with a typed error naming `DROP ARRAY`, and an unknown name is
//! `UnknownTable`.

use nodedb_sql::types::{CollectionInfo, EngineType};
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlError, SqlPlan, plan_sql};
use nodedb_types::DatabaseId;

struct Catalog;

fn info(name: &str, engine: EngineType) -> CollectionInfo {
    CollectionInfo {
        name: name.into(),
        engine,
        columns: Vec::new(),
        primary_key: Some("id".into()),
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        open_schema: CollectionInfo::open_schema_for(engine),
    }
}

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        let info = match name {
            "docs" => Some(info(name, EngineType::DocumentSchemaless)),
            "strict" => Some(info(name, EngineType::DocumentStrict)),
            "kv" => Some(info(name, EngineType::KeyValue)),
            "cols" => Some(info(name, EngineType::Columnar)),
            "ts" => Some(info(name, EngineType::Timeseries)),
            "geo" => Some(info(name, EngineType::Spatial)),
            "vecs" => {
                let mut i = info(name, EngineType::DocumentSchemaless);
                i.primary = nodedb_types::PrimaryEngine::Vector;
                i.vector_primary = Some(nodedb_types::VectorPrimaryConfig {
                    vector_field: "emb".into(),
                    dim: 3,
                    ..nodedb_types::VectorPrimaryConfig::default()
                });
                Some(i)
            }
            _ => None,
        };
        Ok(info)
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, name: &str) -> bool {
        name == "arr"
    }
}

fn plan_one(sql: &str) -> SqlPlan {
    let mut plans = plan_sql(sql, &Catalog).expect("planning must succeed");
    assert_eq!(plans.len(), 1, "expected exactly one plan for: {sql}");
    plans.pop().expect("one plan")
}

#[test]
fn truncate_tags_each_table_engine() {
    for (name, engine) in [
        ("docs", EngineType::DocumentSchemaless),
        ("strict", EngineType::DocumentStrict),
        ("kv", EngineType::KeyValue),
        ("cols", EngineType::Columnar),
        ("ts", EngineType::Timeseries),
        ("geo", EngineType::Spatial),
    ] {
        match plan_one(&format!("TRUNCATE {name}")) {
            SqlPlan::Truncate {
                collection,
                engine: got,
                restart_identity,
            } => {
                assert_eq!(collection, name);
                assert_eq!(got, engine);
                assert!(!restart_identity);
            }
            other => panic!("expected SqlPlan::Truncate for {name}, got {other:?}"),
        }
    }
}

#[test]
fn truncate_restart_identity_is_carried() {
    match plan_one("TRUNCATE kv RESTART IDENTITY") {
        SqlPlan::Truncate {
            restart_identity, ..
        } => assert!(restart_identity),
        other => panic!("expected SqlPlan::Truncate, got {other:?}"),
    }
}

#[test]
fn truncate_vector_primary_lowers_to_its_own_plan() {
    match plan_one("TRUNCATE vecs RESTART IDENTITY") {
        SqlPlan::VectorPrimaryTruncate {
            collection,
            field,
            restart_identity,
        } => {
            assert_eq!(collection, "vecs");
            assert_eq!(field, "emb");
            assert!(restart_identity);
        }
        other => panic!("expected SqlPlan::VectorPrimaryTruncate, got {other:?}"),
    }
}

#[test]
fn truncate_array_is_refused_naming_drop_array() {
    let err = plan_sql("TRUNCATE arr", &Catalog).expect_err("array truncate must refuse");
    match err {
        SqlError::Unsupported { detail } => {
            assert!(detail.contains("DROP ARRAY <name>"), "{detail}");
            assert!(
                detail.contains("DELETE FROM ARRAY <name> WHERE COORDS IN (...)"),
                "{detail}"
            );
        }
        other => panic!("expected SqlError::Unsupported, got {other:?}"),
    }
}

#[test]
fn truncate_unknown_collection_is_unknown_table() {
    let err = plan_sql("TRUNCATE nope", &Catalog).expect_err("unknown table must refuse");
    assert!(
        matches!(err, SqlError::UnknownTable { ref name } if name == "nope"),
        "got {err:?}"
    );
}

#[test]
fn truncate_many_plans_one_per_table() {
    let plans = plan_sql("TRUNCATE docs, kv", &Catalog).expect("planning must succeed");
    let engines: Vec<EngineType> = plans
        .iter()
        .map(|p| match p {
            SqlPlan::Truncate { engine, .. } => *engine,
            other => panic!("expected SqlPlan::Truncate, got {other:?}"),
        })
        .collect();
    assert_eq!(
        engines,
        vec![EngineType::DocumentSchemaless, EngineType::KeyValue]
    );
}
