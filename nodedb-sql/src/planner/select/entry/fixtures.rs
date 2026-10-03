// SPDX-License-Identifier: Apache-2.0

//! Shared query-planner test catalog and SQL preparation.

use super::query::plan_query;
use crate::functions::registry::FunctionRegistry;
use crate::parser::preprocess::pipeline::preprocess;
use crate::parser::statement::parse_sql;
use crate::types::*;
use sqlparser::ast::Statement;

struct TestCatalog;

impl SqlCatalog for TestCatalog {
    fn get_collection(
        &self,
        _: nodedb_types::DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        let info = match name {
            "products" => Some(CollectionInfo {
                name: "products".into(),
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
            "users" => Some(CollectionInfo {
                name: "users".into(),
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
            "orders" => Some(CollectionInfo {
                name: "orders".into(),
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
            "docs" => Some(CollectionInfo {
                name: "docs".into(),
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
            "tags" => Some(CollectionInfo {
                name: "tags".into(),
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
            "user_prefs" => Some(CollectionInfo {
                name: "user_prefs".into(),
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
            "embeddings" => Some(CollectionInfo {
                name: "embeddings".into(),
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
            "articles" => Some(CollectionInfo {
                name: "articles".into(),
                engine: EngineType::DocumentStrict,
                columns: vec![
                    strict_column("id", SqlDataType::String, "TEXT", true),
                    strict_column("title", SqlDataType::String, "TEXT", false),
                    strict_column("views", SqlDataType::Int64, "INT", false),
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

    fn lookup_array(&self, name: &str) -> Option<crate::types::ArrayCatalogView> {
        if name == "genome" {
            Some(crate::types::ArrayCatalogView {
                name: "genome".into(),
                dims: vec![
                    crate::types_array::ArrayDimAst {
                        name: "chrom".into(),
                        dtype: crate::types_array::ArrayDimType::Int64,
                        lo: crate::types_array::ArrayDomainBound::Int64(1),
                        hi: crate::types_array::ArrayDomainBound::Int64(23),
                    },
                    crate::types_array::ArrayDimAst {
                        name: "pos".into(),
                        dtype: crate::types_array::ArrayDimType::Int64,
                        lo: crate::types_array::ArrayDomainBound::Int64(0),
                        hi: crate::types_array::ArrayDomainBound::Int64(1_000_000),
                    },
                ],
                attrs: vec![crate::types_array::ArrayAttrAst {
                    name: "qual".into(),
                    dtype: crate::types_array::ArrayAttrType::Float64,
                    nullable: true,
                }],
                tile_extents: vec![1, 1_000_000],
            })
        } else {
            None
        }
    }
}

fn strict_column(
    name: &str,
    data_type: SqlDataType,
    raw: &str,
    is_primary_key: bool,
) -> ColumnInfo {
    ColumnInfo {
        name: name.into(),
        data_type,
        nullable: !is_primary_key,
        is_primary_key,
        default: None,
        raw_type: Some(raw.into()),
        int_width: None,
        float_width: None,
    }
}

pub(super) fn plan_select_sql(sql: &str) -> SqlPlan {
    try_plan_select_sql(sql).unwrap()
}

/// Plan `sql` against the test catalog, keeping the planner's error.
pub(super) fn try_plan_select_sql(sql: &str) -> crate::error::Result<SqlPlan> {
    // Run preprocessor so operator rewrites (`<->`, `<=>`, `<#>`) are applied
    // before sqlparser sees the SQL.
    let (preprocessed_sql, temporal) = match preprocess(sql).unwrap() {
        Some(p) => (p.sql, p.temporal),
        None => (sql.to_string(), crate::TemporalScope::default()),
    };
    let statements = parse_sql(&preprocessed_sql).unwrap();
    let Statement::Query(query) = &statements[0] else {
        panic!("expected query statement");
    };
    plan_query(query, &TestCatalog, &FunctionRegistry::new(), temporal)
}
