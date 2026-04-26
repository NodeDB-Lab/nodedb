//! Set operations and miscellaneous plan conversions (UNION, INTERSECT, EXCEPT, CTE, etc.).

use nodedb_sql::types::{Projection, SqlPlan, SqlValue};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::*;
use crate::types::{TenantId, VShardId};

use super::super::physical::{PhysicalTask, PostSetOp};
use super::convert::{ConvertContext, convert_one};
use super::expr::inline_cte;
use super::value::sql_value_to_nodedb_value;

pub(super) fn convert_constant_result(
    columns: &[String],
    values: &[SqlValue],
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut map = std::collections::HashMap::new();
    for (col, val) in columns.iter().zip(values.iter()) {
        map.insert(col.clone(), sql_value_to_nodedb_value(val));
    }
    let row = nodedb_types::Value::Object(map);
    let payload =
        nodedb_types::value_to_msgpack(&row).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("constant result: {e}"),
        })?;
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: VShardId::from_collection(""),
        plan: PhysicalPlan::Meta(MetaOp::RawResponse { payload }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_truncate(
    collection: &str,
    restart_identity: bool,
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Document(DocumentOp::Truncate {
            collection: collection.into(),
            restart_identity,
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_union(
    inputs: &[SqlPlan],
    distinct: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut all_tasks = Vec::new();
    for input in inputs {
        all_tasks.extend(convert_one(input, tenant_id, ctx)?);
    }
    if distinct {
        for task in &mut all_tasks {
            task.post_set_op = PostSetOp::UnionDistinct;
        }
    }
    Ok(all_tasks)
}

pub(super) fn convert_intersect(
    left: &SqlPlan,
    right: &SqlPlan,
    all: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut left_tasks = convert_one(left, tenant_id, ctx)?;
    let mut right_tasks = convert_one(right, tenant_id, ctx)?;
    let op = if all {
        PostSetOp::IntersectAll
    } else {
        PostSetOp::Intersect
    };
    for task in &mut left_tasks {
        task.post_set_op = op;
    }
    for task in &mut right_tasks {
        task.post_set_op = op;
    }
    left_tasks.extend(right_tasks);
    Ok(left_tasks)
}

pub(super) fn convert_except(
    left: &SqlPlan,
    right: &SqlPlan,
    all: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut left_tasks = convert_one(left, tenant_id, ctx)?;
    let mut right_tasks = convert_one(right, tenant_id, ctx)?;
    let op = if all {
        PostSetOp::ExceptAll
    } else {
        PostSetOp::Except
    };
    for task in &mut left_tasks {
        task.post_set_op = op;
    }
    for task in &mut right_tasks {
        task.post_set_op = op;
    }
    left_tasks.extend(right_tasks);
    Ok(left_tasks)
}

pub(super) fn convert_insert_select(
    target: &str,
    source: &SqlPlan,
    tenant_id: TenantId,
    _ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let SqlPlan::Scan {
        collection,
        filters,
        projection,
        sort_keys,
        limit,
        offset,
        distinct,
        window_functions,
        ..
    } = source
    else {
        return Err(crate::Error::PlanError {
            detail: "INSERT ... SELECT currently requires a direct source scan".into(),
        });
    };

    let projection_is_passthrough = projection.is_empty()
        || projection.iter().all(|p| {
            matches!(p, Projection::Star)
                || matches!(p, Projection::QualifiedStar(name) if name == collection)
        });

    if !projection_is_passthrough
        || !sort_keys.is_empty()
        || *offset != 0
        || *distinct
        || !window_functions.is_empty()
    {
        return Err(crate::Error::PlanError {
            detail: "INSERT ... SELECT currently supports only SELECT * with optional WHERE/LIMIT"
                .into(),
        });
    }

    let filter_bytes = super::filter::serialize_filters(filters)?;
    let vshard = VShardId::from_collection(target);

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: target.into(),
            source_collection: collection.clone(),
            source_filters: filter_bytes,
            source_limit: limit.unwrap_or(10_000),
        }),
        post_set_op: PostSetOp::None,
    }])
}

pub(super) fn convert_cte(
    definitions: &[(String, SqlPlan)],
    outer: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    // Inline CTE definitions: replace scans on CTE names with the
    // CTE's actual subquery plan.
    let mut resolved = outer.clone();
    for (name, cte_plan) in definitions {
        resolved = inline_cte(&resolved, name, cte_plan);
    }
    convert_one(&resolved, tenant_id, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types::EngineType;

    #[test]
    fn convert_insert_select_builds_document_op() {
        let source = SqlPlan::Scan {
            collection: "batch_test".into(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: Some(50),
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::TemporalScope::default(),
        };

        let tasks = convert_insert_select(
            "batch_copy",
            &source,
            TenantId::new(1),
            &ConvertContext {
                retention_registry: None,
                array_catalog: None,
                credentials: None,
                wal: None,
                surrogate_assigner: None,
                cluster_enabled: false,
            },
        )
        .expect("convert insert-select");

        assert_eq!(tasks.len(), 1);
        match &tasks[0].plan {
            PhysicalPlan::Document(DocumentOp::InsertSelect {
                target_collection,
                source_collection,
                source_limit,
                ..
            }) => {
                assert_eq!(target_collection, "batch_copy");
                assert_eq!(source_collection, "batch_test");
                assert_eq!(*source_limit, 50);
            }
            other => panic!("expected DocumentOp::InsertSelect, got {other:?}"),
        }
    }

    #[test]
    fn convert_insert_select_allows_star_projection() {
        let source = SqlPlan::Scan {
            collection: "batch_test".into(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection: vec![Projection::Star],
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::TemporalScope::default(),
        };

        let tasks = convert_insert_select(
            "batch_copy",
            &source,
            TenantId::new(1),
            &ConvertContext {
                retention_registry: None,
                array_catalog: None,
                credentials: None,
                wal: None,
                surrogate_assigner: None,
                cluster_enabled: false,
            },
        )
        .expect("convert insert-select with star");

        assert_eq!(tasks.len(), 1);
    }
}
