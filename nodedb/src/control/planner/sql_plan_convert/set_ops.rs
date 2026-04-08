//! Set operations and miscellaneous plan conversions (UNION, INTERSECT, EXCEPT, CTE, etc.).

use nodedb_sql::types::{SqlPlan, SqlValue};

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
    tenant_id: TenantId,
) -> crate::Result<Vec<PhysicalTask>> {
    let vshard = VShardId::from_collection(collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        plan: PhysicalPlan::Document(DocumentOp::Truncate {
            collection: collection.into(),
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
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    // Execute the source query, then insert results into target.
    let source_tasks = convert_one(source, tenant_id, ctx)?;
    // For now, return source tasks — the routing layer reads results
    // and inserts them into the target collection.
    // TODO: implement proper two-phase insert-select execution.
    let _ = target;
    Ok(source_tasks)
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
