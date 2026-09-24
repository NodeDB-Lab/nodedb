// SPDX-License-Identifier: BUSL-1.1

//! Native `KvIncr` / `KvIncrFloat` plan builders.

use nodedb_physical::physical_plan::KvOp;
use nodedb_sql::planner::dml_helpers::KvCounterKind;
use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;

use super::kv::assign_kv_surrogate;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::sql_plan_convert::kv_counter_shape::kv_counter_shape;
use crate::control::server::native::dispatch::DispatchCtx;

fn required_key(fields: &TextFields) -> crate::Result<&str> {
    fields
        .key
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'key'".to_string(),
        })
}

pub(crate) fn build_incr(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let key = required_key(fields)?;
    let delta = fields.incr_delta.unwrap_or(1);
    let ttl_ms = fields.ttl_ms.unwrap_or(0);
    let surrogate = assign_kv_surrogate(ctx, collection, key.as_bytes())?;
    // An absent key takes the collection's shape, as a SQL `KV_INCR` does.
    let shape = kv_counter_shape(
        ctx.state,
        ctx.tenant_id(),
        ctx.database_id(),
        collection,
        key,
        KvCounterKind::Integer,
    )?;

    Ok(PhysicalPlan::Kv(KvOp::Incr {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key: key.as_bytes().to_vec(),
        delta,
        ttl_ms,
        surrogate,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        shape,
    }))
}

pub(crate) fn build_incr_float(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let key = required_key(fields)?;
    // The delta stays the client's decimal text, so the engine adds every
    // digit the client sent.
    let delta = fields.incr_float_delta.as_deref().unwrap_or("1");
    if !nodedb_physical::kv_atomic::float_text::is_decimal_number(delta) {
        return Err(crate::Error::BadRequest {
            detail: format!("KvIncrFloat: delta must be a decimal number, got '{delta}'"),
        });
    }
    let surrogate = assign_kv_surrogate(ctx, collection, key.as_bytes())?;
    let shape = kv_counter_shape(
        ctx.state,
        ctx.tenant_id(),
        ctx.database_id(),
        collection,
        key,
        KvCounterKind::Float,
    )?;

    Ok(PhysicalPlan::Kv(KvOp::IncrFloat {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key: key.as_bytes().to_vec(),
        delta: delta.to_string(),
        surrogate,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        shape,
    }))
}
