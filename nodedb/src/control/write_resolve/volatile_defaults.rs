// SPDX-License-Identifier: BUSL-1.1

//! Bind volatile strict-column DEFAULTs on the proposer, before the write
//! reaches a log.
//!
//! A strict collection's DEFAULT fills a column a write body leaves unset.
//! Every replica and every WAL replay applies the record's body, so a
//! volatile DEFAULT (`now()`, `uuid_v7()`, `gen_random_uuid()`, `nextval`)
//! evaluated during apply yields a different value on each apply. The SQL
//! `VALUES` planner binds them per row already. This pass binds them for
//! every other document write shape: `INSERT ... SELECT` pages, `MERGE`
//! inserts, native and HTTP puts, and staged writes. The bound value then
//! travels in the body, and the apply-time fill finds the column present.
//!
//! A deterministic DEFAULT stays for the Data Plane to fill: every apply
//! computes the same value.

use nodedb_physical::physical_plan::DocumentOp;
use nodedb_sql::planner::defaults::CompiledDefault;
use nodedb_types::Value;
use nodedb_types::columnar::{DocumentMode, StrictSchema};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::catalog_adapter::OriginCatalog;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// `plan` with every volatile DEFAULT its bodies leave unset bound to one
/// value, or `None` when nothing needed binding.
///
/// Covers `PointPut`, `PointInsert`, `Upsert`, `BatchInsert` and
/// `BatchUpsert`. An upsert body takes the DEFAULT exactly as the SQL
/// `UPSERT ... VALUES` planner binds it. A body that does not decode is left
/// unchanged: the apply refuses it as it always has.
pub(crate) fn bind_volatile_defaults(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<Option<PhysicalPlan>> {
    let PhysicalPlan::Document(op) = plan else {
        return Ok(None);
    };
    Ok(bind_document_op(state, tenant_id, database_id, op)?.map(PhysicalPlan::Document))
}

/// [`bind_volatile_defaults`] for one document op.
pub(super) fn bind_document_op(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    op: &DocumentOp,
) -> crate::Result<Option<DocumentOp>> {
    let Some(collection) = written_collection(op) else {
        return Ok(None);
    };
    let name = nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?
        .name()
        .to_owned();
    let Some(stored) =
        state
            .credentials
            .catalog()
            .get_collection(database_id, tenant_id.as_u64(), &name)?
    else {
        return Ok(None);
    };
    let Some(DocumentMode::Strict(schema)) = stored.collection_type.document_mode() else {
        return Ok(None);
    };
    let defaults = compile_volatile_defaults(schema)?;
    if defaults.is_empty() {
        return Ok(None);
    }
    let catalog = OriginCatalog::new(
        std::sync::Arc::clone(&state.credentials),
        state.array_catalog.clone(),
        tenant_id.as_u64(),
        database_id,
        None,
    )
    .with_sequence_registry(std::sync::Arc::clone(&state.sequence_registry));
    let mut bound = op.clone();
    let mut changed = false;
    for body in bodies_mut(&mut bound) {
        changed |= bind_body(body, &defaults, &catalog)?;
    }
    Ok(changed.then_some(bound))
}

/// The volatile DEFAULTs of `schema`, compiled once. The reserved bitemporal
/// columns take no DEFAULT, as the apply-time fill skips them too.
pub(crate) fn compile_volatile_defaults(
    schema: &StrictSchema,
) -> crate::Result<Vec<CompiledDefault>> {
    let mut defaults = Vec::new();
    for column in &schema.columns {
        let Some(text) = column.default.as_deref() else {
            continue;
        };
        if nodedb_types::columnar::schema::is_reserved_bitemporal_column(&column.name) {
            continue;
        }
        let compiled = CompiledDefault::compile(&column.name, text).map_err(|error| {
            crate::Error::PlanError {
                detail: format!(
                    "column '{}' DEFAULT '{text}' cannot compile: {error}",
                    column.name
                ),
            }
        })?;
        if compiled.is_volatile() {
            defaults.push(compiled);
        }
    }
    Ok(defaults)
}

/// Bind each DEFAULT whose column `body` leaves unset. Returns whether the
/// body changed. A body keeps its encoding: a native object stays native, and
/// a plain map or JSON body becomes a plain map.
pub(crate) fn bind_body(
    body: &mut Vec<u8>,
    defaults: &[CompiledDefault],
    catalog: &dyn nodedb_sql::catalog::SqlCatalog,
) -> crate::Result<bool> {
    let Ok(Value::Object(mut fields)) =
        crate::data::executor::response_codec::decode_row_body(body)
    else {
        return Ok(false);
    };
    let mut changed = false;
    for default in defaults {
        if fields.contains_key(default.column()) {
            continue;
        }
        let value = default
            .evaluate(catalog)
            .map_err(|error| crate::Error::PlanError {
                detail: format!(
                    "column '{}' DEFAULT cannot evaluate: {error}",
                    default.column()
                ),
            })?;
        fields.insert(default.column().to_owned(), value);
        changed = true;
    }
    if !changed {
        return Ok(false);
    }
    let native = crate::data::executor::response_codec::is_native_row(body);
    let row = Value::Object(fields);
    *body = if native {
        zerompk::to_msgpack_vec(&row).map_err(|error| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("bound DEFAULT row cannot encode: {error}"),
        })?
    } else {
        nodedb_types::value_to_msgpack(&row).map_err(|error| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("bound DEFAULT row cannot encode: {error}"),
        })?
    };
    Ok(true)
}

/// The collection a whole-body document write lands its body in, `None` for
/// every other op. Exhaustive so a new op decides here.
fn written_collection(op: &DocumentOp) -> Option<&str> {
    match op {
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::BatchUpsert { collection, .. } => Some(collection.as_str()),
        DocumentOp::PointGet { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => None,
    }
}

/// Every body `op` writes. Empty for an op [`written_collection`] skips.
fn bodies_mut(op: &mut DocumentOp) -> Vec<&mut Vec<u8>> {
    match op {
        DocumentOp::PointPut { value, .. }
        | DocumentOp::PointInsert { value, .. }
        | DocumentOp::Upsert { value, .. } => vec![value],
        DocumentOp::BatchInsert { documents, .. } | DocumentOp::BatchUpsert { documents, .. } => {
            documents.iter_mut().map(|(_, body)| body).collect()
        }
        DocumentOp::PointGet { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::ApplyBalanceDelta { .. } => Vec::new(),
    }
}
