// SPDX-License-Identifier: BUSL-1.1

//! KV engine plan builders.

use nodedb_types::QualifiedCollection;
use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::native::dispatch::DispatchCtx;
use nodedb_physical::physical_plan::KvOp;

pub(crate) fn build_scan(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let cursor = fields.cursor.clone().unwrap_or_default();
    let count = fields.limit.unwrap_or(100) as usize;
    let filters = fields.filters.clone().unwrap_or_default();
    let match_pattern = fields.match_pattern.clone();

    Ok(PhysicalPlan::Kv(KvOp::Scan {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        cursor,
        count,
        filters,
        match_pattern,
        sort_keys: Vec::new(),
        surrogate_ceiling: None,
        projection: Vec::new(),
        computed_columns: Vec::new(),
    }))
}

pub(crate) fn build_expire(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let key = require_key_bytes(fields)?;
    let ttl_ms = fields.ttl_ms.ok_or_else(|| crate::Error::BadRequest {
        detail: "missing 'ttl_ms'".to_string(),
    })?;

    // Every RLS slot below is left empty here and filled by the injection pass
    // this dispatch path runs before the plan reaches the Data Plane.
    Ok(PhysicalPlan::Kv(KvOp::Expire {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key,
        ttl_ms,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}

pub(crate) fn build_persist(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let key = require_key_bytes(fields)?;

    Ok(PhysicalPlan::Kv(KvOp::Persist {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}

pub(crate) fn build_get_ttl(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let key = require_key_bytes(fields)?;

    Ok(PhysicalPlan::Kv(KvOp::GetTtl {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key,
    }))
}

pub(crate) fn build_batch_get(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let keys = fields
        .keys
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'keys'".to_string(),
        })?
        .clone();
    if keys.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "keys array is empty".to_string(),
        });
    }

    Ok(PhysicalPlan::Kv(KvOp::BatchGet {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        keys,
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_batch_put(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let entries = fields
        .entries
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'entries'".to_string(),
        })?
        .clone();
    if entries.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "entries array is empty".to_string(),
        });
    }
    let ttl_ms = fields.ttl_ms.unwrap_or(0);

    // Assign each entry's stable cross-engine surrogate the SAME way a
    // single-key `Put` does (`assign_kv_surrogate` below): an existing key
    // resolves to its already-bound surrogate, a new key mints a fresh one.
    // Without this every batch-put row would land with `Surrogate::ZERO`,
    // making it invisible to any surrogate-keyed cross-engine read/join.
    let surrogates = entries
        .iter()
        .map(|(key, _value)| assign_kv_surrogate(ctx, collection, key))
        .collect::<crate::Result<Vec<_>>>()?;

    Ok(PhysicalPlan::Kv(KvOp::BatchPut {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        entries,
        ttl_ms,
        surrogates,
        returning: None,
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_field_get(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let key = require_key_bytes(fields)?;
    let field_names = fields
        .fields
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'fields'".to_string(),
        })?
        .clone();

    Ok(PhysicalPlan::Kv(KvOp::FieldGet {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key,
        fields: field_names,
        rls_filters: Vec::new(),
    }))
}

pub(crate) fn build_field_set(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let key = require_key_bytes(fields)?;
    let updates = fields
        .updates
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'updates'".to_string(),
        })?
        .clone();
    let surrogate = assign_kv_surrogate(ctx, collection, &key)?;

    Ok(PhysicalPlan::Kv(KvOp::FieldSet {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key,
        updates,
        surrogate,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}

/// Extract key bytes from `document_id` or `key` field.
fn require_key_bytes(fields: &TextFields) -> crate::Result<Vec<u8>> {
    if let Some(ref doc_id) = fields.document_id {
        return Ok(doc_id.as_bytes().to_vec());
    }
    if let Some(ref key) = fields.key {
        return Ok(key.as_bytes().to_vec());
    }
    Err(crate::Error::BadRequest {
        detail: "missing 'document_id' or 'key'".to_string(),
    })
}

pub(crate) fn build_register_index(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let field = fields
        .field
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'field'".to_string(),
        })?
        .clone();
    let field_position = fields.field_position.unwrap_or(0) as usize;
    let backfill = fields.backfill.unwrap_or(true);

    Ok(PhysicalPlan::Kv(KvOp::RegisterIndex {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        field,
        field_position,
        backfill,
    }))
}

pub(crate) fn build_drop_index(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let field = fields
        .field
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'field'".to_string(),
        })?
        .clone();

    Ok(PhysicalPlan::Kv(KvOp::DropIndex {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        field,
    }))
}

pub(crate) fn build_truncate(
    ctx: &DispatchCtx<'_>,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::Truncate {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
    }))
}

/// Resolve the stable cross-engine surrogate for a KV atomic op, content-
/// addressed on `(collection, key)` — the same binding a normal insert of that
/// key allocated, so an atomic op on an existing key keeps its identity.
fn assign_kv_surrogate(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    key: &[u8],
) -> crate::Result<nodedb_types::Surrogate> {
    ctx.state
        .surrogate_assigner
        .assign(ctx.database_id(), ctx.tenant_id(), collection, key)
}

pub(crate) fn build_incr(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let key = fields
        .key
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'key'".to_string(),
        })?;
    let delta = fields.incr_delta.unwrap_or(1);
    let ttl_ms = fields.ttl_ms.unwrap_or(0);
    let surrogate = assign_kv_surrogate(ctx, collection, key.as_bytes())?;

    Ok(PhysicalPlan::Kv(KvOp::Incr {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key: key.as_bytes().to_vec(),
        delta,
        ttl_ms,
        surrogate,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}

pub(crate) fn build_incr_float(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let key = fields
        .key
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'key'".to_string(),
        })?;
    let delta = fields.incr_float_delta.unwrap_or(1.0);
    let surrogate = assign_kv_surrogate(ctx, collection, key.as_bytes())?;

    Ok(PhysicalPlan::Kv(KvOp::IncrFloat {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key: key.as_bytes().to_vec(),
        delta,
        surrogate,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}

pub(crate) fn build_cas(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let key = fields
        .key
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'key'".to_string(),
        })?;
    let expected = fields.expected.clone().unwrap_or_default();
    let new_value = fields
        .new_value
        .clone()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'new_value'".to_string(),
        })?;
    let surrogate = assign_kv_surrogate(ctx, collection, key.as_bytes())?;

    Ok(PhysicalPlan::Kv(KvOp::Cas {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key: key.as_bytes().to_vec(),
        expected,
        new_value,
        surrogate,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}

pub(crate) fn build_getset(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let key = fields
        .key
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'key'".to_string(),
        })?;
    let new_value = fields
        .new_value
        .clone()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'new_value'".to_string(),
        })?;
    let surrogate = assign_kv_surrogate(ctx, collection, key.as_bytes())?;

    Ok(PhysicalPlan::Kv(KvOp::GetSet {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key: key.as_bytes().to_vec(),
        new_value,
        surrogate,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}

pub(crate) fn build_register_sorted_index(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    fields: &TextFields,
) -> crate::Result<PhysicalPlan> {
    let index_name = fields
        .index_name
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'index_name'".into(),
        })?;
    let sort_columns = fields.sort_columns.clone().unwrap_or_default();
    let key_column = fields.key_column.clone().unwrap_or_default();
    let window_type = fields.window_type.clone().unwrap_or_else(|| "none".into());
    let window_timestamp_column = fields.window_timestamp_column.clone().unwrap_or_default();
    let window_start_ms = fields.window_start_ms.unwrap_or(0);
    let window_end_ms = fields.window_end_ms.unwrap_or(0);

    Ok(PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        index_name: index_name.to_string(),
        sort_columns,
        key_column,
        window_type,
        window_timestamp_column,
        window_start_ms,
        window_end_ms,
    }))
}

pub(crate) fn build_drop_sorted_index(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let index_name = fields
        .index_name
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'index_name'".into(),
        })?;
    Ok(PhysicalPlan::Kv(KvOp::DropSortedIndex {
        index_name: index_name.to_string(),
    }))
}

pub(crate) fn build_sorted_index_rank(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let index_name = fields
        .index_name
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'index_name'".into(),
        })?;
    let key = fields
        .key
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'key'".into(),
        })?;
    Ok(PhysicalPlan::Kv(KvOp::SortedIndexRank {
        index_name: index_name.to_string(),
        primary_key: key.as_bytes().to_vec(),
    }))
}

pub(crate) fn build_sorted_index_top_k(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let index_name = fields
        .index_name
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'index_name'".into(),
        })?;
    let k = fields.top_k_count.unwrap_or(10);
    Ok(PhysicalPlan::Kv(KvOp::SortedIndexTopK {
        index_name: index_name.to_string(),
        k,
    }))
}

pub(crate) fn build_sorted_index_range(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let index_name = fields
        .index_name
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'index_name'".into(),
        })?;
    Ok(PhysicalPlan::Kv(KvOp::SortedIndexRange {
        index_name: index_name.to_string(),
        score_min: fields.score_min.clone(),
        score_max: fields.score_max.clone(),
    }))
}

pub(crate) fn build_sorted_index_count(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let index_name = fields
        .index_name
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'index_name'".into(),
        })?;
    Ok(PhysicalPlan::Kv(KvOp::SortedIndexCount {
        index_name: index_name.to_string(),
    }))
}

pub(crate) fn build_sorted_index_score(fields: &TextFields) -> crate::Result<PhysicalPlan> {
    let index_name = fields
        .index_name
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'index_name'".into(),
        })?;
    let key = fields
        .key
        .as_deref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'key'".into(),
        })?;
    Ok(PhysicalPlan::Kv(KvOp::SortedIndexScore {
        index_name: index_name.to_string(),
        primary_key: key.as_bytes().to_vec(),
    }))
}
