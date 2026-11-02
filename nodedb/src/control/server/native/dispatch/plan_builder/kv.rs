// SPDX-License-Identifier: BUSL-1.1

//! KV engine plan builders.

use nodedb_types::protocol::TextFields;
use nodedb_types::{QualifiedCollection, SystemTimeScope};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::native::dispatch::DispatchCtx;
use nodedb_physical::physical_plan::KvOp;

pub(crate) async fn build_scan(
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
        projection: Vec::new(),
        computed_columns: Vec::new(),
        match_pattern,
        sort_keys: Vec::new(),
        surrogate_ceiling: None,
        system_time: SystemTimeScope::Current,
    }))
}

pub(crate) async fn build_expire(
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

pub(crate) async fn build_persist(
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

pub(crate) async fn build_get_ttl(
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

pub(crate) async fn build_batch_get(
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
        system_time: SystemTimeScope::Current,
    }))
}

pub(crate) async fn build_batch_put(
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

    // Every entry's stable cross-engine surrogate, in one batch at the
    // collection's home: an existing key resolves to its bound surrogate, a
    // new key mints one. A row left at `Surrogate::ZERO` is invisible to any
    // surrogate-keyed cross-engine read or join.
    let keys: Vec<&[u8]> = entries.iter().map(|(key, _value)| key.as_slice()).collect();
    let surrogates = super::helpers::assign_surrogates(ctx, collection, &keys).await?;

    Ok(PhysicalPlan::Kv(KvOp::BatchPut {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        entries,
        ttl_ms,
        intent: nodedb_physical::physical_plan::KvBatchIntent::Put,
        surrogates,
        returning: None,
        rls_filters: Vec::new(),
    }))
}

pub(crate) async fn build_field_get(
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
        system_time: SystemTimeScope::Current,
    }))
}

pub(crate) async fn build_field_set(
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
    let surrogate = super::helpers::assign_surrogate(ctx, collection, &key).await?;

    Ok(PhysicalPlan::Kv(KvOp::FieldSet {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key,
        updates,
        surrogate,
        // Native `field_set` is the RESP HSET family: an absent key is created.
        if_present: false,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // The native field-set carries no RETURNING clause.
        returning: None,
        rls_filters: Vec::new(),
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

pub(crate) async fn build_truncate(
    ctx: &DispatchCtx<'_>,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Kv(KvOp::Truncate {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        restart_identity: false,
    }))
}

pub(crate) async fn build_cas(
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
    let surrogate = super::helpers::assign_surrogate(ctx, collection, key.as_bytes()).await?;

    Ok(PhysicalPlan::Kv(KvOp::Cas {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key: key.as_bytes().to_vec(),
        expected,
        new_value,
        surrogate,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}

pub(crate) async fn build_getset(
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
    let surrogate = super::helpers::assign_surrogate(ctx, collection, key.as_bytes()).await?;

    Ok(PhysicalPlan::Kv(KvOp::GetSet {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        key: key.as_bytes().to_vec(),
        new_value,
        surrogate,
        rls_filters: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
    }))
}
