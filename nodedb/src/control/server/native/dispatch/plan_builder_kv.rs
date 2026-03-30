//! KV engine plan builders for native protocol opcodes.

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::KvOp;

pub(crate) fn build_kv_scan(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let cursor = fields.cursor.clone().unwrap_or_default();
    let count = fields.limit.unwrap_or(100) as usize;
    let filters = fields.filters.clone().unwrap_or_default();
    let match_pattern = fields.match_pattern.clone();

    Ok(PhysicalPlan::Kv(KvOp::Scan {
        collection: collection.to_string(),
        cursor,
        count,
        filters,
        match_pattern,
    }))
}

pub(crate) fn build_kv_expire(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let key = require_key_bytes(fields)?;
    let ttl_ms = fields.ttl_ms.ok_or_else(|| crate::Error::BadRequest {
        detail: "missing 'ttl_ms'".to_string(),
    })?;

    Ok(PhysicalPlan::Kv(KvOp::Expire {
        collection: collection.to_string(),
        key,
        ttl_ms,
    }))
}

pub(crate) fn build_kv_persist(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let key = require_key_bytes(fields)?;

    Ok(PhysicalPlan::Kv(KvOp::Persist {
        collection: collection.to_string(),
        key,
    }))
}

pub(crate) fn build_kv_get_ttl(
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let key = require_key_bytes(fields)?;

    Ok(PhysicalPlan::Kv(KvOp::GetTtl {
        collection: collection.to_string(),
        key,
    }))
}

pub(crate) fn build_kv_batch_get(
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
        collection: collection.to_string(),
        keys,
    }))
}

pub(crate) fn build_kv_batch_put(
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

    Ok(PhysicalPlan::Kv(KvOp::BatchPut {
        collection: collection.to_string(),
        entries,
        ttl_ms,
    }))
}

pub(crate) fn build_kv_field_get(
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
        collection: collection.to_string(),
        key,
        fields: field_names,
    }))
}

pub(crate) fn build_kv_field_set(
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

    Ok(PhysicalPlan::Kv(KvOp::FieldSet {
        collection: collection.to_string(),
        key,
        updates,
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
