// SPDX-License-Identifier: BUSL-1.1

//! Columnar engine plan builders.

use nodedb_types::QualifiedCollection;
use nodedb_types::Surrogate;
use nodedb_types::protocol::TextFields;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::native::dispatch::DispatchCtx;
use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp};

pub(crate) async fn build_scan(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let limit = super::requested_limit(fields)?;
    let filters = fields.filters.clone().unwrap_or_default();

    Ok(PhysicalPlan::Columnar(ColumnarOp::Scan {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        projection: Vec::new(),
        limit,
        filters,
        rls_filters: Vec::new(),
        sort_keys: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
        computed_columns: Vec::new(),
    }))
}

pub(crate) async fn build_insert(
    ctx: &DispatchCtx<'_>,
    fields: &TextFields,
    collection: &str,
) -> crate::Result<PhysicalPlan> {
    let payload = fields
        .payload
        .as_ref()
        .or(fields.data.as_ref())
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'payload' or 'data'".to_string(),
        })?
        .clone();
    let format = fields.format.as_deref().unwrap_or("json");

    // Decode the payload at the CP boundary so each row's primary key
    // can be resolved into a stable cross-engine identity before the
    // batch lands on the Data Plane. Row order matches the payload's
    // wire order one-to-one.
    let surrogates = derive_surrogates(ctx, collection, &payload, format).await?;

    // The Data Plane decodes a columnar payload as MessagePack only. A JSON
    // payload converts here, at the API boundary, with its row order kept.
    let (payload, format) = match format {
        "json" => (json_payload_to_msgpack(collection, &payload)?, "msgpack"),
        other => (payload, other),
    };
    let format = format.to_string();

    Ok(PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: QualifiedCollection::new(ctx.database_id(), collection),
        payload,
        format,
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates,
        schema_bytes: Vec::new(),
        provenance: None,
        wal_lsn: None,
        system_from: None,
        rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        // The native insert API takes rows, not a projection; a caller wanting
        // rows back issues SQL, where `inject_returning_spec` fills this slot.
        returning: None,
        rls_filters: Vec::new(),
    }))
}

/// Decode a bulk insert payload (JSON array of objects, or MessagePack
/// array of maps), extract the conventional primary-key column from
/// each row (`id` / `document_id` / `key`), and resolve a per-row
/// surrogate through the async routed exchange. A row with no PK column gets
/// a fresh anonymous surrogate.
async fn derive_surrogates(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    payload: &[u8],
    format: &str,
) -> crate::Result<Vec<Surrogate>> {
    let pks: Vec<Vec<u8>> = match format {
        "json" => extract_pks_json(payload)?,
        "msgpack" => extract_pks_msgpack(payload)?,
        // ILP and other engine-internal formats route to the timeseries
        // ingest path; for unknown formats, decline to enumerate rows
        // and let the engine integration re-derive identity.
        _ => return Ok(Vec::new()),
    };
    // Every keyed row's identity in one batch at the collection's home. A
    // row with no key gets a fresh anonymous surrogate.
    let keyed: Vec<&[u8]> = pks
        .iter()
        .filter(|pk| !pk.is_empty())
        .map(Vec::as_slice)
        .collect();
    let mut bound = super::helpers::assign_surrogates(ctx, collection, &keyed)
        .await?
        .into_iter();
    // Every keyless row draws in one catalog transaction.
    let keyless_count = pks.len() - keyed.len();
    let mut keyless = if keyless_count > 0 {
        ctx.state
            .surrogate_assigner
            .assign_anonymous_many(
                nodedb_types::CollectionKey::from_bare(ctx.database_id(), collection),
                ctx.tenant_id(),
                keyless_count,
            )
            .await?
    } else {
        Vec::new()
    }
    .into_iter();
    let mut out = Vec::with_capacity(pks.len());
    for pk in &pks {
        let next = if pk.is_empty() {
            keyless.next()
        } else {
            bound.next()
        };
        out.push(next.ok_or_else(|| crate::Error::Internal {
            detail: format!("columnar ingest into '{collection}': a row got no surrogate"),
        })?);
    }
    Ok(out)
}

/// The MessagePack form of a JSON columnar payload.
fn json_payload_to_msgpack(collection: &str, bytes: &[u8]) -> crate::Result<Vec<u8>> {
    let value: serde_json::Value =
        crate::util::bounded_json::from_slice(bytes).map_err(|e| crate::Error::Serialization {
            format: "json".into(),
            detail: format!("columnar insert into '{collection}': decode the JSON payload: {e}"),
        })?;
    nodedb_types::json_to_msgpack(&value).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("columnar insert into '{collection}': encode the payload: {e}"),
    })
}

fn extract_pks_json(bytes: &[u8]) -> crate::Result<Vec<Vec<u8>>> {
    let value: sonic_rs::Value =
        crate::util::bounded_json::from_slice(bytes).map_err(|e| crate::Error::Serialization {
            format: "json".into(),
            detail: format!("columnar bulk decode: {e}"),
        })?;
    let arr = value.as_array().ok_or_else(|| crate::Error::BadRequest {
        detail: "columnar bulk insert payload must be a JSON array".into(),
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for row in arr.iter() {
        let pk = match row.as_object() {
            Some(map) => ["id", "document_id", "key"]
                .iter()
                .find_map(|k| map.get(&k))
                .map(json_value_to_pk_bytes)
                .unwrap_or_default(),
            None => Vec::new(),
        };
        out.push(pk);
    }
    Ok(out)
}

fn json_value_to_pk_bytes(v: &sonic_rs::Value) -> Vec<u8> {
    if let Some(s) = v.as_str() {
        s.as_bytes().to_vec()
    } else if let Some(n) = v.as_i64() {
        n.to_string().into_bytes()
    } else if let Some(n) = v.as_u64() {
        n.to_string().into_bytes()
    } else if let Some(n) = v.as_f64() {
        n.to_string().into_bytes()
    } else if let Some(b) = v.as_bool() {
        b.to_string().into_bytes()
    } else {
        Vec::new()
    }
}

fn extract_pks_msgpack(bytes: &[u8]) -> crate::Result<Vec<Vec<u8>>> {
    // Decode as Vec<HashMap<String, nodedb_types::Value>> to recover
    // each row's conventional PK column.
    use std::collections::HashMap;
    let rows: Vec<HashMap<String, nodedb_types::Value>> =
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("columnar bulk decode: {e}"),
        })?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let pk = ["id", "document_id", "key"]
            .iter()
            .find_map(|k| row.get(*k))
            .map(value_to_pk_bytes)
            .unwrap_or_default();
        out.push(pk);
    }
    Ok(out)
}

fn value_to_pk_bytes(v: &nodedb_types::Value) -> Vec<u8> {
    use nodedb_types::Value as V;
    match v {
        V::String(s) | V::Uuid(s) | V::Ulid(s) | V::Regex(s) => s.as_bytes().to_vec(),
        V::Integer(n) => n.to_string().into_bytes(),
        V::Float(f) => f.to_string().into_bytes(),
        V::Bool(b) => b.to_string().into_bytes(),
        V::Bytes(b) => b.clone(),
        V::Null
        | V::Object(_)
        | V::Array(_)
        | V::DateTime(_)
        | V::NaiveDateTime(_)
        | V::Duration(_)
        | V::Decimal(_)
        | V::Geometry(_)
        | V::Set(_)
        | V::Range { .. }
        | V::Record { .. }
        | V::ArrayCell(_) => Vec::new(),
        // Value is #[non_exhaustive]; future variants with no PK representation
        // yield empty bytes (treated as null key by callers).
        _ => Vec::new(),
    }
}
