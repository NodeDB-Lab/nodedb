//! Columnar engine plan builders.

use nodedb_types::Surrogate;
use nodedb_types::protocol::TextFields;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::{ColumnarInsertIntent, ColumnarOp};
use crate::control::server::native::dispatch::DispatchCtx;

pub(crate) fn build_scan(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let limit = fields.limit.unwrap_or(10_000) as usize;
    let filters = fields.filters.clone().unwrap_or_default();

    Ok(PhysicalPlan::Columnar(ColumnarOp::Scan {
        collection: collection.to_string(),
        projection: Vec::new(),
        limit,
        filters,
        rls_filters: Vec::new(),
        sort_keys: Vec::new(),
        system_as_of_ms: None,
        valid_at_ms: None,
    }))
}

pub(crate) fn build_insert(
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
    let format = fields.format.as_deref().unwrap_or("json").to_string();

    // Decode the payload at the CP boundary so each row's primary key
    // can be resolved into a stable cross-engine identity before the
    // batch lands on the Data Plane. Row order matches the payload's
    // wire order one-to-one.
    let surrogates = derive_surrogates(ctx, collection, &payload, &format)?;

    Ok(PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: collection.to_string(),
        payload,
        format,
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates,
    }))
}

/// Decode a bulk insert payload (JSON array of objects, or MessagePack
/// array of maps), extract the conventional primary-key column from
/// each row (`id` / `document_id` / `key`), and resolve a per-row
/// surrogate via the assigner. Rows with no PK column receive
/// `Surrogate::ZERO` (matching the SQL VALUES path's empty-PK fallback).
fn derive_surrogates(
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
    let assigner = &ctx.state.surrogate_assigner;
    let mut out = Vec::with_capacity(pks.len());
    for pk in pks {
        if pk.is_empty() {
            out.push(Surrogate::ZERO);
        } else {
            out.push(assigner.assign(collection, &pk)?);
        }
    }
    Ok(out)
}

fn extract_pks_json(bytes: &[u8]) -> crate::Result<Vec<Vec<u8>>> {
    let value: sonic_rs::Value =
        sonic_rs::from_slice(bytes).map_err(|e| crate::Error::Serialization {
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
        | V::Duration(_)
        | V::Decimal(_)
        | V::Geometry(_)
        | V::Set(_)
        | V::Range { .. }
        | V::Record { .. }
        | V::NdArrayCell(_) => Vec::new(),
    }
}
