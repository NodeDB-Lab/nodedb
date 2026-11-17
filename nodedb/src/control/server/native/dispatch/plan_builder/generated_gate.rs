// SPDX-License-Identifier: BUSL-1.1

//! The refusal of a native write that supplies a generated column.
//!
//! A native put, batch insert or upsert reaches the Data Plane without the SQL
//! planner, so its body is checked here. A column the engine computes on every
//! write cannot take a supplied value: the write is refused with SQLSTATE
//! `428C9`, as a SQL INSERT naming the column is.

use super::super::DispatchCtx;

/// The columns `collection` computes on every write.
fn generated_columns(ctx: &DispatchCtx<'_>, collection: &str) -> crate::Result<Vec<String>> {
    Ok(ctx
        .state
        .credentials
        .catalog()
        .get_collection(
            ctx.database_id(),
            ctx.identity.tenant_id.as_u64(),
            collection,
        )?
        .map(|coll| coll.generated_columns())
        .unwrap_or_default())
}

/// Whether `body`, a MessagePack map or JSON object text, carries `field`.
fn body_has_field(body: &[u8], field: &str) -> bool {
    if crate::data::executor::response_codec::is_native_row(body) {
        return nodedb_types::ObjectView::new(body)
            .ok()
            .is_some_and(|view| view.get(field).ok().flatten().is_some());
    }
    if nodedb_query::msgpack_scan::map_header(body, 0).is_some() {
        return nodedb_query::msgpack_scan::extract_field(body, 0, field).is_some();
    }
    sonic_rs::from_slice::<serde_json::Value>(body)
        .ok()
        .is_some_and(|value| value.get(field).is_some())
}

/// Refuse a body that supplies a generated column of `collection`.
pub(super) fn refuse_generated_body(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    body: &[u8],
) -> crate::Result<()> {
    let generated = generated_columns(ctx, collection)?;
    crate::control::security::catalog::collection_generated::refuse_generated_write(
        collection,
        &generated,
        generated
            .iter()
            .filter(|column| body_has_field(body, column))
            .map(String::as_str),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every body encoding reports the fields it carries.
    #[test]
    fn body_has_field_reads_msgpack_and_json() {
        let json = br#"{"id":"a","tax":1}"#;
        assert!(body_has_field(json, "tax"));
        assert!(!body_has_field(json, "price"));

        let msgpack = nodedb_types::json_to_msgpack(&serde_json::json!({ "id": "a", "tax": 1 }))
            .expect("the test body encodes");
        assert!(body_has_field(&msgpack, "tax"));
        assert!(!body_has_field(&msgpack, "price"));

        let native = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(
            [("tax".to_string(), nodedb_types::Value::Null)].into(),
        ))
        .expect("the test body encodes");
        assert!(body_has_field(&native, "tax"));
        assert!(!body_has_field(&native, "price"));
    }
}
