// SPDX-License-Identifier: BUSL-1.1

//! Shape-preserving redaction hooks, one per wire shape a client-facing path
//! can deliver.
//!
//! Every hook here rewrites a delivered result IN PLACE and leaves its wire
//! shape byte-for-byte intact — only a ruled cell's value changes. They all
//! route the actual matching through [`RedactionStore::apply_flat_row`], so
//! the mask / hash / null semantics stay defined exactly once
//! (`security::redaction::apply`), no matter which transport delivered the row.
//!
//! These exist because several client-facing paths never reach the
//! named-projection shaping core: they decode a Data-Plane payload and write
//! the answer straight to the client. The shapes those payloads arrive in are:
//!
//! - a scan envelope or flat row map — [`redact_envelope_row`]
//! - a positional `RETURNING` payload — [`redact_rows_payload`]
//! - stored bytes handed back verbatim — [`redact_stored_value_bytes`]
//! - one stored document row's MessagePack bytes — [`redact_document_row_bytes`]
//!
//! [`redact_decoded_value`] dispatches over the first two for callers holding
//! a decoded payload of unknown shape.

use crate::control::security::redaction::RedactionStore;
use crate::control::server::response_shape::project::is_scan_wrapper_json;

use super::query::QueryRedaction;

/// Redact one raw scan-envelope row in place, leaving its wire shape intact.
///
/// The document scan's `{id, data}` wrapper is unwrapped first so the rules,
/// which name stored fields, match the fields the row actually carries. This
/// is the shared hook for every client-facing path whose rows never reach the
/// named-projection shaping core — the pgwire single-column streamed-text
/// shape and the WS-RPC orchestrated `InsertSelect`/`Merge`/`UpdateFromJoin`
/// RETURNING results both ship whatever the payload decodes to, envelope
/// wrapper included, so redaction has to be applied at this level instead of
/// inside `shape_decoded_rows`. The Redis-wire field-get and scan-entry
/// payloads are flat field-keyed maps, which take the non-wrapper branch.
pub fn redact_envelope_row(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    item: &mut serde_json::Value,
) {
    let Some(resolved) = redaction else {
        return;
    };
    let ctx = resolved.ctx(store);
    let Some(map) = item.as_object_mut() else {
        return;
    };
    let target = if is_scan_wrapper_json(map) {
        map.get_mut("data")
            .and_then(serde_json::Value::as_object_mut)
    } else {
        Some(map)
    };
    if let Some(fields) = target {
        ctx.store
            .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, fields);
    }
}

/// Redact a decoded result value in place, whichever of the shapes it decoded
/// into.
///
/// WS-RPC's orchestrated `InsertSelect`/`Merge`/`UpdateFromJoin` statements,
/// its generic dispatch path, and `COPY ... TO`'s export scan all turn
/// `decode_payload_to_json` output straight into a `serde_json::Value` and
/// hand it to the client, never routing through the named-projection shaping
/// core — see [`redact_envelope_row`]'s doc comment for why that hook exists
/// at this level instead. Three shapes reach here:
///
/// - A JSON array of rows (a plain multi-row SELECT/scan result, whose
///   elements are `{id, data}` document envelopes or flat column maps) — each
///   element is redacted via [`redact_envelope_row`].
/// - A `RowsPayload` DML-`RETURNING` object (`{"columns": [...], "rows":
///   [[cell, ...], ...]}`) — cells are positional, keyed by the sibling
///   `columns` list rather than carried inline per row, so
///   `redact_envelope_row`'s field-keyed matching cannot reach them; these go
///   through [`redact_rows_payload`] instead.
/// - Anything else (a scalar count object like `{"inserted": N}`, or a single
///   scan-envelope row) — redacted via [`redact_envelope_row`], a no-op when
///   there is nothing shaped like a stored field to match.
pub fn redact_decoded_value(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    value: &mut serde_json::Value,
) {
    if redaction.is_none() {
        return;
    }
    if let serde_json::Value::Array(items) = value {
        for item in items {
            redact_envelope_row(redaction, store, item);
        }
        return;
    }
    if is_rows_payload_shape(value) {
        redact_rows_payload(redaction, store, value);
        return;
    }
    redact_envelope_row(redaction, store, value);
}

/// True when `value` has the `RowsPayload` DML-`RETURNING` shape: a
/// `"columns"` array of strings alongside a `"rows"` array of arrays.
///
/// This is a structural check, not a plan-driven one — the WS-RPC sites that
/// call [`redact_decoded_value`] cover every `DocumentOp` variant that can
/// carry a `ReturningSpec` (point/bulk update, point/bulk delete, the join
/// orchestrators), so matching on the decoded shape instead of enumerating
/// those variants keeps this one check correct as new `RETURNING`-capable ops
/// are added.
fn is_rows_payload_shape(value: &serde_json::Value) -> bool {
    let Some(obj) = value.as_object() else {
        return false;
    };
    let Some(columns) = obj.get("columns").and_then(serde_json::Value::as_array) else {
        return false;
    };
    if columns.is_empty() || !columns.iter().all(serde_json::Value::is_string) {
        return false;
    }
    let Some(rows) = obj.get("rows").and_then(serde_json::Value::as_array) else {
        return false;
    };
    rows.iter().all(serde_json::Value::is_array)
}

/// Redact one decoded `RowsPayload` RETURNING response in place.
///
/// Each row is positional (`rows[i][j]` is the value of `columns[j]`), so it
/// is round-tripped through [`RedactionStore::apply_flat_row`]'s name-keyed
/// matching by zipping it into a scratch map keyed by `columns`, then the
/// (possibly rewritten) cells are written back at their original positions —
/// the `{"columns": ..., "rows": ...}` wire shape itself never changes.
pub fn redact_rows_payload(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    item: &mut serde_json::Value,
) {
    let Some(resolved) = redaction else {
        return;
    };
    let ctx = resolved.ctx(store);
    let Some(obj) = item.as_object_mut() else {
        return;
    };
    let columns: Vec<String> = match obj.get("columns").and_then(serde_json::Value::as_array) {
        Some(cols) => cols
            .iter()
            .filter_map(|c| c.as_str().map(str::to_string))
            .collect(),
        None => return,
    };
    if columns.is_empty() {
        return;
    }
    let Some(rows) = obj
        .get_mut("rows")
        .and_then(serde_json::Value::as_array_mut)
    else {
        return;
    };
    // One scratch map for the whole payload, cleared per row: `apply_flat_row`
    // is name-keyed while the wire rows are positional, so each row has to be
    // zipped into a map and written back. Allocating that map per row would put
    // an allocation on every RETURNING row of every request.
    let mut scratch: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    for row in rows {
        let Some(cells) = row.as_array_mut() else {
            continue;
        };
        if cells.len() != columns.len() {
            continue;
        }
        scratch.clear();
        for (col, cell) in columns.iter().zip(cells.iter()) {
            scratch.insert(col.clone(), cell.clone());
        }
        ctx.store
            .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, &mut scratch);
        for (cell, col) in cells.iter_mut().zip(columns.iter()) {
            if let Some(v) = scratch.get(col) {
                *cell = v.clone();
            }
        }
    }
}

/// Redact one KV-stored value in place, preserving its storage encoding.
///
/// The Redis-wire surface hands the client the stored bytes themselves rather
/// than a decoded row map, so neither [`redact_envelope_row`] nor
/// [`redact_rows_payload`] can reach the fields inside them. The KV engine
/// stores exactly the two shapes its scan handler decodes, and both are
/// matched here the way that handler presents them to the SELECT path:
///
/// - A msgpack map of typed columns — its keys ARE the stored field names, so
///   the rules match them directly.
/// - Opaque bytes from a single-value put — the scan handler wraps these as
///   `{value: <bytes>}` before any SQL-side read sees them, so the rule that
///   covers them is the one naming the column `value`.
///
/// The bytes are rewritten only when a rule actually changed something: a read
/// under no policy hands back the identical bytes it was given, never a
/// re-encoded round trip. A value that a rule DOES cover but that cannot be
/// re-encoded is cleared rather than delivered — an unreadable result is a
/// recoverable client error, an unredacted one is a policy bypass.
pub fn redact_stored_value_bytes(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    value: &mut Vec<u8>,
) {
    let Some(resolved) = redaction else {
        return;
    };
    if value.is_empty() || !resolved.has_any_rule(store) {
        return;
    }
    let ctx = resolved.ctx(store);

    // Same discriminator the Data-Plane KV scan handler uses to tell the two
    // storage shapes apart, so a value is matched under the same column names
    // on both paths.
    if nodedb_query::msgpack_scan::map_header(value, 0).is_some() {
        let Ok(serde_json::Value::Object(fields)) = nodedb_types::json_from_msgpack(value) else {
            value.clear();
            return;
        };
        let mut redacted = fields.clone();
        ctx.store
            .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, &mut redacted);
        if redacted == fields {
            return;
        }
        match nodedb_types::json_to_msgpack(&serde_json::Value::Object(redacted)) {
            Ok(bytes) => *value = bytes,
            Err(_) => value.clear(),
        }
        return;
    }

    let original = String::from_utf8_lossy(value).into_owned();
    let mut row = serde_json::Map::with_capacity(1);
    row.insert(
        "value".to_string(),
        serde_json::Value::String(original.clone()),
    );
    ctx.store
        .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, &mut row);
    match row.get("value") {
        // A `Null` rule leaves no value to deliver, so the read presents as a
        // miss rather than as the literal text `null`.
        Some(serde_json::Value::Null) => value.clear(),
        Some(serde_json::Value::String(redacted)) => {
            if redacted.as_str() != original {
                *value = redacted.as_bytes().to_vec();
            }
        }
        Some(other) => *value = other.to_string().into_bytes(),
        None => {}
    }
}

/// Redact one stored document row's MessagePack bytes in place, reporting
/// whether the result is safe to deliver.
///
/// The device-sync surfaces hand the client the stored row bytes themselves:
/// a shape snapshot is a msgpack array of `{id, data}` envelopes whose `data`
/// value is the storage map verbatim, and a CRDT row push carries the same map
/// as its payload. Neither reaches a decoded row map, so
/// [`redact_envelope_row`] cannot see the fields, and
/// [`redact_stored_value_bytes`] does not fit either — its non-map branch is
/// the KV single-value form, and its failure mode is to clear the value, which
/// in a snapshot envelope would leave a `data` key with no value at all and
/// corrupt the frame.
///
/// Returns `false` when a rule covers this row's collection but the bytes
/// could not be read or rewritten. The caller must then deliver nothing —
/// these are the bytes that would have gone out unredacted, and on a sync
/// surface they would be persisted on the device rather than merely displayed.
/// Returns `true` in every other case, including when no rule applies at all,
/// and the bytes are then left byte-identical rather than round-tripped.
pub fn redact_document_row_bytes(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    row: &mut Vec<u8>,
) -> bool {
    let Some(resolved) = redaction else {
        return true;
    };
    // An empty row is a delete tombstone on the row-push path; there is
    // nothing stored in it to redact.
    if row.is_empty() || !resolved.has_any_rule(store) {
        return true;
    }
    let ctx = resolved.ctx(store);

    let Ok(serde_json::Value::Object(fields)) = nodedb_types::json_from_msgpack(row) else {
        return false;
    };
    let mut redacted = fields.clone();
    ctx.store
        .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, &mut redacted);
    if redacted == fields {
        return true;
    }
    match nodedb_types::json_to_msgpack(&serde_json::Value::Object(redacted)) {
        Ok(bytes) => {
            *row = bytes;
            true
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};
    use nodedb_types::TenantId;

    use super::*;

    fn store_with_mask(collection: &str, role: &str, field: &str, mask: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: 1,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask(mask.into()),
            }],
        });
        store
    }

    fn redaction_for(collection: &str, role: &str) -> QueryRedaction {
        QueryRedaction::new(
            TenantId::new(1),
            vec![role.to_string()],
            vec![(String::new(), collection.to_string())],
        )
    }

    /// `UPDATE ... FROM <source> RETURNING <col>` (autocommit, orchestrated
    /// via `update_from_join_orchestrator`) encodes its response as exactly
    /// this `{"columns": [...], "rows": [[...]]}` shape — see
    /// `data::executor::handlers::returning_rows::build_rows_payload`. Before
    /// wiring `redact_decoded_value` into the WS-RPC dispatch loop, this shape
    /// shipped over WS-RPC untouched: `redact_envelope_row` alone cannot reach
    /// it, since its cells are positional rather than name-keyed. This is the
    /// regression guard for that leak.
    #[test]
    fn redact_rows_payload_masks_the_ruled_column_by_position() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = serde_json::json!({
            "columns": ["id", "email"],
            "rows": [["1", "a@b.c"], ["2", "d@e.f"]],
        });

        redact_rows_payload(Some(&redaction), &store, &mut value);

        assert_eq!(value["rows"][0][0], "1");
        assert_eq!(value["rows"][0][1], "***");
        assert_eq!(value["rows"][1][0], "2");
        assert_eq!(value["rows"][1][1], "***");
        // The wire shape itself — column list, row count, cell positions —
        // must be untouched, only the ruled cell's value.
        assert_eq!(value["columns"], serde_json::json!(["id", "email"]));
    }

    /// A role with no matching policy must see the RETURNING cells in the
    /// clear — the fix must not over-redact.
    #[test]
    fn redact_rows_payload_leaves_unruled_role_untouched() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "analyst");
        let mut value = serde_json::json!({
            "columns": ["id", "email"],
            "rows": [["1", "a@b.c"]],
        });

        redact_rows_payload(Some(&redaction), &store, &mut value);

        assert_eq!(value["rows"][0][1], "a@b.c");
    }

    /// The dispatcher `redact_decoded_value` — the entry point wired into
    /// every WS-RPC result-decode site and into the `COPY ... TO` export —
    /// must route the `RowsPayload` shape to `redact_rows_payload` rather than
    /// treating it as a plain object (which would be a silent no-op, since it
    /// has no `email` key to match).
    #[test]
    fn redact_decoded_value_routes_rows_payload_shape_correctly() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = serde_json::json!({
            "columns": ["id", "email"],
            "rows": [["1", "a@b.c"]],
        });

        redact_decoded_value(Some(&redaction), &store, &mut value);

        assert_eq!(value["rows"][0][1], "***");
    }

    /// A plain multi-row scan array — the shape a generic (non-RETURNING)
    /// WS-RPC dispatch or a `COPY ... TO` document export decodes into — must
    /// still be redacted per-element via `redact_envelope_row`, unwrapping
    /// each element's `{id, data}` wrapper.
    #[test]
    fn redact_decoded_value_routes_array_of_envelope_rows_correctly() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = serde_json::json!([
            {"id": "1", "data": {"email": "a@b.c"}},
            {"id": "2", "data": {"email": "d@e.f"}},
        ]);

        redact_decoded_value(Some(&redaction), &store, &mut value);

        assert_eq!(value[0]["data"]["email"], "***");
        assert_eq!(value[1]["data"]["email"], "***");
    }

    /// A KV or columnar scan decodes into flat column maps instead of
    /// envelopes; the same dispatcher must reach those without a wrapper to
    /// unwrap.
    #[test]
    fn redact_decoded_value_routes_array_of_flat_rows_correctly() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = serde_json::json!([{"key": "k1", "email": "a@b.c"}]);

        redact_decoded_value(Some(&redaction), &store, &mut value);

        assert_eq!(value[0]["email"], "***");
        assert_eq!(value[0]["key"], "k1");
    }

    /// A scalar command-tag object (`{"affected": N}` / `{"inserted": N}`,
    /// the shape non-RETURNING orchestrated statements return) must survive
    /// `redact_decoded_value` unchanged — it has no ruled field to match.
    #[test]
    fn redact_decoded_value_leaves_scalar_count_object_untouched() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = serde_json::json!({ "affected": 3 });

        redact_decoded_value(Some(&redaction), &store, &mut value);

        assert_eq!(value, serde_json::json!({ "affected": 3 }));
    }

    /// `None` redaction (no policy could possibly apply) must be a hard
    /// no-op, never a panic, across every shape.
    #[test]
    fn redact_decoded_value_is_a_no_op_without_a_resolved_redaction() {
        let store = RedactionStore::new();
        let mut rows_payload = serde_json::json!({
            "columns": ["id", "email"],
            "rows": [["1", "a@b.c"]],
        });
        let mut array = serde_json::json!([{"id": "1", "data": {"email": "a@b.c"}}]);

        redact_decoded_value(None, &store, &mut rows_payload);
        redact_decoded_value(None, &store, &mut array);

        assert_eq!(rows_payload["rows"][0][1], "a@b.c");
        assert_eq!(array[0]["data"]["email"], "a@b.c");
    }

    /// The RESP `GET` shape for a row of typed columns: the stored bytes are a
    /// msgpack map keyed by field name, handed to the client verbatim. Before
    /// the fix the Redis wire shipped them unredacted.
    #[test]
    fn stored_msgpack_map_value_is_masked_field_by_field() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value = nodedb_types::json_to_msgpack(&serde_json::json!({
            "email": "a@b.c",
            "name": "Alice",
        }))
        .expect("encode stored value");

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        let decoded = nodedb_types::json_from_msgpack(&value).expect("decode redacted value");
        assert_eq!(decoded["email"], "***");
        assert_eq!(decoded["name"], "Alice");
    }

    /// A role the policy does not name reads the stored bytes unchanged — and
    /// byte-identical, not a re-encoded round trip.
    #[test]
    fn stored_value_is_byte_identical_for_an_unruled_role() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "analyst");
        let original = nodedb_types::json_to_msgpack(&serde_json::json!({"email": "a@b.c"}))
            .expect("encode stored value");
        let mut value = original.clone();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        assert_eq!(value, original);
    }

    /// No policy at all must leave the bytes exactly as delivered.
    #[test]
    fn stored_value_is_byte_identical_without_any_policy() {
        let store = RedactionStore::new();
        let redaction = redaction_for("users", "support");
        let original = b"plain-resp-set-value".to_vec();
        let mut value = original.clone();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);
        redact_stored_value_bytes(None, &store, &mut value);

        assert_eq!(value, original);
    }

    /// A `SET key value` writes opaque bytes; every SQL-side scan of that row
    /// presents them under the column `value`, so that is the rule that
    /// covers them on the Redis wire too.
    #[test]
    fn single_value_stored_form_is_masked_under_the_value_column() {
        let store = store_with_mask("cache", "support", "value", "***");
        let redaction = redaction_for("cache", "support");
        let mut value = b"secret-token".to_vec();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        assert_eq!(value, b"***".to_vec());
    }

    /// A `Null` rule on the single-value form leaves nothing to deliver, so
    /// the value is cleared and the read presents as a miss.
    #[test]
    fn single_value_null_rule_clears_the_stored_value() {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: "cache_support_value".into(),
            tenant_id: 1,
            collection: "cache".into(),
            display_collection: "cache".into(),
            for_role: "support".into(),
            rules: vec![RedactionRule {
                field: "value".into(),
                mode: RedactionMode::Null,
            }],
        });
        let redaction = redaction_for("cache", "support");
        let mut value = b"secret-token".to_vec();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        assert!(value.is_empty());
    }

    /// The device-sync shape: the stored row map is shipped verbatim inside a
    /// snapshot envelope, so the rule has to reach it at the byte level.
    #[test]
    fn document_row_bytes_are_masked_field_by_field() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut row = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": "u1",
            "email": "a@b.c",
            "name": "Alice",
        }))
        .expect("encode stored row");

        assert!(redact_document_row_bytes(
            Some(&redaction),
            &store,
            &mut row
        ));

        let decoded = nodedb_types::json_from_msgpack(&row).expect("decode redacted row");
        assert_eq!(decoded["email"], "***");
        assert_eq!(decoded["name"], "Alice");
        assert_eq!(decoded["id"], "u1");
    }

    /// A role the policy does not name, and a store with no policy at all,
    /// both leave the stored bytes byte-identical — never a re-encoded round
    /// trip that could perturb the wire shape.
    #[test]
    fn document_row_bytes_are_byte_identical_without_a_matching_rule() {
        let store = store_with_mask("users", "support", "email", "***");
        let original = nodedb_types::json_to_msgpack(&serde_json::json!({"email": "a@b.c"}))
            .expect("encode stored row");

        let mut unruled_role = original.clone();
        assert!(redact_document_row_bytes(
            Some(&redaction_for("users", "analyst")),
            &store,
            &mut unruled_role
        ));
        assert_eq!(unruled_role, original);

        let mut no_policy = original.clone();
        assert!(redact_document_row_bytes(
            Some(&redaction_for("users", "support")),
            &RedactionStore::new(),
            &mut no_policy
        ));
        assert_eq!(no_policy, original);

        let mut no_redaction = original.clone();
        assert!(redact_document_row_bytes(None, &store, &mut no_redaction));
        assert_eq!(no_redaction, original);
    }

    /// A row a rule covers but whose bytes cannot be read is refused, not
    /// delivered: these are exactly the bytes that would otherwise reach the
    /// device unredacted.
    #[test]
    fn unreadable_document_row_is_refused_when_a_rule_applies() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut row = vec![0xc1_u8];

        assert!(!redact_document_row_bytes(
            Some(&redaction),
            &store,
            &mut row
        ));
    }

    /// A delete tombstone carries no stored row, so there is nothing to refuse.
    #[test]
    fn empty_document_row_is_deliverable() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut row = Vec::new();

        assert!(redact_document_row_bytes(
            Some(&redaction),
            &store,
            &mut row
        ));
        assert!(row.is_empty());
    }

    /// A rule naming a different column must not disturb an opaque value.
    #[test]
    fn single_value_form_is_untouched_by_a_rule_on_another_column() {
        let store = store_with_mask("cache", "support", "email", "***");
        let redaction = redaction_for("cache", "support");
        let original = b"secret-token".to_vec();
        let mut value = original.clone();

        redact_stored_value_bytes(Some(&redaction), &store, &mut value);

        assert_eq!(value, original);
    }
}
