// SPDX-License-Identifier: BUSL-1.1

//! Redaction of decoded result values: scan envelopes, flat rows and
//! positional `RETURNING` payloads.

use crate::control::security::redaction::RedactionStore;
use crate::control::server::response_shape::project::is_scan_wrapper_json;
use crate::data::executor::response_codec::RowContext;

use super::super::query::QueryRedaction;

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
///
/// `context` is the row shape of the payload's producer. Only a
/// [`RowContext::ScanEnvelope`] row is unwrapped: a flat row whose columns
/// are `id` and `data` is redacted as the user row it is.
pub fn redact_envelope_row(
    redaction: Option<&QueryRedaction>,
    store: &RedactionStore,
    context: RowContext,
    item: &mut serde_json::Value,
) {
    let Some(resolved) = redaction else {
        return;
    };
    let ctx = resolved.ctx(store);
    let Some(map) = item.as_object_mut() else {
        return;
    };
    let target = if context == RowContext::ScanEnvelope && is_scan_wrapper_json(map) {
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
    context: RowContext,
    value: &mut serde_json::Value,
) {
    if redaction.is_none() {
        return;
    }
    if let serde_json::Value::Array(items) = value {
        for item in items {
            redact_envelope_row(redaction, store, context, item);
        }
        return;
    }
    if is_rows_payload_shape(value) {
        redact_rows_payload(redaction, store, value);
        return;
    }
    redact_envelope_row(redaction, store, context, value);
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

#[cfg(test)]
mod tests {
    use super::super::test_support::{redaction_for, store_with_mask};
    use super::*;

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

        redact_decoded_value(Some(&redaction), &store, RowContext::Flat, &mut value);

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

        redact_decoded_value(
            Some(&redaction),
            &store,
            RowContext::ScanEnvelope,
            &mut value,
        );

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

        redact_decoded_value(Some(&redaction), &store, RowContext::Flat, &mut value);

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

        redact_decoded_value(Some(&redaction), &store, RowContext::Flat, &mut value);

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

        redact_decoded_value(None, &store, RowContext::Flat, &mut rows_payload);
        redact_decoded_value(None, &store, RowContext::ScanEnvelope, &mut array);

        assert_eq!(rows_payload["rows"][0][1], "a@b.c");
        assert_eq!(array[0]["data"]["email"], "a@b.c");
    }

    /// A flat user row whose columns are `id` and `data` is redacted as a
    /// flat row: its `data` column is never unwrapped.
    #[test]
    fn a_flat_id_data_row_is_redacted_as_a_flat_row() {
        let store = store_with_mask("users", "support", "email", "***");
        let redaction = redaction_for("users", "support");
        let mut value =
            serde_json::json!([{"id": "1", "data": {"email": "a@b.c"}, "email": "d@e.f"}]);

        redact_decoded_value(Some(&redaction), &store, RowContext::Flat, &mut value);

        assert_eq!(value[0]["email"], "***");
        assert_eq!(value[0]["data"]["email"], "a@b.c");
    }
}
