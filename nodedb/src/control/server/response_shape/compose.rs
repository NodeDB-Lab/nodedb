// SPDX-License-Identifier: BUSL-1.1

//! Composed, protocol-neutral materialized response shaping.
//!
//! `shape_response_materialized` and `shape_decoded_rows` are the canonical
//! SELECT-read shaping used by every protocol entrypoint. `shape_response_materialized`
//! performs the full per-payload shaping order (`apply_kv_wrap` ->
//! `translate_search_response` -> decode -> scan-envelope unwrap -> optional
//! SELECT-list projection) as a single call, producing an already-shaped,
//! already-projected [`ShapeOutcome`]. Every SELECT-read producer — pgwire's
//! non-streaming dispatch, native's dispatch loop — calls this directly and
//! hands the resulting `ShapedRows` to its own protocol encoder; each
//! protocol then encodes those rows in its own wire format (pgwire's
//! RowDescription/DataRow, native's MessagePack, http's JSON).
//!
//! Producers with no `PhysicalPlan` in scope (ClusterArray, set-op merges,
//! gateway forwarding, clone merges) call [`shape_payload_no_plan`], which
//! skips the plan-dependent `apply_kv_wrap` / `translate_search_response` transforms
//! those callers never ran. The pure kernel [`shape_decoded_rows`] is shared
//! with per-batch lazy streaming callers, which have an already-decoded batch
//! and only need the envelope-unwrap + projection logic.

use std::collections::HashSet;

use serde_json::{Map, Value as JsonValue};

use crate::control::server::response_translate::dispatch::translate_search_response;
use crate::data::executor::response_codec::{ArraySliceResponse, decode_payload_to_json};
use nodedb_types::NodeDbError;
use nodedb_types::columnar::schema::is_reserved_bitemporal_column;

use super::kv::apply_kv_wrap;
use super::project::push_flat_rows;
use super::redaction::RedactionCtx;
use super::request::MaterializedShapeRequest;
use super::returning::shape_returning_rows;
use super::schema::OutputSchema;
use super::types::{DdlColType, PlanKind, ShapedRows};

/// NOTICE text for an `AS OF SYSTEM TIME` cutoff older than the oldest
/// retained tile version. This is the canonical definition, surfaced to
/// every protocol via [`ShapedRows::notice`].
const TRUNCATED_BEFORE_HORIZON_NOTICE: &str = "AS OF SYSTEM TIME cutoff is older than the oldest retained tile version; \
     results may be incomplete";

/// Outcome of materialized response shaping.
///
/// Row-producing plan kinds (`SingleDocument`, `MultiRow`, `ReturningRows`,
/// `ArraySlice`) yield `Rows`. Tag/execution kinds (`Execution`,
/// `DmlResult`) yield `Passthrough` — a `ShapedRows` cannot represent a bare
/// `CommandComplete` tag or affected-row count, so callers keep their
/// existing tag / `rows_affected` handling for those.
pub enum ShapeOutcome {
    Rows(ShapedRows),
    Passthrough,
}

/// Shape a single Data-Plane payload into protocol-neutral rows, applying
/// the canonical shaping order: KV point-get wrap, vector surrogate->PK
/// translation, payload decode, scan-envelope unwrap, and (when
/// `projection` names columns) SELECT-list column selection.
pub fn shape_response_materialized(
    request: MaterializedShapeRequest<'_>,
) -> Result<ShapeOutcome, NodeDbError> {
    let MaterializedShapeRequest {
        payload,
        plan,
        plan_kind,
        projection,
        state,
        database_id,
        tenant_id,
        redaction,
    } = request;

    match plan_kind {
        PlanKind::Execution | PlanKind::DmlResult(_) => return Ok(ShapeOutcome::Passthrough),
        PlanKind::ArraySlice
        | PlanKind::ReturningRows
        | PlanKind::SingleDocument
        | PlanKind::MultiRow => {}
    }

    // Seam-1 order, exactly as pgwire's `dispatch_task_loop` applies it
    // (apply_kv_wrap -> translate_search_response) before any decode/shape step.
    let wrapped = apply_kv_wrap(plan, payload);
    let translated = translate_search_response(&wrapped, plan, state, database_id, tenant_id);

    let shaped = match plan_kind {
        PlanKind::ArraySlice => shape_array_slice(&translated, redaction)?,
        // `RETURNING` rows are held to the columns already announced to the
        // client, when any were — see `super::returning`.
        PlanKind::ReturningRows => shape_returning_rows(&translated, projection, redaction)?,
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            shape_generic_rows(&translated, projection, redaction)?
        }
        // Handled by the early return above; kept exhaustive (no catch-all,
        // no panic) so a future PlanKind desync degrades to passthrough
        // rather than crashing the connection.
        PlanKind::Execution | PlanKind::DmlResult(_) => return Ok(ShapeOutcome::Passthrough),
    };
    Ok(ShapeOutcome::Rows(shaped))
}

/// Shape a Data-Plane payload with no `PhysicalPlan` in scope.
///
/// Producers that never had a plan to KV-wrap or vector-translate
/// (ClusterArray, set-op merges, gateway forwarding, clone merges) call this
/// instead of [`shape_response_materialized`]: it applies only the decode +
/// scan-envelope unwrap + optional SELECT-list projection steps, skipping the
/// plan-dependent `apply_kv_wrap` / `translate_search_response` transforms those
/// callers never ran.
pub fn shape_payload_no_plan(
    payload: &[u8],
    plan_kind: PlanKind,
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
) -> Result<ShapeOutcome, NodeDbError> {
    Ok(match plan_kind {
        PlanKind::Execution | PlanKind::DmlResult(_) => ShapeOutcome::Passthrough,
        PlanKind::ArraySlice => ShapeOutcome::Rows(shape_array_slice(payload, redaction)?),
        PlanKind::ReturningRows => {
            ShapeOutcome::Rows(shape_returning_rows(payload, projection, redaction)?)
        }
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            ShapeOutcome::Rows(shape_generic_rows(payload, projection, redaction)?)
        }
    })
}

/// Shape an `ArrayOp::Slice` response: decode the `ArraySliceResponse`
/// envelope (falling back to a plain payload decode for legacy shapes),
/// unwrap the row envelope, and surface `truncated_before_horizon` as a
/// notice.
///
/// Array slices never carry a SELECT-list projection today (matching the
/// pre-extraction behavior), so `shape_decoded_rows` is always called with
/// a `None` projection here — but redaction still applies to the cells.
fn shape_array_slice(
    payload: &[u8],
    redaction: Option<RedactionCtx<'_>>,
) -> crate::Result<ShapedRows> {
    if payload.is_empty() {
        return Ok(empty_shaped());
    }
    let (rows_json, truncated) =
        if let Ok(resp) = zerompk::from_msgpack::<ArraySliceResponse>(payload) {
            (
                decode_payload_to_json(&resp.rows_msgpack),
                resp.truncated_before_horizon,
            )
        } else {
            (decode_payload_to_json(payload), false)
        };
    let notice = truncated.then(|| TRUNCATED_BEFORE_HORIZON_NOTICE.to_string());

    let mut shaped = match sonic_rs::from_str::<JsonValue>(&rows_json) {
        Ok(value) => shape_decoded_rows(&value, None, redaction)?,
        Err(_) => empty_shaped(),
    };
    shaped.notice = notice;
    Ok(shaped)
}

/// Shape a `SingleDocument` / `MultiRow` response: decode to JSON, then
/// hand the parsed value to the pure [`shape_decoded_rows`] core.
///
/// Non-JSON scalar payloads (undecodable envelope) fall back to a single
/// "result" column, matching pgwire's single-row fallback.
fn shape_generic_rows(
    payload: &[u8],
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
) -> crate::Result<ShapedRows> {
    if payload.is_empty() {
        return Ok(empty_shaped());
    }
    let text = decode_payload_to_json(payload);
    match sonic_rs::from_str::<JsonValue>(&text) {
        Ok(value) => shape_decoded_rows(&value, projection, redaction),
        Err(_) => Ok(single_result_row(text)),
    }
}

/// Pure shaping core: given an already-decoded Data-Plane JSON payload,
/// unwrap the `{id, data}` scan envelope via `push_flat_rows`, then either
/// select the named SELECT-list columns when a projection is given, or
/// derive the id-first column union across all rows when no named
/// projection applies.
///
/// Callers needing the composed materialized-shaping order (KV wrap, vector
/// translation, payload decode) should use [`shape_response_materialized`];
/// this function does none of that — it is the shared core both the
/// materialized path and a per-batch lazy streaming caller (native's
/// `emit_sql_stream`) call directly, since a streamed scan batch has no plan
/// to KV-wrap or vector-translate but still needs the same envelope-unwrap +
/// projection logic applied per batch.
pub fn shape_decoded_rows(
    decoded: &JsonValue,
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
) -> crate::Result<ShapedRows> {
    let mut rows = Vec::new();
    push_flat_rows(decoded.clone(), &mut rows)?;

    // Column-level redaction runs on the flat row maps, AFTER the scan
    // envelope is unwrapped and BEFORE any projection or column derivation.
    //
    // After projection, `SELECT email AS contact` would have renamed the
    // field out from under its rule; after column derivation, a
    // `RedactionMode::Null` column would be missing from a `SELECT *` result
    // instead of present and null. Both orderings deliver data the policy
    // says to withhold, so the hook belongs exactly here.
    redact_rows(redaction.as_ref(), &mut rows);

    match projection {
        Some(s) if !s.is_star && !s.columns.is_empty() => {
            let lookup_keys: Vec<String> = s.columns.iter().map(|c| c.lookup_key.clone()).collect();
            let display_names: Vec<String> =
                s.columns.iter().map(|c| c.display_name.clone()).collect();
            // Row cells are stored under per-column unique keys, not display
            // names: duplicate display names (`SELECT w.id, b.id` → `id`,
            // `id`) would collide in the row map and collapse both wire
            // columns to the last value. Encoders re-derive the same keys via
            // `cell_keys` when reading cells.
            let keys = super::project::cell_keys(&display_names);
            let projected_rows = rows
                .iter()
                .map(|row| project_row(row, &lookup_keys, &display_names, &keys))
                .collect();
            // Carry each projected column's real catalog type, aligned in
            // order with `display_names`. Only the pgwire encoder consumes
            // these — mapping them to typed RowDescription OIDs and rendering
            // each cell in that type's PostgreSQL text form; native/http
            // ignore column types entirely.
            let column_types: Vec<DdlColType> = s.columns.iter().map(|c| c.ty).collect();
            Ok(ShapedRows {
                columns: display_names,
                column_types,
                rows: projected_rows,
                notice: None,
            })
        }
        _ => {
            // Star / derived columns come from JSON rows with no catalog type,
            // so they stay TEXT — typing them would regress `SELECT *` on
            // schemaless collections.
            let columns = derive_columns(&rows);
            let column_types = ShapedRows::text_types(columns.len());
            Ok(ShapedRows {
                columns,
                column_types,
                rows,
                notice: None,
            })
        }
    }
}

/// Apply the statement's column-level redaction policy to every flat row.
///
/// A `None` context means the producer has no requester identity in scope and
/// therefore no roles to evaluate a policy against.
pub(super) fn redact_rows(
    redaction: Option<&RedactionCtx<'_>>,
    rows: &mut [Map<String, JsonValue>],
) {
    let Some(ctx) = redaction else {
        return;
    };
    for row in rows.iter_mut() {
        ctx.store
            .apply_flat_row(ctx.tenant_id, ctx.roles, ctx.collections, row);
    }
}

/// Select and rename one flat row's fields per the projection lists, trying
/// each candidate key in order: the full lookup key, then the bare
/// (post-dot) column name, then the SELECT alias.
///
/// Cells are inserted under `cell_keys` (unique per column, see
/// [`super::project::cell_keys`]) rather than the display names, which may
/// repeat across columns and would otherwise collapse in the output map.
pub(super) fn project_row(
    row: &Map<String, JsonValue>,
    lookup_keys: &[String],
    display_names: &[String],
    cell_keys: &[String],
) -> Map<String, JsonValue> {
    let mut out = Map::new();
    for (i, lookup_key) in lookup_keys.iter().enumerate() {
        let bare = lookup_key
            .rfind('.')
            .map(|dot_pos| &lookup_key[dot_pos + 1..])
            .unwrap_or(lookup_key.as_str());
        let display_name = display_names
            .get(i)
            .map(String::as_str)
            .unwrap_or(lookup_key.as_str());
        let value = row
            .get(lookup_key.as_str())
            .or_else(|| {
                if bare != lookup_key {
                    row.get(bare)
                } else {
                    None
                }
            })
            .or_else(|| {
                if display_name != lookup_key.as_str() && display_name != bare {
                    row.get(display_name)
                } else {
                    None
                }
            })
            .cloned()
            .unwrap_or(JsonValue::Null);
        let cell_key = cell_keys.get(i).map(String::as_str).unwrap_or(display_name);
        out.insert(cell_key.to_string(), value);
    }
    out
}

/// Derive the id-first column union across all rows: `id` first (if
/// present), then each row's remaining keys in first-seen order.
///
/// The order is user-visible wire column order and is pinned by callers and
/// tests, so it stays exactly first-seen. Membership lives in a set beside the
/// vec rather than being answered by rescanning the vec: the vec alone makes
/// the union quadratic in the number of distinct columns, on a path that runs
/// once per result set.
fn derive_columns(rows: &[Map<String, JsonValue>]) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    if let Some(first) = rows.first() {
        if first.contains_key("id") {
            cols.push("id".to_string());
            seen.insert("id");
        }
        for key in first.keys() {
            if key != "id" && !is_reserved_bitemporal_column(key) {
                cols.push(key.clone());
                seen.insert(key.as_str());
            }
        }
    }
    for row in rows.iter().skip(1) {
        for key in row.keys() {
            if !is_reserved_bitemporal_column(key) && seen.insert(key.as_str()) {
                cols.push(key.clone());
            }
        }
    }
    cols
}

fn empty_shaped() -> ShapedRows {
    ShapedRows {
        columns: Vec::new(),
        column_types: Vec::new(),
        rows: Vec::new(),
        notice: None,
    }
}

pub(super) fn single_result_row(text: String) -> ShapedRows {
    let mut map = Map::new();
    map.insert("result".to_string(), JsonValue::String(text));
    ShapedRows {
        columns: vec!["result".to_string()],
        column_types: ShapedRows::text_types(1),
        rows: vec![map],
        notice: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::wal::WalManager;
    use nodedb_types::CrdtPreviewResult;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::response_shape::types::describe_plan;
    use crate::control::state::SharedState;
    use nodedb_types::{DatabaseId, TenantId};

    fn preview_plan() -> PhysicalPlan {
        PhysicalPlan::Crdt(nodedb_physical::physical_plan::CrdtOp::PreviewApply {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "tasks"),
            document_id: "task-1".to_string(),
            delta: vec![0x92, 0x01],
        })
    }

    fn preview_payload() -> (CrdtPreviewResult, Vec<u8>) {
        let result = CrdtPreviewResult {
            post_image_msgpack: vec![0xc0],
            imported_ops: 17,
            trimmed_ops: 0,
            frontier_digest: [0x5a; 32],
        };
        let payload = zerompk::to_msgpack_vec(&result).expect("preview result serializes");
        (result, payload)
    }

    /// Build the minimum real shared state needed by the materialized entry
    /// point. Execution plans return before consulting it, which is exactly
    /// the property this test protects.
    fn shared_state() -> Arc<SharedState> {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("response-shape.wal"))
                .expect("test WAL"),
        );
        let (dispatcher, _) = Dispatcher::new(1, 1);
        SharedState::new(dispatcher, wal).expect("test shared state")
    }

    #[tokio::test]
    async fn crdt_preview_is_byte_preserving_through_both_shaping_entry_points() {
        let plan = preview_plan();
        let kind = describe_plan(&plan);
        assert!(matches!(kind, PlanKind::Execution));
        let (expected, payload) = preview_payload();
        let original_payload = payload.clone();

        let state = shared_state();
        let materialized = shape_response_materialized(MaterializedShapeRequest {
            payload: &payload,
            plan: &plan,
            plan_kind: kind,
            projection: None,
            state: &state,
            database_id: DatabaseId::new(1),
            tenant_id: TenantId::new(1),
            redaction: None,
        })
        .expect("execution plan passthrough");
        assert!(matches!(materialized, ShapeOutcome::Passthrough));
        assert_eq!(
            payload, original_payload,
            "materialized path must not rewrite bytes"
        );
        assert_eq!(
            zerompk::from_msgpack::<CrdtPreviewResult>(&payload)
                .expect("materialized passthrough remains decodable"),
            expected
        );

        let no_plan = shape_payload_no_plan(&payload, kind, None, None);
        assert!(matches!(no_plan, Ok(ShapeOutcome::Passthrough)));
        assert_eq!(
            payload, original_payload,
            "no-plan path must not rewrite bytes"
        );
        assert_eq!(
            zerompk::from_msgpack::<CrdtPreviewResult>(&payload)
                .expect("no-plan passthrough remains decodable"),
            expected
        );
    }

    /// Two projected columns sharing the display name `id` (`SELECT w.id,
    /// b.id`) must keep both values in the shaped row instead of collapsing
    /// to the last table's value.
    #[test]
    fn project_row_keeps_both_columns_with_duplicate_display_names() {
        let mut row = Map::new();
        row.insert("w.id".to_string(), JsonValue::String("w1".to_string()));
        row.insert("b.id".to_string(), JsonValue::String("b1".to_string()));

        let lookup_keys = vec!["w.id".to_string(), "b.id".to_string()];
        let display_names = vec!["id".to_string(), "id".to_string()];
        let keys = super::super::project::cell_keys(&display_names);

        let out = project_row(&row, &lookup_keys, &display_names, &keys);
        assert_eq!(out.len(), 2, "both cells must survive the projection");
        assert_eq!(out.get("id"), Some(&JsonValue::String("w1".to_string())));
        assert_eq!(out.get("id_1"), Some(&JsonValue::String("b1".to_string())));
    }

    // ── Column-level redaction ──────────────────────────────────────────

    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };
    use crate::control::server::response_shape::schema::OutputColumn;

    fn policy(collection: &str, role: &str, field: &str, mode: RedactionMode) -> RedactionPolicy {
        RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: 1,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode,
            }],
        }
    }

    fn store_with(policies: Vec<RedactionPolicy>) -> RedactionStore {
        let store = RedactionStore::new();
        for p in policies {
            store.create_policy(p);
        }
        store
    }

    fn ctx<'a>(
        store: &'a RedactionStore,
        roles: &'a [String],
        collections: &'a [(String, String)],
    ) -> RedactionCtx<'a> {
        RedactionCtx {
            store,
            tenant_id: 1,
            roles,
            collections,
        }
    }

    fn named_projection(pairs: &[(&str, &str)]) -> OutputSchema {
        OutputSchema {
            columns: pairs
                .iter()
                .map(|(lookup, display)| OutputColumn {
                    display_name: (*display).to_string(),
                    lookup_key: (*lookup).to_string(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
        }
    }

    fn one_row(fields: JsonValue) -> JsonValue {
        JsonValue::Array(vec![fields])
    }

    /// A `Mask` rule redacts for the role that holds the policy.
    #[test]
    fn mask_rule_redacts_for_the_policy_role() {
        let store = store_with(vec![policy(
            "users",
            "support",
            "email",
            RedactionMode::Mask("***".into()),
        )]);
        let roles = vec!["support".to_string()];
        let sources = vec![(String::new(), "users".to_string())];
        let decoded = one_row(serde_json::json!({"email": "a@b.c", "name": "Alice"}));

        let shaped = shape_decoded_rows(&decoded, None, Some(ctx(&store, &roles, &sources)))
            .expect("shape rows");
        assert_eq!(shaped.rows[0]["email"], JsonValue::String("***".into()));
        assert_eq!(shaped.rows[0]["name"], JsonValue::String("Alice".into()));
    }

    /// A role with no policy sees the value in the clear, and the rows are
    /// otherwise identical to the unredacted shaping.
    #[test]
    fn role_without_a_policy_passes_rows_through_unchanged() {
        let store = store_with(vec![policy(
            "users",
            "support",
            "email",
            RedactionMode::Mask("***".into()),
        )]);
        let roles = vec!["analyst".to_string()];
        let sources = vec![(String::new(), "users".to_string())];
        let decoded = one_row(serde_json::json!({"email": "a@b.c", "name": "Alice"}));

        let baseline = shape_decoded_rows(&decoded, None, None).expect("shape rows");
        let shaped = shape_decoded_rows(&decoded, None, Some(ctx(&store, &roles, &sources)))
            .expect("shape rows");
        assert_eq!(shaped.rows, baseline.rows);
        assert_eq!(shaped.columns, baseline.columns);
    }

    /// `SELECT email AS contact` must still be redacted: the rule names the
    /// stored field, and redaction runs before the projection renames it.
    #[test]
    fn select_alias_does_not_escape_the_rule() {
        let store = store_with(vec![policy(
            "users",
            "support",
            "email",
            RedactionMode::Mask("***".into()),
        )]);
        let roles = vec!["support".to_string()];
        let sources = vec![(String::new(), "users".to_string())];
        let decoded = one_row(serde_json::json!({"email": "a@b.c"}));
        let projection = named_projection(&[("email", "contact")]);

        let shaped = shape_decoded_rows(
            &decoded,
            Some(&projection),
            Some(ctx(&store, &roles, &sources)),
        )
        .expect("shape rows");
        assert_eq!(shaped.columns, vec!["contact".to_string()]);
        assert_eq!(shaped.rows[0]["contact"], JsonValue::String("***".into()));
    }

    /// Two joined collections both carry `id`, but only the left side has a
    /// rule. Matching the bare name would redact the right side too.
    #[test]
    fn join_redacts_only_the_side_the_rule_belongs_to() {
        let store = store_with(vec![policy(
            "workspaces",
            "support",
            "id",
            RedactionMode::Mask("***".into()),
        )]);
        let roles = vec!["support".to_string()];
        let sources = vec![
            ("w".to_string(), "workspaces".to_string()),
            ("b".to_string(), "boards".to_string()),
        ];
        let decoded = one_row(serde_json::json!({"w.id": "w1", "b.id": "b1"}));
        let projection = named_projection(&[("w.id", "id"), ("b.id", "id")]);

        let shaped = shape_decoded_rows(
            &decoded,
            Some(&projection),
            Some(ctx(&store, &roles, &sources)),
        )
        .expect("shape rows");
        // `cell_keys` suffixes the duplicate display name.
        assert_eq!(shaped.rows[0]["id"], JsonValue::String("***".into()));
        assert_eq!(shaped.rows[0]["id_1"], JsonValue::String("b1".into()));
    }

    /// `RedactionMode::Null` must leave the column in a `SELECT *` result,
    /// valued null — removing the key would drop it from the derived schema.
    #[test]
    fn star_keeps_a_null_redacted_column_in_the_schema() {
        let store = store_with(vec![policy(
            "users",
            "support",
            "email",
            RedactionMode::Null,
        )]);
        let roles = vec!["support".to_string()];
        let sources = vec![(String::new(), "users".to_string())];
        let decoded = one_row(serde_json::json!({"id": "u1", "email": "a@b.c"}));

        let shaped = shape_decoded_rows(&decoded, None, Some(ctx(&store, &roles, &sources)))
            .expect("shape rows");
        assert!(
            shaped.columns.contains(&"email".to_string()),
            "redacted column must stay in the derived SELECT * schema: {:?}",
            shaped.columns
        );
        assert_eq!(shaped.rows[0]["email"], JsonValue::Null);
    }

    /// Duplicate-free projections still store cells under the display name
    /// (`cell_keys` is the identity), so existing readers are unaffected.
    #[test]
    fn project_row_uses_display_names_when_unique() {
        let mut row = Map::new();
        row.insert("w.id".to_string(), JsonValue::String("w1".to_string()));
        row.insert("b.title".to_string(), JsonValue::String("t".to_string()));

        let lookup_keys = vec!["w.id".to_string(), "b.title".to_string()];
        let display_names = vec!["id".to_string(), "title".to_string()];
        let keys = super::super::project::cell_keys(&display_names);

        let out = project_row(&row, &lookup_keys, &display_names, &keys);
        assert_eq!(out.get("id"), Some(&JsonValue::String("w1".to_string())));
        assert_eq!(out.get("title"), Some(&JsonValue::String("t".to_string())));
    }

    fn row_of(keys: &[&str]) -> Map<String, JsonValue> {
        let mut row = Map::new();
        for k in keys {
            row.insert((*k).to_string(), JsonValue::String((*k).to_string()));
        }
        row
    }

    /// Column order is user-visible: `id` first when the first row has it,
    /// then every other column in the order it is first seen, scanning rows in
    /// order. Overlapping and disjoint rows must not reorder or duplicate.
    ///
    /// Every `row_of` list here is already in ascending key order, so the
    /// within-row iteration order is the same whichever map backs
    /// `serde_json::Map`; what this pins is the cross-row order.
    #[test]
    fn derive_columns_pins_id_first_then_first_seen_order() {
        let rows = vec![
            row_of(&["a", "b", "id"]),
            row_of(&["a", "z"]),
            row_of(&["b", "y"]),
            row_of(&["q"]),
        ];

        assert_eq!(
            derive_columns(&rows),
            vec!["id", "a", "b", "z", "y", "q"],
            "id leads; later rows append only their newly-seen columns"
        );
    }

    #[test]
    fn derive_columns_without_id_keeps_the_first_rows_columns_leading() {
        let rows = vec![row_of(&["a", "b"]), row_of(&["c", "id"])];

        assert_eq!(
            derive_columns(&rows),
            vec!["a", "b", "c", "id"],
            "a late `id` appends where it is first seen; it is not hoisted"
        );
    }

    #[test]
    fn derive_columns_skips_reserved_bitemporal_columns() {
        let rows = vec![
            row_of(&["__system_from_ms", "id"]),
            row_of(&["__valid_from_ms", "name"]),
        ];

        assert_eq!(derive_columns(&rows), vec!["id", "name"]);
    }
}
