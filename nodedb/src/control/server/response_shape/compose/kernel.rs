// SPDX-License-Identifier: BUSL-1.1

//! Pure shaping kernel over an already-decoded Data-Plane value.
//!
//! [`shape_decoded_rows`] unwraps the `{id, data}` scan envelope, redacts,
//! and either projects the SELECT list or derives the column union. It is
//! the shared core both the materialized path and every per-batch lazy
//! streaming caller (pgwire, native, http) call directly.

use std::collections::HashSet;

use nodedb_types::Value;
use nodedb_types::columnar::schema::is_reserved_bitemporal_column;

use super::super::project::push_flat_rows;
use super::super::redaction::RedactionCtx;
use super::super::schema::OutputSchema;
use super::super::types::{DdlColType, ShapedRow, ShapedRows};

/// Pure shaping core: given an already-decoded Data-Plane value, unwrap the
/// `{id, data}` scan envelope via `push_flat_rows`, then either select the
/// named SELECT-list columns when a projection is given, or derive the
/// id-first column union across all rows when no named projection applies.
///
/// Callers needing the composed materialized-shaping order (KV wrap, vector
/// translation, payload decode) use `shape_response_materialized`; this
/// function does none of that. A streamed scan batch has no plan to KV-wrap
/// or vector-translate but still needs the same envelope-unwrap + projection
/// logic applied per batch, so streaming callers call this directly.
pub fn shape_decoded_rows(
    decoded: Value,
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
) -> crate::Result<ShapedRows> {
    let mut rows = Vec::new();
    push_flat_rows(decoded, &mut rows)?;

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
            let keys = super::super::project::cell_keys(&display_names);
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
            Ok(ShapedRows::from_rows(
                display_names,
                column_types,
                projected_rows,
            ))
        }
        _ => {
            // Star / derived columns come from rows with no catalog type, so
            // they stay TEXT — typing them would regress `SELECT *` on
            // schemaless collections.
            let columns = derive_columns(&rows);
            let column_types = ShapedRows::text_types(columns.len());
            Ok(ShapedRows::from_rows(columns, column_types, rows))
        }
    }
}

/// Apply the statement's column-level redaction policy to every flat row.
///
/// A `None` context means the producer has no requester identity in scope and
/// therefore no roles to evaluate a policy against.
pub(in crate::control::server::response_shape) fn redact_rows(
    redaction: Option<&RedactionCtx<'_>>,
    rows: &mut [ShapedRow],
) {
    let Some(ctx) = redaction else {
        return;
    };
    for row in rows.iter_mut() {
        ctx.store
            .apply_flat_row_typed(ctx.tenant_id, ctx.roles, ctx.collections, row);
    }
}

/// Select and rename one flat row's fields per the projection lists, trying
/// each candidate key in order: the full lookup key, then the bare
/// (post-dot) column name, then the SELECT alias.
///
/// Cells are inserted under `cell_keys` (unique per column, see
/// [`super::super::project::cell_keys`]) rather than the display names, which
/// may repeat across columns and would otherwise collapse in the output map.
pub(in crate::control::server::response_shape) fn project_row(
    row: &ShapedRow,
    lookup_keys: &[String],
    display_names: &[String],
    cell_keys: &[String],
) -> ShapedRow {
    let mut out = ShapedRow::new();
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
            .unwrap_or(Value::Null);
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
fn derive_columns(rows: &[ShapedRow]) -> Vec<String> {
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

/// A result set with no columns and no rows.
pub(in crate::control::server::response_shape) fn empty_shaped() -> ShapedRows {
    ShapedRows::from_rows(Vec::new(), Vec::new(), Vec::new())
}

/// A single `result` text column holding one row of `text`.
pub(in crate::control::server::response_shape) fn single_result_row(text: String) -> ShapedRows {
    let mut row = ShapedRow::new();
    row.insert("result".to_string(), Value::String(text));
    ShapedRows::from_rows(
        vec!["result".to_string()],
        ShapedRows::text_types(1),
        vec![row],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };
    use crate::control::server::response_shape::schema::OutputColumn;
    use nodedb_types::NdbDateTime;

    fn text(s: &str) -> Value {
        Value::String(s.to_string())
    }

    fn row_of_pairs(pairs: &[(&str, Value)]) -> ShapedRow {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// Two projected columns sharing the display name `id` (`SELECT w.id,
    /// b.id`) must keep both values in the shaped row instead of collapsing
    /// to the last table's value.
    #[test]
    fn project_row_keeps_both_columns_with_duplicate_display_names() {
        let row = row_of_pairs(&[("w.id", text("w1")), ("b.id", text("b1"))]);

        let lookup_keys = vec!["w.id".to_string(), "b.id".to_string()];
        let display_names = vec!["id".to_string(), "id".to_string()];
        let keys = crate::control::server::response_shape::project::cell_keys(&display_names);

        let out = project_row(&row, &lookup_keys, &display_names, &keys);
        assert_eq!(out.len(), 2, "both cells must survive the projection");
        assert_eq!(out.get("id"), Some(&text("w1")));
        assert_eq!(out.get("id_1"), Some(&text("b1")));
    }

    // ── Column-level redaction ──────────────────────────────────────────

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

    /// One decoded row: a one-element array holding the object `pairs`.
    fn one_row(pairs: &[(&str, Value)]) -> Value {
        let object = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        Value::Array(vec![Value::Object(object)])
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
        let decoded = one_row(&[("email", text("a@b.c")), ("name", text("Alice"))]);

        let shaped = shape_decoded_rows(decoded, None, Some(ctx(&store, &roles, &sources)))
            .expect("shape rows");
        assert_eq!(shaped.rows[0]["email"], text("***"));
        assert_eq!(shaped.rows[0]["name"], text("Alice"));
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
        let decoded = one_row(&[("email", text("a@b.c")), ("name", text("Alice"))]);

        let baseline = shape_decoded_rows(decoded.clone(), None, None).expect("shape rows");
        let shaped = shape_decoded_rows(decoded, None, Some(ctx(&store, &roles, &sources)))
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
        let decoded = one_row(&[("email", text("a@b.c"))]);
        let projection = named_projection(&[("email", "contact")]);

        let shaped = shape_decoded_rows(
            decoded,
            Some(&projection),
            Some(ctx(&store, &roles, &sources)),
        )
        .expect("shape rows");
        assert_eq!(shaped.columns, vec!["contact".to_string()]);
        assert_eq!(shaped.rows[0]["contact"], text("***"));
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
        let decoded = one_row(&[("w.id", text("w1")), ("b.id", text("b1"))]);
        let projection = named_projection(&[("w.id", "id"), ("b.id", "id")]);

        let shaped = shape_decoded_rows(
            decoded,
            Some(&projection),
            Some(ctx(&store, &roles, &sources)),
        )
        .expect("shape rows");
        // `cell_keys` suffixes the duplicate display name.
        assert_eq!(shaped.rows[0]["id"], text("***"));
        assert_eq!(shaped.rows[0]["id_1"], text("b1"));
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
        let decoded = one_row(&[("id", text("u1")), ("email", text("a@b.c"))]);

        let shaped = shape_decoded_rows(decoded, None, Some(ctx(&store, &roles, &sources)))
            .expect("shape rows");
        assert!(
            shaped.columns.contains(&"email".to_string()),
            "redacted column must stay in the derived SELECT * schema: {:?}",
            shaped.columns
        );
        assert_eq!(shaped.rows[0]["email"], Value::Null);
    }

    /// Duplicate-free projections still store cells under the display name
    /// (`cell_keys` is the identity), so existing readers are unaffected.
    #[test]
    fn project_row_uses_display_names_when_unique() {
        let row = row_of_pairs(&[("w.id", text("w1")), ("b.title", text("t"))]);

        let lookup_keys = vec!["w.id".to_string(), "b.title".to_string()];
        let display_names = vec!["id".to_string(), "title".to_string()];
        let keys = crate::control::server::response_shape::project::cell_keys(&display_names);

        let out = project_row(&row, &lookup_keys, &display_names, &keys);
        assert_eq!(out.get("id"), Some(&text("w1")));
        assert_eq!(out.get("title"), Some(&text("t")));
    }

    /// A typed instant cell passes through projection as itself; its
    /// ISO-8601 text is the protocol edge's rendering, not the kernel's.
    #[test]
    fn an_instant_cell_survives_projection_as_a_typed_value() {
        let at = NdbDateTime::from_micros(1_583_402_400_000_000);
        let decoded = one_row(&[("at", Value::NaiveDateTime(at)), ("id", text("r1"))]);
        let projection = named_projection(&[("at", "at")]);

        let shaped = shape_decoded_rows(decoded, Some(&projection), None).expect("shape rows");
        assert_eq!(shaped.rows[0]["at"], Value::NaiveDateTime(at));
        assert_eq!(
            crate::control::server::response_shape::cell::value_to_wire_json(&shaped.rows[0]["at"]),
            serde_json::Value::String("2020-03-05T10:00:00.000000Z".into())
        );
    }

    fn row_of(keys: &[&str]) -> ShapedRow {
        keys.iter().map(|k| ((*k).to_string(), text(k))).collect()
    }

    /// Column order is user-visible: `id` first when the first row has it,
    /// then every other column in the order it is first seen, scanning rows in
    /// order. Overlapping and disjoint rows must not reorder or duplicate.
    ///
    /// Every `row_of` list here is already in ascending key order, so the
    /// within-row iteration order is the sorted order a `ShapedRow` iterates
    /// in; what this pins is the cross-row order.
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
