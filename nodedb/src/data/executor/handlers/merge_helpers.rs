// SPDX-License-Identifier: BUSL-1.1

//! Pure helper functions for MERGE statement execution (arm selection, action application).

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::doc_format;
use nodedb_physical::physical_plan::UpdateValue;
use nodedb_physical::physical_plan::document::merge_types::{
    MergeClauseKind as MergeClauseKindOp, MergeClauseOp,
};

/// Find the first clause of the given kind whose extra_predicate is satisfied
/// against `context_doc`.
///
/// A MERGE clause's `AND <condition>` extra predicate is WHERE-shaped, so a
/// division/modulo-by-zero inside it fails the whole statement — the same
/// behavior-flip rule 4 applies to WHERE/projection — rather than silently
/// skipping to the next clause.
pub(super) fn find_arm<'a>(
    clauses: &'a [MergeClauseOp],
    kind: MergeClauseKindOp,
    context_doc: &serde_json::Value,
) -> crate::Result<Option<&'a MergeClauseOp>> {
    let context_bytes = doc_format::encode_to_msgpack(context_doc);
    for c in clauses {
        if c.kind != kind {
            continue;
        }
        if c.extra_predicate.is_empty() {
            return Ok(Some(c));
        }
        let filters: Vec<ScanFilter> =
            zerompk::from_msgpack(&c.extra_predicate).unwrap_or_default();
        let mut all_match = true;
        for f in &filters {
            if !f.matches_binary(&context_bytes)? {
                all_match = false;
                break;
            }
        }
        if all_match {
            return Ok(Some(c));
        }
    }
    Ok(None)
}

/// Build the JSON document a NOT-MATCHED `INSERT` arm produces from a source
/// row. Empty `columns` copies all source fields; an explicit column list
/// evaluates each `UpdateValue` against the qualified source document (source
/// fields keyed as `"<alias>.<field>"`) — literals decode directly, expressions
/// (`s.new_embedding`, `s.qty * 2`) evaluate against the merged doc — and stores
/// the result under the *target* column name. Mirrors [`build_update_doc`].
/// Shared by the legacy per-row insert path and the orchestrated resolve/apply
/// passes so both derive byte-identical bodies.
pub(in crate::data::executor) fn build_insert_doc(
    columns: &[String],
    values: &[UpdateValue],
    source_doc: &serde_json::Value,
    source_alias: &str,
) -> crate::Result<serde_json::Value> {
    let mut new_doc = serde_json::Map::new();
    if columns.is_empty() {
        if let Some(obj) = source_doc.as_object() {
            for (k, v) in obj {
                new_doc.insert(k.clone(), v.clone());
            }
        }
    } else {
        // There is no target row for an insert, so the merged document is the
        // qualified source alone (target side is an empty object).
        let merged = build_merged(
            &serde_json::Value::Object(Default::default()),
            source_doc,
            source_alias,
        );
        let merged_ndb: nodedb_types::Value = merged.into();
        for (col, val) in columns.iter().zip(values.iter()) {
            new_doc.insert(col.clone(), resolve_update_value(val, &merged_ndb)?);
        }
    }
    Ok(serde_json::Value::Object(new_doc))
}

/// Build the post-update JSON document a MATCHED / NOT-MATCHED-BY-SOURCE
/// `UPDATE` arm produces. Assignment expressions evaluate against the merged
/// document (target fields at top level, source fields as `<alias>.<field>`),
/// then overwrite fields on a clone of the target. Shared by the legacy per-row
/// update path and the orchestrated resolve/apply passes.
pub(in crate::data::executor) fn build_update_doc(
    target_collection: &str,
    target_doc: &serde_json::Value,
    source_doc: &serde_json::Value,
    source_alias: &str,
    updates: &[(String, UpdateValue)],
    declared_primary_key: Option<&str>,
) -> crate::Result<serde_json::Value> {
    let merged = build_merged(target_doc, source_doc, source_alias);
    let merged_ndb: nodedb_types::Value = merged.into();
    let mut updated = target_doc.clone();
    if let Some(obj) = updated.as_object_mut() {
        for (field, update_val) in updates {
            obj.insert(
                field.clone(),
                resolve_update_value(update_val, &merged_ndb)?,
            );
        }
    }
    check_declared_pk_not_null(target_collection, &updated, declared_primary_key)?;
    Ok(updated)
}

/// A declared `PRIMARY KEY` implies `NOT NULL`. Callers pass `Some` only when
/// the check has not already run at encode time — a strict target does, so
/// its callers pass `None`.
pub(in crate::data::executor) fn check_declared_pk_not_null(
    collection: &str,
    doc: &serde_json::Value,
    declared_primary_key: Option<&str>,
) -> crate::Result<()> {
    if let Some(pk) = declared_primary_key
        && matches!(doc.get(pk), None | Some(serde_json::Value::Null))
    {
        return Err(crate::Error::RejectedConstraint {
            collection: collection.to_string(),
            constraint: "not_null".to_string(),
            detail: format!("primary key '{pk}' cannot be NULL or omitted"),
        });
    }
    Ok(())
}

/// Resolve one `UpdateValue` to JSON: a literal decodes directly from its
/// msgpack encoding, an expression evaluates against the merged document.
/// Shared by [`build_insert_doc`] and [`build_update_doc`]. An assignment
/// expression is write-path-shaped, so a division/modulo-by-zero fails the
/// whole MERGE statement.
fn resolve_update_value(
    val: &UpdateValue,
    merged_ndb: &nodedb_types::Value,
) -> crate::Result<serde_json::Value> {
    Ok(match val {
        UpdateValue::Literal(bytes) => {
            nodedb_types::json_from_msgpack(bytes).unwrap_or(serde_json::Value::Null)
        }
        UpdateValue::Expr(expr) => expr.eval(merged_ndb)?.into(),
    })
}

/// Build merged document: target fields at top level, source fields as
/// `"alias.field"` qualified entries.
pub(super) fn build_merged(
    target: &serde_json::Value,
    source: &serde_json::Value,
    source_alias: &str,
) -> serde_json::Value {
    let mut merged = target.clone();
    if let (Some(m), Some(src)) = (merged.as_object_mut(), source.as_object()) {
        for (k, v) in src {
            m.insert(format!("{source_alias}.{k}"), v.clone());
        }
    }
    merged
}

pub(super) fn json_to_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}
