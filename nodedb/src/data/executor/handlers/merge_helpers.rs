// SPDX-License-Identifier: BUSL-1.1

//! Pure helper functions for MERGE statement execution (arm selection, action application).

use crate::bridge::scan_filter::decode_scan_filters;
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
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Option<&'a MergeClauseOp>> {
    let context_bytes = doc_format::encode_to_msgpack(context_doc);
    for c in clauses {
        if c.kind != kind {
            continue;
        }
        if c.extra_predicate.is_empty() {
            return Ok(Some(c));
        }
        // A malformed predicate is an error. Reading it as "no predicate"
        // would fire this arm for every row.
        let filters = decode_scan_filters(&c.extra_predicate, "MERGE clause predicate")?;
        let mut all_match = true;
        for f in &filters {
            if !f.matches_binary(&context_bytes, crs)? {
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

/// Build the typed document a NOT-MATCHED `INSERT` arm produces from a
/// source row. Empty `columns` copies all source fields; an explicit column
/// list evaluates each `UpdateValue` against the qualified source document
/// (source fields keyed as `"<alias>.<field>"`) — literals decode to their
/// typed value, expressions (`s.new_embedding`, `s.qty * 2`) evaluate against
/// the merged doc — and stores the result under the *target* column name.
/// Mirrors [`build_update_doc`]. Shared by the legacy per-row insert path and
/// the orchestrated resolve/apply passes so both derive byte-identical
/// bodies.
pub(in crate::data::executor) fn build_insert_doc(
    columns: &[String],
    values: &[UpdateValue],
    source_doc: &serde_json::Value,
    source_alias: &str,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<nodedb_types::Value> {
    if columns.is_empty() {
        return Ok(match nodedb_types::Value::from(source_doc.clone()) {
            object @ nodedb_types::Value::Object(_) => object,
            _ => nodedb_types::Value::Object(Default::default()),
        });
    }
    // There is no target row for an insert, so the merged document is the
    // qualified source alone (target side is an empty object).
    let merged = build_merged(
        &serde_json::Value::Object(Default::default()),
        source_doc,
        source_alias,
    );
    let merged_ndb: nodedb_types::Value = merged.into();
    let mut new_doc = std::collections::HashMap::with_capacity(columns.len());
    for (col, val) in columns.iter().zip(values.iter()) {
        new_doc.insert(col.clone(), resolve_update_value(val, &merged_ndb, crs)?);
    }
    Ok(nodedb_types::Value::Object(new_doc))
}

/// The target row an UPDATE arm rewrites: its typed value, which the
/// post-image starts from, and its JSON, which expressions evaluate against.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct MergeTarget<'a> {
    pub typed: &'a nodedb_types::Value,
    pub doc: &'a serde_json::Value,
}

/// Build the typed post-update document a MATCHED / NOT-MATCHED-BY-SOURCE
/// `UPDATE` arm produces. Assignment expressions evaluate against the merged
/// document (target fields at top level, source fields as
/// `<alias>.<field>`), then overwrite fields on a clone of the typed target,
/// so a field the arm leaves alone keeps its stored type and an assigned
/// literal keeps its own. Shared by the legacy per-row update path and the
/// orchestrated resolve/apply passes.
pub(in crate::data::executor) fn build_update_doc(
    target_collection: &str,
    target: MergeTarget<'_>,
    source_doc: &serde_json::Value,
    source_alias: &str,
    updates: &[(String, UpdateValue)],
    declared_primary_key: Option<&str>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<nodedb_types::Value> {
    let merged = build_merged(target.doc, source_doc, source_alias);
    let merged_ndb: nodedb_types::Value = merged.into();
    let mut updated = target.typed.clone();
    if let nodedb_types::Value::Object(obj) = &mut updated {
        for (field, update_val) in updates {
            obj.insert(
                field.clone(),
                resolve_update_value(update_val, &merged_ndb, crs)?,
            );
        }
    }
    check_declared_pk_not_null_typed(target_collection, &updated, declared_primary_key)?;
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
        return Err(pk_null(collection, pk));
    }
    Ok(())
}

/// [`check_declared_pk_not_null`] over a typed document.
fn check_declared_pk_not_null_typed(
    collection: &str,
    doc: &nodedb_types::Value,
    declared_primary_key: Option<&str>,
) -> crate::Result<()> {
    if let Some(pk) = declared_primary_key
        && matches!(doc.get(pk), None | Some(nodedb_types::Value::Null))
    {
        return Err(pk_null(collection, pk));
    }
    Ok(())
}

fn pk_null(collection: &str, pk: &str) -> crate::Error {
    crate::Error::RejectedConstraint {
        collection: collection.to_string(),
        constraint: "not_null".to_string(),
        detail: format!("primary key '{pk}' cannot be NULL or omitted"),
    }
}

/// Resolve one `UpdateValue` to its typed value: a literal decodes from its
/// msgpack encoding, which keeps a UUID, an instant, a decimal and bytes
/// typed; an expression evaluates against the merged document. Shared by
/// [`build_insert_doc`] and [`build_update_doc`]. An assignment expression
/// is write-path-shaped, so a division/modulo-by-zero fails the whole MERGE
/// statement. A literal that does not decode is an error, never a NULL
/// written in its place.
fn resolve_update_value(
    val: &UpdateValue,
    merged_ndb: &nodedb_types::Value,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<nodedb_types::Value> {
    Ok(match val {
        UpdateValue::Literal(bytes) => {
            nodedb_types::value_from_msgpack(bytes).map_err(|e| crate::Error::Codec {
                detail: format!("MERGE assignment literal: {e}"),
            })?
        }
        UpdateValue::Expr(expr) => expr.eval(merged_ndb, crs)?,
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
