// SPDX-License-Identifier: BUSL-1.1

//! WHERE predicate application and RETURN column projection.

use super::super::ast::*;
use super::core::execute_clause;
use super::expansion::VarLenCaps;
use super::types::{BindingRow, ExecutionState};
use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::sparse::btree::SparseEngine;

/// Borrowed context needed to resolve a MATCH property predicate
/// (`WHERE a.field = 'v'`) against a node's stored document.
///
/// A graph node's properties live as a document in the sparse engine. The
/// document is NOT keyed by the user-visible node-id string — it is keyed by
/// `surrogate_to_doc_id(surrogate)`, the fixed-width hex form of the row's
/// global surrogate. A graph node and its same-pk document share one surrogate
/// (the CSR node surrogate is set from the edge surrogate allocated by the same
/// pk-keyed allocator), so the fetch chain is:
/// `node name → Surrogate (via `csr`) → surrogate_to_doc_id → sparse.get`.
///
/// The CSR/graph is keyed per `(database_id, tenant_id)` only, so the collection
/// holding the document comes from the MATCH query's `IN '<collection>'`
/// clause (`collection`). All of this is threaded as ONE param through the
/// predicate pipeline rather than as loose arguments.
pub struct PropertyLookup<'a> {
    pub sparse: &'a SparseEngine,
    pub csr: &'a CsrIndex,
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: Option<&'a str>,
}

/// Apply a WHERE predicate to filter rows.
///
/// `varlen_caps` carries the same per-expansion caps as the outer query so a
/// variable-length sub-pattern (e.g. inside `NOT EXISTS`) truncates at the
/// configured ceiling rather than a hardcoded one.
///
/// `props` carries the sparse-engine handle and `(database_id, tenant_id,
/// collection)` needed to resolve property predicates (`WHERE a.field = 'v'`)
/// against each bound node's stored document.
pub(super) fn apply_predicate(
    rows: &[BindingRow],
    predicate: &WherePredicate,
    csr: &CsrIndex,
    _edge_store: &EdgeStore,
    varlen_caps: VarLenCaps,
    props: &PropertyLookup<'_>,
    overlay: Option<&GraphOverlayDelta>,
) -> Result<Vec<BindingRow>, crate::Error> {
    match predicate {
        WherePredicate::Equals {
            binding,
            field,
            value,
        } => {
            if field.is_empty() {
                Ok(rows
                    .iter()
                    .filter(|row| row.get(binding).is_some_and(|v| v == value))
                    .cloned()
                    .collect())
            } else {
                let expected_value = coerce_literal(value);
                let mut kept = Vec::new();
                for row in rows {
                    let keep = match row.get(binding) {
                        Some(node_id) => check_property(
                            props,
                            node_id,
                            field,
                            &ComparisonOp::Eq,
                            &expected_value,
                        )?,
                        None => false,
                    };
                    if keep {
                        kept.push(row.clone());
                    }
                }
                Ok(kept)
            }
        }

        WherePredicate::Comparison {
            binding,
            field,
            op,
            value,
        } => {
            if field.is_empty() {
                // Node-identity comparison: `WHERE a <> b` or `WHERE a <> 'literal'`.
                //
                // The parser stores `value` as the raw RHS string — either a binding
                // name (no quotes, e.g. `p3` from `WHERE p1 <> p3`) or a stripped
                // literal (e.g. `alice` from `WHERE p1 <> 'alice'`). We distinguish by
                // attempting to resolve `value` as a binding in the current row. If it
                // resolves, we compare two bound node identities (binding-vs-binding).
                // If it does not resolve, we compare the bound node identity against
                // the literal string (binding-vs-literal).
                //
                // This covers the LSQB `WHERE p1 <> p3` anti-self-join filter as well
                // as identity equality/inequality against a fixed value.
                Ok(rows
                    .iter()
                    .filter(|row| {
                        let lhs = match row.get(binding.as_str()) {
                            Some(v) => v.as_str(),
                            // Binding not yet resolved in this row → keep row
                            // (predicate is unevaluable; don't silently drop).
                            None => return true,
                        };
                        // Resolve RHS: prefer binding lookup, fall back to literal.
                        let rhs: &str = match row.get(value.as_str()) {
                            Some(v) => v.as_str(),
                            None => value.as_str(),
                        };
                        apply_op(op, lhs, rhs)
                    })
                    .cloned()
                    .collect())
            } else {
                // Property comparison: `WHERE a.age > 25`.
                //
                // Each bound node's properties are fetched from the sparse
                // engine (its document, keyed by node-id within the query's
                // `IN '<collection>'`), decoded, and the field compared against
                // the coerced literal using `op`. Coercion happens once here
                // rather than inside `check_property` (which runs per row).
                let expected_value = coerce_literal(value);
                let mut kept = Vec::new();
                for row in rows {
                    let keep = match row.get(binding.as_str()) {
                        Some(node_id) => {
                            check_property(props, node_id, field, op, &expected_value)?
                        }
                        None => false,
                    };
                    if keep {
                        kept.push(row.clone());
                    }
                }
                Ok(kept)
            }
        }

        WherePredicate::NotExists { sub_pattern } => {
            let mut result = Vec::new();
            // NOT EXISTS sub-patterns run in their own local state: any
            // truncation inside the sub-query would make the anti-join
            // unsound (a truncated "empty" isn't really empty), so we
            // instead propagate truncation to the outer query via the
            // top-level `ExecutionState` that the caller of
            // `apply_predicate` already tracks. Here we keep a throwaway
            // local state and inspect it.
            for row in rows {
                let mut sub_state = ExecutionState::new(None, varlen_caps);
                // Scope the sub-pattern's edge traversal to the same collection
                // as the outer query (`IN '<collection>'`) so NOT EXISTS never
                // consults another collection's edges.
                sub_state.collection_filter =
                    super::expansion::resolve_collection_filter(props.collection, csr);
                // NOT EXISTS sub-patterns check structural connectivity
                // against already-bound variables — no anchor enumeration
                // occurs, so the frontier bitmap does not apply here.
                let sub_rows = execute_clause(
                    sub_pattern,
                    csr,
                    std::slice::from_ref(row),
                    &mut sub_state,
                    None,
                    overlay,
                )?;
                if sub_state.truncated() {
                    // Sub-pattern hit a cap — treat the outer match as
                    // truncated too. The outer caller of apply_predicate
                    // is responsible for surfacing this, but we have no
                    // handle to it from inside predicate evaluation, so
                    // the safest contract is to conservatively drop the
                    // row: a truncated "did not match" might actually
                    // have matched. Emitting it would be a false-positive.
                    continue;
                }
                if sub_rows.is_empty() {
                    result.push(row.clone());
                }
            }
            Ok(result)
        }
    }
}

/// Apply a `ComparisonOp` to two string-typed node identities.
///
/// For node-identity comparisons (empty `field`), identities are strings so
/// only `Eq` and `Neq` have defined semantics. Ordering operators (`Lt`, `Lte`,
/// `Gt`, `Gte`) are not meaningful for opaque node names; we conservatively
/// keep every row rather than silently drop on an unevaluable predicate.
fn apply_op(op: &ComparisonOp, lhs: &str, rhs: &str) -> bool {
    match op {
        ComparisonOp::Eq => lhs == rhs,
        ComparisonOp::Neq => lhs != rhs,
        // Ordering on node identities is undefined — preserve the row.
        ComparisonOp::Lt | ComparisonOp::Lte | ComparisonOp::Gt | ComparisonOp::Gte => true,
    }
}

/// Coerce a raw WHERE-literal string into a typed [`nodedb_types::Value`].
///
/// The parser stores the RHS of a property predicate as a bare string (quotes
/// already stripped), so `"25"` must compare as the integer `25` against an
/// integer field. We try, in order: integer, float, bool, else text. The
/// numeric/bool ladder matches how `value_ops` coerces on comparison, so e.g.
/// `WHERE a.age = '25'` against an integer field `25` is equal.
fn coerce_literal(expected: &str) -> nodedb_types::Value {
    use nodedb_types::Value;
    if let Ok(i) = expected.parse::<i64>() {
        Value::Integer(i)
    } else if let Ok(f) = expected.parse::<f64>() {
        Value::Float(f)
    } else if let Ok(b) = expected.parse::<bool>() {
        Value::Bool(b)
    } else {
        Value::String(expected.to_string())
    }
}

/// Fetch and decode the stored document for `node_id` within `collection`.
///
/// Returns `Ok(Some(doc))` when the document is present and successfully
/// decoded, `Ok(None)` when the node has no stored document, and `Err` for
/// storage or decode failures.
///
/// The document is keyed by the node's GLOBAL SURROGATE, not by the node-id
/// string. We resolve `node_id → Surrogate` through the CSR (a graph node and
/// its same-pk document share one surrogate), derive the redb storage key via
/// `surrogate_to_doc_id`, then fetch. A node that is unknown to the partition
/// or has no surrogate set (the ZERO sentinel) is treated as having no
/// document → `Ok(None)`.
///
/// The `collection` argument is already resolved by the caller (both
/// `check_property` and `project_property` guard `props.collection` first
/// because their `BadRequest` messages reference context the caller owns).
fn fetch_node_doc(
    props: &PropertyLookup<'_>,
    collection: &str,
    node_id: &str,
) -> Result<Option<nodedb_types::Value>, crate::Error> {
    let Some(surrogate) = props.csr.node_surrogate(node_id) else {
        return Ok(None);
    };
    let doc_id = crate::engine::document::store::key::StorageKey::for_surrogate(surrogate);
    let bytes = match props
        .sparse
        .get(props.database_id, props.tenant_id, collection, &doc_id)?
    {
        Some(b) => b,
        None => return Ok(None),
    };

    let doc =
        nodedb_types::value_from_msgpack(&bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("decode graph node `{node_id}` document: {e}"),
        })?;

    Ok(Some(doc))
}

/// Evaluate a property predicate (`<node_id>.<field> <op> <expected>`) against
/// the node's stored document.
///
/// A graph node's properties live as a document in the sparse engine, keyed by
/// the node-id string within the query's `IN '<collection>'`. This:
///
/// - returns a typed `BadRequest` when no collection is available — a property
///   predicate is unresolvable without one, so we must NOT silently pass or drop;
/// - returns `Ok(false)` when the node has no document (a node with no
///   properties cannot satisfy a property predicate);
/// - returns `Ok(false)` when the document lacks `field`;
/// - otherwise compares the stored field value against `expected_value`
///   using `op` (type-aware, via `nodedb_query::value_ops`).
///
/// `expected_value` is pre-coerced by the caller so that `coerce_literal` is
/// called once per predicate rather than once per matching row.
fn check_property(
    props: &PropertyLookup<'_>,
    node_id: &str,
    field: &str,
    op: &ComparisonOp,
    expected_value: &nodedb_types::Value,
) -> Result<bool, crate::Error> {
    use nodedb_query::value_ops::{coerced_eq, compare_values};
    use std::cmp::Ordering;

    let collection = props.collection.ok_or_else(|| crate::Error::BadRequest {
        detail: format!(
            "MATCH property predicate `{node_id}.{field}` requires an \
             `IN '<collection>'` clause to resolve node properties"
        ),
    })?;

    // Fetch and decode the node's document. Absent document → cannot satisfy a
    // property predicate.
    let doc = match fetch_node_doc(props, collection, node_id)? {
        Some(d) => d,
        None => return Ok(false),
    };

    // Look up the field. Missing field → predicate not satisfiable.
    let field_value = match &doc {
        nodedb_types::Value::Object(map) => map.get(field),
        _ => None,
    };
    let field_value = match field_value {
        Some(v) => v,
        None => return Ok(false),
    };

    let result = match op {
        ComparisonOp::Eq => coerced_eq(field_value, expected_value),
        ComparisonOp::Neq => !coerced_eq(field_value, expected_value),
        ComparisonOp::Lt => compare_values(field_value, expected_value) == Ordering::Less,
        ComparisonOp::Lte => {
            matches!(
                compare_values(field_value, expected_value),
                Ordering::Less | Ordering::Equal
            )
        }
        ComparisonOp::Gt => compare_values(field_value, expected_value) == Ordering::Greater,
        ComparisonOp::Gte => {
            matches!(
                compare_values(field_value, expected_value),
                Ordering::Greater | Ordering::Equal
            )
        }
    };
    Ok(result)
}

/// Test-only re-export of [`check_property`] so sibling-module tests can
/// exercise the property-predicate evaluation path directly against a real
/// `SparseEngine` without standing up a full executor run.
///
/// Coerces `expected` via [`coerce_literal`] (matching `apply_predicate`'s
/// per-predicate coerce-once pattern) before forwarding to `check_property`.
#[cfg(test)]
pub(super) fn check_property_for_test(
    props: &PropertyLookup<'_>,
    node_id: &str,
    field: &str,
    op: &ComparisonOp,
    expected: &str,
) -> Result<bool, crate::Error> {
    let expected_value = coerce_literal(expected);
    check_property(props, node_id, field, op, &expected_value)
}

/// Project RETURN columns from rows.
///
/// Non-dotted exprs (`a`, or an aliased binding) project the node identity bound
/// in the row, `"NULL"` if absent — the mirror of node-identity resolution.
///
/// Dotted exprs (`a.field`) project the node's stored PROPERTY: the binding is
/// resolved to a node-id, the node's document is fetched from the sparse engine
/// (keyed by node-id within the query's `IN '<collection>'`), decoded, and the
/// `field` extracted and stringified via the canonical `value_ops` display
/// convention so projected scalars match how binding values are otherwise
/// stringified. The same resolution rules as the predicate path apply:
///
/// - binding not in the row → `"NULL"` (unevaluable for that row);
/// - no `IN '<collection>'` clause → typed `BadRequest` (a property is
///   unresolvable without one; never silently `"NULL"`);
/// - node has no document → `"NULL"` (SQL projection: missing row → NULL);
/// - document lacks `field` → `"NULL"`.
pub(super) fn project_columns(
    rows: &[BindingRow],
    columns: &[ReturnColumn],
    props: &PropertyLookup<'_>,
) -> Result<Vec<BindingRow>, crate::Error> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut projected = BindingRow::new();
        for col in columns {
            let key = col.alias.as_deref().unwrap_or(&col.expr);

            let value = if let Some(dot) = col.expr.find('.') {
                let binding = &col.expr[..dot];
                let field = &col.expr[dot + 1..];
                match row.get(binding) {
                    // Binding not yet resolved in this row → NULL (unchanged).
                    None => "NULL".to_string(),
                    Some(node_id) => project_property(props, node_id, field)?,
                }
            } else {
                row.get(&col.expr)
                    .cloned()
                    .unwrap_or_else(|| "NULL".to_string())
            };

            projected.insert(key.to_string(), value);
        }
        out.push(projected);
    }
    Ok(out)
}

/// Resolve `<node_id>.<field>` to its projected string value against the node's
/// stored document. Mirrors [`check_property`]'s fetch+decode contract:
///
/// - returns a typed `BadRequest` when no collection is available (a property is
///   unresolvable without one);
/// - returns `"NULL"` when the node has no document (missing row → NULL);
/// - returns `"NULL"` when the document lacks `field`;
/// - otherwise stringifies the stored value via the canonical `value_ops`
///   display convention so it matches how binding values are stringified.
fn project_property(
    props: &PropertyLookup<'_>,
    node_id: &str,
    field: &str,
) -> Result<String, crate::Error> {
    use nodedb_query::value_ops::value_to_display_string;

    let collection = props.collection.ok_or_else(|| crate::Error::BadRequest {
        detail: format!(
            "MATCH property projection `{node_id}.{field}` requires an \
             `IN '<collection>'` clause to resolve node properties"
        ),
    })?;

    // Absent document → NULL (SQL projection semantics: missing row → NULL).
    let doc = match fetch_node_doc(props, collection, node_id)? {
        Some(d) => d,
        None => return Ok("NULL".to_string()),
    };

    // Missing field → NULL.
    let field_value = match &doc {
        nodedb_types::Value::Object(map) => map.get(field),
        _ => None,
    };
    match field_value {
        Some(v) => Ok(value_to_display_string(v)),
        None => Ok("NULL".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `apply_op` is the pure filtering kernel for node-identity comparisons.
    // These tests prove that `WHERE a <> b` (Neq) and `WHERE a = b` (Eq) actually
    // filter rather than the old no-op behaviour, without needing real CsrIndex /
    // EdgeStore instances.

    #[test]
    fn neq_filters_equal_values() {
        // lhs == rhs → Neq must return false (row dropped).
        assert!(!apply_op(&ComparisonOp::Neq, "alice", "alice"));
        // lhs != rhs → Neq must return true (row kept).
        assert!(apply_op(&ComparisonOp::Neq, "alice", "bob"));
    }

    #[test]
    fn eq_keeps_only_matching_values() {
        assert!(apply_op(&ComparisonOp::Eq, "alice", "alice"));
        assert!(!apply_op(&ComparisonOp::Eq, "alice", "bob"));
    }

    #[test]
    fn self_comparison_neq_is_always_false() {
        // WHERE p1 <> p1: the same binding resolves to the same value → always false.
        assert!(!apply_op(&ComparisonOp::Neq, "x", "x"));
        assert!(!apply_op(&ComparisonOp::Neq, "carol", "carol"));
    }

    #[test]
    fn ordering_ops_on_node_identities_preserve_row() {
        // Lt/Lte/Gt/Gte on opaque node names are undefined; we conservatively
        // keep the row rather than silently drop.
        for op in &[
            ComparisonOp::Lt,
            ComparisonOp::Lte,
            ComparisonOp::Gt,
            ComparisonOp::Gte,
        ] {
            assert!(apply_op(op, "alice", "bob"), "{op:?} should preserve row");
            assert!(apply_op(op, "alice", "alice"), "{op:?} should preserve row");
        }
    }

    // End-to-end simulation of the binding-row filtering logic that
    // `apply_predicate` runs for empty-field Comparison predicates.
    // We call the inner closure logic directly (no EdgeStore needed) by
    // hand-rolling what `apply_predicate` does for the Comparison + empty-field branch.
    //
    // This proves the RHS resolution strategy: `value` is first looked up as a
    // binding name in the row; if absent, treated as a literal.
    #[test]
    fn rhs_resolved_as_binding_when_present_in_row() {
        use std::collections::HashMap;

        // Simulate WHERE p1 <> p2 over a binding row set.
        let rows: Vec<HashMap<String, String>> = vec![
            [("p1", "alice"), ("p2", "alice")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // same → drop
            [("p1", "alice"), ("p2", "bob")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // differ → keep
            [("p1", "carol"), ("p2", "carol")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // same → drop
        ];

        let binding = "p1";
        let value = "p2"; // RHS is a binding name, not a literal
        let op = ComparisonOp::Neq;

        let result: Vec<_> = rows
            .iter()
            .filter(|row| {
                let lhs = match row.get(binding) {
                    Some(v) => v.as_str(),
                    None => return true,
                };
                let rhs: &str = match row.get(value) {
                    Some(v) => v.as_str(),
                    None => value,
                };
                apply_op(&op, lhs, rhs)
            })
            .collect();

        assert_eq!(
            result.len(),
            1,
            "only the alice→bob row survives WHERE p1 <> p2"
        );
        assert_eq!(result[0]["p1"], "alice");
        assert_eq!(result[0]["p2"], "bob");
    }

    #[test]
    fn rhs_used_as_literal_when_not_a_binding_in_row() {
        use std::collections::HashMap;

        // Simulate WHERE p1 <> 'alice' (literal not present as a binding key).
        let rows: Vec<HashMap<String, String>> = vec![
            [("p1", "alice")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // equals literal → drop
            [("p1", "bob")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(), // differs → keep
        ];

        let binding = "p1";
        let value = "alice"; // literal (no binding named "alice" in any row)
        let op = ComparisonOp::Neq;

        let result: Vec<_> = rows
            .iter()
            .filter(|row| {
                let lhs = match row.get(binding) {
                    Some(v) => v.as_str(),
                    None => return true,
                };
                let rhs: &str = match row.get(value) {
                    Some(v) => v.as_str(),
                    None => value,
                };
                apply_op(&op, lhs, rhs)
            })
            .collect();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["p1"], "bob");
    }
}
