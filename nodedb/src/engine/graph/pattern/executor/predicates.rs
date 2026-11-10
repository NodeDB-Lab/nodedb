// SPDX-License-Identifier: BUSL-1.1

//! WHERE predicate application and RETURN column projection.

use nodedb_types::Value;

use super::super::ast::*;
use super::core::execute_clause;
use super::expansion::VarLenCaps;
pub(super) use super::properties::QueryTail;
use super::properties::compare;
use super::row::{Bound, lookup};
use super::types::{BindingRow, ExecutionState};
use crate::engine::graph::csr::{CsrIndex, GraphOverlayDelta};
use crate::engine::graph::edge_store::EdgeStore;
use crate::engine::sparse::btree::SparseEngine;

/// Borrowed context needed to resolve a MATCH property predicate
/// (`WHERE a.field = 'v'`) against a node's stored document.
///
/// A graph node's properties live as a document in the sparse engine. The
/// document is NOT keyed by the user-visible node-id string — it is keyed by
/// `StorageKey::for_surrogate(surrogate)`, the fixed-width hex form of the
/// row's global surrogate. A graph node and its same-pk document share one
/// surrogate (the CSR node surrogate is set from the edge surrogate
/// allocated by the same pk-keyed allocator), so the fetch chain is:
/// `node name → Surrogate (via `csr`) → StorageKey → sparse.get`.
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
    /// The `IN '<collection>'` collection under the name the edge store and the
    /// CSR hold it by, with its database's qualifier.
    pub collection: Option<&'a str>,
    /// Where the node collection keeps its documents, and the cut a
    /// versioned read takes.
    pub document_cut: DocumentCut,
    /// The cut a relationship property is read at: the MATCH's own graph
    /// read scope.
    pub edge_cut: nodedb_physical::physical_plan::GraphReadScope,
    /// The reading transaction's staged edge puts in the collection. A
    /// staged put's property object replaces the stored one.
    pub staged_edges: Option<&'a super::edge_props::StagedEdgeProperties>,
}

/// How a node's document is read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DocumentCut {
    /// A plain collection: the current document.
    #[default]
    Plain,
    /// A bitemporal collection: the newest version at or before the
    /// system-time ordinal `system_as_of` (`None` = current) whose valid
    /// time contains `valid_at_ms` (`None` = any). The ordinal is the cut
    /// the MATCH reads its edges at, so documents and edges split alike.
    Versioned {
        system_as_of: Option<i64>,
        valid_at_ms: Option<i64>,
    },
}

/// Apply a WHERE predicate to filter rows.
///
/// `varlen_caps` carries the same per-expansion caps as the outer query so a
/// variable-length sub-pattern (e.g. inside `NOT EXISTS`) truncates at the
/// configured ceiling rather than a hardcoded one.
///
/// `tail` resolves property predicates: `WHERE a.field = 'v'` against each
/// bound node's stored document, and `WHERE r.field > 5` against the property
/// object a relationship row captured where the edge was bound. Both compare
/// the exact stored value against the literal through the shared typed
/// comparison. A row whose variable is unbound or NULL, or whose node or edge
/// lacks the field, fails a property predicate.
pub(super) fn apply_predicate(
    rows: &[BindingRow],
    predicate: &WherePredicate,
    csr: &CsrIndex,
    _edge_store: &EdgeStore,
    varlen_caps: VarLenCaps,
    tail: &QueryTail<'_>,
    overlay: Option<&GraphOverlayDelta>,
) -> Result<Vec<BindingRow>, crate::Error> {
    match predicate {
        WherePredicate::Equals {
            binding,
            field,
            value,
        } => filter_by(rows, tail, binding, field, ComparisonOp::Eq, value),
        WherePredicate::Comparison {
            binding,
            field,
            op,
            value,
        } => filter_by(rows, tail, binding, field, *op, value),
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
                    super::expansion::resolve_collection_filter(tail.props.collection, csr);
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

/// Keep the rows where `binding[.field] <op> value` holds.
///
/// An empty `field` compares node identities: `WHERE a = 'alice'`,
/// `WHERE a <> b` or `WHERE a <> 'alice'`. The parser stores `value` as the
/// raw RHS string, either a binding name (`p3` from `WHERE p1 <> p3`) or a
/// stripped literal. `=` compares against the literal. Another operator
/// compares a `value` the row binds as that binding (binding-vs-binding, the
/// LSQB anti-self-join filter), else the literal. An unbound left side fails
/// `=` and keeps the row for every other operator, which is unevaluable
/// there. A side bound to NULL drops the row: NULL compares as unknown.
///
/// A non-empty `field` reads the property through `tail` and compares it
/// against the coerced literal. The literal is coerced once per predicate.
fn filter_by(
    rows: &[BindingRow],
    tail: &QueryTail<'_>,
    binding: &str,
    field: &str,
    op: ComparisonOp,
    value: &str,
) -> Result<Vec<BindingRow>, crate::Error> {
    if field.is_empty() {
        return Ok(rows
            .iter()
            .filter(|row| {
                let lhs = match lookup(row, binding) {
                    Bound::Text(text) => text,
                    Bound::Null => return false,
                    // An Eq on an unbound identity is false, an ordering or
                    // Neq is unevaluable and keeps the row.
                    Bound::Unbound => return op != ComparisonOp::Eq,
                };
                // `=` compares against the literal. Every other operator
                // reads a `value` the row binds as that binding.
                let rhs = match (op, lookup(row, value)) {
                    (ComparisonOp::Eq, _) | (_, Bound::Unbound) => value,
                    (_, Bound::Text(text)) => text,
                    (_, Bound::Null) => return false,
                };
                apply_op(&op, lhs, rhs)
            })
            .cloned()
            .collect());
    }
    let expected = coerce_literal(value);
    let mut kept = Vec::new();
    for row in rows {
        let keep = match tail.property(row, binding, field)? {
            Some(stored) => compare(op, &stored, &expected),
            None => false,
        };
        if keep {
            kept.push(row.clone());
        }
    }
    Ok(kept)
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

/// Test-only entry to the node-property predicate: `node_id.field <op>
/// expected` against the node's stored document, `expected` coerced as a
/// WHERE literal is. A node without a document, or a document without the
/// field, fails.
#[cfg(test)]
pub(super) fn check_property_for_test(
    props: &PropertyLookup<'_>,
    node_id: &str,
    field: &str,
    op: &ComparisonOp,
    expected: &str,
) -> Result<bool, crate::Error> {
    let expected_value = coerce_literal(expected);
    Ok(super::properties::node_property(props, node_id, field)?
        .is_some_and(|stored| compare(*op, &stored, &expected_value)))
}

/// Project RETURN columns from rows, each cell the exact typed value.
///
/// A non-dotted expr (`a`, or an aliased binding) projects what the row
/// binds: a node name, edge text, or NULL. A dotted expr projects a
/// property: `a.field` from node `a`'s stored document, `r.field` from the
/// property object relationship `r` captured. A variable that is unbound or
/// NULL, a node with no document, and a missing field project NULL. A node
/// property with no `IN '<collection>'` clause is a typed `BadRequest`,
/// never a silent NULL.
pub(super) fn project_columns(
    rows: &[BindingRow],
    columns: &[ReturnColumn],
    tail: &QueryTail<'_>,
) -> Result<Vec<BindingRow>, crate::Error> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut projected = BindingRow::new();
        for col in columns {
            let key = col.alias.as_deref().unwrap_or(&col.expr);
            let value = match col.expr.split_once('.') {
                Some((binding, field)) => tail.property(row, binding, field)?,
                None => row.get(&col.expr).cloned(),
            };
            projected.insert(key.to_string(), value.unwrap_or(Value::Null));
        }
        out.push(projected);
    }
    Ok(out)
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
