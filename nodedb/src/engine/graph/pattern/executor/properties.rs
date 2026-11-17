// SPDX-License-Identifier: BUSL-1.1

//! Property reads for `WHERE` and `RETURN`: `a.field` for a node variable,
//! `r.field` for a relationship variable.
//!
//! A node's properties live as a document in the sparse engine. A
//! relationship's properties are the object the row captured where the edge
//! was bound ([`super::edge_props`]). Both answer the exact stored
//! [`Value`], and both compare through the shared `value_ops` comparison.

use std::cmp::Ordering;
use std::collections::HashSet;

use nodedb_query::value_ops::{coerced_eq, partial_compare_values};
use nodedb_types::Value;

use super::super::ast::ComparisonOp;
use super::predicates::{DocumentCut, PropertyLookup};
use super::row::{Bound, edge_properties, lookup};
use super::types::BindingRow;

/// What the query tail reads properties with: which variables are edges,
/// and how a node's document is found.
pub(super) struct QueryTail<'a> {
    /// The query's edge variables. A dotted access on one of these reads the
    /// relationship's properties.
    pub edge_vars: &'a HashSet<String>,
    pub props: &'a PropertyLookup<'a>,
}

impl QueryTail<'_> {
    /// The value of `binding.field` in `row`. `Ok(None)` when the row binds
    /// `binding` to nothing, or the node or edge holds no such field.
    pub(super) fn property(
        &self,
        row: &BindingRow,
        binding: &str,
        field: &str,
    ) -> crate::Result<Option<Value>> {
        let name = match lookup(row, binding) {
            Bound::Unbound | Bound::Null => return Ok(None),
            Bound::Text(name) => name,
        };
        if !self.edge_vars.contains(binding) {
            return node_property(self.props, name, field);
        }
        match edge_properties(row, binding) {
            Some(Value::Object(fields)) => Ok(fields.get(field).cloned()),
            Some(other) => Err(crate::Error::Internal {
                detail: format!(
                    "MATCH: relationship '{binding}' carries properties that are not an \
                     object: {other:?}"
                ),
            }),
            None => Err(crate::Error::Internal {
                detail: format!(
                    "MATCH: relationship '{binding}' reached '{binding}.{field}' without its \
                     captured properties"
                ),
            }),
        }
    }
}

/// `field` of node `node_id`'s stored document. `Ok(None)` when the node has
/// no document or the document lacks `field`. A query with no
/// `IN '<collection>'` is a `BadRequest`: a property is unresolvable without
/// one, so it is never a silent NULL.
pub(super) fn node_property(
    props: &PropertyLookup<'_>,
    node_id: &str,
    field: &str,
) -> crate::Result<Option<Value>> {
    let collection = props.collection.ok_or_else(|| crate::Error::BadRequest {
        detail: format!(
            "MATCH property `{node_id}.{field}` requires an `IN '<collection>'` clause to \
             resolve node properties"
        ),
    })?;
    Ok(match fetch_node_doc(props, collection, node_id)? {
        Some(Value::Object(mut map)) => map.remove(field),
        Some(_) | None => None,
    })
}

/// `stored <op> expected` through the shared typed comparison
/// (`value_ops`): numbers of any kind compare by value, instants by time.
pub(super) fn compare(op: ComparisonOp, stored: &Value, expected: &Value) -> bool {
    let order = || partial_compare_values(stored, expected);
    match op {
        ComparisonOp::Eq => coerced_eq(stored, expected),
        ComparisonOp::Neq => !coerced_eq(stored, expected),
        ComparisonOp::Lt => order() == Some(Ordering::Less),
        ComparisonOp::Lte => matches!(order(), Some(Ordering::Less | Ordering::Equal)),
        ComparisonOp::Gt => order() == Some(Ordering::Greater),
        ComparisonOp::Gte => matches!(order(), Some(Ordering::Greater | Ordering::Equal)),
    }
}

/// Fetch and decode the stored document for `node_id` within `collection`.
///
/// Returns `Ok(Some(doc))` when the document is present and decoded,
/// `Ok(None)` when the node has no stored document, and `Err` for storage
/// or decode errors.
///
/// The document is keyed by the node's GLOBAL SURROGATE, not by the node-id
/// string: `node_id → Surrogate` resolves through the CSR (a graph node and
/// its same-pk document share one surrogate), and `StorageKey::for_surrogate`
/// gives the redb key. A node unknown to the partition, or with no surrogate
/// set, has no document.
fn fetch_node_doc(
    props: &PropertyLookup<'_>,
    collection: &str,
    node_id: &str,
) -> crate::Result<Option<Value>> {
    let Some(surrogate) = props.csr.node_surrogate(node_id) else {
        return Ok(None);
    };
    let doc_id = crate::engine::document::store::key::StorageKey::for_surrogate(surrogate);
    let stored = match props.document_cut {
        DocumentCut::Plain => {
            props
                .sparse
                .get(props.database_id, props.tenant_id, collection, &doc_id)?
        }
        DocumentCut::Versioned {
            system_as_of,
            valid_at_ms,
        } => props.sparse.versioned_get_as_of(
            props.database_id,
            props.tenant_id,
            collection,
            &doc_id,
            system_as_of,
            valid_at_ms,
        )?,
    };
    let Some(bytes) = stored else {
        return Ok(None);
    };

    // A stored body is an object context: a plain map or a native object.
    let doc = nodedb_types::decode_object(&bytes).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("decode graph node `{node_id}` document: {e}"),
    })?;
    Ok(Some(doc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_orders_typed_values() {
        let price = Value::Decimal("12.50".parse().expect("decimal"));
        assert!(compare(ComparisonOp::Gt, &price, &Value::Integer(12)));
        assert!(compare(ComparisonOp::Lt, &price, &Value::Float(13.0)));
        assert!(compare(ComparisonOp::Eq, &price, &Value::Float(12.5)));
        assert!(!compare(ComparisonOp::Gt, &price, &Value::Integer(13)));
        assert!(compare(ComparisonOp::Neq, &price, &Value::Integer(12)));
    }
}
