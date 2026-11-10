// SPDX-License-Identifier: BUSL-1.1

//! Reads and writes on a typed [`BindingRow`].
//!
//! A variable is unbound (absent), bound to `Value::Null` (an `OPTIONAL
//! MATCH` that found nothing), or bound to a node name or edge text.
//!
//! A row also carries hidden keys: an edge variable's property object,
//! captured where the edge is traversed, and each ORDER BY key, evaluated
//! before projection so the coordinator can sort merged rows by a key the
//! RETURN list does not project. A hidden key opens with a NUL byte, which
//! no MATCH variable name holds, so it never collides with a binding. The
//! coordinator strips every hidden key before a row reaches a client.

use nodedb_types::Value;

use super::types::BindingRow;

/// What a row binds a variable to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Bound<'r> {
    /// The row does not bind the variable.
    Unbound,
    /// The variable is bound to SQL NULL: it matched nothing.
    Null,
    /// The variable is bound to this node name or edge text.
    Text(&'r str),
}

/// What `row` binds `var` to. A value that is not text, as only a projected
/// column holds, reads as `Null`: it names no node.
pub(super) fn lookup<'r>(row: &'r BindingRow, var: &str) -> Bound<'r> {
    match row.get(var) {
        None => Bound::Unbound,
        Some(Value::String(text)) => Bound::Text(text),
        Some(_) => Bound::Null,
    }
}

/// Bind `var` to `text` unless the row already binds it.
pub(super) fn bind_text(row: &mut BindingRow, var: &str, text: &str) {
    row.entry(var.to_string())
        .or_insert_with(|| Value::String(text.to_string()));
}

/// The prefix of a hidden edge-property key.
const EDGE_PROPERTIES_PREFIX: &str = "\u{0}edge-properties:";

/// The hidden key carrying edge variable `var`'s property object.
pub(super) fn edge_properties_key(var: &str) -> String {
    format!("{EDGE_PROPERTIES_PREFIX}{var}")
}

/// The property object `row` carries for edge variable `var`. `None` when
/// the row binds no edge to `var` or captured none.
pub(super) fn edge_properties<'r>(row: &'r BindingRow, var: &str) -> Option<&'r Value> {
    row.get(&edge_properties_key(var))
}

/// The prefix every hidden key opens with.
const HIDDEN_PREFIX: char = '\u{0}';

/// The prefix of a hidden ORDER BY key.
const ORDER_KEY_PREFIX: &str = "\u{0}order:";

/// The hidden key carrying the row's value for ORDER BY column `index`.
pub(super) fn order_key(index: usize) -> String {
    format!("{ORDER_KEY_PREFIX}{index}")
}

/// Remove every hidden key: a row that reaches a client carries its
/// bindings or projected columns only.
pub(super) fn strip_hidden(row: &mut BindingRow) {
    row.retain(|key, _| !key.starts_with(HIDDEN_PREFIX));
}

/// [`row_fingerprint`] of the row's visible entries only, for DISTINCT.
pub(super) fn visible_fingerprint(row: &BindingRow) -> crate::Result<Vec<u8>> {
    let mut visible = row.clone();
    strip_hidden(&mut visible);
    row_fingerprint(&visible)
}

/// A byte image of `row` that two rows share exactly when they hold the
/// same entries. The tagged encoding of an object sorts its keys at every
/// level, so `HashMap` order does not reach it.
pub fn row_fingerprint(row: &BindingRow) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&Value::Object(row.clone())).map_err(|e| crate::Error::Codec {
        detail: format!("MATCH row fingerprint: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_tells_unbound_null_and_text_apart() {
        let mut row = BindingRow::new();
        row.insert("n".into(), Value::Null);
        bind_text(&mut row, "a", "alice");
        bind_text(&mut row, "a", "bob");
        assert_eq!(lookup(&row, "a"), Bound::Text("alice"), "a binding is kept");
        assert_eq!(lookup(&row, "n"), Bound::Null);
        assert_eq!(lookup(&row, "x"), Bound::Unbound);
    }

    #[test]
    fn equal_rows_share_a_fingerprint_in_any_order() {
        let mut a = BindingRow::new();
        let mut b = BindingRow::new();
        for (k, v) in [("x", 1), ("y", 2), ("z", 3)] {
            a.insert(k.into(), Value::Integer(v));
        }
        for (k, v) in [("z", 3), ("y", 2), ("x", 1)] {
            b.insert(k.into(), Value::Integer(v));
        }
        assert_eq!(row_fingerprint(&a).unwrap(), row_fingerprint(&b).unwrap());
        b.insert("x".into(), Value::Integer(9));
        assert_ne!(row_fingerprint(&a).unwrap(), row_fingerprint(&b).unwrap());
    }

    #[test]
    fn hidden_edge_properties_are_stripped() {
        let mut row = BindingRow::new();
        bind_text(&mut row, "r", "a|L|b");
        row.insert(edge_properties_key("r"), Value::Object(Default::default()));
        assert!(edge_properties(&row, "r").is_some());
        row.insert(order_key(0), Value::Integer(1));
        assert_ne!(
            visible_fingerprint(&row).unwrap(),
            row_fingerprint(&row).unwrap()
        );
        strip_hidden(&mut row);
        assert_eq!(row.len(), 1);
        assert!(edge_properties(&row, "r").is_none());
    }
}
