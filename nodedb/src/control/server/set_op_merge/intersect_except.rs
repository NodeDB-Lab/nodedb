// SPDX-License-Identifier: BUSL-1.1

//! `INTERSECT` / `EXCEPT` merge over msgpack row payloads, comparing rows
//! by their positional canonical identity.

use crate::control::server::payload_merge::{encode_msgpack_array, payload_rows};

use super::row_key::RowComparison;

/// Which filter [`merge_set_op_payloads`] applies to the first payload.
pub(crate) enum SetMergeMode {
    /// Keep rows present in every payload.
    Intersect,
    /// Keep rows of the first payload absent from every later payload.
    Except,
}

/// Merge payloads for INTERSECT or EXCEPT.
///
/// Payload `i` holds the flat row bodies, plain or native, of branch `i`,
/// and `columns[i]` is that branch's output column keys in SELECT-list
/// order. Two rows match only when every position holds an equal value (see
/// `row_key`). The output holds the surviving rows of the first payload,
/// byte for byte and deduplicated by identity, as one msgpack array. No
/// payloads yields an empty array. Branches of different column counts, a
/// payload [`payload_rows`] refuses, or a row that does not decode fail the
/// merge.
pub(crate) fn merge_set_op_payloads(
    payloads: &[Vec<u8>],
    mode: SetMergeMode,
    columns: &[Vec<String>],
) -> crate::Result<Vec<u8>> {
    let compare = RowComparison::for_branches(columns, payloads.len())?;
    let Some((first, others)) = payloads.split_first() else {
        return Ok(encode_msgpack_array(&[]));
    };

    let first_rows = keyed_rows(&compare, 0, first)?;
    let mut other_sets: Vec<std::collections::HashSet<Vec<String>>> = Vec::new();
    for (offset, payload) in others.iter().enumerate() {
        other_sets.push(
            keyed_rows(&compare, offset + 1, payload)?
                .into_iter()
                .map(|(identity, _)| identity)
                .collect(),
        );
    }
    let keep = |identity: &Vec<String>| match mode {
        SetMergeMode::Intersect => other_sets.iter().all(|set| set.contains(identity)),
        SetMergeMode::Except => !other_sets.iter().any(|set| set.contains(identity)),
    };

    let mut seen = std::collections::HashSet::with_capacity(first_rows.len());
    let unique: Vec<Vec<u8>> = first_rows
        .into_iter()
        .filter(|(identity, _)| keep(identity) && seen.insert(identity.clone()))
        .map(|(_, row)| row)
        .collect();

    Ok(encode_msgpack_array(&unique))
}

/// Each row of branch `branch`'s payload with its identity.
fn keyed_rows(
    compare: &RowComparison<'_>,
    branch: usize,
    payload: &[u8],
) -> crate::Result<Vec<(Vec<String>, Vec<u8>)>> {
    payload_rows(payload)?
        .into_iter()
        .map(|row| Ok((compare.identity(branch, &row)?, row)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{SetMergeMode, merge_set_op_payloads};
    use crate::control::server::payload_merge::encode_msgpack_array;

    fn encode_array(rows: &[serde_json::Value]) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows.to_vec())).unwrap()
    }

    fn columns(branches: &[&[&str]]) -> Vec<Vec<String>> {
        branches
            .iter()
            .map(|keys| keys.iter().map(|k| (*k).to_string()).collect())
            .collect()
    }

    /// `SELECT b, a ... INTERSECT SELECT x, y ...` matches by position:
    /// {b:1,a:2} matches {x:1,y:2} and not {x:2,y:1}.
    #[test]
    fn intersect_compares_rows_by_position() {
        let left = encode_array(&[
            serde_json::json!({"b": 1, "a": 2}),
            serde_json::json!({"b": 3, "a": 4}),
        ]);
        let right = encode_array(&[
            serde_json::json!({"x": 1, "y": 2}),
            serde_json::json!({"x": 4, "y": 3}),
        ]);
        let declared = columns(&[&["b", "a"], &["x", "y"]]);
        let merged =
            merge_set_op_payloads(&[left, right], SetMergeMode::Intersect, &declared).unwrap();
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);
        assert_eq!(json, r#"[{"a":2,"b":1}]"#);
    }

    /// EXCEPT keeps the rows no later branch holds at every position.
    #[test]
    fn except_returns_the_surviving_rows() {
        let left = encode_array(&[
            serde_json::json!({"id": "u1"}),
            serde_json::json!({"id": "u2"}),
        ]);
        let right = encode_array(&[serde_json::json!({"user_id": "u1"})]);
        let declared = columns(&[&["id"], &["user_id"]]);
        let merged =
            merge_set_op_payloads(&[left, right], SetMergeMode::Except, &declared).unwrap();
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);
        assert_eq!(json, r#"[{"id":"u2"}]"#);
    }

    /// Branches of different column counts fail with the arity error: a
    /// two-column row never matches a one-column row.
    #[test]
    fn intersect_of_different_arity_is_refused() {
        let left = encode_array(&[serde_json::json!({"a": 1, "b": 2})]);
        let right = encode_array(&[serde_json::json!({"a": 1})]);
        let declared = columns(&[&["a", "b"], &["a"]]);
        for mode in [SetMergeMode::Intersect, SetMergeMode::Except] {
            assert!(matches!(
                merge_set_op_payloads(&[left.clone(), right.clone()], mode, &declared),
                Err(crate::Error::BadRequest { .. })
            ));
        }
    }

    /// A native row and the plain row of the same values match, and the
    /// surviving row keeps its own bytes.
    #[test]
    fn native_and_plain_rows_match_by_value() {
        use nodedb_types::Value;

        let row = Value::Object(
            [
                ("price".to_string(), Value::Decimal("5.25".parse().unwrap())),
                ("n".to_string(), Value::Integer(7)),
            ]
            .into(),
        );
        let native = zerompk::to_msgpack_vec(&row).unwrap();
        // A plain map keeps a decimal typed only as the decimal ext.
        let plain = nodedb_types::value_to_typed_msgpack(&Value::Object(
            [
                ("n".to_string(), Value::Integer(7)),
                (
                    "price".to_string(),
                    Value::Decimal("5.250".parse().unwrap()),
                ),
            ]
            .into(),
        ))
        .unwrap();
        let left = encode_msgpack_array(std::slice::from_ref(&native));
        let right = encode_msgpack_array(std::slice::from_ref(&plain));
        let declared = columns(&[&["price", "n"], &["price", "n"]]);

        let merged = merge_set_op_payloads(
            &[left.clone(), right.clone()],
            SetMergeMode::Intersect,
            &declared,
        )
        .unwrap();
        assert_eq!(merged, left);
        let merged =
            merge_set_op_payloads(&[left, right], SetMergeMode::Except, &declared).unwrap();
        assert_eq!(merged, encode_msgpack_array(&[]));
    }

    #[test]
    fn no_payloads_is_an_empty_array() {
        assert_eq!(
            merge_set_op_payloads(&[], SetMergeMode::Intersect, &[]).unwrap(),
            vec![0x90]
        );
    }

    /// A branch payload in no row shape is refused in either mode.
    #[test]
    fn a_scalar_branch_is_refused() {
        let left = encode_array(&[serde_json::json!({"id":"u1"})]);
        for mode in [SetMergeMode::Intersect, SetMergeMode::Except] {
            assert!(matches!(
                merge_set_op_payloads(
                    &[left.clone(), vec![0x2a]],
                    mode,
                    &columns(&[&["id"], &["id"]])
                ),
                Err(crate::Error::Codec { .. })
            ));
        }
    }
}
