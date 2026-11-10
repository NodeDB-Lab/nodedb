// SPDX-License-Identifier: BUSL-1.1

//! `UNION DISTINCT` merge: every row from every payload, deduplicated by
//! positional canonical identity, in encounter order.

use crate::control::server::payload_merge::{encode_msgpack_array, payload_rows};

use super::row_key::RowComparison;

/// Merge multiple row payloads and drop duplicate rows.
///
/// Payload `i` holds the flat row bodies, plain or native, of branch `i`, in
/// a shape [`payload_rows`] reads, and `columns[i]` is that branch's output
/// column keys in SELECT-list order. Two rows are duplicates when their
/// values are equal position by position (see `row_key`). The first row of
/// each identity is kept, byte for byte. Branches of different column
/// counts, a payload [`payload_rows`] refuses, or a row that does not decode
/// fail the merge. The output is one msgpack array of the unique rows in
/// encounter order.
pub(crate) fn dedup_union_payloads(
    payloads: &[Vec<u8>],
    columns: &[Vec<String>],
) -> crate::Result<Vec<u8>> {
    let compare = RowComparison::for_branches(columns, payloads.len())?;
    let mut seen: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
    let mut unique_rows: Vec<Vec<u8>> = Vec::new();
    for (branch, payload) in payloads.iter().enumerate() {
        for row in payload_rows(payload)? {
            if seen.insert(compare.identity(branch, &row)?) {
                unique_rows.push(row);
            }
        }
    }

    Ok(encode_msgpack_array(&unique_rows))
}

#[cfg(test)]
mod tests {
    use super::dedup_union_payloads;

    fn x() -> Vec<Vec<String>> {
        vec![vec!["x".to_string()], vec!["x".to_string()]]
    }

    fn encode_array(rows: &[serde_json::Value]) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows.to_vec())).unwrap()
    }

    #[test]
    fn union_distinct_drops_byte_identical_rows_across_payloads() {
        let left = encode_array(&[serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]);
        let right = encode_array(&[serde_json::json!({"x": 2}), serde_json::json!({"x": 3})]);

        let merged = dedup_union_payloads(&[left, right], &x()).unwrap();
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);

        assert_eq!(json, r#"[{"x":1},{"x":2},{"x":3}]"#);
    }

    #[test]
    fn union_distinct_of_no_payloads_is_empty_array() {
        assert_eq!(dedup_union_payloads(&[], &[]).unwrap(), vec![0x90]);
    }

    /// A point-get branch answers its one row as a bare map.
    #[test]
    fn a_point_get_branch_is_one_row() {
        let left = encode_array(&[serde_json::json!({"x": 1})]);
        let map = nodedb_types::json_to_msgpack(&serde_json::json!({"x": 2})).unwrap();
        let merged = dedup_union_payloads(&[left, map], &x()).unwrap();
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);
        assert_eq!(json, r#"[{"x":1},{"x":2}]"#);
    }

    /// A branch payload in no row shape is refused, never read as one row.
    #[test]
    fn a_scalar_branch_is_refused() {
        let left = encode_array(&[serde_json::json!({"x": 1})]);
        assert!(matches!(
            dedup_union_payloads(&[left, vec![0x2a]], &x()),
            Err(crate::Error::Codec { .. })
        ));
    }

    /// A native row and the plain row of the same values, in another map
    /// order, are one row.
    #[test]
    fn union_distinct_dedups_native_and_plain_rows() {
        use nodedb_types::Value;

        let row = Value::Object(
            [
                ("a".to_string(), Value::Integer(1)),
                ("b".to_string(), Value::String("x".into())),
            ]
            .into(),
        );
        let native = zerompk::to_msgpack_vec(&row).unwrap();
        let reordered = nodedb_types::ordered_map_to_msgpack(&[
            ("b", &Value::String("x".into())),
            ("a", &Value::Integer(1)),
        ])
        .unwrap();
        let left = crate::control::server::payload_merge::encode_msgpack_array(
            std::slice::from_ref(&native),
        );
        let right = crate::control::server::payload_merge::encode_msgpack_array(&[reordered]);
        let ab = vec!["a".to_string(), "b".to_string()];
        let merged = dedup_union_payloads(&[left, right], &[ab.clone(), ab]).unwrap();
        assert_eq!(
            merged,
            crate::control::server::payload_merge::encode_msgpack_array(&[native])
        );
    }

    /// `SELECT b, a ... UNION SELECT x, y ...` dedups by position: {b:1,a:2}
    /// and {x:1,y:2} are one row, {x:2,y:1} is another.
    #[test]
    fn union_distinct_compares_by_position() {
        let left = encode_array(&[serde_json::json!({"b": 1, "a": 2})]);
        let right = encode_array(&[
            serde_json::json!({"x": 1, "y": 2}),
            serde_json::json!({"x": 2, "y": 1}),
        ]);
        let columns = vec![
            vec!["b".to_string(), "a".to_string()],
            vec!["x".to_string(), "y".to_string()],
        ];
        let merged = dedup_union_payloads(&[left, right], &columns).unwrap();
        let rows =
            crate::control::server::payload_merge::extract_msgpack_elements(&merged).unwrap();
        assert_eq!(rows.len(), 2);
    }

    /// Branches of different column counts fail with the arity error.
    #[test]
    fn union_of_different_arity_is_refused() {
        let left = encode_array(&[serde_json::json!({"a": 1, "b": 2})]);
        let right = encode_array(&[serde_json::json!({"a": 1})]);
        let columns = vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string()],
        ];
        assert!(matches!(
            dedup_union_payloads(&[left, right], &columns),
            Err(crate::Error::BadRequest { .. })
        ));
    }
}
