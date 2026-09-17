// SPDX-License-Identifier: BUSL-1.1

//! `UNION DISTINCT` merge: every row from every payload, deduplicated on
//! its raw msgpack bytes, in encounter order.

use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};

/// Merge multiple row payloads and drop duplicate rows.
///
/// Each payload is a msgpack array of rows. A row's raw msgpack bytes are
/// its dedup key, so no decode round-trip runs. A payload that is not a
/// msgpack array is treated as one row. The output is one msgpack array of
/// the unique rows in encounter order.
pub(crate) fn dedup_union_payloads(payloads: &[Vec<u8>]) -> Vec<u8> {
    let rows: Vec<Vec<u8>> = payloads
        .iter()
        .flat_map(|payload| extract_msgpack_elements(payload))
        .collect();
    let mut seen: std::collections::HashSet<Vec<u8>> =
        std::collections::HashSet::with_capacity(rows.len());
    let mut unique_rows: Vec<Vec<u8>> = Vec::with_capacity(rows.len());

    for row in rows {
        if seen.insert(row.clone()) {
            unique_rows.push(row);
        }
    }

    encode_msgpack_array(&unique_rows)
}

#[cfg(test)]
mod tests {
    use super::dedup_union_payloads;

    fn encode_array(rows: &[serde_json::Value]) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows.to_vec())).unwrap()
    }

    #[test]
    fn union_distinct_drops_byte_identical_rows_across_payloads() {
        let left = encode_array(&[serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]);
        let right = encode_array(&[serde_json::json!({"x": 2}), serde_json::json!({"x": 3})]);

        let merged = dedup_union_payloads(&[left, right]);
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);

        assert_eq!(json, r#"[{"x":1},{"x":2},{"x":3}]"#);
    }

    #[test]
    fn union_distinct_of_no_payloads_is_empty_array() {
        assert_eq!(dedup_union_payloads(&[]), vec![0x90]);
    }
}
