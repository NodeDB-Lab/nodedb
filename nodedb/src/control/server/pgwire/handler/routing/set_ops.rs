//! Set operation payload merging: UNION DISTINCT, INTERSECT, EXCEPT.
//!
//! Operates on raw msgpack payloads — no decode/re-encode round-trip.

use crate::control::planner::physical::PostSetOp;

use super::super::plan::{PlanKind, payload_to_response};

/// Apply set operation merging to collected sub-query payloads.
pub(super) fn apply_set_ops(
    dedup_payloads: &[Vec<u8>],
    dedup_set_op: PostSetOp,
) -> pgwire::api::results::Response {
    let merged = match dedup_set_op {
        PostSetOp::Intersect | PostSetOp::IntersectAll => {
            merge_set_op_payloads(dedup_payloads, SetMergeMode::Intersect)
        }
        PostSetOp::Except | PostSetOp::ExceptAll => {
            merge_set_op_payloads(dedup_payloads, SetMergeMode::Except)
        }
        _ => dedup_union_payloads(dedup_payloads),
    };
    payload_to_response(&merged, PlanKind::MultiRow).response
}

/// Merge multiple Data Plane response payloads and deduplicate rows (UNION DISTINCT).
///
/// Each payload is a msgpack-encoded array of rows. Deduplication is performed
/// at the binary level: each row's raw msgpack bytes serve as the canonical key,
/// eliminating the decode → JSON string → re-encode round-trip.
///
/// Output: a single msgpack array containing all unique rows in encounter order.
fn dedup_union_payloads(payloads: &[Vec<u8>]) -> Vec<u8> {
    use nodedb_query::msgpack_scan;

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut unique_row_bytes: Vec<Vec<u8>> = Vec::new();

    for payload in payloads {
        if payload.is_empty() {
            continue;
        }

        let bytes = payload.as_slice();
        let first = bytes[0];

        let (count, hdr_len) = if (0x90..=0x9f).contains(&first) {
            ((first & 0x0f) as usize, 1)
        } else if first == 0xdc && bytes.len() >= 3 {
            (u16::from_be_bytes([bytes[1], bytes[2]]) as usize, 3)
        } else if first == 0xdd && bytes.len() >= 5 {
            (
                u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize,
                5,
            )
        } else {
            tracing::warn!(
                payload_len = bytes.len(),
                "dedup_union_payloads: payload is not a msgpack array; treating as single row"
            );
            let key = bytes.to_vec();
            if seen.insert(key.clone()) {
                unique_row_bytes.push(key);
            }
            continue;
        };

        let mut pos = hdr_len;
        for _ in 0..count {
            if pos >= bytes.len() {
                break;
            }
            let elem_start = pos;
            match msgpack_scan::skip_value(bytes, pos) {
                Some(next_pos) => {
                    let row_bytes = bytes[elem_start..next_pos].to_vec();
                    if seen.insert(row_bytes.clone()) {
                        unique_row_bytes.push(row_bytes);
                    }
                    pos = next_pos;
                }
                None => {
                    tracing::warn!(
                        pos,
                        payload_len = bytes.len(),
                        "dedup_union_payloads: could not skip msgpack element; stopping early"
                    );
                    break;
                }
            }
        }
    }

    let row_count = unique_row_bytes.len();
    let total_data: usize = unique_row_bytes.iter().map(|r| r.len()).sum();
    let mut out = Vec::with_capacity(total_data + 5);
    write_array_header(&mut out, row_count);
    for row in unique_row_bytes {
        out.extend_from_slice(&row);
    }
    out
}

enum SetMergeMode {
    Intersect,
    Except,
}

/// Merge payloads for INTERSECT or EXCEPT set operations.
///
/// For INTERSECT: keep rows that appear in ALL payloads.
/// For EXCEPT: keep rows from first payload that don't appear in any subsequent payload.
fn merge_set_op_payloads(payloads: &[Vec<u8>], mode: SetMergeMode) -> Vec<u8> {
    use nodedb_query::msgpack_scan;

    if payloads.is_empty() {
        return vec![0x90];
    }

    fn extract_rows(payload: &[u8]) -> Vec<Vec<u8>> {
        if payload.is_empty() {
            return Vec::new();
        }
        let first = payload[0];
        let (count, hdr_len) = if (0x90..=0x9f).contains(&first) {
            ((first & 0x0f) as usize, 1)
        } else if first == 0xdc && payload.len() >= 3 {
            (u16::from_be_bytes([payload[1], payload[2]]) as usize, 3)
        } else if first == 0xdd && payload.len() >= 5 {
            (
                u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]) as usize,
                5,
            )
        } else {
            return vec![payload.to_vec()];
        };

        let mut rows = Vec::with_capacity(count);
        let mut pos = hdr_len;
        for _ in 0..count {
            if pos >= payload.len() {
                break;
            }
            let start = pos;
            match msgpack_scan::skip_value(payload, pos) {
                Some(next) => {
                    rows.push(payload[start..next].to_vec());
                    pos = next;
                }
                None => break,
            }
        }
        rows
    }

    fn logical_row_bytes(row: &[u8]) -> &[u8] {
        msgpack_scan::extract_field(row, 0, "data")
            .map(|(start, end)| &row[start..end])
            .unwrap_or(row)
    }

    fn write_values_only_key(value: &[u8], out: &mut Vec<u8>) -> Option<()> {
        if let Some((count, mut pos)) = msgpack_scan::map_header(value, 0) {
            write_array_header(out, count);
            for _ in 0..count {
                pos = msgpack_scan::skip_value(value, pos)?;
                let val_start = pos;
                pos = msgpack_scan::skip_value(value, pos)?;
                write_values_only_key(&value[val_start..pos], out)?;
            }
            return Some(());
        }

        if let Some((count, mut pos)) = msgpack_scan::array_header(value, 0) {
            write_array_header(out, count);
            for _ in 0..count {
                let elem_start = pos;
                pos = msgpack_scan::skip_value(value, pos)?;
                write_values_only_key(&value[elem_start..pos], out)?;
            }
            return Some(());
        }

        out.extend_from_slice(value);
        Some(())
    }

    fn extract_value_parts(row: &[u8]) -> Vec<Vec<u8>> {
        let logical = logical_row_bytes(row);

        if let Some((count, mut pos)) = msgpack_scan::map_header(logical, 0) {
            let mut parts = Vec::with_capacity(count);
            for _ in 0..count {
                pos = match msgpack_scan::skip_value(logical, pos) {
                    Some(next) => next,
                    None => return vec![logical.to_vec()],
                };
                let val_start = pos;
                pos = match msgpack_scan::skip_value(logical, pos) {
                    Some(next) => next,
                    None => return vec![logical.to_vec()],
                };
                let mut normalized = Vec::with_capacity(pos - val_start);
                if write_values_only_key(&logical[val_start..pos], &mut normalized).is_none() {
                    return vec![logical.to_vec()];
                }
                parts.push(normalized);
            }
            return parts;
        }

        if let Some((count, mut pos)) = msgpack_scan::array_header(logical, 0) {
            let mut parts = Vec::with_capacity(count);
            for _ in 0..count {
                let elem_start = pos;
                pos = match msgpack_scan::skip_value(logical, pos) {
                    Some(next) => next,
                    None => return vec![logical.to_vec()],
                };
                let mut normalized = Vec::with_capacity(pos - elem_start);
                if write_values_only_key(&logical[elem_start..pos], &mut normalized).is_none() {
                    return vec![logical.to_vec()];
                }
                parts.push(normalized);
            }
            return parts;
        }

        vec![logical.to_vec()]
    }

    fn extract_values_key(row: &[u8]) -> Vec<u8> {
        let parts = extract_value_parts(row);
        let mut vals = Vec::new();
        write_array_header(&mut vals, parts.len());
        for part in parts {
            vals.extend_from_slice(&part);
        }
        vals
    }

    fn rows_match(left: &[u8], right: &[u8]) -> bool {
        let left_parts = extract_value_parts(left);
        let right_parts = extract_value_parts(right);
        let shared_len = left_parts.len().min(right_parts.len());

        if shared_len == 0 {
            return left_parts.is_empty() && right_parts.is_empty();
        }

        left_parts[..shared_len] == right_parts[..shared_len]
            && (left_parts.len() == shared_len || right_parts.len() == shared_len)
    }

    let first_rows = extract_rows(&payloads[0]);
    let mut result_rows: Vec<Vec<u8>> = match mode {
        SetMergeMode::Intersect => {
            let other_rows: Vec<Vec<Vec<u8>>> =
                payloads[1..].iter().map(|p| extract_rows(p)).collect();
            first_rows
                .into_iter()
                .filter(|row| {
                    other_rows
                        .iter()
                        .all(|rows| rows.iter().any(|other| rows_match(row, other)))
                })
                .map(|row| logical_row_bytes(&row).to_vec())
                .collect()
        }
        SetMergeMode::Except => {
            let other_rows: Vec<Vec<u8>> =
                payloads[1..].iter().flat_map(|p| extract_rows(p)).collect();
            first_rows
                .into_iter()
                .filter(|row| !other_rows.iter().any(|other| rows_match(row, other)))
                .map(|row| logical_row_bytes(&row).to_vec())
                .collect()
        }
    };

    let mut seen = std::collections::HashSet::new();
    result_rows.retain(|r| seen.insert(extract_values_key(r)));

    let row_count = result_rows.len();
    let total: usize = result_rows.iter().map(|r| r.len()).sum();
    let mut out = Vec::with_capacity(total + 5);
    write_array_header(&mut out, row_count);
    for row in result_rows {
        out.extend_from_slice(&row);
    }
    out
}

fn write_array_header(out: &mut Vec<u8>, count: usize) {
    if count < 16 {
        out.push(0x90 | count as u8);
    } else if count <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(count as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(count as u32).to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::{SetMergeMode, merge_set_op_payloads};

    fn encode_array(rows: &[serde_json::Value]) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows.to_vec())).unwrap()
    }

    #[test]
    fn intersect_compares_wrapped_rows_by_logical_data_values() {
        let left = encode_array(&[
            serde_json::json!({"id":"u1","data":{"id":"u1","name":"Alice"}}),
            serde_json::json!({"id":"u2","data":{"id":"u2","name":"Bob"}}),
        ]);
        let right = encode_array(&[
            serde_json::json!({"id":"doc-1","data":{"user_id":"u1"}}),
            serde_json::json!({"id":"doc-2","data":{"user_id":"u3"}}),
        ]);

        let merged = merge_set_op_payloads(&[left, right], SetMergeMode::Intersect);
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);

        assert_eq!(json, r#"[{"id":"u1","name":"Alice"}]"#);
    }

    #[test]
    fn except_returns_unwrapped_logical_rows() {
        let left = encode_array(&[
            serde_json::json!({"id":"u1","data":{"id":"u1"}}),
            serde_json::json!({"id":"u2","data":{"id":"u2"}}),
        ]);
        let right = encode_array(&[serde_json::json!({"id":"doc-1","data":{"user_id":"u1"}})]);

        let merged = merge_set_op_payloads(&[left, right], SetMergeMode::Except);
        let json = crate::data::executor::response_codec::decode_payload_to_json(&merged);

        assert_eq!(json, r#"[{"id":"u2"}]"#);
    }
}
