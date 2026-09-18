// SPDX-License-Identifier: BUSL-1.1

//! Row-identity helpers for set-operation merging: the logical row inside a
//! `{id, data}` wrapper, and the values-only key that compares two rows by
//! column values regardless of column names.

use nodedb_query::msgpack_scan;

/// The logical row: the `data` field of an `{id, data}` storage wrapper, or
/// the row itself when it carries no wrapper.
pub(super) fn logical_row_bytes(row: &[u8]) -> &[u8] {
    msgpack_scan::extract_field(row, 0, "data")
        .map(|(start, end)| &row[start..end])
        .unwrap_or(row)
}

/// Write a msgpack array header for `count` elements.
pub(super) fn write_array_header(out: &mut Vec<u8>, count: usize) {
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

/// Append `value` to `out` with every map rewritten as an array of its
/// values, recursively, so two rows with different column names but equal
/// values compare equal. `None` when `value` is malformed msgpack.
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

/// The logical row's column values, each normalized to a values-only key,
/// in column order. A scalar or malformed row yields itself as one part.
pub(super) fn extract_value_parts(row: &[u8]) -> Vec<Vec<u8>> {
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

/// One key per row: its value parts as a msgpack array.
pub(super) fn extract_values_key(row: &[u8]) -> Vec<u8> {
    let parts = extract_value_parts(row);
    let mut vals = Vec::new();
    write_array_header(&mut vals, parts.len());
    for part in parts {
        vals.extend_from_slice(&part);
    }
    vals
}

/// Whether two rows match by value: their shared column prefix is equal and
/// one row has no columns beyond that prefix.
pub(super) fn rows_match(left: &[u8], right: &[u8]) -> bool {
    let left_parts = extract_value_parts(left);
    let right_parts = extract_value_parts(right);
    let shared_len = left_parts.len().min(right_parts.len());

    if shared_len == 0 {
        return left_parts.is_empty() && right_parts.is_empty();
    }

    left_parts[..shared_len] == right_parts[..shared_len]
        && (left_parts.len() == shared_len || right_parts.len() == shared_len)
}
