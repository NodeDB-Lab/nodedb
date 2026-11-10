// SPDX-License-Identifier: BUSL-1.1

//! Pure msgpack merge/filter helpers for the clone CoW read-path.

/// If `payload` is a single row from a point-get (a msgpack map or a native
/// object `[7, {…}]`), wrap it as a 1-element msgpack array so that tombstone
/// filters and `merge_msgpack_arrays` operate on a uniform array shape. If
/// `payload` is already a row array (from a scan) or is empty, return it
/// unchanged.
pub(super) fn wrap_single_map_as_array(payload: Vec<u8>) -> Vec<u8> {
    use nodedb_query::msgpack_scan;
    if payload.is_empty() {
        return payload;
    }
    // A native object is one row, though its bytes open with an array header.
    let single_native_row = crate::data::executor::response_codec::is_native_row(&payload);
    // Already a row array — leave as-is.
    if !single_native_row && msgpack_scan::array_header(&payload, 0).is_some() {
        return payload;
    }
    // Single map row: wrap as fixarray(1) + map bytes.
    let mut buf = Vec::with_capacity(1 + payload.len());
    buf.push(0x91); // fixarray with 1 element
    buf.extend_from_slice(&payload);
    buf
}

/// Merge two msgpack arrays into one by concatenating their elements.
///
/// If either slice is empty, returns the other unchanged. Both inputs MUST
/// have been passed through `wrap_single_map_as_array` first; if either
/// non-empty input is not a valid msgpack array header an error is returned
/// — silently re-encoding a bogus header on top of the concatenated bytes
/// would corrupt downstream parsers in a way no caller could detect.
pub(super) fn merge_msgpack_arrays(a: &[u8], b: &[u8]) -> crate::Result<Vec<u8>> {
    use nodedb_query::msgpack_scan;

    if a.is_empty() {
        return Ok(b.to_vec());
    }
    if b.is_empty() {
        return Ok(a.to_vec());
    }

    let (count_a, body_a_start) = msgpack_scan::array_header(a, 0).ok_or_else(|| {
        crate::Error::Storage {
            engine: "clone_merge".into(),
            detail: format!(
                "merge_msgpack_arrays: left input is not a msgpack array (len={}, first_byte=0x{:02x})",
                a.len(),
                a.first().copied().unwrap_or(0)
            ),
        }
    })?;
    let (count_b, body_b_start) = msgpack_scan::array_header(b, 0).ok_or_else(|| {
        crate::Error::Storage {
            engine: "clone_merge".into(),
            detail: format!(
                "merge_msgpack_arrays: right input is not a msgpack array (len={}, first_byte=0x{:02x})",
                b.len(),
                b.first().copied().unwrap_or(0)
            ),
        }
    })?;
    let total = count_a + count_b;
    let body_a = &a[body_a_start..];
    let body_b = &b[body_b_start..];

    let mut buf = Vec::with_capacity(5 + body_a.len() + body_b.len());

    // Write array header for `total`.
    if total <= 15 {
        buf.push(0x90 | (total as u8));
    } else if total <= 0xFFFF {
        buf.push(0xdc);
        buf.push((total >> 8) as u8);
        buf.push(total as u8);
    } else {
        buf.push(0xdd);
        buf.push((total >> 24) as u8);
        buf.push((total >> 16) as u8);
        buf.push((total >> 8) as u8);
        buf.push(total as u8);
    }

    buf.extend_from_slice(body_a);
    buf.extend_from_slice(body_b);
    Ok(buf)
}

/// Filter a msgpack array of KV rows, removing any whose `"key"` field is in
/// `tombstoned`.
///
/// KV scan responses are msgpack arrays of maps.  Each row map may have a `"key"`
/// field (injected by `apply_kv_wrap` for point-get responses, or
/// already present for typed KV scans).  Rows whose `"key"` value is in the
/// tombstoned set are excluded from the result.
///
/// Returns `None` when the input is not a well-formed msgpack array (caller
/// falls back to the original slice unchanged).  Returns `Some(bytes)` — which
/// may be a shorter array or the original bytes when nothing was filtered.
pub(super) fn filter_kv_tombstoned_rows(
    payload: &[u8],
    tombstoned: &std::collections::HashSet<String>,
) -> Option<Vec<u8>> {
    use nodedb_query::msgpack_scan;

    if tombstoned.is_empty() || payload.is_empty() {
        return Some(payload.to_vec());
    }

    // Callers (clone_read dispatch) MUST pass a payload that has been
    // through `wrap_single_map_as_array`, so the input is guaranteed to be
    // a valid msgpack array. Non-array input here means the upstream
    // normalization or the Data Plane response shape changed — return None
    // so the caller logs and degrades safely instead of silently producing
    // wrong tombstone behaviour for a single-map shape we no longer expect.
    let (count, body_start) = msgpack_scan::array_header(payload, 0)?;

    // Walk elements, collecting start/end byte offsets for rows to keep.
    let mut kept_ranges: Vec<(usize, usize)> = Vec::with_capacity(count);
    let mut pos = body_start;
    for _ in 0..count {
        let row_start = pos;
        // Advance `pos` by one msgpack value (the row map).
        pos = msgpack_scan::skip_value(payload, pos)?;
        let row_bytes = &payload[row_start..pos];

        // Extract the "key" field from this row map. A KV row without a
        // "key" field is a protocol contract violation (every KV scan/point-get
        // response is expected to carry a key after `apply_kv_wrap`
        // normalization). Log a warn and treat as not-tombstoned so we err on
        // the side of returning the row to the user — silent drop would be
        // worse than silent include.
        let extracted_key = msgpack_scan::extract_field(row_bytes, 0, "key")
            .and_then(|(start, _)| msgpack_scan::read_str(row_bytes, start));
        let is_tombstoned = match extracted_key {
            Some(k) => tombstoned.contains(k),
            None => {
                tracing::warn!(
                    row_len = row_bytes.len(),
                    "clone read: KV row in source response has no `key` field; including unfiltered (protocol contract violation upstream)"
                );
                false
            }
        };

        if !is_tombstoned {
            kept_ranges.push((row_start, pos));
        }
    }

    if kept_ranges.len() == count {
        // Nothing was filtered.
        return Some(payload.to_vec());
    }

    let kept = kept_ranges.len();
    let mut buf = Vec::with_capacity(payload.len());
    if kept <= 15 {
        buf.push(0x90 | (kept as u8));
    } else if kept <= 0xFFFF {
        buf.push(0xdc);
        buf.push((kept >> 8) as u8);
        buf.push(kept as u8);
    } else {
        buf.push(0xdd);
        buf.push((kept >> 24) as u8);
        buf.push((kept >> 16) as u8);
        buf.push((kept >> 8) as u8);
        buf.push(kept as u8);
    }
    for (start, end) in kept_ranges {
        buf.extend_from_slice(&payload[start..end]);
    }
    Some(buf)
}

/// Replace a msgpack array with its first element's raw bytes (or empty).
/// `SingleDocument` plans dispatch a bare row; `apply_kv_wrap` misparses an
/// array as one raw scalar, garbling every field, if left wrapped.
pub(super) fn unwrap_single_row(payload: Vec<u8>) -> Vec<u8> {
    use nodedb_query::msgpack_scan;

    let Some((count, body_start)) = msgpack_scan::array_header(&payload, 0) else {
        return payload;
    };
    if count == 0 {
        return Vec::new();
    }
    match msgpack_scan::skip_value(&payload, body_start) {
        Some(end) => payload[body_start..end].to_vec(),
        None => payload,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A native point-get body is one row: it wraps to a one-row array and
    /// unwraps back to the same bytes.
    #[test]
    fn a_native_row_wraps_as_one_row() {
        let row = nodedb_types::Value::Object(std::collections::HashMap::from([(
            "id".to_string(),
            nodedb_types::Value::String("n1".into()),
        )]));
        let native = zerompk::to_msgpack_vec(&row).expect("encode native row");
        assert!(crate::data::executor::response_codec::is_native_row(
            &native
        ));
        let wrapped = wrap_single_map_as_array(native.clone());
        assert_eq!(
            nodedb_query::msgpack_scan::array_header(&wrapped, 0).map(|(n, _)| n),
            Some(1)
        );
        assert_eq!(unwrap_single_row(wrapped), native);
    }
}
