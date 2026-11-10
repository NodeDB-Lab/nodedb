// SPDX-License-Identifier: BUSL-1.1

//! Merge several standalone msgpack-array payloads into one msgpack array.
//!
//! A streamed scan result arrives as multiple chunks, and each chunk is encoded
//! as its OWN msgpack array (`encode_raw_document_rows` per chunk). Downstream
//! consumers decode a single msgpack array, so the chunks must be merged into
//! one array — raw byte concatenation would leave every chunk after the first as
//! a separate trailing array that the decoder silently ignores (truncating the
//! result to the first chunk).
//!
//! [`merge_msgpack_arrays`] takes only whole msgpack arrays. A relational
//! read also answers a point read's one row as a bare msgpack map, which
//! [`payload_rows`] and [`merge_payload_rows`] read as one row. A payload of
//! any other shape is a typed error: wrapping it as one element will hand
//! the caller rows that no core produced.
//!
//! Used by both `local_dispatch::collect_bounded_response` (the per-request
//! bounded collector) and `exchange::gather_outcome` (the cross-core/vShard
//! gather).

use nodedb_query::msgpack_scan;

/// Extract individual msgpack elements from one msgpack-array payload.
///
/// An empty payload holds no elements. A payload that is not one whole
/// msgpack array is a `Codec` error: a non-array header, an element that
/// does not decode, or bytes after the last element.
pub(crate) fn extract_msgpack_elements(payload: &[u8]) -> crate::Result<Vec<Vec<u8>>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }

    let Some((count, mut pos)) = msgpack_scan::array_header(payload, 0) else {
        return Err(crate::Error::Codec {
            detail: format!(
                "payload_merge: a {}-byte payload starting with byte 0x{:02x} is not a \
                 msgpack array; only a row-array payload can merge into rows",
                payload.len(),
                payload[0]
            ),
        });
    };

    // Every element takes at least one byte, so the header cannot reserve
    // more elements than the payload has bytes left.
    let mut rows = Vec::with_capacity(count.min(payload.len() - pos));
    for index in 0..count {
        let start = pos;
        let Some(next) = msgpack_scan::skip_value(payload, pos) else {
            return Err(crate::Error::Codec {
                detail: format!(
                    "payload_merge: element {index} of a {count}-element msgpack array does \
                     not decode at offset {start} of a {}-byte payload",
                    payload.len()
                ),
            });
        };
        rows.push(payload[start..next].to_vec());
        pos = next;
    }
    if pos != payload.len() {
        return Err(crate::Error::Codec {
            detail: format!(
                "payload_merge: {} bytes follow the {count}-element msgpack array in a \
                 {}-byte payload; a payload must hold exactly one array",
                payload.len() - pos,
                payload.len()
            ),
        });
    }
    Ok(rows)
}

/// The rows one relational response payload holds, each as its raw msgpack
/// bytes.
///
/// A scan answers a msgpack array of rows. A point read answers its one row
/// as a bare msgpack map or a bare native object (`[7, {...}]`), and that
/// row is the payload's one row: a native object is never split into its two
/// array elements. An empty payload holds no rows. Any other payload is a
/// `Codec` error from [`extract_msgpack_elements`].
pub(crate) fn payload_rows(payload: &[u8]) -> crate::Result<Vec<Vec<u8>>> {
    let single_map = msgpack_scan::map_header(payload, 0).is_some()
        && msgpack_scan::skip_value(payload, 0) == Some(payload.len());
    if single_map || crate::data::executor::response_codec::is_native_row(payload) {
        return Ok(vec![payload.to_vec()]);
    }
    extract_msgpack_elements(payload)
}

/// Merge the rows of several relational response payloads into one msgpack
/// array, in order. Each payload holds its rows in a shape [`payload_rows`]
/// reads.
pub(crate) fn merge_payload_rows(payloads: &[Vec<u8>]) -> crate::Result<Vec<u8>> {
    let mut rows: Vec<Vec<u8>> = Vec::new();
    for payload in payloads {
        rows.extend(payload_rows(payload)?);
    }
    Ok(encode_msgpack_array(&rows))
}

/// Encode a list of pre-extracted msgpack elements into a single msgpack array.
pub(crate) fn encode_msgpack_array(rows: &[Vec<u8>]) -> Vec<u8> {
    let total_data: usize = rows.iter().map(|r| r.len()).sum();
    let mut out = Vec::with_capacity(total_data + 5);

    let n = rows.len();
    if n < 16 {
        out.push(0x90 | n as u8);
    } else if n <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }

    for row in rows {
        out.extend_from_slice(row);
    }
    out
}

/// Merge several standalone msgpack-array payloads into one msgpack array
/// containing every element from every input array, in order.
///
/// Fails with the first payload [`extract_msgpack_elements`] refuses.
pub(crate) fn merge_msgpack_arrays(payloads: &[Vec<u8>]) -> crate::Result<Vec<u8>> {
    let mut elements: Vec<Vec<u8>> = Vec::new();
    for payload in payloads {
        elements.extend(extract_msgpack_elements(payload)?);
    }
    Ok(encode_msgpack_array(&elements))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A msgpack array of `n` small integer elements (0..n), as a standalone
    /// array payload like a streamed scan chunk.
    fn int_array(start: usize, n: usize) -> Vec<u8> {
        let rows: Vec<Vec<u8>> = (start..start + n).map(|i| vec![(i % 128) as u8]).collect();
        encode_msgpack_array(&rows)
    }

    fn is_codec_error(result: crate::Result<Vec<Vec<u8>>>) -> bool {
        matches!(result, Err(crate::Error::Codec { .. }))
    }

    #[test]
    fn merge_concatenates_all_elements() {
        let chunks = vec![
            int_array(0, 1000),
            int_array(1000, 1000),
            int_array(2000, 500),
        ];
        let merged = merge_msgpack_arrays(&chunks).expect("merge");
        let elements = extract_msgpack_elements(&merged).expect("extract");
        assert_eq!(
            elements.len(),
            2500,
            "merging three array chunks must yield every element, not just the first chunk"
        );
    }

    #[test]
    fn single_array_round_trips() {
        let one = int_array(0, 3);
        let merged = merge_msgpack_arrays(std::slice::from_ref(&one)).expect("merge");
        assert_eq!(extract_msgpack_elements(&merged).expect("extract").len(), 3);
    }

    #[test]
    fn empty_input_is_empty_array() {
        let merged = merge_msgpack_arrays(&[]).expect("merge");
        assert_eq!(extract_msgpack_elements(&merged).expect("extract").len(), 0);
    }

    #[test]
    fn an_empty_payload_holds_no_elements() {
        assert!(extract_msgpack_elements(&[]).expect("extract").is_empty());
    }

    /// A msgpack map, such as an array slice envelope, is not a row array.
    #[test]
    fn a_map_payload_is_refused() {
        // fixmap of 1: "k" => 1.
        assert!(is_codec_error(extract_msgpack_elements(&[
            0x81, 0xa1, b'k', 1
        ])));
    }

    /// Bytes that are not msgpack at all, such as a raw little-endian LSN,
    /// are refused rather than wrapped as one element.
    #[test]
    fn raw_bytes_are_refused() {
        assert!(is_codec_error(extract_msgpack_elements(
            &42u64.to_le_bytes()
        )));
    }

    /// A header that claims more elements than the payload holds is refused,
    /// never cut short at the bytes that are there.
    #[test]
    fn a_truncated_array_is_refused() {
        let mut payload = int_array(0, 3);
        payload.pop();
        assert!(is_codec_error(extract_msgpack_elements(&payload)));
    }

    /// Two arrays back to back are two payloads, not one.
    #[test]
    fn bytes_after_the_array_are_refused() {
        let mut payload = int_array(0, 2);
        payload.extend_from_slice(&int_array(2, 2));
        assert!(is_codec_error(extract_msgpack_elements(&payload)));
    }

    #[test]
    fn a_non_array_payload_fails_the_merge() {
        let chunks = vec![int_array(0, 2), vec![0x81, 0xa1, b'k', 1]];
        assert!(matches!(
            merge_msgpack_arrays(&chunks),
            Err(crate::Error::Codec { .. })
        ));
    }

    /// fixmap of 1: "k" => `value`.
    fn map_row(value: u8) -> Vec<u8> {
        vec![0x81, 0xa1, b'k', value]
    }

    #[test]
    fn a_row_array_holds_its_rows() {
        let payload = encode_msgpack_array(&[map_row(1), map_row(2)]);
        assert_eq!(
            payload_rows(&payload).expect("rows"),
            vec![map_row(1), map_row(2)]
        );
    }

    #[test]
    fn a_point_read_map_is_one_row() {
        assert_eq!(payload_rows(&map_row(3)).expect("rows"), vec![map_row(3)]);
    }

    /// A point read's bare native row is one row, never its two elements.
    #[test]
    fn a_point_read_native_row_is_one_row() {
        let native = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(
            [("k".to_string(), nodedb_types::Value::Integer(3))].into(),
        ))
        .expect("encode native");
        assert_eq!(payload_rows(&native).expect("rows"), vec![native.clone()]);
        let merged = merge_payload_rows(std::slice::from_ref(&native)).expect("merge");
        assert_eq!(
            extract_msgpack_elements(&merged).expect("extract"),
            vec![native]
        );
    }

    #[test]
    fn an_empty_payload_holds_no_rows() {
        assert!(payload_rows(&[]).expect("rows").is_empty());
    }

    /// A map followed by more bytes is not one row.
    #[test]
    fn a_map_with_trailing_bytes_is_not_a_row() {
        let mut payload = map_row(1);
        payload.push(0x01);
        assert!(is_codec_error(payload_rows(&payload)));
    }

    #[test]
    fn a_scalar_payload_holds_no_rows() {
        assert!(is_codec_error(payload_rows(&[0x2a])));
    }

    #[test]
    fn row_payloads_merge_point_reads_and_scans_in_order() {
        let payloads = vec![encode_msgpack_array(&[map_row(1)]), map_row(2), Vec::new()];
        let merged = merge_payload_rows(&payloads).expect("merge");
        assert_eq!(
            extract_msgpack_elements(&merged).expect("extract"),
            vec![map_row(1), map_row(2)]
        );
    }

    #[test]
    fn a_scalar_payload_fails_the_row_merge() {
        let payloads = vec![encode_msgpack_array(&[map_row(1)]), vec![0x2a]];
        assert!(matches!(
            merge_payload_rows(&payloads),
            Err(crate::Error::Codec { .. })
        ));
    }
}
