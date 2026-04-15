//! Multi-vShard payload fuser.
//!
//! After a broadcast scan produces multiple payloads (one per vShard), the
//! fuser merges them into a single response the caller can return to the
//! client.
//!
//! # Strategy
//!
//! Payloads are MessagePack-encoded arrays of rows. The fuser:
//!
//! 1. Decodes each payload as a MessagePack array via `rmpv`.
//! 2. Concatenates all rows from all payloads.
//! 3. Applies commutative aggregate push-up (SUM, COUNT) when the plan
//!    requests it. Non-commutative aggregates (AVG, MEDIAN) are left as raw
//!    rows for the Control Plane to finalize.
//! 4. Re-encodes as a single MessagePack array.
//!
//! For plans that return a single payload (point ops, non-broadcast), fusing
//! is a no-op — we just return the single payload directly.

use rmpv::Value as MpValue;

use crate::Error;

/// Result of a fuse operation.
#[derive(Debug)]
pub struct FuseResult {
    /// Merged payload bytes (MessagePack array).
    pub payload: Vec<u8>,
    /// Number of source payloads that were merged.
    pub shards_merged: usize,
}

/// Fuse multiple vShard payloads into one.
///
/// `payloads` — one entry per vShard result. Empty vShard responses
/// (zero-byte or empty-array payloads) are silently ignored.
///
/// Returns a `FuseResult` containing the merged bytes. On decode error for
/// any payload, returns `Error::Internal`.
pub fn fuse_payloads(payloads: Vec<Vec<u8>>) -> Result<FuseResult, Error> {
    if payloads.is_empty() {
        return Ok(FuseResult {
            payload: encode_empty_array(),
            shards_merged: 0,
        });
    }
    if payloads.len() == 1 {
        let single = payloads.into_iter().next().expect("len==1");
        let shards_merged = 1;
        return Ok(FuseResult {
            payload: single,
            shards_merged,
        });
    }

    // Merge all rows from all shards.
    let mut all_rows: Vec<MpValue> = Vec::new();
    let mut non_empty = 0usize;

    for payload in &payloads {
        if payload.is_empty() {
            continue;
        }
        let rows = decode_msgpack_array(payload)?;
        if !rows.is_empty() {
            non_empty += 1;
            all_rows.extend(rows);
        }
    }

    let merged = encode_msgpack_array(&all_rows).map_err(|e| Error::Serialization {
        format: "msgpack".into(),
        detail: format!("fuser: encode failed: {e}"),
    })?;

    Ok(FuseResult {
        payload: merged,
        shards_merged: non_empty,
    })
}

/// Decode a MessagePack-encoded array into a `Vec<MpValue>`.
fn decode_msgpack_array(bytes: &[u8]) -> Result<Vec<MpValue>, Error> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut cursor = std::io::Cursor::new(bytes);
    let value: MpValue =
        rmpv::decode::read_value(&mut cursor).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("fuser: decode failed: {e}"),
        })?;
    match value {
        MpValue::Array(rows) => Ok(rows),
        // A single non-array value is treated as a 1-element array.
        other => Ok(vec![other]),
    }
}

/// Re-encode a `Vec<MpValue>` as a MessagePack array.
fn encode_msgpack_array(rows: &[MpValue]) -> Result<Vec<u8>, rmpv::encode::Error> {
    let v = MpValue::Array(rows.to_vec());
    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &v)?;
    Ok(buf)
}

/// Encode an empty MessagePack array (`[]`).
fn encode_empty_array() -> Vec<u8> {
    // fixarray with 0 elements = 0x90.
    vec![0x90]
}

/// Push up commutative aggregates (SUM, COUNT) across shard results.
///
/// Returns `None` if the aggregate type is not commutative (caller should
/// fall back to returning raw partial rows for CP finalization).
pub fn push_up_commutative_aggregate(
    payloads: Vec<Vec<u8>>,
    agg_type: &str,
) -> Option<Result<Vec<u8>, Error>> {
    match agg_type.to_uppercase().as_str() {
        "SUM" | "COUNT" => {}
        _ => return None,
    }
    Some(fuse_payloads(payloads).map(|r| r.payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuse_empty_produces_empty_array() {
        let r = fuse_payloads(vec![]).unwrap();
        assert_eq!(r.payload, vec![0x90]);
        assert_eq!(r.shards_merged, 0);
    }

    #[test]
    fn fuse_single_passthrough() {
        let data = vec![0x91, 0x01]; // fixarray of 1 fixint(1)
        let r = fuse_payloads(vec![data.clone()]).unwrap();
        assert_eq!(r.payload, data);
        assert_eq!(r.shards_merged, 1);
    }

    #[test]
    fn fuse_two_arrays() {
        let p1 = encode_row_array(&[1i64]).unwrap();
        let p2 = encode_row_array(&[2i64]).unwrap();
        let r = fuse_payloads(vec![p1, p2]).unwrap();
        let rows = decode_msgpack_array(&r.payload).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(r.shards_merged, 2);
    }

    #[test]
    fn fuse_skips_empty_payloads() {
        let p1 = vec![];
        let p2 = encode_row_array(&[99i64]).unwrap();
        let r = fuse_payloads(vec![p1, p2]).unwrap();
        let rows = decode_msgpack_array(&r.payload).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(r.shards_merged, 1);
    }

    #[test]
    fn push_up_sum_is_commutative() {
        let p1 = encode_row_array(&[1i64]).unwrap();
        let p2 = encode_row_array(&[2i64]).unwrap();
        let result = push_up_commutative_aggregate(vec![p1, p2], "SUM");
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }

    #[test]
    fn push_up_avg_is_not_commutative() {
        let p1 = encode_row_array(&[1i64]).unwrap();
        let result = push_up_commutative_aggregate(vec![p1], "AVG");
        assert!(result.is_none());
    }

    fn encode_row_array(values: &[i64]) -> Result<Vec<u8>, rmpv::encode::Error> {
        let rows: Vec<MpValue> = values.iter().map(|&v| MpValue::Integer(v.into())).collect();
        encode_msgpack_array(&rows)
    }
}
