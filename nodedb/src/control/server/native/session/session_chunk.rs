// SPDX-License-Identifier: BUSL-1.1

//! Response chunking for the native binary protocol.

use nodedb_types::protocol::{MAX_FRAME_SIZE, NativeResponse, ResponseStatus};

use super::codec;

/// Split a large `NativeResponse` into multiple encoded frames that each
/// fit within `MAX_FRAME_SIZE`. Intermediate frames use `Partial` status;
/// the last frame uses the original status.
///
/// Only responses with `rows` data are split. Error or auth responses
/// that somehow exceed the frame limit are returned as-is (best effort).
pub(super) fn chunk_large_response(
    response: NativeResponse,
    format: codec::FrameFormat,
) -> crate::Result<Vec<Vec<u8>>> {
    let rows = match response.rows {
        Some(ref rows) if !rows.is_empty() => rows,
        _ => {
            // No rows to split — send as-is (shouldn't happen, but safe).
            return Ok(vec![codec::encode_response(&response, format)?]);
        }
    };

    // Estimate how many rows per chunk: target ~12 MiB per frame (75% of max)
    // to leave headroom for envelope overhead.
    let target_size = (MAX_FRAME_SIZE as usize) * 3 / 4;
    let total_rows = rows.len();

    // Estimate per-row size from a sample of the first rows.
    let sample_resp = NativeResponse {
        seq: response.seq,
        status: ResponseStatus::Ok,
        columns: response.columns.clone(),
        rows: Some(rows[..total_rows.min(100)].to_vec()),
        rows_affected: None,
        command: None,
        watermark_lsn: response.watermark_lsn,
        error: None,
        auth: None,
        warnings: Vec::new(),
    };
    let sample_bytes = codec::encode_response(&sample_resp, format)?;
    let sample_count = total_rows.min(100);
    let per_row_estimate = sample_bytes.len().checked_div(sample_count).unwrap_or(256);

    let rows_per_chunk = target_size
        .checked_div(per_row_estimate)
        .map(|v| v.max(1))
        .unwrap_or(1000);

    let mut frames = Vec::new();
    let chunks: Vec<_> = rows.chunks(rows_per_chunk).collect();
    let last_idx = chunks.len().saturating_sub(1);

    for (i, chunk) in chunks.iter().enumerate() {
        let is_last = i == last_idx;
        let frame_resp = NativeResponse {
            seq: response.seq,
            status: if is_last {
                response.status
            } else {
                ResponseStatus::Partial
            },
            columns: if i == 0 {
                response.columns.clone()
            } else {
                None
            },
            rows: Some(chunk.to_vec()),
            rows_affected: if is_last {
                response.rows_affected
            } else {
                None
            },
            command: if is_last {
                response.command.clone()
            } else {
                None
            },
            watermark_lsn: response.watermark_lsn,
            error: if is_last {
                response.error.clone()
            } else {
                None
            },
            auth: None,
            warnings: Vec::new(),
        };
        frames.push(codec::encode_response(&frame_resp, format)?);
    }

    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;
    use nodedb_types::protocol::opcodes::ResponseStatus;

    #[test]
    fn chunk_large_response_splits_rows() {
        // Build a response with 100 rows, each ~200 bytes when serialized.
        let columns = vec!["id".to_string(), "data".to_string()];
        let rows: Vec<Vec<Value>> = (0..100)
            .map(|i| {
                vec![
                    Value::Integer(i),
                    Value::String(format!("row-data-{i}-padding-{}", "x".repeat(150))),
                ]
            })
            .collect();

        let response = NativeResponse {
            seq: 1,
            status: ResponseStatus::Ok,
            columns: Some(columns),
            rows: Some(rows),
            rows_affected: None,
            command: None,
            watermark_lsn: 42,
            error: None,
            auth: None,
            warnings: Vec::new(),
        };

        let frames = chunk_large_response(response, codec::FrameFormat::MessagePack).unwrap();

        // With 100 rows of ~200 bytes each (~20KB total), this should fit in
        // one frame (MAX_FRAME_SIZE = 16MB). Test with a scenario that forces splitting.
        assert!(!frames.is_empty());

        // Decode each frame and verify structure.
        for (i, frame) in frames.iter().enumerate() {
            let resp: NativeResponse = zerompk::from_msgpack(frame).unwrap();
            assert!(resp.rows.is_some());
            if i < frames.len() - 1 {
                assert_eq!(resp.status, ResponseStatus::Partial);
            } else {
                assert_eq!(resp.status, ResponseStatus::Ok);
            }
        }
    }

    #[test]
    fn chunk_large_response_no_rows_passthrough() {
        let response = NativeResponse {
            seq: 1,
            status: ResponseStatus::Ok,
            columns: None,
            rows: None,
            rows_affected: Some(5),
            command: None,
            watermark_lsn: 42,
            error: None,
            auth: None,
            warnings: Vec::new(),
        };

        let frames = chunk_large_response(response, codec::FrameFormat::MessagePack).unwrap();
        assert_eq!(
            frames.len(),
            1,
            "no-rows response should pass through as-is"
        );
    }

    #[test]
    fn chunk_large_response_preserves_all_rows() {
        // Create a response that's guaranteed to exceed MAX_FRAME_SIZE.
        // Each row ~200 bytes * 100K rows = ~20MB > 16MB limit.
        let columns = vec!["id".to_string(), "value".to_string()];
        let row_count = 100_000;
        let rows: Vec<Vec<Value>> = (0..row_count)
            .map(|i| {
                vec![
                    Value::Integer(i),
                    Value::String(format!("v{i}-{}", "p".repeat(150))),
                ]
            })
            .collect();

        let response = NativeResponse {
            seq: 42,
            status: ResponseStatus::Ok,
            columns: Some(columns.clone()),
            rows: Some(rows),
            rows_affected: None,
            command: None,
            watermark_lsn: 99,
            error: None,
            auth: None,
            warnings: Vec::new(),
        };

        let frames = chunk_large_response(response, codec::FrameFormat::MessagePack).unwrap();
        assert!(frames.len() > 1, "should produce multiple frames");

        // Reassemble all rows from frames (simulating client behavior).
        let mut total_rows: Vec<Vec<Value>> = Vec::new();
        for frame in &frames {
            let resp: NativeResponse = zerompk::from_msgpack(frame).unwrap();
            if let Some(rows) = resp.rows {
                total_rows.extend(rows);
            }
        }
        assert_eq!(total_rows.len(), row_count as usize);

        // First frame should have columns.
        let first: NativeResponse = zerompk::from_msgpack(&frames[0]).unwrap();
        assert_eq!(first.columns, Some(columns));
        assert_eq!(first.status, ResponseStatus::Partial);

        // Last frame should have Ok status.
        let last: NativeResponse = zerompk::from_msgpack(frames.last().unwrap()).unwrap();
        assert_eq!(last.status, ResponseStatus::Ok);

        // Each frame should be <= MAX_FRAME_SIZE.
        for frame in &frames {
            assert!(
                frame.len() <= MAX_FRAME_SIZE as usize,
                "frame size {} exceeds MAX_FRAME_SIZE {}",
                frame.len(),
                MAX_FRAME_SIZE
            );
        }
    }
}
