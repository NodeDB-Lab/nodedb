//! Native protocol framing and serialization.
//!
//! Handles reading/writing length-prefixed frames and auto-detecting
//! whether the client is sending JSON (legacy) or MessagePack (binary).

use nodedb_types::protocol::{FRAME_HEADER_LEN, MAX_FRAME_SIZE, NativeRequest, NativeResponse};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Wire format detected from the first byte of the first payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameFormat {
    /// Legacy JSON format (first byte is `{` = 0x7B).
    Json,
    /// MessagePack binary format.
    MessagePack,
}

impl FrameFormat {
    /// Detect format from the first byte of a payload.
    ///
    /// JSON payloads always start with `{` (0x7B).
    /// MessagePack map payloads start with 0x80..0x8F (fixmap),
    /// 0xDE (map16), or 0xDF (map32) — none of which collide with `{`.
    pub fn detect(first_byte: u8) -> Self {
        if first_byte == b'{' {
            FrameFormat::Json
        } else {
            FrameFormat::MessagePack
        }
    }
}

/// Read a single frame from the stream.
///
/// Returns `Ok(None)` on clean EOF (client disconnected).
/// Returns `Err` on framing errors or I/O failures.
pub async fn read_frame<R: AsyncRead + Unpin>(stream: &mut R) -> crate::Result<Option<Vec<u8>>> {
    // Read 4-byte length prefix.
    let mut len_buf = [0u8; FRAME_HEADER_LEN];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(crate::Error::Io(e)),
    }

    let payload_len = u32::from_be_bytes(len_buf);
    if payload_len > MAX_FRAME_SIZE {
        return Err(crate::Error::BadRequest {
            detail: format!("frame size {payload_len} exceeds maximum {MAX_FRAME_SIZE}"),
        });
    }
    if payload_len == 0 {
        return Err(crate::Error::BadRequest {
            detail: "empty frame".into(),
        });
    }

    let mut payload = vec![0u8; payload_len as usize];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(crate::Error::Io)?;

    Ok(Some(payload))
}

/// Write a single frame to the stream.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    stream: &mut W,
    payload: &[u8],
) -> crate::Result<()> {
    let len = payload.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(crate::Error::Io)?;
    stream.write_all(payload).await.map_err(crate::Error::Io)?;
    stream.flush().await.map_err(crate::Error::Io)?;
    Ok(())
}

/// Decode a request payload according to the detected format.
pub fn decode_request(payload: &[u8], format: FrameFormat) -> crate::Result<NativeRequest> {
    match format {
        FrameFormat::Json => sonic_rs::from_slice(payload).map_err(|e| crate::Error::BadRequest {
            detail: format!("invalid JSON request: {e}"),
        }),
        FrameFormat::MessagePack => {
            zerompk::from_msgpack(payload).map_err(|e| crate::Error::BadRequest {
                detail: format!("invalid MessagePack request: {e}"),
            })
        }
    }
}

/// Encode a response in the given format.
pub fn encode_response(resp: &NativeResponse, format: FrameFormat) -> crate::Result<Vec<u8>> {
    match format {
        FrameFormat::Json => sonic_rs::to_vec(resp).map_err(|e| crate::Error::Serialization {
            format: "json".into(),
            detail: format!("response encode: {e}"),
        }),
        FrameFormat::MessagePack => {
            zerompk::to_msgpack_vec(resp).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("response encode: {e}"),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::protocol::{OpCode, RequestFields, TextFields};
    use nodedb_types::value::Value;

    #[test]
    fn detect_json() {
        assert_eq!(FrameFormat::detect(b'{'), FrameFormat::Json);
    }

    #[test]
    fn detect_msgpack_fixmap() {
        // fixmap with 1 element = 0x81
        assert_eq!(FrameFormat::detect(0x81), FrameFormat::MessagePack);
    }

    #[test]
    fn detect_msgpack_map16() {
        assert_eq!(FrameFormat::detect(0xDE), FrameFormat::MessagePack);
    }

    #[test]
    fn json_roundtrip() {
        let req = NativeRequest {
            op: OpCode::Ping,
            seq: 1,
            fields: RequestFields::Text(TextFields::default()),
        };
        let json = sonic_rs::to_vec(&req).unwrap();
        assert_eq!(FrameFormat::detect(json[0]), FrameFormat::Json);

        let decoded = decode_request(&json, FrameFormat::Json).unwrap();
        assert_eq!(decoded.op, OpCode::Ping);
        assert_eq!(decoded.seq, 1);
    }

    #[test]
    fn msgpack_roundtrip() {
        let req = NativeRequest {
            op: OpCode::Sql,
            seq: 42,
            fields: RequestFields::Text(TextFields {
                sql: Some("SELECT 1".into()),
                ..Default::default()
            }),
        };
        let bytes = zerompk::to_msgpack_vec(&req).unwrap();
        assert_eq!(FrameFormat::detect(bytes[0]), FrameFormat::MessagePack);

        let decoded = decode_request(&bytes, FrameFormat::MessagePack).unwrap();
        assert_eq!(decoded.op, OpCode::Sql);
        assert_eq!(decoded.seq, 42);
    }

    #[test]
    fn response_encode_json() {
        let resp = NativeResponse::ok(1);
        let bytes = encode_response(&resp, FrameFormat::Json).unwrap();
        let s = String::from_utf8(bytes).unwrap();
        assert!(s.contains("\"seq\":1"));
    }

    #[test]
    fn response_encode_msgpack() {
        let resp = NativeResponse::from_query_result(
            5,
            nodedb_types::result::QueryResult {
                columns: vec!["x".into()],
                rows: vec![vec![Value::Integer(42)]],
                rows_affected: 0,
            },
            100,
        );
        let bytes = encode_response(&resp, FrameFormat::MessagePack).unwrap();
        let decoded: NativeResponse = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.seq, 5);
        assert_eq!(decoded.watermark_lsn, 100);
    }

    #[tokio::test]
    async fn frame_read_write_roundtrip() {
        let payload = b"hello world";
        let mut buf = Vec::new();
        write_frame(&mut buf, payload).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let read_back = read_frame(&mut cursor).await.unwrap().unwrap();
        assert_eq!(read_back, payload);
    }

    #[tokio::test]
    async fn frame_read_eof() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        let result = read_frame(&mut cursor).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn frame_reject_oversized() {
        // Write a length prefix that exceeds MAX_FRAME_SIZE.
        let bad_len = (MAX_FRAME_SIZE + 1).to_be_bytes();
        let mut cursor = std::io::Cursor::new(bad_len.to_vec());
        let result = read_frame(&mut cursor).await;
        assert!(result.is_err());
    }
}
