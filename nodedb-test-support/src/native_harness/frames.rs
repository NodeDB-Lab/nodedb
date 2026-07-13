// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol frame helpers: handshake, raw frame read/write, and a
//! JSON-session `send_sql` convenience for dispatch-routing tests.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use nodedb_types::protocol::request_fields::RequestFields;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{
    AuthMethod, FRAME_HEADER_LEN, HELLO_ACK_MAGIC, HELLO_ERROR_MAGIC_U32, HelloAckFrame,
    HelloErrorFrame, HelloFrame, NativeRequest, NativeResponse, OpCode,
};

/// Perform the handshake with a custom `HelloFrame`.
/// Returns `(stream, ack_frame)` on success, or the parsed `HelloErrorFrame` via `Err`.
pub async fn do_handshake(
    addr: std::net::SocketAddr,
    hello: &HelloFrame,
) -> Result<(TcpStream, HelloAckFrame), HelloErrorFrame> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(&hello.encode())
        .await
        .expect("write hello");
    stream.flush().await.expect("flush");

    let mut magic_buf = [0u8; 4];
    stream.read_exact(&mut magic_buf).await.expect("read magic");
    let magic = u32::from_be_bytes(magic_buf);

    if magic == HELLO_ERROR_MAGIC_U32 {
        // Read error code + msg_len + message.
        let mut code_buf = [0u8; 1];
        stream.read_exact(&mut code_buf).await.expect("read code");
        let mut len_buf = [0u8; 1];
        stream.read_exact(&mut len_buf).await.expect("read msg_len");
        let msg_len = len_buf[0] as usize;
        let mut msg = vec![0u8; msg_len];
        if msg_len > 0 {
            stream.read_exact(&mut msg).await.expect("read msg");
        }
        // Reassemble the full error frame bytes for HelloErrorFrame::decode.
        let mut full = Vec::with_capacity(6 + msg_len);
        full.extend_from_slice(b"NDBE");
        full.push(code_buf[0]);
        full.push(len_buf[0]);
        full.extend_from_slice(&msg);
        let err_frame = HelloErrorFrame::decode(&full).expect("decode error frame");
        return Err(err_frame);
    }

    assert_eq!(magic, HELLO_ACK_MAGIC, "expected HelloAck magic");

    // Read fixed rest: proto_version(2) + capabilities(8) + sv_len(1).
    let mut fixed_rest = [0u8; 11];
    stream
        .read_exact(&mut fixed_rest)
        .await
        .expect("read fixed");
    let sv_len = fixed_rest[10] as usize;
    let var_len = sv_len + 1 + 7 * 5;
    let mut var_buf = vec![0u8; var_len];
    stream.read_exact(&mut var_buf).await.expect("read var");

    let mut ack_buf = Vec::with_capacity(4 + 11 + var_len);
    ack_buf.extend_from_slice(&magic_buf);
    ack_buf.extend_from_slice(&fixed_rest);
    ack_buf.extend_from_slice(&var_buf);

    let ack = HelloAckFrame::decode(&ack_buf).expect("decode ack");
    Ok((stream, ack))
}

/// Write a length-prefixed frame payload to the stream.
pub async fn write_frame(stream: &mut TcpStream, payload: &[u8]) {
    let len = (payload.len() as u32).to_be_bytes();
    stream.write_all(&len).await.expect("write len");
    stream.write_all(payload).await.expect("write payload");
    stream.flush().await.expect("flush");
}

/// Read a length-prefixed frame from the stream.
/// Returns `None` on EOF.
pub async fn read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; FRAME_HEADER_LEN];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return None,
        Err(e) => panic!("read_frame error: {e}"),
    }
    let payload_len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).await.expect("read payload");
    Some(payload)
}

/// Send any native-protocol request (opcode + `TextFields`) over an
/// established JSON-encoding session and decode the `NativeResponse`.
/// Assumes the session's first frame already selected JSON (see
/// `json_request_gets_json_response`) — callers that open a fresh connection
/// must send one JSON frame before calling this. Shared by [`send_sql`] and
/// any test driving a direct-op opcode (`PointGet`, `RangeScan`,
/// `VectorSearch`, `KvBatchPut`, ...) directly rather than through SQL text.
pub async fn send_request(
    stream: &mut TcpStream,
    seq: u64,
    op: OpCode,
    fields: TextFields,
) -> NativeResponse {
    let req = NativeRequest {
        op,
        seq,
        fields: RequestFields::Text(fields),
    };
    let json_bytes = sonic_rs::to_vec(&req).expect("json encode");
    write_frame(stream, &json_bytes).await;

    let response_payload = tokio::time::timeout(Duration::from_secs(5), read_frame(stream))
        .await
        .expect("timeout waiting for response")
        .expect("response frame");
    sonic_rs::from_slice(&response_payload).expect("json decode NativeResponse")
}

/// Authenticate a fresh native connection with an API key. The JSON Auth
/// request also selects JSON framing for the rest of the session.
pub async fn send_api_key_auth(stream: &mut TcpStream, seq: u64, token: String) -> NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::ApiKey { token }),
            ..Default::default()
        },
    )
    .await
}

/// Send a `SHOW`/SQL statement over an established JSON-encoding session and
/// decode the `NativeResponse`. Assumes the session's first frame already
/// selected JSON (see `json_request_gets_json_response`) — callers that open
/// a fresh connection must send one JSON frame before calling this.
pub async fn send_sql(stream: &mut TcpStream, seq: u64, sql: &str) -> NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::Sql,
        TextFields {
            sql: Some(sql.into()),
            ..Default::default()
        },
    )
    .await
}
