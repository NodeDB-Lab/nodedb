//! Shared response encoder for array dispatch handlers.
//!
//! Emits a top-level msgpack array where each element is a `Value` written
//! via `value_to_msgpack` — the native (untagged) writer. This is the
//! shape the pgwire `msgpack_to_json_string` transcoder expects, and it
//! keeps `Value::NdArrayCell` representable as a clean
//! `{"coords": [...], "attrs": [...]}` map (the writer was extended in
//! tier-6 fixup wave 2 to emit that map shape rather than nil).
//!
//! Using the tagged zerompk codec instead leaks the internal
//! `[18, <bin>]` shape to clients, which is unreadable.

use nodedb_types::Value;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

pub(super) fn encode_value_rows(core: &CoreLoop, task: &ExecutionTask, rows: &[Value]) -> Response {
    let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 64);
    write_array_header(&mut buf, rows.len());
    for row in rows {
        match nodedb_types::value_to_msgpack(row) {
            Ok(b) => buf.extend_from_slice(&b),
            Err(e) => {
                return core.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array response encode: {e}"),
                    },
                );
            }
        }
    }
    core.response_with_payload(task, buf)
}

fn write_array_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x90 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDC);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}
