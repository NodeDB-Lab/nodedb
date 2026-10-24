// SPDX-License-Identifier: Apache-2.0

pub mod array_batch;
pub mod auth;
pub mod batch;
pub mod error_cause;
pub mod frames;
pub mod handshake;
pub mod opcodes;
pub mod request_fields;
pub mod text_fields;

pub use array_batch::{ArrayCellWrite, ArrayPutBatch};
pub use auth::{AuthMethod, AuthResponse};
pub use batch::{BatchDocument, BatchVector, MAX_BATCH_TEXT_BYTES, MAX_EDGES_PER_BATCH_STATEMENT};
pub use error_cause::ErrorCausePayload;
pub use frames::{ErrorPayload, NativeRequest, NativeResponse};
pub use handshake::{
    CAP_COLUMNAR, CAP_CRDT, CAP_FTS, CAP_GRAPHRAG, CAP_MSGPACK, CAP_SPATIAL, CAP_STREAMING,
    CAP_TIMESERIES, DEFAULT_NATIVE_PORT, FRAME_HEADER_LEN, HELLO_ACK_MAGIC, HELLO_ERROR_MAGIC,
    HELLO_ERROR_MAGIC_U32, HELLO_MAGIC, HelloAckFrame, HelloErrorCode, HelloErrorFrame, HelloFrame,
    Limits, MAX_FRAME_SIZE, PROTO_VERSION, PROTO_VERSION_MAX, PROTO_VERSION_MIN,
};
pub use opcodes::{OpCode, ResponseStatus, UnknownOpCode};
pub use request_fields::RequestFields;
pub use text_fields::TextFields;
