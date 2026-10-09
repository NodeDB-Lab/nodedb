// SPDX-License-Identifier: Apache-2.0

#[cfg(not(target_arch = "wasm32"))]
pub mod aborted;
#[cfg(not(target_arch = "wasm32"))]
pub mod anchor;
#[cfg(not(target_arch = "wasm32"))]
pub mod fts_spatial;
pub mod header;
#[cfg(not(target_arch = "wasm32"))]
pub mod padding;
#[cfg(not(target_arch = "wasm32"))]
pub mod restore_point;
#[cfg(not(target_arch = "wasm32"))]
pub mod surrogate;
#[cfg(not(target_arch = "wasm32"))]
pub mod sync_seq;
#[cfg(not(target_arch = "wasm32"))]
pub mod types;
#[cfg(not(target_arch = "wasm32"))]
pub mod wal_record;

#[cfg(not(target_arch = "wasm32"))]
pub use aborted::{WRITE_ABORTED_PAYLOAD_SIZE, WriteAbortedPayload};
#[cfg(not(target_arch = "wasm32"))]
pub use anchor::{TIME_ANCHOR_PAYLOAD_SIZE, TimeAnchorPayload};
#[cfg(not(target_arch = "wasm32"))]
pub use fts_spatial::{FtsDeletePayload, FtsIndexPayload, SpatialDeletePayload, SpatialPutPayload};
pub use header::{
    ENCRYPTED_FLAG, HEADER_SIZE, MAX_WAL_PAYLOAD_SIZE, NO_EVENT_SOURCE, RecordHeader,
    WAL_FORMAT_VERSION, WAL_MAGIC,
};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use padding::pad_buffer_to_alignment;
#[cfg(not(target_arch = "wasm32"))]
pub use padding::{MIN_PADDING_RECORD_SIZE, padding_record, padding_span};
#[cfg(not(target_arch = "wasm32"))]
pub use restore_point::{RESTORE_POINT_PAYLOAD_SIZE, RestorePointPayload};
#[cfg(not(target_arch = "wasm32"))]
pub use surrogate::{SURROGATE_PAYLOAD_SIZE, SurrogateAllocPayload, SurrogateBindPayload};
#[cfg(not(target_arch = "wasm32"))]
pub use sync_seq::{SYNC_SEQ_ADVANCE_PAYLOAD_SIZE, SyncSeqAdvancePayload};
#[cfg(not(target_arch = "wasm32"))]
pub use types::RecordType;
#[cfg(not(target_arch = "wasm32"))]
pub use wal_record::{RecordStamp, RecordTarget, WalRecord, WalRecordArgs};
