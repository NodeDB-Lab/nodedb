// SPDX-License-Identifier: Apache-2.0

pub mod aborted;
pub mod anchor;
pub mod calvin;
pub mod fts_spatial;
pub mod header;
pub mod padding;
pub mod surrogate;
pub mod sync_seq;
pub mod types;
pub mod wal_record;

pub use aborted::{WRITE_ABORTED_PAYLOAD_SIZE, WriteAbortedPayload};
pub use anchor::{ANCHOR_PAYLOAD_SIZE, LsnMsAnchorPayload};
pub use calvin::CalvinAppliedPayload;
pub use fts_spatial::{FtsDeletePayload, FtsIndexPayload, SpatialDeletePayload, SpatialPutPayload};
pub use header::{
    ENCRYPTED_FLAG, HEADER_SIZE, MAX_WAL_PAYLOAD_SIZE, RecordHeader, WAL_FORMAT_VERSION, WAL_MAGIC,
};
pub(crate) use padding::pad_buffer_to_alignment;
pub use padding::{MIN_PADDING_RECORD_SIZE, padding_record, padding_span};
pub use surrogate::{SURROGATE_PAYLOAD_SIZE, SurrogateAllocPayload, SurrogateBindPayload};
pub use sync_seq::{SYNC_SEQ_ADVANCE_PAYLOAD_SIZE, SyncSeqAdvancePayload};
pub use types::RecordType;
pub use wal_record::{RecordTarget, WalRecord, WalRecordArgs};
