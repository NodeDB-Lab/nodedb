// SPDX-License-Identifier: Apache-2.0

//! Payload for the `SyncSeqAdvance` WAL record.
//!
//! Advances the durable per-stream high-watermark for a given producer.
//! The receiver uses this watermark to safely deduplicate re-delivered
//! ingest frames after a reconnect.
//!
//! Payload layout (fixed 40 bytes, little-endian, no serde dep):
//!
//! ```text
//! ┌─────────────────┬────────────┬───────────────┬─────────┬───────────────┐
//! │ producer_id u64 │ epoch u64  │ stream_id u64 │ seq u64 │ frame_lsn u64 │
//! └─────────────────┴────────────┴───────────────┴─────────┴───────────────┘
//! ```
//!
//! `frame_lsn` names the record of the frame whose handling moved the mark.
//! A refused frame's mark is appended once its refusal reaches the Control
//! Plane, after records the WAL already holds above the frame, while the
//! core moved the mark at the frame's place. `0` places the mark at its own
//! record.
//!
//! Fixed-width LE encoding is used for consistency with the existing
//! `SurrogateAllocPayload` pattern: no serde or msgpack dependency,
//! zero-allocation encode/decode, replay is one `memcpy` + five
//! `from_le_bytes` calls.

use crate::error::{Result, WalError};

/// Size of a `SyncSeqAdvance` payload on disk.
pub const SYNC_SEQ_ADVANCE_PAYLOAD_SIZE: usize = 40;

/// Producer identity and per-stream sequence high-watermark.
///
/// Durable once the WAL record is fsync'd. On replay the idempotency
/// layer advances its in-memory HWM map past `seq` for `(producer_id,
/// epoch, stream_id)` so post-restart deduplication is correct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncSeqAdvancePayload {
    /// Stable identity of the originating producer (Lite peer ID or
    /// Origin node ID).
    pub producer_id: u64,
    /// Monotonic epoch counter incremented on every producer restart.
    pub epoch: u64,
    /// Per-producer stream identifier.
    pub stream_id: u64,
    /// Per-stream monotonic sequence number. Gaps signal message loss.
    pub seq: u64,
    /// The LSN of the frame record whose handling moved the mark. `0` places
    /// the mark at its own record.
    pub frame_lsn: u64,
}

impl SyncSeqAdvancePayload {
    pub const fn new(producer_id: u64, epoch: u64, stream_id: u64, seq: u64) -> Self {
        Self {
            producer_id,
            epoch,
            stream_id,
            seq,
            frame_lsn: 0,
        }
    }

    /// This mark, moved at the frame record at `frame_lsn`.
    pub const fn at_frame(self, frame_lsn: u64) -> Self {
        Self { frame_lsn, ..self }
    }

    /// The LSN the mark takes effect at, when its own record is at
    /// `record_lsn`.
    pub const fn effective_lsn(&self, record_lsn: u64) -> u64 {
        if self.frame_lsn == 0 {
            record_lsn
        } else {
            self.frame_lsn
        }
    }

    pub fn to_bytes(&self) -> [u8; SYNC_SEQ_ADVANCE_PAYLOAD_SIZE] {
        let mut buf = [0u8; SYNC_SEQ_ADVANCE_PAYLOAD_SIZE];
        buf[0..8].copy_from_slice(&self.producer_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.epoch.to_le_bytes());
        buf[16..24].copy_from_slice(&self.stream_id.to_le_bytes());
        buf[24..32].copy_from_slice(&self.seq.to_le_bytes());
        buf[32..40].copy_from_slice(&self.frame_lsn.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() != SYNC_SEQ_ADVANCE_PAYLOAD_SIZE {
            return Err(WalError::InvalidPayload {
                detail: format!(
                    "SyncSeqAdvance payload must be {SYNC_SEQ_ADVANCE_PAYLOAD_SIZE} bytes, got {}",
                    buf.len()
                ),
            });
        }
        let producer_id = u64::from_le_bytes(
            buf[0..8]
                .try_into()
                .expect("invariant: bounds-checked above (buf.len() == 40)"),
        );
        let epoch = u64::from_le_bytes(
            buf[8..16]
                .try_into()
                .expect("invariant: bounds-checked above (buf.len() == 40)"),
        );
        let stream_id = u64::from_le_bytes(
            buf[16..24]
                .try_into()
                .expect("invariant: bounds-checked above (buf.len() == 40)"),
        );
        let seq = u64::from_le_bytes(
            buf[24..32]
                .try_into()
                .expect("invariant: bounds-checked above (buf.len() == 40)"),
        );
        let frame_lsn = u64::from_le_bytes(
            buf[32..40]
                .try_into()
                .expect("invariant: bounds-checked above (buf.len() == 40)"),
        );
        Ok(Self {
            producer_id,
            epoch,
            stream_id,
            seq,
            frame_lsn,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_seq_advance_roundtrip() {
        let p = SyncSeqAdvancePayload::new(0xCAFE_BABE_DEAD_BEEF, 7, 42, 1_000_000);
        let bytes = p.to_bytes();
        assert_eq!(SyncSeqAdvancePayload::from_bytes(&bytes).unwrap(), p);
    }

    #[test]
    fn a_mark_names_the_frame_it_moved_at() {
        let p = SyncSeqAdvancePayload::new(1, 2, 3, 4).at_frame(90);
        assert_eq!(SyncSeqAdvancePayload::from_bytes(&p.to_bytes()).unwrap(), p);
        assert_eq!(p.effective_lsn(120), 90);
        assert_eq!(
            SyncSeqAdvancePayload::new(1, 2, 3, 4).effective_lsn(120),
            120
        );
    }

    #[test]
    fn sync_seq_advance_zero() {
        let p = SyncSeqAdvancePayload::new(0, 0, 0, 0);
        assert_eq!(SyncSeqAdvancePayload::from_bytes(&p.to_bytes()).unwrap(), p);
    }

    #[test]
    fn sync_seq_advance_max() {
        let p = SyncSeqAdvancePayload::new(u64::MAX, u64::MAX, u64::MAX, u64::MAX);
        assert_eq!(SyncSeqAdvancePayload::from_bytes(&p.to_bytes()).unwrap(), p);
    }

    #[test]
    fn sync_seq_advance_wrong_size_rejected() {
        assert!(SyncSeqAdvancePayload::from_bytes(&[0u8; 39]).is_err());
        assert!(SyncSeqAdvancePayload::from_bytes(&[0u8; 41]).is_err());
    }

    #[test]
    fn sync_seq_advance_payload_size_constant() {
        assert_eq!(
            core::mem::size_of::<u64>() * 5,
            SYNC_SEQ_ADVANCE_PAYLOAD_SIZE
        );
    }
}
