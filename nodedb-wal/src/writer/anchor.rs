// SPDX-License-Identifier: Apache-2.0

//! One time anchor per group-commit batch.
//!
//! `sync` appends the anchor to the write buffer before the batch's single
//! `pwrite` + `fsync`, so an anchor costs no extra write and no extra fsync.
//! Every data append keeps room for it, so it never forces a flush of its own.

use crate::crypto::AUTH_TAG_SIZE;
use crate::error::Result;
use crate::record::{
    HEADER_SIZE, NO_EVENT_SOURCE, RecordTarget, RecordType, TIME_ANCHOR_PAYLOAD_SIZE,
    TimeAnchorPayload,
};

use super::core::WalWriter;

impl WalWriter {
    /// Buffer bytes a data append leaves free for the batch's anchor.
    pub(super) fn anchor_reserve(&self) -> usize {
        if self.config.time_anchors.is_none() {
            return 0;
        }
        let tag = if self.encryption_ring().is_some() {
            AUTH_TAG_SIZE
        } else {
            0
        };
        HEADER_SIZE + TIME_ANCHOR_PAYLOAD_SIZE + tag
    }

    /// Append the anchor closing this batch, if anchors are on and a record
    /// was appended since the last one. Its LSN is the batch's last LSN.
    pub(super) fn append_time_anchor(&mut self) -> Result<()> {
        if !self.unanchored {
            return Ok(());
        }
        let Some(anchors) = self.config.time_anchors.clone() else {
            return Ok(());
        };
        let hlc_wall_ns = anchors.stamp();
        let target = RecordTarget {
            record_type: RecordType::TimeAnchor as u32,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            event_source: NO_EVENT_SOURCE,
            commit_hlc: 0,
            system_ordinal: crate::record::NO_SYSTEM_ORDINAL,
        };
        let payload = TimeAnchorPayload::new(hlc_wall_ns).to_bytes();
        let lsn = self.append_reserving(target, &payload, 0, 0)?;
        self.unanchored = false;
        self.pending_anchor = Some((lsn, hlc_wall_ns));
        Ok(())
    }

    /// Record the batch's anchor once its fsync succeeded.
    pub(super) fn publish_time_anchor(&mut self) {
        let Some((lsn, hlc_wall_ns)) = self.pending_anchor.take() else {
            return;
        };
        self.record_time_anchor(lsn, hlc_wall_ns);
    }

    /// Record one anchor whose fsync succeeded.
    pub(super) fn record_time_anchor(&self, lsn: u64, hlc_wall_ns: u64) {
        let Some(anchors) = &self.config.time_anchors else {
            return;
        };
        if let Err(error) = anchors.record(lsn, hlc_wall_ns) {
            tracing::warn!(lsn, hlc_wall_ns, %error, "WAL time anchor not recorded");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::HlcClock;

    use crate::record::RecordType;
    use crate::time_anchors::TimeAnchors;
    use crate::writer::{WalWriter, WalWriterConfig};

    fn open(path: &std::path::Path, anchors: &Arc<TimeAnchors>) -> WalWriter {
        WalWriter::open(
            path,
            WalWriterConfig {
                use_direct_io: false,
                time_anchors: Some(Arc::clone(anchors)),
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn put(writer: &mut WalWriter) -> u64 {
        writer
            .append(RecordType::Put as u32, 1, 0, 0, b"row")
            .unwrap()
    }

    #[test]
    fn each_sync_closes_its_batch_with_one_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.wal");
        let anchors = Arc::new(TimeAnchors::new(Arc::new(HlcClock::new())));
        let mut writer = open(&path, &anchors);

        put(&mut writer);
        put(&mut writer);
        writer.sync().unwrap();
        // Nothing new: no anchor, no write.
        let offset = writer.file_offset();
        writer.sync().unwrap();
        assert_eq!(writer.file_offset(), offset);
        put(&mut writer);
        writer.sync().unwrap();

        let held = anchors.anchors();
        assert_eq!(held.iter().map(|a| a.lsn).collect::<Vec<_>>(), vec![3, 5]);
        assert!(held[0].hlc_wall_ns < held[1].hlc_wall_ns);

        let reader = crate::reader::WalReader::open(&path, None).unwrap();
        let types: Vec<_> = reader
            .records()
            .map(|r| RecordType::from_raw(r.unwrap().logical_record_type()))
            .collect();
        assert_eq!(
            types,
            vec![
                Some(RecordType::Put),
                Some(RecordType::Put),
                Some(RecordType::TimeAnchor),
                Some(RecordType::Put),
                Some(RecordType::TimeAnchor),
            ]
        );
    }

    /// An anchor never adds a write: a batch that fills the buffer to the
    /// last usable byte still takes exactly one `pwrite` at sync.
    #[test]
    fn anchor_fits_without_an_extra_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.wal");
        let anchors = Arc::new(TimeAnchors::new(Arc::new(HlcClock::new())));
        let mut writer = WalWriter::open(
            &path,
            WalWriterConfig {
                use_direct_io: false,
                write_buffer_size: 4096,
                time_anchors: Some(Arc::clone(&anchors)),
                ..Default::default()
            },
        )
        .unwrap();

        let room = writer.buffer.capacity()
            - writer.padding_reserve()
            - writer.anchor_reserve()
            - crate::record::HEADER_SIZE;
        writer
            .append(RecordType::Put as u32, 1, 0, 0, &vec![7u8; room])
            .unwrap();
        assert_eq!(writer.file_offset(), 0, "the data record fit unflushed");
        writer.sync().unwrap();
        assert_eq!(anchors.anchors().len(), 1);
        assert_eq!(anchors.anchors()[0].lsn, 2);
    }
}
