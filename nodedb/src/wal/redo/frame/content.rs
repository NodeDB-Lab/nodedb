// SPDX-License-Identifier: BUSL-1.1

//! Decode the framed content of a chunk stream into the record it installs
//! as.
//!
//! The content holds the header record first, then the header frames
//! (messages, identities, reads), then the resolved lists (row sources,
//! sub-records, row changes). The Control Plane binds the identities before
//! the install, so this decode steps over them.

use nodedb_types::calvin::VersionedReadEntry;

use super::codec::{RedoFrameTag, RedoFrames, decode_frame};
use super::header::RedoContentHeader;
use crate::wal::redo::stream_ref::RedoStreamRef;
use crate::wal::{RedoPublish, RedoRecord, RedoRowChange, RedoRowSource, RedoSubRecord};

fn malformed(detail: String) -> crate::Error {
    crate::Error::Serialization {
        format: "framed redo content".into(),
        detail,
    }
}

/// The refusal of content that holds no header record.
fn no_record() -> crate::Error {
    malformed("the content holds no record".into())
}

/// Checks that framed content opens with its one header record. A reader
/// that walks the content record by record admits each tag in order.
#[derive(Debug, Default)]
pub struct FrameOrder {
    header_seen: bool,
}

impl FrameOrder {
    /// Admit the next record's tag. The first record must be the header,
    /// and no later record may be a header.
    pub fn admit(&mut self, tag: RedoFrameTag) -> crate::Result<()> {
        match (tag, self.header_seen) {
            (RedoFrameTag::Header, false) => {
                self.header_seen = true;
                Ok(())
            }
            (RedoFrameTag::Header, true) => {
                Err(malformed("the content holds a second header".into()))
            }
            (tag, false) => Err(malformed(format!(
                "the first record is a {tag:?} record, not the header"
            ))),
            (_, true) => Ok(()),
        }
    }

    /// Refuse content that ended before its header.
    pub fn finish(&self) -> crate::Result<()> {
        if self.header_seen {
            Ok(())
        } else {
            Err(no_record())
        }
    }
}

/// A stream's content, decoded.
#[derive(Debug, Clone, PartialEq)]
pub struct FramedRedo {
    pub header: RedoContentHeader,
    pub ops: Vec<RedoSubRecord>,
    pub row_sources: Vec<RedoRowSource>,
    pub row_changes: Vec<RedoRowChange>,
    pub publishes: Vec<RedoPublish>,
    pub reads: Vec<VersionedReadEntry>,
}

/// Decodes a stream's content one run of whole records at a time.
#[derive(Default)]
pub struct FramedRedoBuilder {
    order: FrameOrder,
    header: Option<RedoContentHeader>,
    ops: Vec<RedoSubRecord>,
    row_sources: Vec<RedoRowSource>,
    row_changes: Vec<RedoRowChange>,
    publishes: Vec<RedoPublish>,
    reads: Vec<VersionedReadEntry>,
}

impl FramedRedoBuilder {
    /// Decode the next run of whole records. The first record of the
    /// content must be the one header.
    pub fn push(&mut self, bytes: &[u8]) -> crate::Result<()> {
        for frame in RedoFrames::new(bytes) {
            let (tag, body) = frame?;
            self.order.admit(tag)?;
            match tag {
                RedoFrameTag::Header => self.header = Some(decode_frame(tag, body)?),
                RedoFrameTag::Op => self.ops.push(decode_frame(tag, body)?),
                RedoFrameTag::RowSource => self.row_sources.push(decode_frame(tag, body)?),
                RedoFrameTag::RowChange => self.row_changes.push(decode_frame(tag, body)?),
                RedoFrameTag::Publish => self.publishes.push(decode_frame(tag, body)?),
                RedoFrameTag::Read => self.reads.push(decode_frame(tag, body)?),
                // The Control Plane binds every identity before the install.
                RedoFrameTag::Identity => {}
            }
        }
        Ok(())
    }

    /// The decoded content. Content with no header is refused.
    pub fn finish(self) -> crate::Result<FramedRedo> {
        let header = self.header.ok_or_else(no_record)?;
        Ok(FramedRedo {
            header,
            ops: self.ops,
            row_sources: self.row_sources,
            row_changes: self.row_changes,
            publishes: self.publishes,
            reads: self.reads,
        })
    }
}

impl FramedRedo {
    /// Decode whole framed content.
    pub fn decode(bytes: &[u8]) -> crate::Result<Self> {
        let mut builder = FramedRedoBuilder::default();
        builder.push(bytes)?;
        builder.finish()
    }

    /// The record `marker` installs as, with this content as its body.
    ///
    /// The marker decides `calvin_stamp` and `cross_shard_applied`: a mirror's
    /// apply rewrites both on the marker. Every message takes the reference's
    /// publish position, and every version rises to its stamp floor, as the
    /// funnel and the apply set them on the marker. The record holds the
    /// content's sub-records alone, with no reference.
    pub fn into_installed(
        self,
        marker: &RedoRecord,
        reference: &RedoStreamRef,
    ) -> crate::Result<RedoRecord> {
        let mut publishes = self.publishes;
        if let Some(position) = reference.publish_position {
            RedoPublish::stamp_all(&mut publishes, position);
        }
        let mut record = RedoRecord {
            version: self.header.version,
            ops: self.ops,
            calvin_stamp: marker.calvin_stamp.clone(),
            cross_shard_applied: marker.cross_shard_applied.clone(),
            row_sources: self.row_sources,
            publishes,
            row_changes: self.row_changes,
            user_roles: self.header.user_roles,
            // The install folds with the targets the header carries, so a
            // reader of the installed record folds with the same ones.
            sum_targets: self.header.sum_targets,
            // A reader of the installed record judges under the pins the
            // header carries.
            pins: self.header.pins,
        };
        if let Some(floor) = reference.stamp_floor {
            record.raise_stamps_to(floor)?;
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::redo::frame::push_frame;
    use crate::wal::{PublishPosition, RedoStreamId};

    fn header() -> RedoContentHeader {
        RedoContentHeader {
            version: 1,
            calvin_stamp: None,
            cross_shard_applied: None,
            user_roles: vec!["writer".into()],
            sum_targets: Vec::new(),
            pins: pins(),
        }
    }

    fn pins() -> nodedb_physical::physical_plan::RedoPins {
        use std::sync::Arc;

        use nodedb_physical::physical_plan::{ConfigPin, ConfigPinKey, CrsPin, RedoPins};
        RedoPins {
            configs: vec![Arc::new(ConfigPin {
                key: ConfigPinKey {
                    database_id: nodedb_types::DatabaseId::DEFAULT,
                    tenant_id: nodedb_types::TenantId::new(1),
                    collection: "orders".into(),
                },
                digest: 11,
                wire: vec![0x90, 0x01].into(),
            })],
            crs: Some(Arc::new(CrsPin {
                digest: 12,
                bytes: vec![0x90].into(),
            })),
        }
    }

    fn publish() -> RedoPublish {
        RedoPublish {
            owner: "t".into(),
            database_id: 0,
            tenant_id: 1,
            topic: "events".into(),
            payload: "{}".into(),
            metadata_floor: 0,
            position: None,
        }
    }

    fn content() -> Vec<u8> {
        let mut out = Vec::new();
        push_frame(&mut out, RedoFrameTag::Header, &header()).expect("header");
        push_frame(&mut out, RedoFrameTag::Publish, &publish()).expect("publish");
        for byte in 0..3u8 {
            let op = RedoSubRecord {
                record_type: 7,
                payload: vec![byte; 8],
            };
            push_frame(&mut out, RedoFrameTag::Op, &op).expect("op");
        }
        out
    }

    fn reference() -> RedoStreamRef {
        RedoStreamRef {
            stream: RedoStreamId::Session {
                vshard: 1,
                idempotency_key: 2,
            },
            len: 0,
            chunks: Vec::new(),
            publish_position: Some(PublishPosition {
                partition: 1,
                epoch: 3,
                index: 9,
            }),
            entry: None,
            stamp_floor: None,
        }
    }

    #[test]
    fn content_decodes_in_runs_as_it_decodes_whole() {
        let bytes = content();
        let whole = FramedRedo::decode(&bytes).expect("decode");
        let mut builder = FramedRedoBuilder::default();
        let frames: Vec<_> = RedoFrames::new(&bytes).collect();
        let mut offset = 0;
        for frame in frames {
            let (_, body) = frame.expect("frame");
            let end = offset + super::super::codec::FRAME_HEAD_BYTES + body.len();
            builder.push(&bytes[offset..end]).expect("run");
            offset = end;
        }
        assert_eq!(builder.finish().expect("finish"), whole);
        assert_eq!(whole.ops.len(), 3);
        assert_eq!(whole.publishes.len(), 1);
    }

    #[test]
    fn the_installed_record_takes_the_markers_stamp_and_the_publish_position() {
        let reference = reference();
        let marker = reference.marker(None, None, Vec::new()).expect("marker");
        let framed = FramedRedo::decode(&content()).expect("decode");
        let installed = framed
            .into_installed(&marker, &reference)
            .expect("installed");
        assert_eq!(installed.ops.len(), 3);
        assert_eq!(installed.user_roles, vec!["writer".to_string()]);
        // The stream header carries the pins to the installed record.
        assert_eq!(installed.pins, pins());
        assert_eq!(installed.publishes[0].position, reference.publish_position);
        assert_eq!(RedoStreamRef::of(&installed).expect("ref"), None);
    }

    #[test]
    fn content_without_a_leading_header_is_refused() {
        let mut out = Vec::new();
        let op = RedoSubRecord {
            record_type: 7,
            payload: vec![1],
        };
        push_frame(&mut out, RedoFrameTag::Op, &op).expect("op");
        assert!(FramedRedo::decode(&out).is_err());
        assert!(FramedRedo::decode(&[]).is_err());
    }
}
