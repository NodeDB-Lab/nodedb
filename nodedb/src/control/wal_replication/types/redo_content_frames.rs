// SPDX-License-Identifier: BUSL-1.1

//! The framed form of a [`RedoContent`]: the bytes a chunk stream carries.
//!
//! The first record is the header. The header frames follow it: messages,
//! identities and reads. The resolved lists come last: row sources,
//! sub-records and row changes. Each list keeps its order. The resolving
//! core frames the resolved lists, and the proposer frames the rest, so a
//! proposer joins the parts it pulls to its own header frames.

use nodedb_physical::physical_plan::RedoSumTargets;

use nodedb_types::calvin::VersionedReadEntry;

use super::redo_chunk_wire::RedoContent;
use super::transaction_redo_wire::ReplicatedIdentity;
use crate::wal::RedoRecord;
use crate::wal::redo::frame::{
    RedoContentHeader, RedoFrameTag, RedoFrames, decode_frame, push_frame,
};

fn malformed(detail: String) -> crate::Error {
    crate::Error::Serialization {
        format: "framed redo content".into(),
        detail,
    }
}

/// The parts of a redo's content a framer reads, by reference. A framer
/// frames each item as it reads it and copies no list.
pub struct RedoContentView<'a, I: Iterator<Item = ReplicatedIdentity>> {
    pub redo: &'a RedoRecord,
    pub sum_targets: &'a [RedoSumTargets],
    /// The identities, made one at a time as the framer reads them.
    pub identities: I,
    pub reads: &'a [VersionedReadEntry],
}

impl<I: Iterator<Item = ReplicatedIdentity>> RedoContentView<'_, I> {
    /// Append the header record and the header frames: messages,
    /// identities and reads.
    pub fn push_header_frames(self, out: &mut Vec<u8>) -> crate::Result<()> {
        let header = RedoContentHeader {
            version: self.redo.version,
            calvin_stamp: self.redo.calvin_stamp.clone(),
            cross_shard_applied: self.redo.cross_shard_applied.clone(),
            user_roles: self.redo.user_roles.clone(),
            sum_targets: self.sum_targets.to_vec(),
        };
        push_frame(out, RedoFrameTag::Header, &header)?;
        for publish in &self.redo.publishes {
            push_frame(out, RedoFrameTag::Publish, publish)?;
        }
        for identity in self.identities {
            push_frame(out, RedoFrameTag::Identity, &identity)?;
        }
        for read in self.reads {
            push_frame(out, RedoFrameTag::Read, read)?;
        }
        Ok(())
    }

    /// The whole content framed: the header frames, then the resolved
    /// lists.
    pub fn to_framed(self) -> crate::Result<Vec<u8>> {
        let redo = self.redo;
        let mut out = Vec::new();
        self.push_header_frames(&mut out)?;
        for source in &redo.row_sources {
            push_frame(&mut out, RedoFrameTag::RowSource, source)?;
        }
        for op in &redo.ops {
            push_frame(&mut out, RedoFrameTag::Op, op)?;
        }
        for change in &redo.row_changes {
            push_frame(&mut out, RedoFrameTag::RowChange, change)?;
        }
        Ok(out)
    }
}

impl RedoContent {
    /// This content as a framer reads it.
    pub fn view(
        &self,
    ) -> RedoContentView<'_, std::iter::Cloned<std::slice::Iter<'_, ReplicatedIdentity>>> {
        RedoContentView {
            redo: &self.redo,
            sum_targets: &self.sum_targets,
            identities: self.identities.iter().cloned(),
            reads: &self.reads,
        }
    }

    /// Append the header record and the header frames: messages,
    /// identities and reads.
    pub fn push_header_frames(&self, out: &mut Vec<u8>) -> crate::Result<()> {
        self.view().push_header_frames(out)
    }

    /// The whole content framed: the header frames, then the resolved
    /// lists.
    pub fn to_framed(&self) -> crate::Result<Vec<u8>> {
        self.view().to_framed()
    }

    /// Decode framed content. The first record must be the one header, and
    /// every record must decode.
    pub fn from_framed(bytes: &[u8]) -> crate::Result<Self> {
        let mut frames = RedoFrames::new(bytes);
        let header: RedoContentHeader = match frames.next() {
            Some(Ok((RedoFrameTag::Header, body))) => decode_frame(RedoFrameTag::Header, body)?,
            Some(Ok((tag, _))) => {
                return Err(malformed(format!(
                    "the first record is a {tag:?} record, not the header"
                )));
            }
            Some(Err(e)) => return Err(e),
            None => return Err(malformed("the content holds no record".into())),
        };
        let mut content = RedoContent {
            redo: RedoRecord {
                version: header.version,
                ops: Vec::new(),
                calvin_stamp: header.calvin_stamp,
                cross_shard_applied: header.cross_shard_applied,
                row_sources: Vec::new(),
                publishes: Vec::new(),
                row_changes: Vec::new(),
                user_roles: header.user_roles,
                sum_targets: Vec::new(),
            },
            sum_targets: header.sum_targets,
            identities: Vec::new(),
            reads: Vec::new(),
        };
        for frame in frames {
            let (tag, body) = frame?;
            match tag {
                RedoFrameTag::Header => {
                    return Err(malformed("the content holds a second header".into()));
                }
                RedoFrameTag::Op => content.redo.ops.push(decode_frame(tag, body)?),
                RedoFrameTag::RowChange => content.redo.row_changes.push(decode_frame(tag, body)?),
                RedoFrameTag::RowSource => content.redo.row_sources.push(decode_frame(tag, body)?),
                RedoFrameTag::Publish => content.redo.publishes.push(decode_frame(tag, body)?),
                RedoFrameTag::Identity => content.identities.push(decode_frame(tag, body)?),
                RedoFrameTag::Read => content.reads.push(decode_frame(tag, body)?),
            }
        }
        Ok(content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::types::ReplicatedIdentity;
    use crate::wal::CalvinStamp;
    use crate::wal::redo::frame::frame_resolved_lists;
    use crate::wal::{RedoPublish, RedoRowChange, RedoRowKind, RedoRowSource, RedoSubRecord};

    fn content() -> RedoContent {
        RedoContent {
            redo: RedoRecord {
                version: 1,
                ops: (0..50u8)
                    .map(|b| RedoSubRecord {
                        record_type: 3,
                        payload: vec![b; 64],
                    })
                    .collect(),
                calvin_stamp: Some(CalvinStamp {
                    epoch: 4,
                    position: 2,
                    vshard_id: 9,
                    mirror_database: None,
                }),
                cross_shard_applied: None,
                row_sources: vec![RedoRowSource {
                    collection: "orders".into(),
                    event_source: 2,
                    rows: vec!["o1".into()],
                }],
                publishes: vec![RedoPublish {
                    owner: "t".into(),
                    database_id: 0,
                    tenant_id: 1,
                    topic: "events".into(),
                    payload: "{}".into(),
                    metadata_floor: 3,
                    position: None,
                }],
                row_changes: vec![RedoRowChange {
                    collection: "orders".into(),
                    row: "o1".into(),
                    kind: RedoRowKind::Insert,
                }],
                user_roles: vec!["writer".into()],
                sum_targets: Vec::new(),
            },
            sum_targets: Vec::new(),
            identities: vec![ReplicatedIdentity {
                collection: "orders".into(),
                pk_bytes: b"o1".to_vec(),
                surrogate: 5,
            }],
            reads: Vec::new(),
        }
    }

    #[test]
    fn framed_content_round_trips() {
        let content = content();
        let framed = content.to_framed().expect("frame");
        assert_eq!(RedoContent::from_framed(&framed).expect("decode"), content);
    }

    /// The header frames a proposer writes, joined to the parts a resolving
    /// core frames, decode as the whole content framed at once.
    #[test]
    fn header_frames_and_resolved_parts_join_to_one_decode() {
        let content = content();
        let mut joined = Vec::new();
        let mut header = content.clone();
        header.redo.ops.clear();
        header.redo.row_changes.clear();
        header.redo.row_sources.clear();
        header.push_header_frames(&mut joined).expect("header");
        let parts = frame_resolved_lists(
            content.redo.row_sources.clone(),
            content.redo.ops.clone(),
            content.redo.row_changes.clone(),
            256,
        )
        .expect("parts");
        assert!(parts.parts.len() > 4);
        for part in &parts.parts {
            joined.extend_from_slice(part);
        }
        assert_eq!(joined, content.to_framed().expect("frame"));
        assert_eq!(RedoContent::from_framed(&joined).expect("decode"), content);
    }

    #[test]
    fn content_without_its_header_first_is_refused() {
        let content = content();
        let mut framed = Vec::new();
        push_frame(&mut framed, RedoFrameTag::Op, &content.redo.ops[0]).expect("op");
        framed.extend(content.to_framed().expect("frame"));
        assert!(RedoContent::from_framed(&framed).is_err());
        assert!(RedoContent::from_framed(&[]).is_err());
    }

    #[test]
    fn content_with_two_headers_is_refused() {
        let framed = content().to_framed().expect("frame");
        let mut twice = framed.clone();
        twice.extend(framed);
        assert!(RedoContent::from_framed(&twice).is_err());
    }
}
