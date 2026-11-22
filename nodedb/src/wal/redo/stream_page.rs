// SPDX-License-Identifier: BUSL-1.1

//! One page of a chunked redo's content, read back from the WAL.
//!
//! A page holds the records of one section of the content
//! ([`RedoStreamSection`]), at most a page bound of them, or one record past
//! the bound alone. The read also stops once it stepped over a page bound
//! of other records, so a page reads a bounded stretch of the content
//! however the sections lie. Each page names the record boundary the next
//! page starts at, so the caller holds one page at a time.

use nodedb_physical::physical_plan::{RedoStreamPage, RedoStreamSection};

use super::frame::{FRAME_HEAD_BYTES, FrameBatcher, RedoFrameTag, RedoFrames};
use super::stream_read::{RedoStreamReader, WalReadSource, WalRecordCursor};
use super::stream_ref::RedoStreamRef;

/// What a section does with one record.
enum Take {
    /// The record belongs to the section.
    Keep,
    /// The record belongs to another section.
    Pass,
    /// The section ends before the record.
    End,
}

fn take(section: RedoStreamSection, tag: RedoFrameTag) -> Take {
    match (section, tag) {
        (RedoStreamSection::Identities, RedoFrameTag::Header | RedoFrameTag::Identity) => {
            Take::Keep
        }
        (RedoStreamSection::Identities, RedoFrameTag::Publish | RedoFrameTag::Read) => Take::Pass,
        (
            RedoStreamSection::Identities,
            RedoFrameTag::RowSource | RedoFrameTag::Op | RedoFrameTag::RowChange,
        ) => Take::End,
        (RedoStreamSection::RowChanges, RedoFrameTag::RowChange) => Take::Keep,
        (
            RedoStreamSection::RowChanges,
            RedoFrameTag::Header
            | RedoFrameTag::Identity
            | RedoFrameTag::Publish
            | RedoFrameTag::Read
            | RedoFrameTag::RowSource
            | RedoFrameTag::Op,
        ) => Take::Pass,
    }
}

/// The page of `section` that starts at content offset `from`, read through
/// cursor `C`. `from` is `0` or the `next` of the previous page.
pub fn read_stream_page<C: WalRecordCursor>(
    source: &WalReadSource,
    reference: RedoStreamRef,
    section: RedoStreamSection,
    from: u64,
    max_page_bytes: u64,
) -> crate::Result<RedoStreamPage> {
    let total = reference.len;
    let max = usize::try_from(max_page_bytes)
        .unwrap_or(usize::MAX)
        .max(FRAME_HEAD_BYTES);
    let mut reader = RedoStreamReader::<C>::open(source, reference)?;
    reader.seek_content(from)?;
    let mut batcher = FrameBatcher::new(reader, max);
    let mut frames = Vec::new();
    let mut passed = 0usize;
    let mut offset = from;
    while let Some(batch) = batcher.next_batch()? {
        let mut at = 0usize;
        for frame in RedoFrames::new(&batch) {
            let (tag, body) = frame?;
            let len = FRAME_HEAD_BYTES + body.len();
            let record = batch
                .get(at..at + len)
                .ok_or_else(|| crate::Error::Internal {
                    detail: format!(
                        "a redo stream page read lost a {len}-byte record at batch offset {at}"
                    ),
                })?;
            match take(section, tag) {
                Take::End => {
                    return Ok(RedoStreamPage {
                        frames,
                        next: offset,
                        done: true,
                    });
                }
                Take::Keep => {
                    if !frames.is_empty() && frames.len() + len > max {
                        return Ok(RedoStreamPage {
                            frames,
                            next: offset,
                            done: false,
                        });
                    }
                    frames.extend_from_slice(record);
                }
                Take::Pass => passed += len,
            }
            at += len;
            offset += len as u64;
            if frames.len() >= max || passed >= max {
                return Ok(RedoStreamPage {
                    frames,
                    next: offset,
                    done: offset >= total,
                });
            }
        }
    }
    Ok(RedoStreamPage {
        frames,
        next: offset,
        done: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::{RedoChunkPlacement, WalManager};
    use crate::wal::redo::chunk::{RedoChunkHeader, RedoStreamId};
    use crate::wal::redo::frame::{RedoContentHeader, decode_frame, push_frame};
    use crate::wal::redo::stream_ref::{RedoChunkRef, chunk_crc};
    use crate::wal::{RedoRowChange, RedoRowKind, RedoSubRecord};

    /// Content with a header, `identities` identity records, `ops`
    /// sub-records and `changes` row changes.
    fn content(identities: u32, ops: u32, changes: u32) -> Vec<u8> {
        let header = RedoContentHeader {
            version: 1,
            calvin_stamp: None,
            cross_shard_applied: None,
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
            pins: Default::default(),
        };
        let mut out = Vec::new();
        push_frame(&mut out, RedoFrameTag::Header, &header).expect("header");
        for index in 0..identities {
            let identity = crate::control::wal_replication::types::ReplicatedIdentity {
                collection: "orders".into(),
                pk_bytes: format!("o{index}").into_bytes(),
                surrogate: index + 1,
            };
            push_frame(&mut out, RedoFrameTag::Identity, &identity).expect("identity");
        }
        for index in 0..ops {
            let op = RedoSubRecord {
                record_type: 7,
                payload: vec![index as u8; 50],
            };
            push_frame(&mut out, RedoFrameTag::Op, &op).expect("op");
        }
        for index in 0..changes {
            let change = RedoRowChange {
                collection: "orders".into(),
                row: format!("o{index}"),
                kind: RedoRowKind::Insert,
            };
            push_frame(&mut out, RedoFrameTag::RowChange, &change).expect("change");
        }
        out
    }

    fn append(wal: &WalManager, content: &[u8]) -> RedoStreamRef {
        let stream = RedoStreamId::Session {
            vshard: 1,
            idempotency_key: 3,
        };
        let placement = RedoChunkPlacement {
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(1),
            database_id: DatabaseId::DEFAULT,
        };
        let mut chunks = Vec::new();
        for (index, bytes) in (0u32..).zip(content.chunks(97)) {
            let header = RedoChunkHeader {
                stream,
                group_id: 1,
                term: 1,
                len: content.len() as u64,
                index,
            };
            let lsns = wal
                .append_redo_chunk(1 + u64::from(index), placement, header, bytes)
                .expect("chunk");
            chunks.push(RedoChunkRef {
                index,
                first_lsn: lsns.first.as_u64(),
                last_lsn: lsns.last.as_u64(),
                len: bytes.len() as u64,
                crc: chunk_crc(bytes),
            });
        }
        wal.sync().expect("sync");
        RedoStreamRef {
            stream,
            len: content.len() as u64,
            chunks,
            publish_position: None,
            entry: None,
            stamp_floor: None,
            keys: Default::default(),
        }
    }

    /// Every page of `section`, in order.
    fn pages(
        wal: &WalManager,
        reference: &RedoStreamRef,
        section: RedoStreamSection,
        max: u64,
    ) -> Vec<RedoStreamPage> {
        let mut out = Vec::new();
        let mut from = 0;
        loop {
            let page = read_stream_page::<nodedb_wal::LazyWalReader>(
                &wal.read_source(),
                reference.clone(),
                section,
                from,
                max,
            )
            .expect("page");
            assert!(page.next >= from);
            from = page.next;
            let done = page.done;
            out.push(page);
            if done {
                return out;
            }
        }
    }

    fn tags(pages: &[RedoStreamPage]) -> Vec<RedoFrameTag> {
        pages
            .iter()
            .flat_map(|page| {
                RedoFrames::new(&page.frames)
                    .map(|frame| frame.expect("whole record").0)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The identity section pages through the header and every identity in
    /// order, each page within its bound, and stops at the first
    /// sub-record.
    #[test]
    fn the_identity_section_pages_up_to_the_first_sub_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let reference = append(&wal, &content(40, 30, 20));
        let read = pages(&wal, &reference, RedoStreamSection::Identities, 200);
        assert!(read.len() > 3, "{} pages", read.len());
        for page in &read {
            assert!(page.frames.len() <= 200 + 64);
        }
        let all = tags(&read);
        assert_eq!(all[0], RedoFrameTag::Header);
        assert_eq!(all.len(), 41);
        assert!(all[1..].iter().all(|tag| *tag == RedoFrameTag::Identity));
        let identities: Vec<u32> = read
            .iter()
            .flat_map(|page| {
                RedoFrames::new(&page.frames)
                    .filter_map(|frame| {
                        let (tag, body) = frame.expect("record");
                        (tag == RedoFrameTag::Identity).then(|| {
                            decode_frame::<
                                    crate::control::wal_replication::types::ReplicatedIdentity,
                                >(tag, body)
                                .expect("identity")
                                .surrogate
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(identities, (1..=40).collect::<Vec<_>>());
    }

    /// The row-change section steps over the sub-records in bounded pages
    /// and returns every row change in order.
    #[test]
    fn the_row_change_section_pages_past_the_sub_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let reference = append(&wal, &content(5, 60, 25));
        let read = pages(&wal, &reference, RedoStreamSection::RowChanges, 300);
        assert!(read.iter().any(|page| page.frames.is_empty()));
        let all = tags(&read);
        assert_eq!(all.len(), 25);
        assert!(all.iter().all(|tag| *tag == RedoFrameTag::RowChange));
        assert_eq!(read.last().map(|page| page.next), Some(reference.len));
    }

    #[test]
    fn a_seek_past_the_content_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("wal");
        let reference = append(&wal, &content(1, 1, 1));
        let past = reference.len + 1;
        assert!(
            read_stream_page::<nodedb_wal::LazyWalReader>(
                &wal.read_source(),
                reference,
                RedoStreamSection::RowChanges,
                past,
                100,
            )
            .is_err()
        );
    }
}
