// SPDX-License-Identifier: BUSL-1.1

//! Frame kinds of a run file, and the layout of a data block.
//!
//! A run file is a sequence of `[u32 LE len][body]` frames of three kinds.
//! The first body byte names the kind:
//! - a record body is a MessagePack array, so its first byte is `0x90..=0x9f`
//!   or `0xdc`/`0xdd`;
//! - a bloom body starts with [`BLOOM_TAG`];
//! - an index page body starts with [`PAGE_TAG`].
//!
//! A data block is its record frames followed by one bloom frame over the
//! block's keys. The bloom body is `[BLOOM_TAG][words: u64 LE..][u32 LE word
//! count]`, so a reader finds the bloom from the block's end without
//! decoding a record.

use std::path::Path;

use super::bloom::{BloomView, KeyBloom};
use crate::data::executor::handlers::spill::error::SpillError;

/// First body byte of a block bloom frame.
const BLOOM_TAG: u8 = 0x00;

/// First body byte of an index page frame.
pub(super) const PAGE_TAG: u8 = 0x01;

/// Bytes of a frame header.
pub(in crate::data::executor) const FRAME_HEADER: usize = 4;

/// Bytes of the word count that ends a bloom body.
const WORD_COUNT: usize = 4;

/// What one frame of a run file holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FrameKind {
    Record,
    Bloom,
    Page,
}

/// The kind of the frame whose body is `body`.
pub(super) fn frame_kind(body: &[u8]) -> FrameKind {
    match body.first() {
        Some(&BLOOM_TAG) => FrameKind::Bloom,
        Some(&PAGE_TAG) => FrameKind::Page,
        _ => FrameKind::Record,
    }
}

/// The bloom frame body of `bloom`, for the run file at `path`.
pub(super) fn bloom_body(path: &Path, bloom: &KeyBloom) -> Result<Vec<u8>, SpillError> {
    let words = bloom.words();
    let count = u32::try_from(words.len()).map_err(|_| SpillError::Corrupt {
        path: path.to_path_buf(),
        detail: format!("block bloom of {} words exceeds u32", words.len()),
    })?;
    let mut body = Vec::with_capacity(1 + words.len() * 8 + WORD_COUNT);
    body.push(BLOOM_TAG);
    for word in words {
        body.extend_from_slice(&word.to_le_bytes());
    }
    body.extend_from_slice(&count.to_le_bytes());
    Ok(body)
}

/// Split a data block into its bloom and its record frames.
pub(super) fn split_block<'a>(
    path: &Path,
    block: &'a [u8],
) -> Result<(BloomView<'a>, &'a [u8]), SpillError> {
    let corrupt = |detail: String| SpillError::Corrupt {
        path: path.to_path_buf(),
        detail,
    };
    let count_at = block
        .len()
        .checked_sub(WORD_COUNT)
        .ok_or_else(|| corrupt(format!("block of {} bytes has no bloom", block.len())))?;
    let mut count = [0u8; WORD_COUNT];
    count.copy_from_slice(block.get(count_at..).unwrap_or_default());
    let words = u32::from_le_bytes(count) as usize;
    let body_len = 1 + words * 8 + WORD_COUNT;
    let frame_at = block
        .len()
        .checked_sub(body_len + FRAME_HEADER)
        .ok_or_else(|| corrupt(format!("block bloom of {words} words overruns the block")))?;
    let mut header = [0u8; FRAME_HEADER];
    header.copy_from_slice(
        block
            .get(frame_at..frame_at + FRAME_HEADER)
            .unwrap_or_default(),
    );
    let body = block
        .get(frame_at + FRAME_HEADER..count_at)
        .unwrap_or_default();
    if u32::from_le_bytes(header) as usize != body_len || frame_kind(body) != FrameKind::Bloom {
        return Err(corrupt(format!(
            "block bloom frame at block byte {frame_at} is malformed"
        )));
    }
    let view = BloomView::new(body.get(1..).unwrap_or_default())
        .ok_or_else(|| corrupt(format!("block bloom at block byte {frame_at} has no word")))?;
    Ok((view, block.get(..frame_at).unwrap_or_default()))
}

#[cfg(test)]
mod tests {
    use super::super::bloom::key_hash;
    use super::*;

    #[test]
    fn a_block_splits_into_records_and_its_bloom() {
        let path = Path::new("b");
        let records = [9u8, 9, 9];
        let bloom = KeyBloom::from_hashes(&[key_hash(b"k")]);
        let body = bloom_body(path, &bloom).unwrap();
        let mut block = records.to_vec();
        block.extend_from_slice(&(body.len() as u32).to_le_bytes());
        block.extend_from_slice(&body);
        let (view, rest) = split_block(path, &block).unwrap();
        assert!(view.may_contain(b"k"));
        assert_eq!(rest, &records);
        assert_eq!(frame_kind(&body), FrameKind::Bloom);
        assert!(split_block(path, &block[1..]).is_ok());
        assert!(split_block(path, &block[..block.len() - 1]).is_err());
        assert!(split_block(path, &[1, 0]).is_err());
    }
}
