// SPDX-License-Identifier: Apache-2.0

//! Segment writer: flushes a memtable to an immutable on-disk segment.
//!
//! Produces a byte buffer containing: sorted term dictionary,
//! compressed 128-doc posting blocks per term, and a header/footer.

use std::collections::HashMap;

use crate::block::{CompactPosting, PostingBlock, into_blocks};

use super::error::SegmentError;
use super::format::{self, TermDictEntry};

/// Flush a memtable's postings into an immutable segment byte buffer.
///
/// `term_postings` is one index's drained map from `Memtable::drain_scope()`.
/// Returns the serialized segment bytes or a `SegmentError` if any term
/// exceeds `MAX_TERM_LEN`.
pub fn flush_to_segment(
    term_postings: HashMap<String, Vec<CompactPosting>>,
) -> Result<Vec<u8>, SegmentError> {
    flush_postings_to_segment(&term_postings)
}

/// Build a segment from a borrowed term→postings map. The map stays intact,
/// so a caller can drop it only after the segment is durably written.
pub fn flush_postings_to_segment(
    term_postings: &HashMap<String, Vec<CompactPosting>>,
) -> Result<Vec<u8>, SegmentError> {
    // Sort terms for binary-searchable term dictionary.
    let mut sorted_terms: Vec<(&String, &Vec<CompactPosting>)> = term_postings.iter().collect();
    sorted_terms.sort_by_key(|(term, _)| *term);

    // Phase 1: Encode posting blocks for each term, collect byte offsets.
    let mut posting_data = Vec::new();
    let mut dict_entries = Vec::with_capacity(sorted_terms.len());

    for (term, postings) in &sorted_terms {
        let term_len = term.len();
        if term_len > format::MAX_TERM_LEN {
            return Err(SegmentError::TermTooLong {
                term_len,
                max: format::MAX_TERM_LEN,
            });
        }

        let offset = posting_data.len() as u64;
        let df = postings.len() as u32;

        // Split into 128-doc blocks and serialize each.
        let blocks = into_blocks(postings.to_vec());
        let mut term_bytes = Vec::new();

        // Write number of blocks.
        term_bytes.extend_from_slice(&(blocks.len() as u32).to_le_bytes());

        for block in &blocks {
            let block_bytes = block.to_bytes();
            term_bytes.extend_from_slice(&(block_bytes.len() as u32).to_le_bytes());
            term_bytes.extend_from_slice(&block_bytes);
        }

        let posting_len = term_bytes.len() as u32;
        posting_data.extend_from_slice(&term_bytes);

        dict_entries.push(TermDictEntry {
            term: term.to_string(),
            posting_offset: offset,
            posting_len,
            df,
        });
    }

    // Phase 2: Build the segment buffer.
    let posting_data_offset = format::HEADER_SIZE as u64;
    let term_dict_offset = posting_data_offset + posting_data.len() as u64;

    let mut buf = Vec::new();

    format::write_header(
        &mut buf,
        dict_entries.len() as u32,
        term_dict_offset,
        posting_data_offset,
    );

    // Posting data.
    buf.extend_from_slice(&posting_data);

    // Term dictionary.
    for entry in &dict_entries {
        format::write_term_entry(&mut buf, entry)?;
    }

    // Footer CRC (CRC32C over all preceding bytes).
    format::write_footer_crc(&mut buf);

    Ok(buf)
}

/// Build a segment from pre-sorted, pre-blocked posting data.
///
/// Used by compaction and parallel build where blocks are already formed.
pub fn build_from_blocks(
    term_blocks: &[(String, Vec<PostingBlock>)],
) -> Result<Vec<u8>, SegmentError> {
    let mut posting_data = Vec::new();
    let mut dict_entries = Vec::with_capacity(term_blocks.len());

    for (term, blocks) in term_blocks {
        let term_len = term.len();
        if term_len > format::MAX_TERM_LEN {
            return Err(SegmentError::TermTooLong {
                term_len,
                max: format::MAX_TERM_LEN,
            });
        }

        let offset = posting_data.len() as u64;
        let df: u32 = blocks.iter().map(|b| b.len() as u32).sum();

        let mut term_bytes = Vec::new();
        term_bytes.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
        for block in blocks {
            let block_bytes = block.to_bytes();
            term_bytes.extend_from_slice(&(block_bytes.len() as u32).to_le_bytes());
            term_bytes.extend_from_slice(&block_bytes);
        }

        let posting_len = term_bytes.len() as u32;
        posting_data.extend_from_slice(&term_bytes);

        dict_entries.push(TermDictEntry {
            term: term.to_string(),
            posting_offset: offset,
            posting_len,
            df,
        });
    }

    let posting_data_offset = format::HEADER_SIZE as u64;
    let term_dict_offset = posting_data_offset + posting_data.len() as u64;

    let mut buf = Vec::new();
    format::write_header(
        &mut buf,
        dict_entries.len() as u32,
        term_dict_offset,
        posting_data_offset,
    );
    buf.extend_from_slice(&posting_data);
    for entry in &dict_entries {
        format::write_term_entry(&mut buf, entry)?;
    }

    // Footer CRC.
    format::write_footer_crc(&mut buf);

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::smallfloat;

    fn make_postings() -> HashMap<String, Vec<CompactPosting>> {
        let mut map = HashMap::new();
        map.insert(
            "hello".to_string(),
            vec![
                CompactPosting {
                    doc_id: nodedb_types::Surrogate(0),
                    term_freq: 2,
                    fieldnorm: smallfloat::encode(100),
                    positions: vec![0, 5],
                },
                CompactPosting {
                    doc_id: nodedb_types::Surrogate(1),
                    term_freq: 1,
                    fieldnorm: smallfloat::encode(50),
                    positions: vec![3],
                },
            ],
        );
        map.insert(
            "world".to_string(),
            vec![CompactPosting {
                doc_id: nodedb_types::Surrogate(0),
                term_freq: 1,
                fieldnorm: smallfloat::encode(100),
                positions: vec![1],
            }],
        );
        map
    }

    #[test]
    fn flush_produces_valid_segment() {
        let postings = make_postings();
        let segment = flush_to_segment(postings).unwrap();

        // Should start with magic.
        assert_eq!(&segment[0..4], b"FTSS");

        // Parse header.
        let header = format::parse_header(&segment).unwrap();
        assert_eq!(header.num_terms, 2);

        // CRC must be valid.
        format::verify_footer_crc(&segment).expect("CRC should be valid after flush");
    }

    #[test]
    fn flush_empty_memtable() {
        let segment = flush_to_segment(HashMap::new()).unwrap();
        let header = format::parse_header(&segment).unwrap();
        assert_eq!(header.num_terms, 0);
        format::verify_footer_crc(&segment).expect("CRC should be valid for empty segment");
    }

    #[test]
    fn flush_term_too_long_rejected() {
        let mut map = HashMap::new();
        map.insert(
            "x".repeat(u16::MAX as usize + 1),
            vec![CompactPosting {
                doc_id: nodedb_types::Surrogate(0),
                term_freq: 1,
                fieldnorm: smallfloat::encode(10),
                positions: vec![0],
            }],
        );
        let err = flush_to_segment(map).unwrap_err();
        assert!(
            matches!(err, SegmentError::TermTooLong { .. }),
            "expected TermTooLong, got {err}"
        );
    }
}
