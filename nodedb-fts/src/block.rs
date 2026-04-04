//! 128-document posting block with compressed storage.
//!
//! Each `PostingBlock` holds up to 128 postings. Data is stored as:
//! - Delta-encoded, bitpacked doc IDs (u32)
//! - Bitpacked term frequencies (u32)
//! - SmallFloat fieldnorm bytes (1 byte per doc)
//! - Per-doc position lists (variable length)
//!
//! This is the storage unit for BMW (Block-Max WAND): each block carries
//! `block_max_tf` and `block_min_fieldnorm` for skip-level pruning.

use crate::codec::{bitpack, delta, smallfloat};

/// Maximum number of documents per posting block.
pub const BLOCK_SIZE: usize = 128;

/// A compact posting entry using integer doc IDs.
#[derive(Debug, Clone)]
pub struct CompactPosting {
    pub doc_id: u32,
    pub term_freq: u32,
    pub fieldnorm: u8,
    pub positions: Vec<u32>,
}

/// A block of up to 128 postings in compressed form.
///
/// Provides both in-memory access (via `decode`) and serialized byte
/// representation (via `to_bytes` / `from_bytes`).
#[derive(Debug, Clone)]
pub struct PostingBlock {
    /// Sorted u32 doc IDs in this block.
    pub doc_ids: Vec<u32>,
    /// Term frequencies, parallel to `doc_ids`.
    pub term_freqs: Vec<u32>,
    /// SmallFloat-encoded fieldnorms, parallel to `doc_ids`.
    pub fieldnorms: Vec<u8>,
    /// Per-document position lists, parallel to `doc_ids`.
    pub positions: Vec<Vec<u32>>,
}

/// Block-level metadata for BMW skip index.
#[derive(Debug, Clone, Copy)]
pub struct BlockMeta {
    /// Last (largest) doc ID in this block.
    pub last_doc_id: u32,
    /// Number of documents in this block.
    pub doc_count: u16,
    /// Maximum term frequency in this block (for upper bound scoring).
    pub block_max_tf: u32,
    /// Minimum fieldnorm in this block (shortest doc — highest BM25 potential).
    pub block_min_fieldnorm: u8,
}

impl PostingBlock {
    /// Create a block from a slice of compact postings.
    ///
    /// Postings MUST be sorted by doc_id. Panics in debug if unsorted.
    /// Truncates to `BLOCK_SIZE` if more are provided.
    pub fn from_postings(postings: &[CompactPosting]) -> Self {
        let n = postings.len().min(BLOCK_SIZE);
        debug_assert!(
            postings[..n].windows(2).all(|w| w[0].doc_id <= w[1].doc_id),
            "postings must be sorted by doc_id"
        );

        Self {
            doc_ids: postings[..n].iter().map(|p| p.doc_id).collect(),
            term_freqs: postings[..n].iter().map(|p| p.term_freq).collect(),
            fieldnorms: postings[..n].iter().map(|p| p.fieldnorm).collect(),
            positions: postings[..n].iter().map(|p| p.positions.clone()).collect(),
        }
    }

    /// Number of documents in this block.
    pub fn len(&self) -> usize {
        self.doc_ids.len()
    }

    /// Whether the block is empty.
    pub fn is_empty(&self) -> bool {
        self.doc_ids.is_empty()
    }

    /// Compute block metadata for BMW skip index.
    pub fn meta(&self) -> BlockMeta {
        BlockMeta {
            last_doc_id: self.doc_ids.last().copied().unwrap_or(0),
            doc_count: self.doc_ids.len() as u16,
            block_max_tf: self.term_freqs.iter().copied().max().unwrap_or(0),
            block_min_fieldnorm: self.fieldnorms.iter().copied().min().unwrap_or(0),
        }
    }

    /// Serialize the block to bytes.
    ///
    /// Layout:
    /// ```text
    /// [doc_count: u16 LE]
    /// [packed_doc_id_deltas: len u32 LE + bytes]
    /// [packed_term_freqs: len u32 LE + bytes]
    /// [fieldnorms: doc_count bytes]
    /// [position_data: for each doc, count u16 LE + packed positions]
    /// ```
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Doc count.
        buf.extend_from_slice(&(self.doc_ids.len() as u16).to_le_bytes());

        // Delta-encoded, bitpacked doc IDs.
        let deltas = delta::encode(&self.doc_ids);
        let packed_ids = bitpack::pack(&deltas);
        buf.extend_from_slice(&(packed_ids.len() as u32).to_le_bytes());
        buf.extend_from_slice(&packed_ids);

        // Bitpacked term frequencies.
        let packed_freqs = bitpack::pack(&self.term_freqs);
        buf.extend_from_slice(&(packed_freqs.len() as u32).to_le_bytes());
        buf.extend_from_slice(&packed_freqs);

        // Fieldnorms (raw bytes, 1 per doc).
        buf.extend_from_slice(&self.fieldnorms);

        // Position data: for each doc, [count: u16 LE][packed positions].
        for positions in &self.positions {
            buf.extend_from_slice(&(positions.len() as u16).to_le_bytes());
            if !positions.is_empty() {
                let packed_pos = bitpack::pack(positions);
                buf.extend_from_slice(&(packed_pos.len() as u16).to_le_bytes());
                buf.extend_from_slice(&packed_pos);
            } else {
                buf.extend_from_slice(&0u16.to_le_bytes());
            }
        }

        buf
    }

    /// Deserialize a block from bytes. Returns `None` if malformed.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        let mut pos = 0;

        // Doc count.
        if buf.len() < 2 {
            return None;
        }
        let doc_count = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
        pos += 2;

        if doc_count == 0 {
            return Some(Self {
                doc_ids: Vec::new(),
                term_freqs: Vec::new(),
                fieldnorms: Vec::new(),
                positions: Vec::new(),
            });
        }

        // Packed doc ID deltas.
        if pos + 4 > buf.len() {
            return None;
        }
        let ids_len =
            u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + ids_len > buf.len() {
            return None;
        }
        let packed_deltas = &buf[pos..pos + ids_len];
        let deltas = bitpack::unpack(packed_deltas);
        let doc_ids = delta::decode(&deltas);
        pos += ids_len;

        // Packed term frequencies.
        if pos + 4 > buf.len() {
            return None;
        }
        let freqs_len =
            u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + freqs_len > buf.len() {
            return None;
        }
        let term_freqs = bitpack::unpack(&buf[pos..pos + freqs_len]);
        pos += freqs_len;

        // Fieldnorms.
        if pos + doc_count > buf.len() {
            return None;
        }
        let fieldnorms = buf[pos..pos + doc_count].to_vec();
        pos += doc_count;

        // Position data.
        let mut positions = Vec::with_capacity(doc_count);
        for _ in 0..doc_count {
            if pos + 2 > buf.len() {
                return None;
            }
            let num_positions = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
            pos += 2;

            if num_positions == 0 {
                if pos + 2 > buf.len() {
                    return None;
                }
                pos += 2; // Skip packed_len (0).
                positions.push(Vec::new());
            } else {
                if pos + 2 > buf.len() {
                    return None;
                }
                let packed_pos_len = u16::from_le_bytes([buf[pos], buf[pos + 1]]) as usize;
                pos += 2;
                if pos + packed_pos_len > buf.len() {
                    return None;
                }
                let doc_positions = bitpack::unpack(&buf[pos..pos + packed_pos_len]);
                pos += packed_pos_len;
                positions.push(doc_positions);
            }
        }

        if doc_ids.len() != doc_count || term_freqs.len() != doc_count {
            return None;
        }

        Some(Self {
            doc_ids,
            term_freqs,
            fieldnorms,
            positions,
        })
    }
}

/// Split a list of compact postings into 128-doc blocks.
pub fn into_blocks(mut postings: Vec<CompactPosting>) -> Vec<PostingBlock> {
    postings.sort_by_key(|p| p.doc_id);
    postings
        .chunks(BLOCK_SIZE)
        .map(PostingBlock::from_postings)
        .collect()
}

/// Encode a document length as a SmallFloat fieldnorm byte.
pub fn encode_fieldnorm(doc_length: u32) -> u8 {
    smallfloat::encode(doc_length)
}

/// Decode a SmallFloat fieldnorm byte back to approximate document length.
pub fn decode_fieldnorm(byte: u8) -> u32 {
    smallfloat::decode(byte)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_postings(n: usize) -> Vec<CompactPosting> {
        (0..n)
            .map(|i| CompactPosting {
                doc_id: (i * 3) as u32,
                term_freq: (i % 5 + 1) as u32,
                fieldnorm: smallfloat::encode((i * 10 + 20) as u32),
                positions: vec![i as u32, (i + 5) as u32],
            })
            .collect()
    }

    #[test]
    fn block_roundtrip() {
        let postings = make_postings(50);
        let block = PostingBlock::from_postings(&postings);
        assert_eq!(block.len(), 50);

        let bytes = block.to_bytes();
        let restored = PostingBlock::from_bytes(&bytes).unwrap();

        assert_eq!(restored.doc_ids, block.doc_ids);
        assert_eq!(restored.term_freqs, block.term_freqs);
        assert_eq!(restored.fieldnorms, block.fieldnorms);
        assert_eq!(restored.positions, block.positions);
    }

    #[test]
    fn block_128_roundtrip() {
        let postings = make_postings(128);
        let block = PostingBlock::from_postings(&postings);
        assert_eq!(block.len(), 128);

        let bytes = block.to_bytes();
        let restored = PostingBlock::from_bytes(&bytes).unwrap();
        assert_eq!(restored.doc_ids, block.doc_ids);
    }

    #[test]
    fn block_meta() {
        let postings = make_postings(10);
        let block = PostingBlock::from_postings(&postings);
        let meta = block.meta();

        assert_eq!(meta.doc_count, 10);
        assert_eq!(meta.last_doc_id, 27); // (9 * 3)
        assert_eq!(meta.block_max_tf, 5); // max of (i%5+1) for i=0..9
    }

    #[test]
    fn into_blocks_splits() {
        let postings = make_postings(300);
        let blocks = into_blocks(postings);
        assert_eq!(blocks.len(), 3); // 128 + 128 + 44
        assert_eq!(blocks[0].len(), 128);
        assert_eq!(blocks[1].len(), 128);
        assert_eq!(blocks[2].len(), 44);
    }

    #[test]
    fn empty_block() {
        let block = PostingBlock::from_postings(&[]);
        assert!(block.is_empty());
        let bytes = block.to_bytes();
        let restored = PostingBlock::from_bytes(&bytes).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn fieldnorm_encode_decode() {
        for len in [0, 1, 5, 8, 10, 20, 50, 100, 500, 1000, 10_000] {
            let encoded = encode_fieldnorm(len);
            let decoded = decode_fieldnorm(encoded);
            if len <= 8 {
                assert_eq!(decoded, len, "exact for small: len={len}");
            } else {
                assert!(decoded <= len, "decoded {decoded} > original {len}");
                let error = (len - decoded) as f64 / len as f64;
                assert!(
                    error < 0.25,
                    "len={len}, decoded={decoded}, error={error:.4}"
                );
            }
        }
    }

    #[test]
    fn compression_ratio() {
        // 128 postings with small deltas should compress well.
        let postings: Vec<CompactPosting> = (0..128)
            .map(|i| CompactPosting {
                doc_id: i,
                term_freq: 1,
                fieldnorm: smallfloat::encode(100),
                positions: vec![0],
            })
            .collect();
        let block = PostingBlock::from_postings(&postings);
        let bytes = block.to_bytes();

        // Uncompressed: 128 * (4 + 4 + 1 + ~8) = ~2176 bytes.
        // Compressed should be significantly smaller.
        // Position data is the bulk: 128 docs × (2 pos_count + 2 packed_len + 3 header + 1 data).
        // The IDs and freqs should be very compact (1-bit deltas, 1-bit freqs).
        // Total should be well under uncompressed 128*(4+4+1+8) = 2176 bytes.
        assert!(
            bytes.len() < 1500,
            "compressed block should be < 1500 bytes, got {}",
            bytes.len()
        );
    }
}
