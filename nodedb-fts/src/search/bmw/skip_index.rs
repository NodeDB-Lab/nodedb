// SPDX-License-Identifier: Apache-2.0

//! Skip index for Block-Max WAND.
//!
//! One `SkipEntry` per 128-doc block. Enables two operations:
//! 1. Advance to doc_id ≥ target (binary search on `last_doc_id`).
//! 2. Compute block upper bound without decompressing the block.

use nodedb_types::Surrogate;

use crate::block::{BlockMeta, PostingBlock};

/// Per-block skip entry for BMW.
#[derive(Debug, Clone, Copy)]
pub struct SkipEntry {
    /// Block-level metadata (last_doc_id, block_max_tf, block_min_fieldnorm, doc_count).
    pub meta: BlockMeta,
    /// Index into the parent `TermBlocks::blocks` vec.
    pub block_idx: u32,
}

/// All blocks for a single term, with their skip entries.
#[derive(Debug)]
pub struct TermBlocks {
    /// The decompressed posting blocks for this term.
    pub blocks: Vec<PostingBlock>,
    /// Skip entries parallel to `blocks`.
    pub skip: Vec<SkipEntry>,
    /// Document frequency (total postings across all blocks).
    pub df: u32,
    /// Global max TF across all blocks (for term_max_score).
    pub global_max_tf: u32,
    /// Global min fieldnorm across all blocks (for term_max_score).
    pub global_min_fieldnorm: u8,
}

impl TermBlocks {
    /// Build skip index from a list of posting blocks.
    pub fn from_blocks(blocks: Vec<PostingBlock>) -> Self {
        let mut df = 0u32;
        let mut global_max_tf = 0u32;
        let mut global_min_fieldnorm = u8::MAX;

        let skip: Vec<SkipEntry> = blocks
            .iter()
            .enumerate()
            .map(|(i, block)| {
                let meta = block.meta();
                df += meta.doc_count as u32;
                if meta.block_max_tf > global_max_tf {
                    global_max_tf = meta.block_max_tf;
                }
                if meta.block_min_fieldnorm < global_min_fieldnorm {
                    global_min_fieldnorm = meta.block_min_fieldnorm;
                }
                SkipEntry {
                    meta,
                    block_idx: i as u32,
                }
            })
            .collect();

        if global_min_fieldnorm == u8::MAX {
            global_min_fieldnorm = 0;
        }

        Self {
            blocks,
            skip,
            df,
            global_max_tf,
            global_min_fieldnorm,
        }
    }

    /// Number of blocks.
    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Find the block index containing or following `target_doc_id`.
    ///
    /// Returns `None` if all blocks have `last_doc_id < target_doc_id`
    /// (i.e., the term is exhausted past that point).
    pub fn advance_to_block(&self, target_doc_id: Surrogate) -> Option<usize> {
        // Binary search on skip entries' last_doc_id.
        let pos = self
            .skip
            .partition_point(|entry| entry.meta.last_doc_id < target_doc_id);
        if pos < self.skip.len() {
            Some(pos)
        } else {
            None
        }
    }

    /// The `(term_freq, fieldnorm)` of `doc_id`'s posting, or `None` when the
    /// term does not occur in the document.
    pub fn lookup(&self, doc_id: Surrogate) -> Option<(u32, u8)> {
        let block = &self.blocks[self.advance_to_block(doc_id)?];
        let pos = block.doc_ids.binary_search(&doc_id).ok()?;
        Some((block.term_freqs[pos], block.fieldnorms[pos]))
    }

    /// Every document the term occurs in.
    pub fn doc_ids(&self) -> impl Iterator<Item = Surrogate> + '_ {
        self.blocks.iter().flat_map(|b| b.doc_ids.iter().copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::CompactPosting;
    use crate::codec::smallfloat;

    fn make_term_blocks(doc_ids: &[u32], tf: u32) -> TermBlocks {
        let mut postings: Vec<CompactPosting> = doc_ids
            .iter()
            .map(|&id| CompactPosting {
                doc_id: Surrogate(id),
                term_freq: tf,
                fieldnorm: smallfloat::encode(100),
                positions: vec![0],
            })
            .collect();
        postings.sort_by_key(|p| p.doc_id);

        let blocks = crate::block::into_blocks(postings);
        TermBlocks::from_blocks(blocks)
    }

    #[test]
    fn skip_index_basic() {
        // 200 docs → 2 blocks (128 + 72).
        let ids: Vec<u32> = (0..200).collect();
        let tb = make_term_blocks(&ids, 2);

        assert_eq!(tb.num_blocks(), 2);
        assert_eq!(tb.df, 200);
        assert_eq!(tb.global_max_tf, 2);
        assert_eq!(tb.skip[0].meta.last_doc_id, Surrogate(127));
        assert_eq!(tb.skip[1].meta.last_doc_id, Surrogate(199));
    }

    #[test]
    fn advance_to_block() {
        let ids: Vec<u32> = (0..300).collect();
        let tb = make_term_blocks(&ids, 1);
        // 3 blocks: [0..127], [128..255], [256..299]

        assert_eq!(tb.advance_to_block(Surrogate(0)), Some(0));
        assert_eq!(tb.advance_to_block(Surrogate(50)), Some(0));
        assert_eq!(tb.advance_to_block(Surrogate(127)), Some(0));
        assert_eq!(tb.advance_to_block(Surrogate(128)), Some(1));
        assert_eq!(tb.advance_to_block(Surrogate(200)), Some(1));
        assert_eq!(tb.advance_to_block(Surrogate(256)), Some(2));
        assert_eq!(tb.advance_to_block(Surrogate(299)), Some(2));
        assert_eq!(tb.advance_to_block(Surrogate(300)), None); // Past all blocks.
    }

    #[test]
    fn empty_term() {
        let tb = TermBlocks::from_blocks(Vec::new());
        assert_eq!(tb.df, 0);
        assert_eq!(tb.num_blocks(), 0);
        assert_eq!(tb.advance_to_block(Surrogate(0)), None);
        assert_eq!(tb.lookup(Surrogate(0)), None);
    }

    #[test]
    fn lookup_finds_postings_across_blocks() {
        let ids: Vec<u32> = (0..300).map(|i| i * 2).collect();
        let tb = make_term_blocks(&ids, 3);
        assert_eq!(tb.lookup(Surrogate(0)), Some((3, smallfloat::encode(100))));
        assert_eq!(
            tb.lookup(Surrogate(400)),
            Some((3, smallfloat::encode(100)))
        );
        assert_eq!(tb.lookup(Surrogate(401)), None);
        assert_eq!(tb.lookup(Surrogate(1000)), None);
        assert_eq!(tb.doc_ids().count(), 300);
    }
}
