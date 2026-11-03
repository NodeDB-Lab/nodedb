// SPDX-License-Identifier: Apache-2.0

//! Renumbering a segment codec after compaction.

use super::codec::SegmentCodec;

impl SegmentCodec {
    /// Follow a compaction of the segment: `id_map` maps each old local id
    /// to its new one, `u32::MAX` for a removed node, and `new_len` nodes
    /// remain. Code rows move to their new ids. A graph codec keeps removed
    /// nodes as stale routing nodes. Once stale nodes outnumber the rest,
    /// the graph codec is dropped and the segment carries none, so repeated
    /// compactions never grow it. Returns `true` when the codec was dropped:
    /// the segment then needs a codec rebuild.
    pub fn remap(&mut self, id_map: &[u32], new_len: usize) -> bool {
        match self {
            Self::None => false,
            Self::Sq8 { codec, codes } => {
                *codes = remap_rows(codes, codec.dim(), id_map, new_len);
                false
            }
            Self::Pq { codec, codes } => {
                *codes = remap_rows(codes, codec.m, id_map, new_len);
                false
            }
            Self::Graph(graph) => {
                graph.remap_ids(id_map);
                let stale_majority = graph.stale_count().saturating_mul(2) > graph.len();
                if stale_majority {
                    *self = Self::None;
                }
                stale_majority
            }
        }
    }
}

/// Code rows reordered through `id_map` (old local id → new local id,
/// `u32::MAX` for a removed node), `width` bytes per row.
fn remap_rows(codes: &[u8], width: usize, id_map: &[u32], new_len: usize) -> Vec<u8> {
    let mut out = vec![0u8; width * new_len];
    for (old, &new) in id_map.iter().enumerate() {
        if new == u32::MAX {
            continue;
        }
        let new = new as usize;
        if let (Some(src), Some(dst)) = (
            codes.get(old * width..(old + 1) * width),
            out.get_mut(new * width..(new + 1) * width),
        ) {
            dst.copy_from_slice(src);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_follow_the_id_map() {
        let codes = [1u8, 1, 2, 2, 3, 3, 4, 4];
        let id_map = [0, u32::MAX, 1, 2];
        assert_eq!(remap_rows(&codes, 2, &id_map, 3), vec![1, 1, 3, 3, 4, 4]);
    }
}
