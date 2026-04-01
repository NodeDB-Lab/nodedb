//! Bitmap index for symbol (tag) columns.
//!
//! For each unique symbol value in a partition, stores a bitmap of row
//! positions where that value appears. Enables O(1) equality filter on
//! tag columns without scanning the full column.
//!
//! Uses a simple `Vec<u64>` bitset. Can be swapped for Roaring bitmaps
//! for better compression on sparse data.

use std::collections::HashMap;

/// A simple fixed-size bitmap backed by `Vec<u64>`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Bitmap {
    bits: Vec<u64>,
    len: usize,
}

impl Bitmap {
    /// Create a new bitmap with `len` bits, all cleared.
    pub fn new(len: usize) -> Self {
        let words = len.div_ceil(64);
        Self {
            bits: vec![0u64; words],
            len,
        }
    }

    /// Set bit at position `pos`.
    pub fn set(&mut self, pos: usize) {
        if pos < self.len {
            self.bits[pos / 64] |= 1u64 << (pos % 64);
        }
    }

    /// Test bit at position `pos`.
    pub fn get(&self, pos: usize) -> bool {
        if pos < self.len {
            (self.bits[pos / 64] >> (pos % 64)) & 1 == 1
        } else {
            false
        }
    }

    /// Bitwise AND of two bitmaps. Result has same length as self.
    ///
    /// Dispatches to SIMD (AVX-512/AVX2/NEON) for large bitmaps.
    pub fn and(&self, other: &Bitmap) -> Bitmap {
        let min_words = self.bits.len().min(other.bits.len());
        let mut result = Bitmap::new(self.len);
        let simd = super::bitmap_simd::ops();
        (simd.and_slices)(
            &self.bits[..min_words],
            &other.bits[..min_words],
            &mut result.bits[..min_words],
        );
        result
    }

    /// Bitwise OR of two bitmaps.
    ///
    /// Dispatches to SIMD (AVX-512/AVX2/NEON) for large bitmaps.
    pub fn or(&self, other: &Bitmap) -> Bitmap {
        let max_len = self.len.max(other.len);
        let max_words = self.bits.len().max(other.bits.len());
        let mut result = Bitmap::new(max_len);
        let min_words = self.bits.len().min(other.bits.len());
        let simd = super::bitmap_simd::ops();
        (simd.or_slices)(
            &self.bits[..min_words],
            &other.bits[..min_words],
            &mut result.bits[..min_words],
        );
        // Copy remaining words from the longer bitmap.
        if self.bits.len() > min_words {
            result.bits[min_words..max_words].copy_from_slice(&self.bits[min_words..max_words]);
        } else if other.bits.len() > min_words {
            result.bits[min_words..max_words].copy_from_slice(&other.bits[min_words..max_words]);
        }
        result
    }

    /// Count of set bits.
    pub fn count_ones(&self) -> usize {
        self.bits.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Iterate over set bit positions.
    pub fn iter_ones(&self) -> impl Iterator<Item = u32> + '_ {
        let len = self.len;
        self.bits
            .iter()
            .enumerate()
            .flat_map(move |(word_idx, &word)| {
                let base = word_idx as u32 * 64;
                (0..64u32)
                    .filter(move |bit| (word >> bit) & 1 == 1)
                    .map(move |bit| base + bit)
                    .filter(move |&pos| (pos as usize) < len)
            })
    }

    /// Number of bits in the bitmap.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Bitmap index for a single symbol column.
///
/// Maps each symbol ID to a bitmap of row positions where that symbol appears.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SymbolBitmapIndex {
    /// symbol_id → bitmap of row positions.
    bitmaps: HashMap<u32, Bitmap>,
    /// Total number of rows indexed.
    row_count: usize,
}

impl SymbolBitmapIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build the bitmap index from a symbol column.
    pub fn build(symbol_ids: &[u32]) -> Self {
        let row_count = symbol_ids.len();
        let mut bitmaps: HashMap<u32, Bitmap> = HashMap::new();

        for (pos, &sym_id) in symbol_ids.iter().enumerate() {
            if sym_id == u32::MAX {
                continue; // sentinel for NULL
            }
            bitmaps
                .entry(sym_id)
                .or_insert_with(|| Bitmap::new(row_count))
                .set(pos);
        }

        Self { bitmaps, row_count }
    }

    /// Get the bitmap for a specific symbol value (equality filter).
    pub fn get(&self, symbol_id: u32) -> Option<&Bitmap> {
        self.bitmaps.get(&symbol_id)
    }

    /// Get the bitmap for multiple symbol values (IN filter) via OR.
    pub fn get_any(&self, symbol_ids: &[u32]) -> Bitmap {
        let mut result = Bitmap::new(self.row_count);
        for &id in symbol_ids {
            if let Some(bm) = self.bitmaps.get(&id) {
                result = result.or(bm);
            }
        }
        result
    }

    /// Number of distinct symbol values indexed.
    pub fn cardinality(&self) -> usize {
        self.bitmaps.len()
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Compound filter: AND a timestamp range bitmap with a symbol equality bitmap.
    pub fn compound_filter(&self, symbol_id: u32, timestamp_bitmap: &Bitmap) -> Bitmap {
        match self.get(symbol_id) {
            Some(sym_bm) => sym_bm.and(timestamp_bitmap),
            None => Bitmap::new(self.row_count),
        }
    }
}

/// Build a timestamp range bitmap from a timestamp column.
///
/// Sets bits for rows where `min_ts <= timestamp <= max_ts`.
pub fn timestamp_range_bitmap(timestamps: &[i64], min_ts: i64, max_ts: i64) -> Bitmap {
    let mut bm = Bitmap::new(timestamps.len());
    for (i, &ts) in timestamps.iter().enumerate() {
        if ts >= min_ts && ts <= max_ts {
            bm.set(i);
        }
    }
    bm
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitmap_basic() {
        let mut bm = Bitmap::new(100);
        assert!(!bm.get(0));
        bm.set(0);
        bm.set(50);
        bm.set(99);
        assert!(bm.get(0));
        assert!(bm.get(50));
        assert!(bm.get(99));
        assert!(!bm.get(1));
        assert_eq!(bm.count_ones(), 3);
    }

    #[test]
    fn bitmap_and() {
        let mut a = Bitmap::new(64);
        a.set(1);
        a.set(2);
        a.set(3);

        let mut b = Bitmap::new(64);
        b.set(2);
        b.set(3);
        b.set(4);

        let c = a.and(&b);
        assert!(!c.get(1));
        assert!(c.get(2));
        assert!(c.get(3));
        assert!(!c.get(4));
        assert_eq!(c.count_ones(), 2);
    }

    #[test]
    fn bitmap_or() {
        let mut a = Bitmap::new(64);
        a.set(1);
        let mut b = Bitmap::new(64);
        b.set(2);
        let c = a.or(&b);
        assert!(c.get(1));
        assert!(c.get(2));
        assert_eq!(c.count_ones(), 2);
    }

    #[test]
    fn bitmap_iter_ones() {
        let mut bm = Bitmap::new(200);
        bm.set(5);
        bm.set(100);
        bm.set(199);
        let ones: Vec<u32> = bm.iter_ones().collect();
        assert_eq!(ones, vec![5, 100, 199]);
    }

    #[test]
    fn symbol_bitmap_index_build() {
        // 10 rows, 3 distinct symbols.
        let symbols = [0, 1, 0, 2, 1, 0, 2, 1, 0, 2];
        let idx = SymbolBitmapIndex::build(&symbols);
        assert_eq!(idx.cardinality(), 3);
        assert_eq!(idx.row_count(), 10);

        // Symbol 0 appears at positions 0, 2, 5, 8.
        let bm0 = idx.get(0).unwrap();
        assert_eq!(bm0.count_ones(), 4);
        assert!(bm0.get(0));
        assert!(bm0.get(2));
        assert!(bm0.get(5));
        assert!(bm0.get(8));

        // Symbol 1 appears at positions 1, 4, 7.
        let bm1 = idx.get(1).unwrap();
        assert_eq!(bm1.count_ones(), 3);
    }

    #[test]
    fn symbol_bitmap_get_any() {
        let symbols = [0, 1, 2, 0, 1, 2];
        let idx = SymbolBitmapIndex::build(&symbols);

        // IN (0, 2)
        let bm = idx.get_any(&[0, 2]);
        assert_eq!(bm.count_ones(), 4); // positions 0, 2, 3, 5
        assert!(bm.get(0));
        assert!(!bm.get(1));
        assert!(bm.get(2));
        assert!(bm.get(3));
        assert!(!bm.get(4));
        assert!(bm.get(5));
    }

    #[test]
    fn compound_filter_test() {
        let symbols = [0, 0, 1, 1, 0];
        let timestamps = [100, 200, 300, 400, 500];
        let idx = SymbolBitmapIndex::build(&symbols);

        // Filter: symbol=0 AND timestamp in [100, 300]
        let ts_bm = timestamp_range_bitmap(&timestamps, 100, 300);
        let result = idx.compound_filter(0, &ts_bm);

        // symbol=0 at positions 0,1,4. timestamp in range at 0,1,2.
        // AND: positions 0,1.
        assert_eq!(result.count_ones(), 2);
        assert!(result.get(0));
        assert!(result.get(1));
        assert!(!result.get(4)); // ts=500 out of range
    }

    #[test]
    fn null_symbols_skipped() {
        let symbols = [0, u32::MAX, 1, u32::MAX];
        let idx = SymbolBitmapIndex::build(&symbols);
        assert_eq!(idx.cardinality(), 2); // only 0 and 1, not MAX
        assert!(idx.get(u32::MAX).is_none());
    }

    #[test]
    fn timestamp_range_bitmap_test() {
        let ts = [100, 200, 300, 400, 500];
        let bm = timestamp_range_bitmap(&ts, 200, 400);
        assert_eq!(bm.count_ones(), 3);
        assert!(!bm.get(0));
        assert!(bm.get(1));
        assert!(bm.get(2));
        assert!(bm.get(3));
        assert!(!bm.get(4));
    }
}
