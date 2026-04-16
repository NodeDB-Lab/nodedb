//! Fast Hamming distance using u64 POPCNT.

pub fn fast_hamming(a: &[u8], b: &[u8]) -> u32 {
    assert_eq!(a.len(), b.len(), "fast_hamming: length mismatch");
    let mut dist = 0u32;
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let off = i * 8;
        let xa = u64::from_le_bytes([
            a[off],
            a[off + 1],
            a[off + 2],
            a[off + 3],
            a[off + 4],
            a[off + 5],
            a[off + 6],
            a[off + 7],
        ]);
        let xb = u64::from_le_bytes([
            b[off],
            b[off + 1],
            b[off + 2],
            b[off + 3],
            b[off + 4],
            b[off + 5],
            b[off + 6],
            b[off + 7],
        ]);
        dist += (xa ^ xb).count_ones();
    }
    for i in (chunks * 8)..a.len() {
        dist += (a[i] ^ b[i]).count_ones();
    }
    dist
}
