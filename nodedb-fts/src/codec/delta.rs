//! Delta encoding/decoding for sorted `u32` sequences.
//!
//! Posting lists are sorted by doc ID. Delta encoding stores the first ID
//! absolute and subsequent IDs as `current - previous`. Typical deltas are
//! much smaller than absolute IDs, enabling tighter bitpacking.

/// Delta-encode a sorted slice of u32 values in place.
///
/// After encoding: `out[0] = values[0]`, `out[i] = values[i] - values[i-1]`.
/// The input MUST be sorted ascending. Unsorted input produces garbage.
pub fn encode(values: &[u32]) -> Vec<u32> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut deltas = Vec::with_capacity(values.len());
    deltas.push(values[0]);
    for i in 1..values.len() {
        deltas.push(values[i] - values[i - 1]);
    }
    deltas
}

/// Decode delta-encoded values back to absolute sorted values.
///
/// Prefix-sum: `out[0] = deltas[0]`, `out[i] = out[i-1] + deltas[i]`.
pub fn decode(deltas: &[u32]) -> Vec<u32> {
    if deltas.is_empty() {
        return Vec::new();
    }
    let mut values = Vec::with_capacity(deltas.len());
    values.push(deltas[0]);
    for i in 1..deltas.len() {
        values.push(values[i - 1] + deltas[i]);
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_basic() {
        let values = vec![3, 7, 10, 15, 100];
        let deltas = encode(&values);
        assert_eq!(deltas, vec![3, 4, 3, 5, 85]);
        assert_eq!(decode(&deltas), values);
    }

    #[test]
    fn empty() {
        assert!(encode(&[]).is_empty());
        assert!(decode(&[]).is_empty());
    }

    #[test]
    fn single_element() {
        let values = vec![42];
        let deltas = encode(&values);
        assert_eq!(deltas, vec![42]);
        assert_eq!(decode(&deltas), values);
    }

    #[test]
    fn consecutive_ids() {
        let values: Vec<u32> = (0..128).collect();
        let deltas = encode(&values);
        // First is 0, rest are all 1.
        assert_eq!(deltas[0], 0);
        assert!(deltas[1..].iter().all(|&d| d == 1));
        assert_eq!(decode(&deltas), values);
    }

    #[test]
    fn large_gaps() {
        let values = vec![0, 1_000_000, 2_000_000];
        let deltas = encode(&values);
        assert_eq!(deltas, vec![0, 1_000_000, 1_000_000]);
        assert_eq!(decode(&deltas), values);
    }
}
