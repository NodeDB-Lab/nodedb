//! Statistics computation helpers: numeric min/max, string zone-maps, bloom filters.

use crate::format::BlockStats;
use crate::predicate::{BLOOM_BYTES, bloom_insert};

/// Maximum byte length for string zone-map bounds (truncation threshold).
pub(super) const STRING_BOUND_MAX_BYTES: usize = 32;

/// Minimum distinct values in a block to justify bloom filter overhead (256 bytes).
/// Below this threshold, zone-map min/max is sufficient. Dict-encoded columns
/// with ≤16 distinct values get no bloom.
const BLOOM_DISTINCT_THRESHOLD: usize = 16;

/// Compute `BlockStats` for a string column block.
///
/// Iterates over all non-null values in `[start, end)`, building lexicographic
/// min/max (each truncated to `STRING_BOUND_MAX_BYTES` bytes) and a 256-byte
/// bloom filter for equality-predicate fast-reject.
pub(super) fn compute_string_block_stats(
    data: &[u8],
    offsets: &[u32],
    valid_slice: &[bool],
    start: usize,
    end: usize,
    null_count: u32,
    block_row_count: usize,
) -> BlockStats {
    let mut str_min: Option<String> = None;
    let mut str_max: Option<String> = None;
    let mut distinct = std::collections::HashSet::new();
    let mut has_non_null = false;

    // First pass: compute min/max and count distinct values.
    for (&is_valid, row_idx) in valid_slice.iter().zip(start..end) {
        if !is_valid {
            continue;
        }
        let b_start = offsets[row_idx] as usize;
        let b_end = offsets[row_idx + 1] as usize;
        let raw = &data[b_start..b_end];
        let s = std::str::from_utf8(raw).unwrap_or("");

        has_non_null = true;
        // Track distinct values up to threshold+1 (stop counting early).
        if distinct.len() <= BLOOM_DISTINCT_THRESHOLD {
            distinct.insert(raw);
        }

        let truncated = truncate_to_char_boundary(s, STRING_BOUND_MAX_BYTES);

        match &str_min {
            None => str_min = Some(truncated.to_owned()),
            Some(cur) if truncated < cur.as_str() => str_min = Some(truncated.to_owned()),
            _ => {}
        }
        match &str_max {
            None => str_max = Some(truncated.to_owned()),
            Some(cur) if truncated > cur.as_str() => str_max = Some(truncated.to_owned()),
            _ => {}
        }
    }

    // Only build bloom filter for high-cardinality blocks where zone maps
    // alone cannot efficiently prune. Low-cardinality columns (≤16 distinct)
    // are better served by dict encoding + integer comparison.
    let bloom_opt = if has_non_null && distinct.len() > BLOOM_DISTINCT_THRESHOLD {
        let mut bloom = vec![0u8; BLOOM_BYTES];
        for (&is_valid, row_idx) in valid_slice.iter().zip(start..end) {
            if !is_valid {
                continue;
            }
            let b_start = offsets[row_idx] as usize;
            let b_end = offsets[row_idx + 1] as usize;
            let s = std::str::from_utf8(&data[b_start..b_end]).unwrap_or("");
            bloom_insert(&mut bloom, s);
        }
        Some(bloom)
    } else {
        None
    };
    BlockStats::string_block(
        null_count,
        block_row_count as u32,
        str_min,
        str_max,
        bloom_opt,
    )
}

/// Truncate a string to at most `max_bytes` bytes, preserving valid UTF-8 by
/// cutting only at character boundaries.
pub(super) fn truncate_to_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut boundary = max_bytes;
    while !s.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &s[..boundary]
}

/// Compute min/max for i64 values, skipping nulls.
pub(super) fn numeric_min_max_i64(values: &[i64], valid: &[bool]) -> (i64, i64) {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for (v, &is_valid) in values.iter().zip(valid.iter()) {
        if is_valid {
            min = min.min(*v);
            max = max.max(*v);
        }
    }
    if min == i64::MAX {
        (0, 0) // All nulls.
    } else {
        (min, max)
    }
}

/// Compute min/max for f64 values, skipping nulls.
pub(super) fn numeric_min_max_f64(values: &[f64], valid: &[bool]) -> (f64, f64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for (v, &is_valid) in values.iter().zip(valid.iter()) {
        if is_valid {
            if *v < min {
                min = *v;
            }
            if *v > max {
                max = *v;
            }
        }
    }
    if min == f64::INFINITY {
        (0.0, 0.0)
    } else {
        (min, max)
    }
}
