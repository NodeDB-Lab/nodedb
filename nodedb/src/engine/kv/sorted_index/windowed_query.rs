//! Windowed query helpers for sorted indexes.
//!
//! These functions handle time-windowed queries where only entries within
//! a configurable time window are considered for rank/top_k/range/count.
//!
//! Performance: uses `for_each_in_order` with early termination instead of
//! fetching all entries. windowed_top_k(10) on a 10M-entry tree is O(N) in
//! the worst case (all entries outside window) but O(k) in the typical case
//! where windowed entries are near the front of sort order.

use std::collections::HashMap;

use super::key::SortKeyEncoder;
use super::manager::SortedIndexDef;
use super::tree::OrderStatTree;

/// Internal state for a sorted index (definition + tree).
pub(super) struct SortedIndexRef<'a> {
    pub def: &'a SortedIndexDef,
    pub tree: &'a OrderStatTree,
}

/// Windowed rank: count entries with lower sort key that are in the current window.
pub(super) fn windowed_rank(
    idx: &SortedIndexRef<'_>,
    primary_key: &[u8],
    now_ms: u64,
) -> Option<u32> {
    let window_start = idx.def.window.window_start(now_ms)?;
    let target_sort = idx.tree.get_sort_key(primary_key)?;

    let mut rank = 0u32;
    let mut found = false;

    idx.tree.for_each_in_order(|sort_key, pk| {
        if !entry_in_window(idx, pk, window_start) {
            return true; // Skip, continue.
        }
        rank += 1;
        if pk == primary_key && sort_key == target_sort {
            found = true;
            return false; // Stop.
        }
        true
    });

    if found { Some(rank) } else { None }
}

/// Windowed top-k: collect top K entries that are in the current window.
///
/// Early termination: stops as soon as k windowed entries are found.
pub(super) fn windowed_top_k(idx: &SortedIndexRef<'_>, k: u32, now_ms: u64) -> Vec<(u32, Vec<u8>)> {
    let Some(window_start) = idx.def.window.window_start(now_ms) else {
        return Vec::new();
    };

    let mut result = Vec::with_capacity(k as usize);
    let mut rank = 0u32;

    idx.tree.for_each_in_order(|_, pk| {
        if !entry_in_window(idx, pk, window_start) {
            return true; // Skip, continue.
        }
        rank += 1;
        result.push((rank, pk.to_vec()));
        rank < k // Stop when we have k entries.
    });

    result
}

/// Windowed range: filter range results by window.
pub(super) fn windowed_range(
    idx: &SortedIndexRef<'_>,
    entries: &[(&[u8], &[u8])],
    now_ms: u64,
) -> Vec<(u32, Vec<u8>)> {
    let Some(window_start) = idx.def.window.window_start(now_ms) else {
        return Vec::new();
    };

    // Build windowed ranks via streaming traversal.
    let mut windowed_ranks: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut rank = 0u32;

    idx.tree.for_each_in_order(|_, pk| {
        if entry_in_window(idx, pk, window_start) {
            rank += 1;
            windowed_ranks.insert(pk.to_vec(), rank);
        }
        true // Must scan all to compute correct ranks.
    });

    entries
        .iter()
        .filter_map(|(_, pk)| {
            let r = windowed_ranks.get(*pk)?;
            Some((*r, pk.to_vec()))
        })
        .collect()
}

/// Windowed count: count entries in the current window.
pub(super) fn windowed_count(idx: &SortedIndexRef<'_>, now_ms: u64) -> u32 {
    let Some(window_start) = idx.def.window.window_start(now_ms) else {
        return 0;
    };

    let mut count = 0u32;
    idx.tree.for_each_in_order(|_, pk| {
        if entry_in_window(idx, pk, window_start) {
            count += 1;
        }
        true
    });
    count
}

/// Check if an entry's timestamp is within the window.
///
/// Reads the timestamp from the sort key if the timestamp column is part of
/// the composite sort key. For entries where the timestamp is NOT in the sort
/// key, assumes the entry is in-window (documented limitation).
fn entry_in_window(idx: &SortedIndexRef<'_>, primary_key: &[u8], window_start: u64) -> bool {
    let Some(sort_key_bytes) = idx.tree.get_sort_key(primary_key) else {
        return false;
    };

    // Find the timestamp column's position in the sort key.
    let ts_col = &idx.def.window.timestamp_column;
    let columns = idx.def.encoder.columns();
    let mut offset = 0usize;

    for col in columns {
        // Read length prefix.
        if offset + 4 > sort_key_bytes.len() {
            return true; // Can't parse — assume in-window.
        }
        let len = u32::from_be_bytes(
            sort_key_bytes[offset..offset + 4]
                .try_into()
                .unwrap_or([0; 4]),
        ) as usize;
        offset += 4;

        if col.name == *ts_col {
            if offset + len > sort_key_bytes.len() || len != 8 {
                return true; // Can't parse — assume in-window.
            }
            let mut ts_bytes = [0u8; 8];
            ts_bytes.copy_from_slice(&sort_key_bytes[offset..offset + len]);

            // If DESC, undo the complement.
            let ts_bytes = if col.direction == super::key::SortDirection::Desc {
                let mut unflipped = [0u8; 8];
                for (i, &b) in ts_bytes.iter().enumerate() {
                    unflipped[i] = !b;
                }
                unflipped
            } else {
                ts_bytes
            };

            let ts_ms = SortKeyEncoder::decode_timestamp_ms(&ts_bytes);
            return ts_ms >= window_start;
        }

        offset += len;
    }

    // Timestamp column not in sort key — can't filter, assume in-window.
    true
}
