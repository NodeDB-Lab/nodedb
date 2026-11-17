// SPDX-License-Identifier: Apache-2.0

//! Conservative retained-allocation estimates for native preparation copies.
//!
//! Container layouts and allocator bookkeeping are implementation-specific.
//! Hash tables include spare buckets and control bytes with twofold layout slack.
//! Trees charge eleven entries and twelve links per node, including a possible empty root.
//! The node bound uses five entries per non-root node plus one root.
//! Each nonempty buffer includes 32 bytes for allocation overhead.

use std::collections::HashMap;
use std::mem::size_of;

pub(crate) fn sum(values: impl IntoIterator<Item = usize>) -> usize {
    values.into_iter().fold(0, usize::saturating_add)
}

pub(crate) fn buffer(capacity: usize, element_bytes: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        capacity.saturating_mul(element_bytes).saturating_add(32)
    }
}

pub(crate) fn vector<T>(values: &Vec<T>) -> usize {
    buffer(values.capacity(), size_of::<T>())
}

pub(crate) fn strings<'a>(values: impl IntoIterator<Item = &'a String>) -> usize {
    sum(values.into_iter().map(|value| buffer(value.capacity(), 1)))
}

pub(crate) fn hash<K, V>(values: &HashMap<K, V>) -> usize {
    buffer(
        values.capacity().saturating_mul(2),
        size_of::<(K, V)>().saturating_add(size_of::<usize>()),
    )
}

pub(crate) fn tree<T>(len: usize) -> usize {
    let node_bytes = size_of::<T>()
        .saturating_mul(11)
        .saturating_add(12 * size_of::<usize>())
        .saturating_add(64);
    (len / 5).saturating_add(1).saturating_mul(node_bytes)
}
