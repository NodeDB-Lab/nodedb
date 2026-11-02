// SPDX-License-Identifier: BUSL-1.1

//! Entry visibility over the versioned secondary-index table.
//!
//! An index key is `"{prefix}{value}:{doc_id}\x00{sys_from:020}"`, where
//! `prefix` is `"{db}:{tenant}:{coll}:{field}:"` and `doc_id` renders as 8
//! hex characters. The value is read from the right, so a value that holds
//! `:` parses whole. An `(value, doc_id)` entry is visible at ordinal `S`
//! when its newest version at or below `S` is live. A write tombstones every
//! value it drops and adds every value it gains at its own ordinal, so the
//! entries visible at `S` are exactly the values of the row versions visible
//! at `S`.

use std::ops::ControlFlow;

use nodedb_types::StorageKey;
use redb::ReadableTable;

use super::key::format_sys_from;
use super::value::TAG_LIVE;
use crate::engine::sparse::btree::{KeyedTable, invalid_storage_key_err, redb_err};

/// Width of a rendered storage key.
const DOC_ID_LEN: usize = 8;

/// The key range one index walk covers.
pub(in crate::engine::sparse) struct EntryRange<'a> {
    pub coll: &'a str,
    /// The prefix every key in the range starts with. The value is the part
    /// of the key between this prefix and `:{doc_id}`.
    pub prefix: &'a str,
    /// The first key the walk visits.
    pub start: &'a str,
    /// The exclusive end of the walk.
    pub end: &'a str,
}

/// Split the part of an index key after its prefix into `(value, doc_id)`.
fn split_value_doc<'k>(coll: &str, rest: &'k str, key: &str) -> crate::Result<(&'k str, &'k str)> {
    let invalid = || invalid_storage_key_err(KeyedTable::IndexesVersioned, coll, key);
    let split = rest
        .len()
        .checked_sub(DOC_ID_LEN + 1)
        .filter(|at| rest.is_char_boundary(*at) && rest.as_bytes()[*at] == b':')
        .ok_or_else(invalid)?;
    Ok((&rest[..split], &rest[split + 1..]))
}

/// Visit every `(value, doc_id)` entry of `range` visible at `cutoff`
/// (`None` = the current state), in key order: by value, then by doc id.
/// The visitor ends the walk with `ControlFlow::Break`.
pub(in crate::engine::sparse) fn for_each_visible_entry<T, F>(
    table: &T,
    range: EntryRange<'_>,
    cutoff: Option<i64>,
    mut visit: F,
) -> crate::Result<()>
where
    T: ReadableTable<&'static str, &'static [u8]>,
    F: FnMut(&str, StorageKey) -> crate::Result<ControlFlow<()>>,
{
    let cutoff_key = cutoff.map(format_sys_from);
    let entries = table
        .range(range.start..range.end)
        .map_err(|e| redb_err("index range", e))?;
    // The `"{value}:{doc_id}"` part of the entry being read, and the tag of
    // its newest version at or below the cut.
    let mut group = String::new();
    let mut tag: Option<u8> = None;
    for entry in entries {
        let (key, value) = entry.map_err(|e| redb_err("index entry", e))?;
        let key = key.value();
        let invalid = || invalid_storage_key_err(KeyedTable::IndexesVersioned, range.coll, key);
        let rest = key.strip_prefix(range.prefix).ok_or_else(invalid)?;
        let (value_doc, suffix) = rest.rsplit_once('\x00').ok_or_else(invalid)?;
        if value_doc != group {
            if emit_entry(range.coll, &group, tag, &mut visit)?.is_break() {
                return Ok(());
            }
            group.clear();
            group.push_str(value_doc);
            tag = None;
        }
        if cutoff_key.as_deref().is_some_and(|c| suffix > c) {
            continue;
        }
        tag = Some(value.value().first().copied().ok_or_else(invalid)?);
    }
    // The last group ends the walk whether or not `visit` stops it.
    let _ = emit_entry(range.coll, &group, tag, &mut visit)?;
    Ok(())
}

/// Hand one `(value, doc_id)` group to `visit` when its visible version is
/// live.
fn emit_entry<F>(
    coll: &str,
    group: &str,
    tag: Option<u8>,
    visit: &mut F,
) -> crate::Result<ControlFlow<()>>
where
    F: FnMut(&str, StorageKey) -> crate::Result<ControlFlow<()>>,
{
    if tag != Some(TAG_LIVE) {
        return Ok(ControlFlow::Continue(()));
    }
    let (value, doc) = split_value_doc(coll, group, group)?;
    let doc_id = StorageKey::parse(doc)
        .ok_or_else(|| invalid_storage_key_err(KeyedTable::IndexesVersioned, coll, doc))?;
    visit(value, doc_id)
}

/// An exclusive upper key bound that admits every value strictly below
/// `upper` under `field_prefix`. It can admit values at or above `upper`
/// too, so a caller compares each parsed value against `upper`.
///
/// A key is `{field_prefix}{value}:{doc_id}…`, so a value that is a proper
/// prefix of `upper` sorts by its `:` against the next character of
/// `upper`. The bound is the largest of `{field_prefix}{upper}` and
/// `{field_prefix}{upper[..i]};` for every proper prefix `upper[..i]`.
pub(in crate::engine::sparse) fn upper_value_bound(field_prefix: &str, upper: &str) -> String {
    let mut best = format!("{field_prefix}{upper}");
    for (i, _) in upper.char_indices() {
        let candidate = format!("{field_prefix}{};", &upper[..i]);
        if candidate > best {
            best = candidate;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upper_bound_admits_prefix_values_below_upper() {
        // "a" < "a!" but "p:a:…" sorts above "p:a!". The bound admits it. A
        // caller filters the parsed value against `upper` exactly.
        let bound = upper_value_bound("p:", "a!");
        assert!("p:a:00000001\u{0}00000000000000000001" < bound.as_str());
        assert!("p:b:00000001" >= bound.as_str());
    }

    #[test]
    fn upper_bound_is_plain_when_no_prefix_sorts_higher() {
        assert_eq!(upper_value_bound("p:", "b"), "p:b");
        assert!("p:a:00000001" < upper_value_bound("p:", "b").as_str());
    }

    #[test]
    fn value_splits_from_the_right() {
        let (value, doc) = split_value_doc("c", "a:b:0000000a", "k").unwrap();
        assert_eq!(value, "a:b");
        assert_eq!(doc, "0000000a");
        assert!(split_value_doc("c", "short", "k").is_err());
    }
}
