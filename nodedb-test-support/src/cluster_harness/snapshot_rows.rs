// SPDX-License-Identifier: BUSL-1.1

//! Current document rows of a tenant snapshot.
//!
//! A snapshot carries every document version, keyed
//! `"{db}:{tid}:{collection}:{doc_id}\x00{system_from:020}"`. A row's current
//! image is its newest version. A row whose newest version is a tombstone
//! has no current image.

use std::collections::BTreeMap;

use nodedb::engine::sparse::btree_versioned::{TAG_LIVE, decode_value};
use nodedb::types::TenantDataSnapshot;

/// Every row of `snapshot` whose newest version is live, as
/// `(row_key, body)`. `row_key` is the version key without its
/// `\x00{system_from}` suffix. Panics on a version that does not decode.
pub fn current_document_rows(snapshot: &TenantDataSnapshot) -> Vec<(String, Vec<u8>)> {
    let mut newest: BTreeMap<&str, (i64, &[u8])> = BTreeMap::new();
    for (key, value) in &snapshot.documents_versioned {
        let (row_key, sys) = key
            .split_once('\x00')
            .unwrap_or_else(|| panic!("version key {key:?} carries no system time"));
        let sys: i64 = sys
            .parse()
            .unwrap_or_else(|e| panic!("version key {key:?} system time: {e}"));
        let slot = newest.entry(row_key).or_insert((sys, value.as_slice()));
        if sys >= slot.0 {
            *slot = (sys, value.as_slice());
        }
    }
    newest
        .into_iter()
        .filter_map(|(row_key, (_, raw))| {
            let version =
                decode_value(raw).unwrap_or_else(|e| panic!("version of {row_key:?} decodes: {e}"));
            (version.tag == TAG_LIVE).then(|| (row_key.to_string(), version.body.to_vec()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb::engine::sparse::btree_versioned::{TAG_TOMBSTONE, encode_value};

    fn version(row: &str, sys: i64, tag: u8, body: &[u8]) -> (String, Vec<u8>) {
        (
            format!("{row}\x00{sys:020}"),
            encode_value(tag, i64::MIN, i64::MAX, body),
        )
    }

    #[test]
    fn the_newest_live_version_is_the_current_row() {
        let snapshot = TenantDataSnapshot {
            documents_versioned: vec![
                version("0:1:c:a", 200, TAG_LIVE, b"new"),
                version("0:1:c:a", 100, TAG_LIVE, b"old"),
                version("0:1:c:b", 100, TAG_LIVE, b"gone"),
                version("0:1:c:b", 300, TAG_TOMBSTONE, b""),
            ],
            ..Default::default()
        };
        assert_eq!(
            current_document_rows(&snapshot),
            vec![("0:1:c:a".to_string(), b"new".to_vec())]
        );
    }
}
