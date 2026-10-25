// SPDX-License-Identifier: BUSL-1.1

//! CREATE INDEX backfill of a bitemporal collection's versioned index.
//!
//! The versioned index answers a lookup at a system-time cutoff, so it holds
//! an entry for each value at the system time the value became current. The
//! backfill walks every version of every row in ascending system time and
//! writes what the write path writes at that version's time:
//!
//! - A live version asserts each of its values live.
//! - It tombstones each value the previous version held and it drops.
//! - A tombstone version tombstones every value the previous version held.
//!
//! An `AS OF` lookup before the backfill then finds each row by the values
//! it held at the cutoff.

use std::collections::BTreeSet;

use redb::WriteTransaction;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::read::decode::decode_scanned_document;
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::engine::document::predicate::IndexPredicate;
use crate::engine::document::store::{StorageKey, extract_index_values};
use crate::engine::sparse::btree_versioned::VersionedIndexEntry;

/// The index a versioned backfill builds.
pub(super) struct VersionedBackfill<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub path: &'a str,
    pub is_array: bool,
    pub case_insensitive: bool,
    /// The partial-index predicate. A version it rejects holds no value.
    pub predicate: Option<&'a IndexPredicate>,
    /// How the collection's stored bodies are encoded.
    pub body_format: SparseBodyFormatRef<'a>,
}

impl CoreLoop {
    /// Write the versioned index entries of every version of every row,
    /// each at its version's system time, within `txn`. Does NOT commit.
    ///
    /// A version that does not decode fails the backfill: an index built
    /// without it misses the row at every cutoff the version covers.
    pub(super) fn backfill_versioned_index(
        &self,
        txn: &WriteTransaction,
        b: &VersionedBackfill<'_>,
    ) -> crate::Result<()> {
        let crs = self.crs_context();
        let mut row: Option<StorageKey> = None;
        let mut held: BTreeSet<String> = BTreeSet::new();
        self.sparse.versioned_for_each_version(
            b.database_id,
            b.tid,
            b.collection,
            |doc_id, sys_from, body| {
                if row != Some(*doc_id) {
                    row = Some(*doc_id);
                    held.clear();
                }
                let values = match body {
                    Some(body) => version_values(body, b, &crs)?,
                    None => BTreeSet::new(),
                };
                for value in held.difference(&values) {
                    self.sparse.versioned_index_tombstone_in_txn(
                        txn,
                        index_entry(b, doc_id, value, sys_from),
                    )?;
                }
                for value in &values {
                    self.sparse
                        .versioned_index_put_in_txn(txn, index_entry(b, doc_id, value, sys_from))?;
                }
                held = values;
                Ok(())
            },
        )
    }
}

/// The versioned index entry of `value` for row `doc_id` at `sys_from`.
fn index_entry<'a>(
    b: &VersionedBackfill<'a>,
    doc_id: &'a StorageKey,
    value: &'a str,
    sys_from: i64,
) -> VersionedIndexEntry<'a> {
    VersionedIndexEntry {
        database_id: b.database_id,
        tenant: b.tid,
        coll: b.collection,
        field: b.path,
        value,
        doc_id,
        sys_from,
    }
}

/// The values one live version contributes to the index: none when the
/// partial-index predicate rejects it, lowercased for a case-insensitive
/// index.
fn version_values(
    body: &[u8],
    b: &VersionedBackfill<'_>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<BTreeSet<String>> {
    let doc = decode_scanned_document(body, b.body_format)?;
    if let Some(predicate) = b.predicate
        && !predicate.evaluate_json(&doc, crs)
    {
        return Ok(BTreeSet::new());
    }
    Ok(extract_index_values(&doc, b.path, b.is_array)
        .into_iter()
        .map(|v| {
            if b.case_insensitive {
                v.to_lowercase()
            } else {
                v
            }
        })
        .collect())
}
