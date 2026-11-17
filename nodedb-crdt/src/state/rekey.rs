// SPDX-License-Identifier: Apache-2.0

//! Re-authoring a document's contents under a new Loro peer id.
//!
//! A Loro operation's identity is `(peer_id, counter)`, and the peer id of an
//! operation is fixed the moment it is committed — `LoroDoc::set_peer_id`
//! only changes who authors *subsequent* operations. So a replica that must
//! stop using a peer id (because another replica owns it, or because this
//! replica's own history under it was forked) cannot keep its existing
//! document: every operation in it stays attributed to the peer id being
//! abandoned, and re-exporting them merely re-sends what the owner will
//! refuse or what the merge will discard as a replay.
//!
//! [`CrdtState::rekey`] therefore builds a *new* document under the new peer
//! id and rebuilds the current contents into it, so the result is authored end
//! to end by the new identity.
//!
//! Two properties make the rebuild safe to hand straight to sync:
//!
//! - It walks the live `ValueOrContainer` tree through [`restore_containers`]
//!   rather than re-writing a flattened projection. The flattened form
//!   collapses nested CRDT containers (a row's block list, the archive's
//!   version slots) into plain values, destroying them.
//! - It rebuilds one row at a time and records the counter range each row's
//!   operations occupy, so the caller can export one self-contained delta per
//!   row. A single delta spanning every row is not independently applicable by
//!   a receiver that commits per row.
//!
//! A row's archived versions are rebuilt inside that row's range, so the
//! per-row delta carries the row's history with it.

use loro::LoroValue;

use crate::error::{CrdtError, Result};

use super::bitemporal_archive::{HISTORY_ROOT, parse_archive_key};
use super::core::CrdtState;
use super::restore_containers;

/// One row re-authored by [`CrdtState::rekey`], and the span of the new peer's
/// operation sequence its rebuild occupies.
///
/// Feed the span to [`CrdtState::export_local_range`] to get a delta carrying
/// exactly this row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RekeyedRow {
    /// Row this range rebuilds, including any archived versions of it.
    pub row_id: String,
    /// First counter of the row's operations (inclusive).
    pub from_counter: i32,
    /// Counter past the row's last operation (exclusive).
    pub to_counter: i32,
}

/// The result of re-authoring a document under a new peer id.
pub struct Rekeyed {
    /// The rebuilt document. Shares no operation history with the original —
    /// that is the point, and it means the new version vector is unrelated to
    /// the old one, so callers must re-push rather than resume from a stored
    /// version.
    pub state: CrdtState,
    /// Per-row spans, in rebuild order.
    pub rows: Vec<RekeyedRow>,
}

impl CrdtState {
    /// Re-author this state's `collection` under `new_peer_id`.
    ///
    /// `self` is left untouched, so a failed rekey cannot leave the caller
    /// holding a half-converted document — it is mid-recovery and has no other
    /// copy of the data.
    ///
    /// Fails rather than dropping anything it cannot reproduce: a root
    /// container other than `collection` or the bitemporal archive, or a
    /// rebuild whose result does not match the original value.
    pub fn rekey(&self, collection: &str, new_peer_id: u64) -> Result<Rekeyed> {
        self.reject_unreproducible_roots(collection)?;

        // `LoroDoc::get_map` materializes absent roots. Check both roots before
        // accessing them so source traversal cannot change the document's shape.
        let (has_collection, has_archive) = self.rekey_root_presence(collection);

        let rekeyed = Self::new(new_peer_id)?;
        if has_collection {
            rekeyed.doc.get_map(collection);
        }
        if has_archive {
            // Match the source's shape even when no row carries history: an
            // archive root that exists on one side and not the other is a
            // difference the completeness check below would (correctly) refuse.
            rekeyed.doc.get_map(HISTORY_ROOT);
        }
        let mut rows = Vec::new();

        for row_id in self.rekey_row_ids(collection, has_collection, has_archive) {
            let from_counter = rekeyed.local_op_counter();
            self.rebuild_row_into(&rekeyed, collection, &row_id, has_collection, has_archive)?;
            let to_counter = rekeyed.local_op_counter();
            if to_counter > from_counter {
                rows.push(RekeyedRow {
                    row_id,
                    from_counter,
                    to_counter,
                });
            }
        }

        rekeyed.doc.commit();

        // The rebuild is the caller's only copy of its data once it adopts the
        // result. A mismatch here means some shape was silently not
        // reproduced, and shipping that would lose exactly the writes the
        // rotation exists to save.
        if rekeyed.doc.get_deep_value() != self.doc.get_deep_value() {
            return Err(CrdtError::Loro(format!(
                "rekey of '{collection}' did not reproduce the document's value"
            )));
        }

        Ok(Rekeyed {
            state: rekeyed,
            rows,
        })
    }

    /// Every row to rebuild: the live rows, plus rows that survive only as
    /// archived versions (a deleted row keeps its history).
    fn rekey_row_ids(
        &self,
        collection: &str,
        has_collection: bool,
        has_archive: bool,
    ) -> Vec<String> {
        let mut ids = if has_collection {
            self.row_ids(collection)
        } else {
            Vec::new()
        };
        if !has_archive {
            return ids;
        }
        let live: std::collections::HashSet<&str> = ids.iter().map(String::as_str).collect();

        let mut archived: Vec<String> = self
            .doc
            .get_map(HISTORY_ROOT)
            .keys()
            .filter_map(|key| {
                let (archived_collection, row_id, _) = parse_archive_key(&key)?;
                (archived_collection == collection && !live.contains(row_id))
                    .then(|| row_id.to_string())
            })
            .collect();
        archived.sort();
        archived.dedup();

        ids.extend(archived);
        ids
    }

    /// Rebuild one row — its live container and its archived versions — into
    /// `dst`, so the whole row occupies one contiguous counter range.
    fn rebuild_row_into(
        &self,
        dst: &CrdtState,
        collection: &str,
        row_id: &str,
        has_collection: bool,
        has_archive: bool,
    ) -> Result<()> {
        if has_collection {
            let src_collection = self.doc.get_map(collection);
            if let Some(value) = src_collection.get(row_id) {
                let dst_collection = dst.doc.get_map(collection);
                restore_containers::rebuild_map_field(&dst_collection, row_id, value)?;
            }
        }

        if !has_archive {
            return Ok(());
        }
        let src_archive = self.doc.get_map(HISTORY_ROOT);
        let archive_keys: Vec<String> = src_archive
            .keys()
            .filter(|key| {
                parse_archive_key(key).is_some_and(|(c, r, _)| c == collection && r == row_id)
            })
            .map(|key| key.to_string())
            .collect();
        if archive_keys.is_empty() {
            return Ok(());
        }

        let dst_archive = dst.doc.get_map(HISTORY_ROOT);
        for key in archive_keys {
            if let Some(value) = src_archive.get(&key) {
                restore_containers::rebuild_map_field(&dst_archive, &key, value)?;
            }
        }
        Ok(())
    }

    /// Whether the collection and archive roots already exist on this document.
    ///
    /// Read from the value tree rather than via `get_map`, which would create
    /// the root as a side effect of asking.
    fn rekey_root_presence(&self, collection: &str) -> (bool, bool) {
        match self.doc.get_deep_value() {
            LoroValue::Map(roots) => (
                roots.contains_key(collection),
                roots.contains_key(HISTORY_ROOT),
            ),
            _ => (false, false),
        }
    }

    /// Refuse a document carrying a root the per-row rebuild does not visit.
    ///
    /// Skipping one would drop its contents on a path the caller cannot audit,
    /// which is worse than refusing: the caller is recovering from a refused
    /// identity and has nothing else to fall back on.
    fn reject_unreproducible_roots(&self, collection: &str) -> Result<()> {
        let LoroValue::Map(roots) = self.doc.get_deep_value() else {
            return Ok(());
        };
        for (name, value) in roots.iter() {
            if name.as_str() != collection && name.as_str() != HISTORY_ROOT {
                return Err(CrdtError::Loro(format!(
                    "rekey of '{collection}': unexpected root container '{name}' would not be \
                     re-authored"
                )));
            }
            if !matches!(value, LoroValue::Map(_)) {
                return Err(CrdtError::Loro(format!(
                    "rekey of '{collection}': root '{name}' is not a map and cannot be re-authored"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "rekey_tests.rs"]
mod tests;
