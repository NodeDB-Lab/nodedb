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
//!   collapses nested CRDT containers, such as a row's block list, into plain
//!   values, destroying them.
//! - It rebuilds one row at a time and records the counter range each row's
//!   operations occupy, so the caller can export one self-contained delta per
//!   row. A single delta spanning every row is not independently applicable by
//!   a receiver that commits per row.

use loro::LoroValue;

use crate::error::{CrdtError, Result};

use super::core::CrdtState;
use super::restore_containers;

/// One row re-authored by [`CrdtState::rekey`], and the span of the new peer's
/// operation sequence its rebuild occupies.
///
/// Feed the span to [`CrdtState::export_local_range`] to get a delta carrying
/// exactly this row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RekeyedRow {
    /// Row this range rebuilds.
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
    /// container other than `collection`, or a rebuild whose result does not
    /// match the original value.
    pub fn rekey(&self, collection: &str, new_peer_id: u64) -> Result<Rekeyed> {
        self.reject_unreproducible_roots(collection)?;

        // `LoroDoc::get_map` materializes an absent root. The root is checked
        // before access so source traversal cannot change the document's shape.
        let has_collection = self.rekey_root_present(collection);

        let rekeyed = Self::new(new_peer_id)?;
        let mut rows = Vec::new();
        if has_collection {
            let src_collection = self.doc.get_map(collection);
            let dst_collection = rekeyed.doc.get_map(collection);
            for row_id in self.row_ids(collection) {
                let from_counter = rekeyed.local_op_counter();
                if let Some(value) = src_collection.get(&row_id) {
                    restore_containers::rebuild_map_field(&dst_collection, &row_id, value)?;
                }
                let to_counter = rekeyed.local_op_counter();
                if to_counter > from_counter {
                    rows.push(RekeyedRow {
                        row_id,
                        from_counter,
                        to_counter,
                    });
                }
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

    /// Whether the collection root already exists on this document.
    ///
    /// Read from the value tree rather than via `get_map`, which would create
    /// the root as a side effect of asking.
    fn rekey_root_present(&self, collection: &str) -> bool {
        match self.doc.get_deep_value() {
            LoroValue::Map(roots) => roots.contains_key(collection),
            _ => false,
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
            if name.as_str() != collection {
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
