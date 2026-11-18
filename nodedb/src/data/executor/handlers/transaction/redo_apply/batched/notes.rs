// SPDX-License-Identifier: BUSL-1.1

//! The write notes an install holds until the record settles, collapsed so
//! they grow with the rows the record writes, not with its ops.
//!
//! A note publishes in one of two ways. A keyed row note raises the cached
//! newest ordinal of the row's slice and vShard to what the store holds.
//! A raise never creates a cache entry. A keyless row note, and a wipe,
//! forget entries. Every note also applies its stamp. All of them read the
//! store after the install, so each note has the same effect wherever it
//! falls in the order, and a repeat has none. So:
//!
//! - one note per `(row, stamp)` publishes what all its repeats publish;
//! - wipes can publish first, then the rows;
//! - a row note of a collection that a later wipe of the same stamp
//!   forgets is skipped. A wipe forgets the collection's slices, its home
//!   vShard and the stamp's vShard: everything a surrogate, KV or keyless
//!   note of the collection touches. An edge note raises the vShards of
//!   its endpoints, which the wipe can leave, so it always publishes.

use std::collections::BTreeMap;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_note::{KeyRepr, WriteStamp};
use crate::data::executor::handlers::spill::tiered::{SpillSpace, SpillWeight, TieredMap};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

use super::super::state::{DeferredNote, DeferredNoteKind};

/// Where a note applies: its collection and its stamp.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(super) struct NoteScope {
    db: u64,
    tenant: u64,
    collection: String,
    vshard: u32,
    lsn: u64,
}

impl NoteScope {
    fn of(note: &DeferredNote) -> Self {
        Self {
            db: note.db.as_u64(),
            tenant: note.tenant.as_u64(),
            collection: note.collection.clone(),
            vshard: note.stamp.vshard.as_u32(),
            lsn: note.stamp.lsn.as_u64(),
        }
    }

    fn stamp(&self) -> WriteStamp {
        WriteStamp {
            vshard: VShardId::new(self.vshard),
            lsn: Lsn::new(self.lsn),
        }
    }
}

/// One row note: where it applies, and the row's key. `None` names the
/// collection only.
#[derive(Debug, Clone, PartialEq, Eq, Hash, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(super) struct NoteRow {
    scope: NoteScope,
    key: Option<KeyRepr>,
}

impl SpillWeight for NoteRow {
    fn spill_weight(&self) -> usize {
        let key = match &self.key {
            None | Some(KeyRepr::Surrogate(_)) => 8,
            Some(KeyRepr::KvKey(key)) => 16 + key.len(),
            Some(KeyRepr::Edge { src, label, dst }) => 48 + src.len() + label.len() + dst.len(),
        };
        28 + self.scope.collection.spill_weight() + key
    }
}

/// The held notes of one install. See the module doc.
#[derive(Default)]
pub(in crate::data::executor::handlers::transaction::redo_apply) struct CollapsedNotes {
    /// Each row note, valued with the position of its latest repeat.
    rows: TieredMap<NoteRow, u64>,
    /// Each wipe, valued with the position of its latest repeat. It grows
    /// with the collections the record wipes.
    wiped: BTreeMap<NoteScope, u64>,
    /// The position the next note takes.
    seq: u64,
}

impl CollapsedNotes {
    /// Hold `note`.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn hold(
        &mut self,
        note: DeferredNote,
    ) {
        let seq = self.seq;
        self.seq += 1;
        let scope = NoteScope::of(&note);
        match note.kind {
            DeferredNoteKind::Wipe => {
                self.wiped.insert(scope, seq);
            }
            DeferredNoteKind::Row(key) => {
                self.rows.insert(NoteRow { scope, key }, seq, seq);
            }
        }
    }

    pub(super) fn hot_bytes(&self) -> usize {
        self.rows.hot_bytes()
    }

    pub(super) fn spill(&mut self, space: &mut SpillSpace) -> crate::Result<()> {
        self.rows.spill(space, "notes")
    }

    /// Whether a wipe after the row note `row`, at position `seq`, makes
    /// it publish nothing.
    fn wiped_after(&self, row: &NoteRow, seq: u64) -> bool {
        if matches!(row.key, Some(KeyRepr::Edge { .. })) {
            return false;
        }
        self.wiped.get(&row.scope).is_some_and(|wipe| *wipe > seq)
    }
}

impl CoreLoop {
    /// Publish the notes an install held, once the record settled: every
    /// wipe, then every row note no later wipe forgets.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn publish_collapsed_notes(
        &mut self,
        notes: CollapsedNotes,
    ) -> crate::Result<()> {
        for scope in notes.wiped.keys() {
            self.publish_collection_wipe(
                DatabaseId::new(scope.db),
                TenantId::new(scope.tenant),
                &scope.collection,
                scope.stamp(),
            )?;
        }
        for row in notes.rows.iter() {
            let (row, seq) = row?;
            if notes.wiped_after(&row, *seq) {
                continue;
            }
            self.publish_write(
                DatabaseId::new(row.scope.db),
                TenantId::new(row.scope.tenant),
                &row.scope.collection,
                row.key.clone(),
                row.scope.stamp(),
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_note_row_round_trips() {
        let scope = NoteScope {
            db: 0,
            tenant: 1,
            collection: "orders".into(),
            vshard: 3,
            lsn: 90,
        };
        for key in [
            None,
            Some(KeyRepr::Surrogate(7)),
            Some(KeyRepr::KvKey(Box::from(&b"k"[..]))),
            Some(KeyRepr::Edge {
                src: "a".into(),
                label: "l".into(),
                dst: "b".into(),
            }),
        ] {
            let row = NoteRow {
                scope: scope.clone(),
                key,
            };
            assert!(row.spill_weight() > 0);
            let bytes = zerompk::to_msgpack_vec(&row).expect("encode");
            let back: NoteRow = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(back, row);
        }
    }
}
