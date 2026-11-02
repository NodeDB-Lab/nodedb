// SPDX-License-Identifier: BUSL-1.1

//! Resolve one restored vector row into the nodes its install writes.
//!
//! The resolve reads the destination's newest ordinal for the row at the
//! transaction's turn: the newest start of a live node or end of an ended
//! node bound to the row's surrogate. It keeps the nodes `restore_key_plan`
//! chooses, and names the ordinal the destination's live nodes of the row
//! end at: the first kept node's start, or the stamp a set-again state takes.

use nodedb_types::{StorageKey, Surrogate};

use crate::data::executor::core_loop::CoreLoop;
use crate::wal::{RedoRowKind, RedoSubRecord, VectorRestoreRedo};

use super::restore_changes::RestoreChanges;
use super::restore_key_plan::{BackedUpSpan, KeyRestore, plan_key};
use super::restore_reassert::RestoreScope;

/// The spans of `redo`'s nodes, ascending: every ended node, then the live
/// nodes.
fn spans(redo: &VectorRestoreRedo) -> Vec<BackedUpSpan> {
    redo.ended
        .iter()
        .map(|node| BackedUpSpan {
            sys_from: node.sys_from,
            sys_to: Some(node.sys_to),
        })
        .chain(redo.live.iter().map(|live| BackedUpSpan {
            sys_from: live.sys_from,
            sys_to: None,
        }))
        .collect()
}

/// Rewrite `redo` to the nodes `plan` installs. `None` when nothing
/// installs.
pub(super) fn apply_plan(
    mut redo: VectorRestoreRedo,
    plan: KeyRestore,
) -> Option<VectorRestoreRedo> {
    match plan {
        KeyRestore::Keep => None,
        KeyRestore::Versions { from } => {
            let ended = redo.ended.len();
            redo.ended.drain(..from.min(ended));
            if from > ended {
                redo.live = None;
            }
            let first = redo
                .ended
                .first()
                .map(|node| node.sys_from)
                .or(redo.live.as_ref().map(|live| live.sys_from))?;
            redo.end_live_at = Some(first);
            Some(redo)
        }
        KeyRestore::SetAgain { at } => {
            redo.ended.clear();
            if let Some(live) = redo.live.as_mut() {
                live.sys_from = at;
            }
            redo.end_live_at = Some(at);
            Some(redo)
        }
    }
}

impl CoreLoop {
    /// The sub-record that installs the restored vector row `redo`, or
    /// `None` when the destination keeps its own state.
    pub(super) fn resolve_restored_vector_row(
        &self,
        scope: RestoreScope,
        record_type: u32,
        redo: VectorRestoreRedo,
        changes: &mut RestoreChanges,
    ) -> crate::Result<Option<RedoSubRecord>> {
        let index_key = CoreLoop::vector_index_key(
            scope.database_id.as_u64(),
            scope.tid,
            &redo.collection,
            &redo.field_name,
        );
        let (dest_newest, dest_live) = self
            .vector_collections
            .get(&index_key)
            .map_or((None, false), |coll| {
                coll.row_newest_ordinal(Surrogate::new(redo.surrogate))
            });
        let plan = plan_key(
            &spans(&redo),
            dest_newest,
            dest_live,
            scope.applied,
            scope.cut,
        );
        let Some(redo) = apply_plan(redo, plan) else {
            return Ok(None);
        };
        // A vector-primary row publishes its change by its surrogate's
        // identity, as its live writes do.
        if redo.vector_primary {
            changes.note_row(
                &redo.collection,
                StorageKey::for_surrogate(Surrogate::new(redo.surrogate))
                    .to_identity()
                    .as_str()
                    .to_owned(),
                RedoRowKind::net(dest_live, redo.live.is_some()),
            );
        }
        Ok(Some(RedoSubRecord {
            record_type,
            payload: redo.encode()?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{VectorRestoreLive, VectorRestoreNode};

    fn redo() -> VectorRestoreRedo {
        VectorRestoreRedo {
            collection: "docs".into(),
            field_name: String::new(),
            surrogate: 3,
            dim: 2,
            multi: false,
            ended: vec![VectorRestoreNode {
                vector: vec![1.0, 0.0],
                sys_from: 10,
                sys_to: 20,
            }],
            live: Some(VectorRestoreLive {
                vectors: vec![vec![0.0, 1.0]],
                sys_from: 20,
            }),
            end_live_at: None,
            vector_primary: false,
        }
    }

    /// A client write after the cut survives: nothing installs.
    #[test]
    fn a_write_after_the_cut_survives() {
        let plan = plan_key(&spans(&redo()), Some(3_001), true, 5_000, 3_000);
        assert_eq!(apply_plan(redo(), plan), None);
    }

    /// Nodes above the destination's newest ordinal install, and the
    /// destination's live node ends at the first one's start.
    #[test]
    fn nodes_above_the_destination_install_at_their_spans() {
        let plan = plan_key(&spans(&redo()), Some(15), true, 5_000, 3_000);
        let kept = apply_plan(redo(), plan).expect("kept");
        assert!(kept.ended.is_empty());
        assert_eq!(kept.live.as_ref().map(|l| l.sys_from), Some(20));
        assert_eq!(kept.end_live_at, Some(20));
    }

    /// A destination at or below the cut and above every backed-up node gets
    /// the backup's live vector again at the stamp.
    #[test]
    fn a_newer_destination_below_the_cut_is_set_again() {
        let plan = plan_key(&spans(&redo()), Some(2_500), true, 5_000, 3_000);
        let again = apply_plan(redo(), plan).expect("again");
        assert!(again.ended.is_empty());
        assert_eq!(again.live.as_ref().map(|l| l.sys_from), Some(5_000));
        assert_eq!(again.end_live_at, Some(5_000));
    }

    /// A row the backup holds deleted ends the destination's live node.
    #[test]
    fn a_deleted_row_ends_the_destinations_node() {
        let mut deleted = redo();
        deleted.live = None;
        let plan = plan_key(&spans(&deleted), Some(2_500), true, 5_000, 3_000);
        let again = apply_plan(deleted, plan).expect("again");
        assert!(again.live.is_none());
        assert_eq!(again.end_live_at, Some(5_000));
    }

    /// Against a real index: a client write after the cut keeps the row,
    /// and a destination node at or below it gets the backup's vector again.
    #[test]
    fn a_vector_row_against_the_destination_index() {
        use nodedb_vector::collection::VectorCollection;
        use nodedb_vector::hnsw::HnswParams;

        let s = Surrogate::new(3);
        let backup = spans(&redo());

        let mut after = VectorCollection::new(2, HnswParams::default());
        after.insert_at(vec![0.7, 0.7], s, 3_500).expect("write");
        let (newest, live) = after.row_newest_ordinal(s);
        assert_eq!(
            plan_key(&backup, newest, live, 5_000, 3_000),
            KeyRestore::Keep
        );

        let mut before = VectorCollection::new(2, HnswParams::default());
        before.insert_at(vec![0.7, 0.7], s, 2_500).expect("write");
        let (newest, live) = before.row_newest_ordinal(s);
        let plan = plan_key(&backup, newest, live, 5_000, 3_000);
        let again = apply_plan(redo(), plan).expect("set again");
        let at = again.end_live_at.expect("ends the destination's node");
        before.end_surrogate_at(s, at);
        let live_again = again.live.expect("the backup's vector");
        for vector in live_again.vectors {
            before
                .insert_at(vector, s, live_again.sys_from)
                .expect("install");
        }
        assert_eq!(
            before.vector_for_surrogate_at(s, 6_000),
            Some(vec![0.0, 1.0])
        );
        assert_eq!(
            before.vector_for_surrogate_at(s, 4_000),
            Some(vec![0.7, 0.7])
        );
    }
}
