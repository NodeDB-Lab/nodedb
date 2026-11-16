// SPDX-License-Identifier: BUSL-1.1

//! Payload bitmaps and the clone-copy gate of a restored vector row.
//!
//! A vector-primary row's live node carries payload bitmap entries for the
//! indexed fields of its current sidecar, as every live write leaves them
//! (`write_vector_direct_row`). A restored row's install changes the live
//! node, the current sidecar, or both. The install takes the entries of the
//! row's live node and current sidecar before it, and adds the entries of
//! the row's live node and current sidecar after it. Both read the stored
//! state, so restart replay of the record rebuilds the same entries.
//!
//! The entries are keyed by the live node's id and hold the lower-cased
//! sidecar fields, as a live write keys them. A rolled-back install removes
//! the entries it added and puts back the ones it took
//! ([`UndoEntry::VectorPayloadRows`]).

use nodedb_types::{StorageKey, Surrogate};

use super::core_loop::CoreLoop;
use super::handlers::transaction::undo::UndoEntry;
use super::handlers::transaction::undo::vector_payload::{PayloadRow, VectorPayloadRowsUndo};
use super::handlers::vector_direct_row::VectorIndexKey;
use crate::bridge::envelope::ErrorCode;
use crate::wal::VectorRestoreRedo;

impl CoreLoop {
    /// The payload bitmap row of `redo`'s row: its live node in `index_key`
    /// and the fields of its current sidecar. `None` for a row of no
    /// vector-primary primary index, an index with no payload bitmap, a row
    /// with no live node, or a row with no current sidecar.
    pub(super) fn restored_payload_row(
        &self,
        database_id: u64,
        tenant_id: u64,
        redo: &VectorRestoreRedo,
        index_key: &VectorIndexKey,
    ) -> Result<Option<PayloadRow>, ErrorCode> {
        if redo.primary_index.is_none() {
            return Ok(None);
        }
        let Some(coll) = self.vector_collections.get(index_key) else {
            return Ok(None);
        };
        if coll.payload.is_empty() {
            return Ok(None);
        }
        let surrogate = Surrogate::new(redo.surrogate);
        let Some(node_id) = coll
            .local_for_surrogate(surrogate)
            .filter(|id| coll.is_live(*id))
        else {
            return Ok(None);
        };
        let Some(sidecar) =
            self.vector_sidecar_row(database_id, tenant_id, &redo.collection, surrogate)?
        else {
            return Ok(None);
        };
        Ok(Some(PayloadRow {
            node_id,
            fields: sidecar.fields,
        }))
    }

    /// Replace the payload bitmap row `prior` of `redo`'s row with the row
    /// the install left, and record the undo that reverses it.
    pub(super) fn refresh_restored_payload(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        redo: &VectorRestoreRedo,
        index_key: &VectorIndexKey,
        prior: Option<PayloadRow>,
    ) -> Result<(), ErrorCode> {
        let current = self.restored_payload_row(database_id, tenant_id, redo, index_key)?;
        if prior.is_none() && current.is_none() {
            return Ok(());
        }
        let Some(coll) = self.vector_collections.get_mut(index_key) else {
            return Err(ErrorCode::Internal {
                detail: format!(
                    "vector index for '{}' vanished during a restore",
                    redo.collection
                ),
            });
        };
        if let Some(row) = &prior {
            coll.payload.delete_row(row.node_id, &row.fields);
        }
        if let Some(row) = &current {
            coll.payload.insert_row(row.node_id, &row.fields);
        }
        self.record_redo_undo([UndoEntry::VectorPayloadRows(Box::new(
            VectorPayloadRowsUndo {
                index_key: index_key.clone(),
                removed: prior,
                inserted: current,
            },
        ))]);
        Ok(())
    }

    /// Whether the clone already holds a version of `redo`'s row: a node of
    /// the row in `index_key`, or a stored version of the row at a system
    /// time other than `copy_at`, the version the copy installs.
    pub(super) fn restored_row_holds_a_version(
        &self,
        database_id: u64,
        tenant_id: u64,
        index_key: &VectorIndexKey,
        redo: &VectorRestoreRedo,
        copy_at: i64,
    ) -> crate::Result<bool> {
        let surrogate = Surrogate::new(redo.surrogate);
        let key = StorageKey::for_surrogate(surrogate);
        let stored = self.sparse.versioned_latest_sys_from(
            database_id,
            tenant_id,
            &redo.collection,
            &key,
        )?;
        if stored.is_some_and(|sys_from| sys_from != copy_at) {
            return Ok(true);
        }
        Ok(self
            .vector_collections
            .get(index_key)
            .is_some_and(|coll| coll.row_newest_ordinal(surrogate).0.is_some()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::{PayloadIndexKind, Value};
    use nodedb_vector::collection::FilterPredicate;
    use nodedb_wal::TombstoneSet;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::wal::{VectorRestoreIndex, VectorRestoreLive, VectorSidecarVersion};

    const DB: u64 = 0;
    const TID: u64 = 1;
    const COLLECTION: &str = "vp_restore";

    fn tagged(tag: &str) -> Vec<u8> {
        let mut fields = HashMap::new();
        fields.insert("tag".to_string(), Value::String(tag.into()));
        zerompk::to_msgpack_vec(&fields).expect("encode sidecar")
    }

    fn row(surrogate: u32, vector: Vec<f32>, sys_from: i64, tag: &str) -> VectorRestoreRedo {
        VectorRestoreRedo {
            collection: COLLECTION.into(),
            field_name: "emb".into(),
            surrogate,
            dim: 3,
            multi: false,
            ended: Vec::new(),
            live: Some(VectorRestoreLive {
                vectors: vec![vector],
                sys_from,
            }),
            end_live_at: None,
            vector_primary: true,
            sidecar: vec![VectorSidecarVersion {
                sys_from,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: Some(tagged(tag)),
            }],
            primary_index: Some(VectorRestoreIndex {
                quantization: nodedb_types::VectorQuantization::default(),
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: vec![("tag".into(), PayloadIndexKind::Equality)],
            }),
            clone_copy_at: None,
        }
    }

    fn replay(core: &mut CoreLoop, redo: &VectorRestoreRedo, lsn: u64) -> Option<bool> {
        let payload = redo.encode().expect("encode redo");
        let tombstones = TombstoneSet::new();
        core.try_replay_vector_restore(&payload, TID, DB, lsn, &tombstones.for_database(DB))
    }

    /// The surrogates whose live node the payload bitmap of `tag` admits.
    fn tagged_rows(core: &CoreLoop, tag: &str) -> Vec<u32> {
        let key = CoreLoop::vector_index_key(DB, TID, COLLECTION, "emb");
        let coll = core.vector_collections.get(&key).expect("the index");
        let bitmap = coll
            .payload
            .pre_filter(&FilterPredicate::Eq {
                field: "tag".into(),
                value: Value::String(tag.into()),
            })
            .expect("the tag field carries a payload bitmap");
        let mut rows: Vec<u32> = bitmap
            .iter()
            .filter_map(|id| coll.get_surrogate(id))
            .map(|s| s.as_u32())
            .collect();
        rows.sort_unstable();
        rows
    }

    /// Restart replay of restored vector-primary rows indexes each row's
    /// sidecar fields under its live node. A second replay of a record
    /// leaves one entry per row, under the row's newest node.
    #[test]
    fn a_replayed_restored_row_fills_its_payload_bitmap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let first = row(1, vec![1.0, 0.0, 0.0], 100, "first");
        let second = row(2, vec![0.0, 1.0, 0.0], 110, "second");
        assert_eq!(replay(&mut core, &first, 10), Some(true));
        assert_eq!(replay(&mut core, &second, 11), Some(true));
        assert_eq!(replay(&mut core, &first, 12), Some(true));

        assert_eq!(tagged_rows(&core, "first"), vec![1]);
        assert_eq!(tagged_rows(&core, "second"), vec![2]);
    }

    /// A clone copy installs where the clone holds no version of the row,
    /// and a copy of a row the clone already holds writes nothing.
    #[test]
    fn a_clone_copy_installs_only_where_the_row_holds_no_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let mut copy = row(1, vec![1.0, 0.0, 0.0], 100, "first");
        copy.clone_copy_at = Some(100);
        assert_eq!(replay(&mut core, &copy, 10), Some(true));
        assert_eq!(tagged_rows(&core, "first"), vec![1]);

        let mut again = row(1, vec![0.0, 0.0, 1.0], 100, "other");
        again.clone_copy_at = Some(100);
        assert_eq!(replay(&mut core, &again, 11), Some(false));
        assert_eq!(tagged_rows(&core, "first"), vec![1]);
        assert!(tagged_rows(&core, "other").is_empty());
    }
}
