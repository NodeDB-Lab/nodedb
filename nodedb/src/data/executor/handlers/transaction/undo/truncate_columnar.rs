// SPDX-License-Identifier: BUSL-1.1

//! Undo of a columnar or spatial `TRUNCATE` inside a transaction batch: put
//! back the whole pre-image `execute_columnar_truncate` moved out.
//!
//! Every structure is in-memory (`columnar_engines`, the flushed segment
//! bytes, the per-field R-trees and their reverse map), so an aborted redb
//! transaction reverses none of it and the reinstall here is what restores
//! visibility.

use crate::data::executor::core_loop::CoreLoop;

use super::ColumnarTruncateUndo;

impl CoreLoop {
    pub(super) fn apply_undo_columnar_truncate(
        &mut self,
        entry_index: usize,
        undo: ColumnarTruncateUndo,
    ) -> Result<(), (usize, String)> {
        let ColumnarTruncateUndo {
            collection_key,
            rows,
            flushed_segments,
            flushed_surrogates,
            spatial_indexes,
            spatial_doc_map,
        } = undo;
        let Some(engine) = self.columnar_engines.get_mut(&collection_key) else {
            return Err((
                entry_index,
                format!(
                    "columnar truncate undo: engine for {:?} vanished before rollback",
                    collection_key
                ),
            ));
        };
        engine.restore_truncated(rows);
        match flushed_segments {
            Some(segments) => {
                self.columnar_flushed_segments
                    .insert(collection_key.clone(), segments);
            }
            None => {
                self.columnar_flushed_segments.remove(&collection_key);
            }
        }
        match flushed_surrogates {
            Some(surrogates) => {
                self.columnar_flushed_surrogates
                    .insert(collection_key.clone(), surrogates);
            }
            None => {
                self.columnar_flushed_surrogates.remove(&collection_key);
            }
        }
        // Anything indexed for the collection after the truncate belongs to
        // rows the reverse-order rollback has already removed.
        let (db, tid, coll) = &collection_key;
        self.spatial_indexes
            .retain(|(d, t, c, _), _| !(d == db && t == tid && c == coll));
        self.spatial_doc_map
            .retain(|(d, t, c, _, _), _| !(d == db && t == tid && c == coll));
        self.spatial_indexes.extend(spatial_indexes);
        self.spatial_doc_map.extend(spatial_doc_map);
        Ok(())
    }
}
