// SPDX-License-Identifier: BUSL-1.1

//! Checkpoint encoding of a [`SparseInvertedIndex`]: every live document
//! with its start, and every ended version with its span.

use serde::{Deserialize, Serialize};

use super::index::SparseInvertedIndex;

impl SparseInvertedIndex {
    /// Serialize to bytes for checkpoint persistence.
    ///
    /// Fallible on purpose. The checkpoint that writes these bytes reports an
    /// LSN authorising the WAL segments below it to be unlinked, so an encode
    /// failure that yields empty bytes here publishes an EMPTY index under
    /// a manifest claiming every document was durable — destroying the only
    /// other copy. The error propagates instead, and the caller clamps.
    pub fn checkpoint_to_bytes(&self) -> crate::Result<Vec<u8>> {
        let mut documents: Vec<SparseDocSnapshot> = self
            .doc_id_reverse
            .iter()
            .map(|(&id, name)| SparseDocSnapshot {
                internal_id: id,
                doc_id: name.clone(),
                entries: self.live_entries(id),
                sys_from: self.history.live_from(id),
            })
            .collect();
        documents.sort_unstable_by_key(|d| d.internal_id);
        let ended = self
            .history
            .ended_versions()
            .into_iter()
            .map(|(id, version, entries)| SparseEndedSnapshot {
                internal_id: id,
                doc_id: version.doc_id.clone(),
                entries,
                sys_from: version.sys_from,
                sys_to: version.sys_to,
            })
            .collect();
        let snapshot = SparseIndexSnapshot {
            next_id: self.next_id,
            documents,
            ended,
        };
        zerompk::to_msgpack_vec(&snapshot).map_err(|e| crate::Error::Serialization {
            format: "msgpack".to_string(),
            detail: format!(
                "sparse vector index checkpoint encode failed for {} documents: {e}",
                self.doc_count
            ),
        })
    }

    /// Restore from checkpoint bytes. `None` when the bytes do not decode.
    pub fn from_checkpoint(bytes: &[u8]) -> Option<Self> {
        let snapshot: SparseIndexSnapshot = zerompk::from_msgpack(bytes).ok()?;
        let mut index = Self::new();
        for doc in snapshot.documents {
            index.install_live(&doc.doc_id, doc.internal_id, &doc.entries, doc.sys_from);
        }
        for version in snapshot.ended {
            index.history.end(
                version.internal_id,
                version.doc_id,
                &version.entries,
                version.sys_from,
                version.sys_to,
            );
        }
        index.next_id = snapshot.next_id;
        Some(index)
    }
}

/// Checkpoint serialization format.
#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct SparseIndexSnapshot {
    next_id: u32,
    documents: Vec<SparseDocSnapshot>,
    ended: Vec<SparseEndedSnapshot>,
}

/// One live document and the ordinal its version started at.
#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct SparseDocSnapshot {
    internal_id: u32,
    doc_id: String,
    entries: Vec<(u32, f32)>,
    sys_from: i64,
}

/// One ended version and its span.
#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct SparseEndedSnapshot {
    internal_id: u32,
    doc_id: String,
    entries: Vec<(u32, f32)>,
    sys_from: i64,
    sys_to: i64,
}

#[cfg(test)]
mod tests {
    use nodedb_types::SparseVector;

    use super::*;

    fn make_sv(entries: &[(u32, f32)]) -> SparseVector {
        SparseVector::from_entries(entries.to_vec()).unwrap()
    }

    #[test]
    fn history_survives_a_checkpoint() {
        let mut idx = SparseInvertedIndex::new();
        let old = idx.insert_at("doc1", &make_sv(&[(10, 0.5)]), 10);
        let new = idx.insert_at("doc1", &make_sv(&[(10, 0.9)]), 20);
        let gone = idx.insert_at("doc2", &make_sv(&[(30, 1.0)]), 12);
        idx.delete_at("doc2", 25);

        let bytes = idx.checkpoint_to_bytes().expect("encode");
        let back = SparseInvertedIndex::from_checkpoint(&bytes).expect("decode");

        assert_eq!(back.doc_count(), 1);
        assert_eq!(back.next_internal_id(), idx.next_internal_id());
        assert_eq!(back.doc_image("doc1"), idx.doc_image("doc1"));
        let h = back.history();
        assert!(h.ended_visible_at(old, 15));
        assert!(!h.live_visible_at(new, 19));
        assert!(h.ended_visible_at(gone, 24));
        assert!(!h.ended_visible_at(gone, 25));
        assert_eq!(h.ended_postings(30), &[(gone, 1.0)]);
    }
}
