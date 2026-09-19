// SPDX-License-Identifier: Apache-2.0

//! Truncate, compact, and snapshot operations for `VectorCollection`.

use nodedb_types::Surrogate;

use super::lifecycle::VectorCollection;
use crate::flat::FlatIndex;

/// One exported vector: global node id, full-precision data, optional surrogate.
pub type ExportedVector = (u32, Vec<f32>, Option<Surrogate>);

impl VectorCollection {
    /// Drop every vector, sealed segment, in-flight build, surrogate binding,
    /// and payload bitmap row. Returns the number of live vectors dropped.
    ///
    /// Configuration survives: dimension, HNSW params, index config,
    /// quantization, seal threshold, memory budget, and the registered
    /// payload index fields. `next_id` and `next_segment_id` keep counting
    /// so a build completion for a segment sealed before the truncate finds
    /// no matching entry in `building` and is ignored, and an mmap file name
    /// is never reused. The mmap file of each dropped sealed segment is
    /// removed from disk.
    pub fn truncate(&mut self) -> usize {
        let dropped = self.live_count();
        self.growing = FlatIndex::new(self.dim, self.params.metric);
        self.growing_base_id = self.next_id;
        for seg in self.sealed.drain(..) {
            let mmap_path = seg.mmap_vectors.as_ref().map(|m| m.path().to_path_buf());
            // Unmap before the file goes.
            drop(seg);
            if let Some(path) = mmap_path
                && let Err(e) = std::fs::remove_file(&path)
            {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "vector truncate: mmap segment file not removed"
                );
            }
        }
        self.building.clear();
        self.mmap_segment_count = 0;
        self.surrogate_map.clear();
        self.surrogate_to_local.clear();
        self.multi_doc_map.clear();
        self.codec_dispatch = None;
        self.payload.clear_rows();
        dropped
    }

    /// Compact sealed segments by removing tombstoned nodes.
    ///
    /// Rewrites `surrogate_map` and `multi_doc_map` for every sealed
    /// segment so that global ids continue to resolve to the correct
    /// surrogate after local-id renumbering.
    pub fn compact(&mut self) -> usize {
        let mut total_removed = 0;
        for seg in &mut self.sealed {
            let base_id = seg.base_id;
            let (removed, id_map) = seg.index.compact_with_map();
            total_removed += removed;
            if removed == 0 {
                continue;
            }

            let segment_end = base_id as u64 + id_map.len() as u64;
            let global_keys: Vec<u32> = self
                .surrogate_map
                .keys()
                .copied()
                .filter(|&k| (k as u64) >= base_id as u64 && (k as u64) < segment_end)
                .collect();
            // Two-phase: remove old entries first, then insert new ones
            // so we don't clobber a freshly-remapped entry with a later
            // tombstone removal.
            let mut new_entries: Vec<(u32, Surrogate)> = Vec::with_capacity(global_keys.len());
            for old_global in &global_keys {
                let surrogate = self.surrogate_map.remove(old_global);
                let old_local = (old_global - base_id) as usize;
                let new_local = id_map[old_local];
                if new_local != u32::MAX
                    && let Some(s) = surrogate
                {
                    new_entries.push((base_id + new_local, s));
                } else if let Some(s) = surrogate {
                    // Tombstoned — drop reverse mapping too.
                    self.surrogate_to_local.remove(&s);
                }
            }
            for (k, s) in new_entries {
                self.surrogate_map.insert(k, s);
                self.surrogate_to_local.insert(s, k);
            }

            // Rewrite multi_doc_map entries for this segment.
            for ids in self.multi_doc_map.values_mut() {
                ids.retain_mut(|vid| {
                    let v = *vid;
                    if (v as u64) >= base_id as u64 && (v as u64) < segment_end {
                        let old_local = (v - base_id) as usize;
                        let new_local = id_map[old_local];
                        if new_local == u32::MAX {
                            false
                        } else {
                            *vid = base_id + new_local;
                            true
                        }
                    } else {
                        true
                    }
                });
            }
        }
        total_removed
    }

    /// Export all live vectors for snapshot.
    ///
    /// # Errors
    ///
    /// Propagates [`crate::HnswIndex::export_vectors`] if a sealed segment's
    /// vectors cannot be materialized. A snapshot that silently dropped or
    /// zeroed those vectors would restore an index with missing data.
    pub fn export_snapshot(&self) -> Result<Vec<ExportedVector>, crate::error::VectorError> {
        let mut result = Vec::new();

        for i in 0..self.growing.len() as u32 {
            let vid = self.growing_base_id + i;
            if let Some(data) = self.growing.get_vector(i) {
                let surrogate = self.surrogate_map.get(&vid).copied();
                result.push((vid, data.to_vec(), surrogate));
            }
        }

        for seg in &self.sealed {
            let vectors = seg.index.export_vectors()?;
            for (i, vec_data) in vectors.into_iter().enumerate() {
                let vid = seg.base_id + i as u32;
                let surrogate = self.surrogate_map.get(&vid).copied();
                result.push((vid, vec_data, surrogate));
            }
        }

        for seg in &self.building {
            for i in 0..seg.flat.len() as u32 {
                let vid = seg.base_id + i;
                if let Some(data) = seg.flat.get_vector(i) {
                    let surrogate = self.surrogate_map.get(&vid).copied();
                    result.push((vid, data.to_vec(), surrogate));
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw::HnswParams;
    use nodedb_types::{PayloadIndexKind, Value};
    use std::collections::HashMap;

    #[test]
    fn truncate_drops_every_row_and_keeps_config() {
        let mut coll = VectorCollection::with_seal_threshold(2, HnswParams::default(), 2);
        coll.payload.add_index("owner", PayloadIndexKind::Equality);
        let mut fields = HashMap::new();
        fields.insert("owner".to_string(), Value::String("a".into()));
        for (i, v) in [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]].into_iter().enumerate() {
            let s = Surrogate::new(i as u32 + 1);
            let id = coll.insert_with_surrogate(v.to_vec(), s);
            coll.payload.insert_row(id, &fields);
        }
        assert!(
            coll.seal("k").is_some(),
            "threshold reached, growing sealed"
        );
        assert_eq!(coll.live_count(), 3);
        let next_before = coll.next_id;

        assert_eq!(coll.truncate(), 3);
        assert_eq!(coll.live_count(), 0);
        assert!(coll.surrogate_to_local.is_empty());
        assert!(coll.surrogate_map.is_empty());
        assert!(coll.building.is_empty());
        assert!(coll.sealed.is_empty());
        assert_eq!(coll.dim(), 2);
        assert_eq!(coll.next_id, next_before, "ids stay monotonic");
        assert_eq!(coll.growing_base_id, next_before);
        assert!(
            coll.payload.field_names().any(|f| f == "owner"),
            "registered payload index survives"
        );
        let hits = coll
            .payload
            .pre_filter(&super::super::payload_index::FilterPredicate::Eq {
                field: "owner".to_string(),
                value: Value::String("a".into()),
            })
            .expect("owner is indexed");
        assert!(hits.is_empty(), "payload rows cleared");

        let s = Surrogate::new(42);
        let id = coll.insert_with_surrogate(vec![0.5, 0.5], s);
        assert_eq!(coll.local_for_surrogate(s), Some(id));
        assert_eq!(coll.live_count(), 1);
    }
}
