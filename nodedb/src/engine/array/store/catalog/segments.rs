// SPDX-License-Identifier: BUSL-1.1

//! Segment allocation, registration, replacement, and unlinking.

use std::collections::HashMap;

use super::super::manifest::{SegmentRef, segment_path};
use super::super::segment_handle::SegmentHandle;
use super::error::ArrayStoreError;
use super::store::ArrayStore;

impl ArrayStore {
    /// Allocate the next segment file name and bump the sequence.
    pub fn allocate_segment_id(&mut self) -> String {
        let seq = self.next_segment_seq;
        self.next_segment_seq += 1;
        format!("{seq:010}.ndas")
    }

    /// Register a freshly-flushed (or freshly-merged) segment. The file
    /// must already exist on disk. Updates the manifest in-memory only;
    /// callers must call [`ArrayStore::persist_manifest`] afterwards.
    pub fn install_segment(&mut self, seg: SegmentRef) -> Result<(), ArrayStoreError> {
        let h = SegmentHandle::open(
            &segment_path(&self.root, &seg.id),
            seg.id.clone(),
            self.schema_hash,
            self.kek.as_ref(),
        )?;
        self.note_version_stamps(super::versions::segment_newest_stamps(&h));
        self.add_segment_handle(seg.id.clone(), h);
        self.manifest.append(seg);
        Ok(())
    }

    /// Remove segments from the manifest and drop their handles. The
    /// underlying file is deleted only after the manifest is persisted
    /// (caller's responsibility — see [`ArrayStore::unlink_segment`]).
    pub fn replace_segments(
        &mut self,
        removed: &[String],
        added: Vec<SegmentRef>,
    ) -> Result<(), ArrayStoreError> {
        let mut new_handles = Vec::with_capacity(added.len());
        for seg in &added {
            let h = SegmentHandle::open(
                &segment_path(&self.root, &seg.id),
                seg.id.clone(),
                self.schema_hash,
                self.kek.as_ref(),
            )?;
            new_handles.push(h);
        }
        self.manifest.replace(removed, added);
        for id in removed {
            if let Some(gone) = self.segments.remove(id) {
                remove_placement_bytes(&mut self.placement_bytes, &gone);
            }
        }
        for h in new_handles {
            self.note_version_stamps(super::versions::segment_newest_stamps(&h));
            self.add_segment_handle(h.id().to_string(), h);
        }
        Ok(())
    }

    /// Hold `handle` open under `id` and count its bytes. A handle it
    /// replaces under the same id stops counting.
    fn add_segment_handle(&mut self, id: String, handle: SegmentHandle) {
        add_placement_bytes(&mut self.placement_bytes, &handle);
        if let Some(prior) = self.segments.insert(id, handle) {
            remove_placement_bytes(&mut self.placement_bytes, &prior);
        }
    }

    pub fn persist_manifest(&self) -> Result<(), ArrayStoreError> {
        self.manifest.persist(&self.root)?;
        Ok(())
    }

    pub fn unlink_segment(&self, id: &str) -> Result<(), ArrayStoreError> {
        let path = segment_path(&self.root, id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ArrayStoreError::Io {
                detail: format!("unlink {path:?}: {e}"),
            }),
        }
    }
}

/// Count `handle`'s tile bytes under each of its placements in `into`.
pub(super) fn add_placement_bytes(into: &mut HashMap<u32, u64>, handle: &SegmentHandle) {
    for (placement, bytes) in handle.placement_bytes() {
        let held = into.entry(*placement).or_insert(0);
        *held = held.saturating_add(*bytes);
    }
}

/// Stop counting `handle`'s tile bytes in `from`. A placement that reaches
/// zero loses its entry.
fn remove_placement_bytes(from: &mut HashMap<u32, u64>, handle: &SegmentHandle) {
    for (placement, bytes) in handle.placement_bytes() {
        if let Some(held) = from.get_mut(placement) {
            *held = held.saturating_sub(*bytes);
            if *held == 0 {
                from.remove(placement);
            }
        }
    }
}

pub(super) fn parse_segment_seq(id: &str) -> Option<u64> {
    id.split_once('.').and_then(|(stem, _)| stem.parse().ok())
}
