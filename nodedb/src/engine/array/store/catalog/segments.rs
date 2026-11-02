// SPDX-License-Identifier: BUSL-1.1

//! Segment allocation, registration, replacement, and unlinking.

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
        self.segments.insert(seg.id.clone(), h);
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
            self.segments.remove(id);
        }
        for h in new_handles {
            self.note_version_stamps(super::versions::segment_newest_stamps(&h));
            self.segments.insert(h.id().to_string(), h);
        }
        Ok(())
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

pub(super) fn parse_segment_seq(id: &str) -> Option<u64> {
    id.split_once('.').and_then(|(stem, _)| stem.parse().ok())
}
