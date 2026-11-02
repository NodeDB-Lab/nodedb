// SPDX-License-Identifier: BUSL-1.1

//! `ArrayStore` struct, construction, and plain accessors.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use nodedb_array::schema::ArraySchema;

use nodedb_wal::crypto::WalEncryptionKey;

use super::super::manifest::{Manifest, segment_path};
use super::super::segment_handle::SegmentHandle;
use super::error::ArrayStoreError;
use crate::engine::array::memtable::Memtable;

/// One open array. Owns the directory layout below `root`:
///
/// ```text
/// <root>/manifest.ndam
/// <root>/<segment-id-1>.ndas
/// <root>/<segment-id-2>.ndas
/// ...
/// ```
pub struct ArrayStore {
    pub(super) root: PathBuf,
    pub(super) schema: Arc<ArraySchema>,
    pub(super) schema_hash: u64,
    pub(super) manifest: Manifest,
    pub(crate) memtable: Memtable,
    pub(crate) segments: HashMap<String, SegmentHandle>,
    pub(super) next_segment_seq: u64,
    /// At-rest encryption key for SEGA segment envelopes. When `Some`,
    /// all segment opens use AES-256-GCM decryption.
    pub(super) kek: Option<WalEncryptionKey>,
    /// The newest tile-version stamp any segment or the memtable holds under
    /// each placement, at or above the true newest.
    pub(super) newest_stamps: HashMap<u32, i64>,
}

impl ArrayStore {
    /// Open or create the array store. Loads the manifest if present;
    /// opens every referenced segment and validates schema_hash.
    ///
    /// `kek` is a constructor input rather than a later `set_kek` call because
    /// the segments named by the manifest are opened right here: an at-rest
    /// encrypted (`SEGA`) segment opened without the key is a typed error, so
    /// installing the key afterwards would make every array that had ever
    /// flushed unopenable — and the WAL backing those cells is already gone,
    /// truncated by the checkpoint that the flush advanced.
    pub fn open(
        root: PathBuf,
        schema: Arc<ArraySchema>,
        schema_hash: u64,
        kek: Option<WalEncryptionKey>,
    ) -> Result<Self, ArrayStoreError> {
        std::fs::create_dir_all(&root).map_err(|e| ArrayStoreError::Io {
            detail: format!("mkdir {root:?}: {e}"),
        })?;
        let manifest = Manifest::load_or_new(&root, schema_hash)?;
        if manifest.schema_hash != schema_hash && !manifest.segments.is_empty() {
            return Err(ArrayStoreError::SchemaHashMismatch {
                store: manifest.schema_hash,
                new: schema_hash,
            });
        }
        let mut segments = HashMap::with_capacity(manifest.segments.len());
        let mut max_seq: u64 = 0;
        for seg in &manifest.segments {
            let h = SegmentHandle::open(
                &segment_path(&root, &seg.id),
                seg.id.clone(),
                schema_hash,
                kek.as_ref(),
            )?;
            if let Some(seq) = super::segments::parse_segment_seq(&seg.id) {
                max_seq = max_seq.max(seq);
            }
            segments.insert(seg.id.clone(), h);
        }
        let mut newest_stamps = HashMap::new();
        for handle in segments.values() {
            super::versions::merge_newest_stamps(
                &mut newest_stamps,
                super::versions::segment_newest_stamps(handle),
            );
        }
        Ok(Self {
            root,
            schema,
            schema_hash,
            manifest,
            memtable: Memtable::new(),
            segments,
            next_segment_seq: max_seq + 1,
            kek,
            newest_stamps,
        })
    }

    /// Install the at-rest encryption key on an already-open store.
    ///
    /// This covers key installation that happens *after* a store is open, so
    /// it applies to segments opened from here on — flushes, installs, and
    /// replacements. Handles opened before this call keep their existing
    /// backing, which is correct: those files were written without the key and
    /// re-opening them with one would (rightly) be rejected as plaintext.
    /// Segments named by the manifest at open time take the key through
    /// [`ArrayStore::open`] instead.
    pub fn set_kek(&mut self, kek: WalEncryptionKey) {
        self.kek = Some(kek);
    }

    pub fn kek(&self) -> Option<&WalEncryptionKey> {
        self.kek.as_ref()
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn schema(&self) -> &Arc<ArraySchema> {
        &self.schema
    }

    /// Replace the schema of this open store. The caller passes a schema
    /// with this store's `schema_hash`, so only its georef can differ.
    pub fn adopt_schema(&mut self, schema: Arc<ArraySchema>) {
        self.schema = schema;
    }

    pub fn schema_hash(&self) -> u64 {
        self.schema_hash
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_mut(&mut self) -> &mut Manifest {
        &mut self.manifest
    }
}
