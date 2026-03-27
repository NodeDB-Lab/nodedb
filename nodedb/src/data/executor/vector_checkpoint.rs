//! Vector index checkpoint methods for [`CoreLoop`].
//!
//! Contains HNSW build completion polling and checkpoint load/save operations.

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Drain completed HNSW builds from the background builder thread and
    /// promote the corresponding building segments to sealed segments.
    ///
    /// Called at the top of `tick()` before draining new requests.
    pub fn poll_build_completions(&mut self) {
        let Some(rx) = &self.build_rx else { return };
        while let Ok(complete) = rx.try_recv() {
            if let Some(coll) = self.vector_collections.get_mut(&complete.key) {
                coll.complete_build(complete.segment_id, complete.index);
                tracing::info!(
                    core = self.core_id,
                    key = %complete.key,
                    segment_id = complete.segment_id,
                    "HNSW build completed, segment promoted to sealed"
                );
            }
        }
    }

    /// Write HNSW checkpoints for all vector indexes to disk.
    ///
    /// Called periodically from the TPC event loop (e.g., every 5 minutes
    /// or when idle). Each index is serialized to a file at
    /// `{data_dir}/vector-ckpt/{index_key}.ckpt`.
    ///
    /// After checkpointing, WAL replay only needs to process entries
    /// since the checkpoint — not the entire history.
    pub fn checkpoint_vector_indexes(&self) -> usize {
        if self.vector_collections.is_empty() {
            return 0;
        }

        let ckpt_dir = self.data_dir.join("vector-ckpt");
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            tracing::warn!(
                core = self.core_id,
                "failed to create vector checkpoint dir"
            );
            return 0;
        }

        let mut checkpointed = 0;
        for (key, collection) in &self.vector_collections {
            if collection.is_empty() {
                continue;
            }
            let bytes = collection.checkpoint_to_bytes();
            if bytes.is_empty() {
                continue;
            }
            // Write to temp file, then rename for atomicity.
            let ckpt_path = ckpt_dir.join(format!("{key}.ckpt"));
            let tmp_path = ckpt_dir.join(format!("{key}.ckpt.tmp"));
            if std::fs::write(&tmp_path, &bytes).is_ok()
                && std::fs::rename(&tmp_path, &ckpt_path).is_ok()
            {
                checkpointed += 1;
            }
        }

        if checkpointed > 0 {
            tracing::info!(
                core = self.core_id,
                checkpointed,
                total = self.vector_collections.len(),
                "vector collections checkpointed"
            );
        }
        checkpointed
    }

    /// Load HNSW checkpoints from disk on startup, before WAL replay.
    ///
    /// For each checkpoint file, loads the index. WAL replay then only
    /// needs to process entries after the checkpoint LSN.
    pub fn load_vector_checkpoints(&mut self) {
        let ckpt_dir = self.data_dir.join("vector-ckpt");
        if !ckpt_dir.exists() {
            return;
        }

        let entries = match std::fs::read_dir(&ckpt_dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        let mut loaded = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            let key = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if key.is_empty() {
                continue;
            }

            if let Ok(bytes) = std::fs::read(&path)
                && let Some(collection) =
                    crate::engine::vector::collection::VectorCollection::from_checkpoint(&bytes)
            {
                tracing::info!(
                    core = self.core_id,
                    %key,
                    vectors = collection.len(),
                    "loaded vector checkpoint"
                );
                self.vector_collections.insert(key, collection);
                loaded += 1;
            }
        }

        if loaded > 0 {
            tracing::info!(core = self.core_id, loaded, "vector checkpoints loaded");
        }
    }
}
