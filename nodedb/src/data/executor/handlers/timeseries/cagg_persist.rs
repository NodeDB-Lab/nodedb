// SPDX-License-Identifier: BUSL-1.1

//! Continuous-aggregate images on disk: `<collection dir>/cagg.state`.
//!
//! The image is the only copy of buckets folded from partitions that raw
//! retention has removed. Everything else in it can be refolded from
//! partitions, so a lost write costs refold work at the next restart, never
//! rows. The one ordering that guards rows lives in retention: it writes
//! the image before it removes a partition.
//!
//! The image lives inside the root collection's directory. A TRUNCATE
//! removes it and resets the aggregates, and its undo writes it back.
//!
//! Every core registers every aggregate, and every core sees the same data
//! directory. Only the core that owns the root's vShard flushes the root,
//! so only that core reads, folds, and writes the root's image. Any other
//! core holds the root's aggregates loaded and empty.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::continuous_agg::{
    CAGG_IMAGE_FILE, CaggImage, decode_image, encode_image,
};
use crate::types::DatabaseId;

use super::paths::ts_collection_dir;

impl CoreLoop {
    /// Whether this core owns `root`, and so its partitions and its image.
    pub(in crate::data::executor) fn owns_cagg_root(&self, database_id: u64, root: &str) -> bool {
        self.owns_collection(DatabaseId::new(database_id), root)
            .is_ok_and(|owns| owns)
    }

    /// Write `cagg.state` for one root collection. Unloaded aggregates read
    /// the current image first, so a write never drops state it has not
    /// read yet.
    pub(in crate::data::executor) fn persist_cagg_image(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
    ) -> crate::Result<()> {
        if !self.owns_cagg_root(database_id, root) {
            return Ok(());
        }
        self.load_cagg_images(database_id, tenant_id, root);
        if self
            .continuous_agg_mgr
            .is_quarantined(database_id, tenant_id, root)
        {
            return Err(crate::Error::ObjectNotInPrerequisiteState {
                object: format!("continuous aggregates of '{root}'"),
                detail: "their image or a partition could not be read; they are stale and \
                         their image is kept for repair"
                    .into(),
            });
        }
        let dir = ts_collection_dir(&self.data_dir, database_id, tenant_id, root);
        let view = self
            .continuous_agg_mgr
            .export_image(database_id, tenant_id, root);
        if view.entries.is_empty() && view.orphans.is_empty() && view.raw_dropped_through.is_none()
        {
            return self.remove_cagg_image(database_id, tenant_id, root);
        }
        let bytes = encode_image(&view)?;
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!("create {}: {e}", dir.display()),
        })?;
        nodedb_wal::segment::atomic_write_fsync(&dir, CAGG_IMAGE_FILE, &bytes).map_err(|e| {
            crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!("write {}/{CAGG_IMAGE_FILE}: {e}", dir.display()),
            }
        })
    }

    /// Write every dirty image. A failed write keeps its root dirty for the
    /// next call and files one report.
    pub(in crate::data::executor) fn persist_dirty_cagg_images(&mut self) {
        for (database_id, tenant_id, root) in self.continuous_agg_mgr.take_dirty_images() {
            if self
                .continuous_agg_mgr
                .is_quarantined(database_id, tenant_id, &root)
            {
                continue;
            }
            if let Err(e) = self.persist_cagg_image(database_id, tenant_id, &root) {
                tracing::warn!(
                    core = self.core_id,
                    collection = %root,
                    error = %e,
                    "continuous aggregate image write failed; the root stays dirty"
                );
                crate::diag::continuous_aggregate_state_fault(
                    &e,
                    "image_write",
                    database_id,
                    tenant_id,
                    &root,
                );
                self.continuous_agg_mgr
                    .mark_image_dirty(database_id, tenant_id, &root);
            }
        }
    }

    /// Unlink `cagg.state` of one root collection and fsync its directory.
    /// An absent file is already removed.
    pub(in crate::data::executor) fn remove_cagg_image(
        &self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
    ) -> crate::Result<()> {
        let dir = ts_collection_dir(&self.data_dir, database_id, tenant_id, root);
        match std::fs::remove_file(dir.join(CAGG_IMAGE_FILE)) {
            Ok(()) => {
                nodedb_wal::segment::fsync_directory(&dir).map_err(|e| crate::Error::Storage {
                    engine: "timeseries".into(),
                    detail: format!("fsync {}: {e}", dir.display()),
                })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!("remove {}/{CAGG_IMAGE_FILE}: {e}", dir.display()),
            }),
        }
    }

    /// Remove `root`'s image and return its bytes, so a TRUNCATE leaves its
    /// aggregates empty and its undo can put the image back. `None` when the
    /// root has no image or another core owns it.
    pub(in crate::data::executor) fn take_cagg_image(
        &self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        if !self.owns_cagg_root(database_id, root) {
            return Ok(None);
        }
        let path =
            ts_collection_dir(&self.data_dir, database_id, tenant_id, root).join(CAGG_IMAGE_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(crate::Error::Storage {
                    engine: "timeseries".into(),
                    detail: format!("read {}: {e}", path.display()),
                });
            }
        };
        self.remove_cagg_image(database_id, tenant_id, root)?;
        Ok(Some(bytes))
    }

    /// Write back an image [`Self::take_cagg_image`] removed. The caller
    /// resets the root's aggregates next, so they reload it.
    pub(in crate::data::executor) fn put_back_cagg_image(
        &self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        bytes: &[u8],
    ) -> crate::Result<()> {
        let dir = ts_collection_dir(&self.data_dir, database_id, tenant_id, root);
        nodedb_wal::segment::atomic_write_fsync(&dir, CAGG_IMAGE_FILE, bytes).map_err(|e| {
            crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!("write {}/{CAGG_IMAGE_FILE}: {e}", dir.display()),
            }
        })
    }

    /// Read the image of one root collection. An absent file is an empty
    /// image.
    pub(in crate::data::executor) fn read_cagg_image(
        &self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
    ) -> crate::Result<CaggImage> {
        let path =
            ts_collection_dir(&self.data_dir, database_id, tenant_id, root).join(CAGG_IMAGE_FILE);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(decode_image(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(CaggImage::default()),
            Err(e) => Err(crate::Error::Storage {
                engine: "timeseries".into(),
                detail: format!("read {}: {e}", path.display()),
            }),
        }
    }

    /// Read `root`'s image into its unloaded aggregates. A missing image
    /// loads them empty. An unreadable image files one report and
    /// quarantines every aggregate rooted at `root`. A root with no
    /// registered aggregate reads its image once too, so a write never
    /// drops the entries it holds for aggregates registered later.
    pub(in crate::data::executor) fn load_cagg_images(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
    ) {
        let mgr = &self.continuous_agg_mgr;
        if !mgr.has_unloaded(database_id, tenant_id, root)
            && mgr.image_read(database_id, tenant_id, root)
        {
            return;
        }
        if !self.owns_cagg_root(database_id, root) {
            self.continuous_agg_mgr.install_image(
                database_id,
                tenant_id,
                root,
                CaggImage::default(),
            );
            return;
        }
        match self.read_cagg_image(database_id, tenant_id, root) {
            Ok(image) => {
                self.continuous_agg_mgr
                    .install_image(database_id, tenant_id, root, image);
            }
            Err(e) => {
                tracing::error!(
                    core = self.core_id,
                    collection = %root,
                    error = %e,
                    "continuous aggregate image unreadable; its aggregates are marked stale"
                );
                crate::diag::continuous_aggregate_state_fault(
                    &e,
                    "image_read",
                    database_id,
                    tenant_id,
                    root,
                );
                self.continuous_agg_mgr
                    .quarantine_root(database_id, tenant_id, root);
            }
        }
    }

    /// Boot registered every aggregate the catalog holds. Read the image of
    /// every timeseries collection on this core, drop the entries of
    /// aggregates that are not registered, and write the images.
    pub(in crate::data::executor) fn finish_cagg_boot_registration(&mut self) {
        self.continuous_agg_mgr.finish_boot_registration();
        let roots: Vec<(u64, u64, String)> = self
            .ts_registries
            .keys()
            .map(|(db, tid, root)| (db.as_u64(), tid.as_u64(), root.clone()))
            .collect();
        for (database_id, tenant_id, root) in roots {
            let path = ts_collection_dir(&self.data_dir, database_id, tenant_id, &root)
                .join(CAGG_IMAGE_FILE);
            if self.owns_cagg_root(database_id, &root) && path.exists() {
                self.load_cagg_images(database_id, tenant_id, &root);
            }
        }
        self.persist_dirty_cagg_images();
    }

    /// Install a snapshot's image of `root` next to the partitions the
    /// snapshot restored, which keep their directory names.
    ///
    /// The local image loads first. The snapshot's entries then merge into
    /// every aggregate whose folded set they share no partition with. The
    /// partitions the snapshot restored that no aggregate holds yet fold on
    /// the maintenance tick. The result is written at once.
    pub(in crate::data::executor) fn install_snapshot_cagg_image(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        bytes: &[u8],
    ) -> crate::Result<()> {
        let image = decode_image(bytes)?;
        self.load_cagg_images(database_id, tenant_id, root);
        self.continuous_agg_mgr
            .merge_snapshot_image(database_id, tenant_id, root, image);
        self.continuous_agg_mgr
            .mark_image_dirty(database_id, tenant_id, root);
        self.persist_cagg_image(database_id, tenant_id, root)
    }

    /// Encoded image of `root` for a snapshot, after its aggregates loaded
    /// and folded every partition they fold at flush. A quarantined root
    /// fails the capture.
    pub(in crate::data::executor) fn capture_cagg_image(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        now_ms: i64,
    ) -> crate::Result<Vec<u8>> {
        if self
            .continuous_agg_mgr
            .chain(database_id, tenant_id, root)
            .is_empty()
        {
            return Ok(Vec::new());
        }
        self.complete_cagg_backfill(database_id, tenant_id, root, now_ms)?;
        // A quarantined root's memory holds no trusted state. Shipping it
        // would replace the target's aggregates with empty ones.
        if self
            .continuous_agg_mgr
            .is_quarantined(database_id, tenant_id, root)
        {
            return Err(crate::Error::ObjectNotInPrerequisiteState {
                object: format!("continuous aggregates of '{root}'"),
                detail: "their image or a partition could not be read, so a snapshot cannot \
                         carry them"
                    .into(),
            });
        }
        Ok(encode_image(&self.continuous_agg_mgr.export_image(
            database_id,
            tenant_id,
            root,
        ))?)
    }
}

#[cfg(test)]
mod tests {
    use super::super::cagg_test_support::{ROWS_A, flush, host_def, image_path, ingest, register};
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::timeseries::continuous_agg::{RefreshPolicy, decode_image};

    /// Boot registered only one of the two aggregates the image holds: the
    /// other was dropped. Its entry leaves the image.
    #[test]
    fn boot_registration_prunes_entries_of_dropped_aggregates() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            register(&mut core, host_def("kept", RefreshPolicy::OnFlush));
            register(&mut core, host_def("dropped", RefreshPolicy::OnFlush));
            core.fold_pending_cagg_partitions(0);
            assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
            flush(&mut core);
            core.persist_dirty_cagg_images();
        }
        let names = || -> Vec<String> {
            let bytes = std::fs::read(image_path(dir.path())).expect("read image");
            let mut names: Vec<String> = decode_image(&bytes)
                .expect("decode image")
                .entries
                .into_iter()
                .map(|e| e.def.name)
                .collect();
            names.sort();
            names
        };
        assert_eq!(names(), vec!["dropped".to_string(), "kept".to_string()]);

        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.load_ts_registries().expect("load registries");
        register(&mut core, host_def("kept", RefreshPolicy::OnFlush));
        core.finish_cagg_boot_registration();
        assert_eq!(names(), vec!["kept".to_string()]);
    }
}
