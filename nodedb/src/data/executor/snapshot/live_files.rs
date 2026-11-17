// SPDX-License-Identifier: BUSL-1.1

//! Selection of the live files each engine owns.
//!
//! A capture reads only what the next boot loads: the generation each
//! checkpoint MANIFEST names, the segments each array manifest names, and the
//! partitions each timeseries registry holds. Superseded generations,
//! unreferenced segments, and deleted partitions stay behind.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use nodedb_types::timeseries::PartitionState;

use super::super::{
    columnar_checkpoint, crdt_checkpoint, graph_label_checkpoint, kv_checkpoint,
    sparse_vector_checkpoint, spatial_checkpoint, sync_hwm_checkpoint, vector_checkpoint,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::schema::{TS_SCHEMA_FILE, encode_ts_schema};
use crate::data::snapshot::{SnapshotComponent, SnapshotDir, SnapshotFile};
use crate::engine::array::store::manifest::{MANIFEST_FILENAME, Manifest, segment_path};
use crate::storage::snapshot_files::{collect_file, collect_tree};
use crate::types::{DatabaseId, core_for_vshard};

/// Components published as a MANIFEST naming one generation directory.
pub(super) const GENERATION_COMPONENTS: [SnapshotComponent; 6] = [
    SnapshotComponent::Kv,
    SnapshotComponent::SparseVector,
    SnapshotComponent::Columnar,
    SnapshotComponent::Vector,
    SnapshotComponent::Crdt,
    SnapshotComponent::Spatial,
];

impl CoreLoop {
    /// Append the MANIFEST of a generation-published checkpoint, the files of
    /// the generation directory it names, and that directory itself. The
    /// loader lists the directory, so restore recreates it even when it holds
    /// no file. No MANIFEST appends nothing.
    pub(super) fn collect_live_generation(
        &self,
        component: SnapshotComponent,
        out: &mut Vec<SnapshotFile>,
        dirs: &mut Vec<SnapshotDir>,
    ) -> crate::Result<()> {
        let base = Path::new("");
        let dir = |root: PathBuf| self.data_dir.join(root);
        let (ckpt_rel, manifest_name, gen_rel) = match component {
            SnapshotComponent::Kv => {
                let rel = kv_checkpoint::kv_ckpt_dir(base, self.core_id);
                let generation = self.read_kv_manifest(&dir(rel.clone()))?;
                let gen_rel =
                    generation.map(|m| kv_checkpoint::kv_ckpt_gen_dir(&rel, m.generation));
                (rel, kv_checkpoint::KV_CKPT_MANIFEST, gen_rel)
            }
            SnapshotComponent::Columnar => {
                let rel = columnar_checkpoint::columnar_ckpt_dir(base, self.core_id);
                let generation = self.read_columnar_manifest(&dir(rel.clone()))?;
                let gen_rel = generation
                    .map(|m| columnar_checkpoint::columnar_ckpt_gen_dir(&rel, m.generation));
                (rel, columnar_checkpoint::COLUMNAR_CKPT_MANIFEST, gen_rel)
            }
            SnapshotComponent::SparseVector => {
                let rel = sparse_vector_checkpoint::sparse_vector_ckpt_dir(base, self.core_id);
                let generation = sparse_vector_checkpoint::read_sparse_vector_manifest_at(
                    &dir(rel.clone()),
                    self.core_id,
                )?;
                let gen_rel = generation.map(|m| {
                    sparse_vector_checkpoint::sparse_vector_ckpt_gen_dir(&rel, m.generation)
                });
                (
                    rel,
                    sparse_vector_checkpoint::SPARSE_VECTOR_CKPT_MANIFEST,
                    gen_rel,
                )
            }
            SnapshotComponent::Vector => {
                let rel = vector_checkpoint::vector_ckpt_dir(base, self.core_id);
                let generation = vector_checkpoint::read_vector_manifest_at(&dir(rel.clone()))?;
                let gen_rel =
                    generation.map(|m| vector_checkpoint::vector_ckpt_gen_dir(&rel, m.generation));
                (rel, vector_checkpoint::VECTOR_CKPT_MANIFEST, gen_rel)
            }
            SnapshotComponent::Crdt => {
                let rel = crdt_checkpoint::crdt_ckpt_dir(base, self.core_id);
                let generation = crdt_checkpoint::read_crdt_manifest_at(&dir(rel.clone()))?;
                let gen_rel =
                    generation.map(|m| crdt_checkpoint::crdt_ckpt_gen_dir(&rel, m.generation));
                (rel, crdt_checkpoint::CRDT_CKPT_MANIFEST, gen_rel)
            }
            SnapshotComponent::Spatial => {
                let rel = spatial_checkpoint::spatial_ckpt_dir(base, self.core_id);
                let generation = spatial_checkpoint::read_spatial_manifest_at(&dir(rel.clone()))?;
                let gen_rel = generation
                    .map(|m| spatial_checkpoint::spatial_ckpt_gen_dir(&rel, m.generation));
                (rel, spatial_checkpoint::SPATIAL_CKPT_MANIFEST, gen_rel)
            }
            other => {
                return Err(crate::Error::Internal {
                    detail: format!("{other:?} is not published as a checkpoint generation"),
                });
            }
        };
        let Some(gen_rel) = gen_rel else {
            return Ok(());
        };
        collect_file(
            &self.data_dir,
            &ckpt_rel.join(manifest_name),
            component,
            out,
        )?;
        dirs.push(SnapshotDir {
            component,
            path: crate::storage::snapshot_files::rel_path_string(&gen_rel)?,
        });
        collect_tree(&self.data_dir, &gen_rel, component, out)
    }

    /// Append the single STATE file of the sync gate and graph-label
    /// checkpoints, when one is published.
    pub(super) fn collect_state_files(&self, out: &mut Vec<SnapshotFile>) -> crate::Result<()> {
        let base = Path::new("");
        let states = [
            (
                SnapshotComponent::SyncHwm,
                sync_hwm_checkpoint::sync_hwm_ckpt_state_path(
                    &sync_hwm_checkpoint::sync_hwm_ckpt_dir(base, self.core_id),
                ),
            ),
            (
                SnapshotComponent::GraphLabel,
                graph_label_checkpoint::graph_label_ckpt_state_path(
                    &graph_label_checkpoint::graph_label_ckpt_dir(base, self.core_id),
                ),
            ),
        ];
        for (component, rel) in states {
            if self.data_dir.join(&rel).exists() {
                collect_file(&self.data_dir, &rel, component, out)?;
            }
        }
        Ok(())
    }

    /// Append every array's manifest and the segments it names.
    ///
    /// A directory whose name starts with `.` is a drop tombstone awaiting
    /// removal, never a live array.
    pub(super) fn collect_live_arrays(&self, out: &mut Vec<SnapshotFile>) -> crate::Result<()> {
        let root = super::layout::array_root(Path::new(""), self.core_id);
        let mut pending = vec![root];
        while let Some(rel_dir) = pending.pop() {
            let abs = self.data_dir.join(&rel_dir);
            if !abs.is_dir() {
                continue;
            }
            if abs.join(MANIFEST_FILENAME).is_file() {
                let manifest =
                    Manifest::load_or_new(&abs, 0).map_err(|e| crate::Error::Storage {
                        engine: "snapshot".into(),
                        detail: format!("read array manifest in {}: {e}", abs.display()),
                    })?;
                collect_file(
                    &self.data_dir,
                    &rel_dir.join(MANIFEST_FILENAME),
                    SnapshotComponent::Array,
                    out,
                )?;
                // The directory name is a hash; the key file says which array
                // it holds, and a restored store is refused without a match.
                let key_rel = rel_dir.join(crate::engine::array::store_dir::ARRAY_KEY_FILE);
                if self.data_dir.join(&key_rel).is_file() {
                    collect_file(&self.data_dir, &key_rel, SnapshotComponent::Array, out)?;
                }
                for segment in &manifest.segments {
                    let rel = segment_path(&rel_dir, &segment.id);
                    collect_file(&self.data_dir, &rel, SnapshotComponent::Array, out)?;
                }
                continue;
            }
            // no-objectstore: array engine files are captured from the local data directory.
            let entries = std::fs::read_dir(&abs).map_err(|e| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("list {}: {e}", abs.display()),
            })?;
            let mut subdirs = BTreeSet::new();
            for entry in entries {
                let entry = entry.map_err(|e| crate::Error::Storage {
                    engine: "snapshot".into(),
                    detail: format!("list {}: {e}", abs.display()),
                })?;
                let name = entry.file_name();
                if name.to_string_lossy().starts_with('.') {
                    continue;
                }
                if entry.path().is_dir() {
                    subdirs.insert(rel_dir.join(name));
                }
            }
            pending.extend(subdirs);
        }
        Ok(())
    }

    /// Append the live partitions of every timeseries collection this core
    /// owns.
    ///
    /// Every core loads every registry at boot, so a registry alone does not
    /// say which collections this core owns. The vShard does. A partition
    /// marked deleted is superseded by a merge. An archived partition keeps
    /// its local directory: the archiver copies it to object storage and
    /// never removes it, so it is captured like any other.
    pub(super) fn collect_owned_timeseries(
        &self,
        out: &mut Vec<SnapshotFile>,
    ) -> crate::Result<()> {
        // Each owned memtable's schema, as the next record meets it. An empty
        // memtable does not flush, so its schema is captured from memory.
        let mut memtables: Vec<_> = self.columnar_memtables.iter().collect();
        memtables.sort_by(|(a, _), (b, _)| {
            (a.0.as_u64(), a.1.as_u64(), &a.2).cmp(&(b.0.as_u64(), b.1.as_u64(), &b.2))
        });
        for ((db, tid, collection), mt) in memtables {
            if !self.owns_collection(*db, collection)? {
                continue;
            }
            let rel = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
                Path::new(""),
                db.as_u64(),
                tid.as_u64(),
                collection,
            )
            .join(TS_SCHEMA_FILE);
            out.push(SnapshotFile {
                component: SnapshotComponent::Timeseries,
                path: crate::storage::snapshot_files::rel_path_string(&rel)?,
                bytes: encode_ts_schema(mt.schema())?,
            });
        }

        let mut registries: Vec<_> = self.ts_registries.iter().collect();
        registries.sort_by(|(a, _), (b, _)| {
            (a.0.as_u64(), a.1.as_u64(), &a.2).cmp(&(b.0.as_u64(), b.1.as_u64(), &b.2))
        });
        for ((db, tid, collection), registry) in registries {
            if !self.owns_collection(*db, collection)? {
                continue;
            }
            let collection_rel =
                crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
                    Path::new(""),
                    db.as_u64(),
                    tid.as_u64(),
                    collection,
                );
            let mut partitions: Vec<_> = registry
                .iter()
                .filter(|(_, entry)| entry.meta.state != PartitionState::Deleted)
                .collect();
            partitions.sort_by(|a, b| a.1.dir_name.cmp(&b.1.dir_name));
            for (_, entry) in partitions {
                let rel = collection_rel.join(&entry.dir_name);
                if !self.data_dir.join(&rel).exists() {
                    return Err(crate::Error::Storage {
                        engine: "snapshot".into(),
                        detail: format!(
                            "timeseries partition {} is registered but has no directory",
                            rel.display()
                        ),
                    });
                }
                collect_tree(&self.data_dir, &rel, SnapshotComponent::Timeseries, out)?;
            }
        }
        Ok(())
    }

    /// Append the continuous-aggregate image of every root collection this
    /// core owns. Each image is encoded from memory after its aggregates
    /// loaded and folded every partition they fold at flush, so it names
    /// exactly the partitions captured beside it.
    pub(super) fn collect_owned_cagg_images(
        &mut self,
        out: &mut Vec<SnapshotFile>,
    ) -> crate::Result<()> {
        let now_ms = self.ingest_now_ms();
        for (db, tid, root) in self.continuous_agg_mgr.root_sources() {
            if !self.owns_collection(DatabaseId::new(db), &root)? {
                continue;
            }
            let bytes = self.capture_cagg_image(db, tid, &root, now_ms)?;
            if bytes.is_empty() {
                continue;
            }
            let rel = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
                Path::new(""),
                db,
                tid,
                &root,
            )
            .join(crate::engine::timeseries::continuous_agg::CAGG_IMAGE_FILE);
            out.push(SnapshotFile {
                component: SnapshotComponent::Timeseries,
                path: crate::storage::snapshot_files::rel_path_string(&rel)?,
                bytes,
            });
        }
        Ok(())
    }

    /// Whether this core owns `collection`: the core the dispatcher's vShard
    /// router sends its vShard to.
    pub(in crate::data::executor) fn owns_collection(
        &self,
        database_id: DatabaseId,
        collection: &str,
    ) -> crate::Result<bool> {
        let vshard =
            nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?.vshard();
        Ok(core_for_vshard(vshard, self.redo_apply.num_cores) == self.core_id)
    }
}
