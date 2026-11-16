// SPDX-License-Identifier: BUSL-1.1

//! Install of one restored vector row (`crate::wal::VectorRestoreRedo`), for
//! restart replay and the committed-redo apply alike.
//!
//! The record carries the exact nodes its resolve chose. The install first
//! ends the row's live nodes at `end_live_at`, then installs every ended
//! node with its span, then the live nodes at their start. So no two
//! versions of the row overlap, and a search at any cut reads one. The
//! install reads nothing to choose, so every replica and every replay writes
//! the same nodes. The spans fold into the shared clock, so a later write
//! stamps above every restored node.
//!
//! A vector-primary row's sidecar versions install first, in one sparse
//! write, at the system times the record carries. Restart replay writes the
//! same version keys with the same bytes, so a replayed record leaves the
//! sidecar as the live apply left it.
//!
//! A row of a vector-primary collection's primary index registers the
//! index settings a live write registers, and keeps its live node's payload
//! bitmap entries in step with its current sidecar
//! (`wal_replay_vector_restore_payload`).
//!
//! A clone copy installs only where the clone holds no version of the row.

use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::{StorageKey, Surrogate};
use nodedb_vector::collection::EndedNodeExport;

use super::core_loop::CoreLoop;
use super::wal_replay_vector_redo::RedoVectorWrite;
use crate::bridge::envelope::{ErrorCode, PhysicalPlan};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::vector_direct_row::VectorDirectIndexSpec;
use crate::engine::sparse::btree_versioned::VersionedPut;
use crate::types::{DatabaseId, TenantId};
use crate::wal::VectorRestoreRedo;

impl CoreLoop {
    /// Install a restored vector row. `None` when the payload is not one.
    /// `Some(true)` when the row installed, `Some(false)` when a gate
    /// skipped it or the install failed (reported through `replay_policy`).
    pub(in crate::data::executor) fn try_replay_vector_restore(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::DatabaseTombstones<'_>,
    ) -> Option<bool> {
        let redo = VectorRestoreRedo::decode(payload)?;
        if tombstones.is_tombstoned(tenant_id, &redo.collection, record_lsn) {
            return Some(false);
        }
        // A restored row names a bound row. One under `ZERO` is refused.
        if redo.surrogate == 0 {
            self.replay_record_unapplied(
                "vector",
                "restore_identity",
                record_lsn,
                &format!(
                    "restored vector row of '{}' carries no surrogate",
                    redo.collection
                ),
            );
            return Some(false);
        }
        let dim = redo.dim as usize;
        let mismatch = redo
            .ended
            .iter()
            .map(|node| node.vector.len())
            .chain(
                redo.live
                    .iter()
                    .flat_map(|live| live.vectors.iter().map(Vec::len)),
            )
            .find(|len| *len != dim);
        if let Some(len) = mismatch {
            self.replay_record_unapplied(
                "vector",
                "dim",
                record_lsn,
                &format!(
                    "restored row of '{}' declares dim {dim} but carries {len} components",
                    redo.collection
                ),
            );
            return Some(false);
        }
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, &redo.collection, &redo.field_name);
        let surrogate = Surrogate::new(redo.surrogate);
        if !self.redo_vector_prelude(
            RedoVectorWrite {
                index_key: &index_key,
                tid: tenant_id,
                collection: &redo.collection,
                dim,
                surrogates: &[surrogate],
                ids: &[],
                sidecars: false,
            },
            None,
            record_lsn,
        ) {
            return Some(false);
        }
        // A write into the clone wins over a copy of its source row.
        if let Some(at) = redo.clone_copy_at {
            match self.restored_row_holds_a_version(database_id, tenant_id, &index_key, &redo, at) {
                Ok(false) => {}
                Ok(true) => return Some(false),
                Err(error) => {
                    self.replay_record_unapplied(
                        "vector",
                        "clone_copy_probe",
                        record_lsn,
                        &format!(
                            "clone copy of row {} in '{}' could not read its row: {error}",
                            redo.surrogate, redo.collection
                        ),
                    );
                    return Some(false);
                }
            }
        }
        match self.install_restored_vector_row(database_id, tenant_id, record_lsn, &redo) {
            Ok(()) => Some(true),
            Err(error) => {
                self.replay_record_rejected(
                    "vector",
                    record_lsn,
                    Some(Box::new(error)),
                    &format!("restore of a vector row into '{}' failed", redo.collection),
                );
                Some(false)
            }
        }
    }

    /// End the row's live nodes, then install every node of `redo`.
    fn install_restored_vector_row(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        record_lsn: u64,
        redo: &VectorRestoreRedo,
    ) -> Result<(), ErrorCode> {
        let engine_error =
            |e: nodedb_vector::error::VectorError| ErrorCode::from(crate::Error::from(e));
        let collection = redo.collection.as_str();
        let dim = redo.dim as usize;
        let surrogate = Surrogate::new(redo.surrogate);
        let index_key =
            CoreLoop::vector_index_key(database_id, tenant_id, collection, &redo.field_name);
        for node in &redo.ended {
            self.observe_bitemporal_ordinal(node.sys_from);
            self.observe_bitemporal_ordinal(node.sys_to);
        }
        if let Some(live) = &redo.live {
            self.observe_bitemporal_ordinal(live.sys_from);
        }
        if let Some(at) = redo.end_live_at {
            self.observe_bitemporal_ordinal(at);
        }
        for version in &redo.sidecar {
            self.observe_bitemporal_ordinal(version.sys_from);
        }
        let prior_payload = self.restored_payload_row(database_id, tenant_id, redo, &index_key)?;
        self.install_restored_sidecar(database_id, tenant_id, redo)
            .map_err(ErrorCode::from)?;
        let created = !self.vector_collections.contains_key(&index_key);
        let defer_seal = self.recording_redo_undo();
        // A vector-primary collection's primary index takes the settings a
        // live write registers on a new index, its payload bitmaps included.
        match &redo.primary_index {
            Some(settings) => {
                self.vector_direct_index_in(
                    database_id,
                    tenant_id,
                    &VectorDirectIndexSpec {
                        collection,
                        field: &redo.field_name,
                        dim,
                        quantization: settings.quantization,
                        storage_dtype: settings.storage_dtype,
                        payload_indexes: &settings.payload_indexes,
                    },
                )?;
            }
            None => {
                self.get_or_create_vector_index(
                    database_id,
                    tenant_id,
                    collection,
                    dim,
                    &redo.field_name,
                )?;
            }
        }
        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return Err(ErrorCode::Internal {
                detail: format!("vector index for '{collection}' vanished during a restore"),
            });
        };
        if let Some(at) = redo.end_live_at {
            if redo.multi {
                coll.end_multi_vector_at(surrogate, at);
            } else {
                coll.end_surrogate_at(surrogate, at);
            }
        }
        for node in &redo.ended {
            coll.install_ended_node(EndedNodeExport {
                vector: node.vector.clone(),
                surrogate,
                sys_from: node.sys_from,
                sys_to: node.sys_to,
            })
            .map_err(engine_error)?;
        }
        if let Some(live) = &redo.live {
            if redo.multi {
                let slices: Vec<&[f32]> = live.vectors.iter().map(Vec::as_slice).collect();
                coll.insert_multi_vector_at(&slices, surrogate, live.sys_from)
                    .map_err(engine_error)?;
            } else {
                for vector in &live.vectors {
                    coll.insert_at(vector.clone(), surrogate, live.sys_from)
                        .map_err(engine_error)?;
                }
            }
        }
        self.refresh_restored_payload(database_id, tenant_id, redo, &index_key, prior_payload)?;
        if created && defer_seal {
            self.record_redo_undo([UndoEntry::VectorCollectionCreated {
                index_key: index_key.clone(),
            }]);
        }
        if !defer_seal {
            self.settle_vector_collection(&index_key);
        }
        let written = redo.ended.len() + redo.live.as_ref().map_or(0, |live| live.vectors.len());
        self.checkpoint_coordinator
            .mark_dirty("vector", written.max(1));
        let database = DatabaseId::new(database_id);
        let tid = TenantId::new(tenant_id);
        let qualified =
            nodedb_types::QualifiedCollection::from_stored(database, collection.to_string())
                .map_err(|e| ErrorCode::from(crate::Error::from(e)))?;
        if let Some(vshard) = self.replay_vshard("vector", record_lsn, database, collection) {
            let task = Self::replay_task(
                tid,
                database,
                vshard,
                PhysicalPlan::Vector(VectorOp::Insert {
                    collection: qualified,
                    vector: Vec::new(),
                    dim,
                    field_name: redo.field_name.clone(),
                    surrogate,
                    pk_bytes: None,
                    provenance: None,
                }),
                Some(crate::types::Lsn::new(record_lsn)),
            );
            // The write's version, so a cross-shard read-set check sees it.
            self.note_surrogate_write(&task, tenant_id, collection, surrogate.as_u32())
                .map_err(ErrorCode::from)?;
        }
        Ok(())
    }

    /// Write every sidecar version `redo` carries, in one sparse write
    /// transaction, each at its own system time and valid interval. The
    /// bytes are stored as the backup holds them: the tagged encoding every
    /// live vector-primary write stores. Each version's undo removes it.
    fn install_restored_sidecar(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        redo: &VectorRestoreRedo,
    ) -> crate::Result<()> {
        if redo.sidecar.is_empty() {
            return Ok(());
        }
        let collection = redo.collection.as_str();
        let key = StorageKey::for_surrogate(Surrogate::new(redo.surrogate));
        let txn = self.sparse.begin_write()?;
        for version in &redo.sidecar {
            match &version.body {
                Some(body) => self.sparse.versioned_put_in_txn(
                    &txn,
                    VersionedPut {
                        database_id,
                        tenant: tenant_id,
                        coll: collection,
                        doc_id: &key,
                        sys_from: version.sys_from,
                        valid_from_ms: version.valid_from_ms,
                        valid_until_ms: version.valid_until_ms,
                        body,
                    },
                )?,
                None => self.sparse.versioned_tombstone_in_txn(
                    &txn,
                    database_id,
                    tenant_id,
                    collection,
                    &key,
                    version.sys_from,
                )?,
            }
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit the restored sidecar of {key} in '{collection}': {e}"),
        })?;
        self.doc_cache
            .invalidate(database_id, tenant_id, collection, &key);
        self.record_redo_undo(redo.sidecar.iter().map(|version| UndoEntry::PutDocument {
            collection: collection.to_string(),
            document_id: key,
            version_sys_from: version.sys_from,
            index_tuples: Vec::new(),
            chain_hash_prior: None,
        }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::Value;
    use nodedb_wal::TombstoneSet;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::wal::{VectorRestoreLive, VectorSidecarVersion};

    const DB: u64 = 0;
    const TID: u64 = 1;
    const COLLECTION: &str = "vp_restore";

    fn tagged(tag: &str) -> Vec<u8> {
        let mut fields = HashMap::new();
        fields.insert("tag".to_string(), Value::String(tag.into()));
        zerompk::to_msgpack_vec(&fields).expect("encode sidecar")
    }

    /// Restart replay of a restored vector-primary row stores every sidecar
    /// version in the tagged encoding, at its system time and valid
    /// interval. A second replay of the record leaves the same bytes.
    #[test]
    fn a_replayed_restored_row_stores_its_tagged_sidecar() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let redo = VectorRestoreRedo {
            collection: COLLECTION.into(),
            field_name: "emb".into(),
            surrogate: 7,
            dim: 3,
            multi: false,
            ended: Vec::new(),
            live: Some(VectorRestoreLive {
                vectors: vec![vec![1.0, 0.0, 0.0]],
                sys_from: 100,
            }),
            end_live_at: None,
            vector_primary: true,
            sidecar: vec![
                VectorSidecarVersion {
                    sys_from: 100,
                    valid_from_ms: 5,
                    valid_until_ms: 50,
                    body: Some(tagged("first")),
                },
                VectorSidecarVersion {
                    sys_from: 200,
                    valid_from_ms: i64::MIN,
                    valid_until_ms: i64::MAX,
                    body: Some(tagged("second")),
                },
            ],
            primary_index: None,
            clone_copy_at: None,
        };
        let payload = redo.encode().expect("encode redo");
        let tombstones = TombstoneSet::new();
        for lsn in [10, 11] {
            let applied = core.try_replay_vector_restore(
                &payload,
                TID,
                DB,
                lsn,
                &tombstones.for_database(DB),
            );
            assert_eq!(applied, Some(true), "replay {lsn} installs the row");
        }

        let key = StorageKey::for_surrogate(Surrogate::new(7));
        let read = |sys: i64, valid: Option<i64>| {
            core.sparse
                .versioned_get_as_of(DB, TID, COLLECTION, &key, Some(sys), valid)
                .expect("read sidecar")
        };
        assert_eq!(read(150, None), Some(tagged("first")));
        assert_eq!(read(150, Some(10)), Some(tagged("first")));
        assert_eq!(read(150, Some(60)), None, "outside its valid interval");
        assert_eq!(read(250, None), Some(tagged("second")));
        let current = core
            .sparse
            .get(DB, TID, COLLECTION, &key)
            .expect("read current")
            .expect("a current sidecar");
        let fields: HashMap<String, Value> =
            zerompk::from_msgpack(&current).expect("the sidecar decodes tagged");
        assert_eq!(fields.get("tag"), Some(&Value::String("second".into())));
        assert_eq!(
            core.sparse
                .versioned_latest_sys_from(DB, TID, COLLECTION, &key)
                .expect("newest version"),
            Some(200),
            "a second replay writes no version of its own"
        );
    }
}
