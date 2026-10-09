// SPDX-License-Identifier: BUSL-1.1

//! Single dispatch entry point for a full-tenant snapshot restore, orchestrating
//! the per-engine install helpers in `engines.rs` across every engine.

use std::sync::Arc;

use tracing::info;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::keys::parse_vector_snapshot_key;

impl CoreLoop {
    /// Restore a tenant's data across ALL engines from a snapshot.
    ///
    /// `documents_bytes` carries a MessagePack-serialized
    /// `TenantDataSnapshot` — the full per-tenant snapshot with
    /// documents, indexes, edges, vectors, KV, CRDT, and timeseries.
    pub(in crate::data::executor) fn execute_restore_tenant_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        snapshot_bytes: &[u8],
        replace_mode: bool,
        collections_to_clear: &[nodedb_physical::physical_plan::SnapshotClearTarget],
        group_vshards: &[u32],
    ) -> Response {
        info!(core = self.core_id, tenant_id, "restoring tenant snapshot");

        // A data-group install replaces the edges with an endpoint home in
        // the group, whatever collection homes them, and no other edge.
        let group_install = !group_vshards.is_empty();

        // Clear-then-install: drop stale state for the listed collections before
        // installing, so keys deleted before the snapshot index and dropped
        // collections do not linger on a lagging follower. Empty list = no-op.
        for target in collections_to_clear {
            // Preserve the collection definition: clear-then-install replaces row
            // data from the snapshot, but the snapshot does not carry the schema,
            // so the reinstalled rows must land in the still-defined collection.
            // Fail-closed: if stale state cannot be cleared, abort the restore
            // rather than install the snapshot over rows that survived — those
            // would linger as un-owned data on this follower.
            if let Err(e) = self.clear_collection_all_engines(
                nodedb_types::DatabaseId::new(target.database_id),
                crate::types::TenantId::new(target.tenant_id),
                &target.collection,
                true,
                target.reclaim_l1_files,
                !group_install,
            ) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "clear-then-install purge failed for '{}': {e}",
                            target.collection
                        ),
                    },
                );
            }
        }

        let snap: crate::types::TenantDataSnapshot = match zerompk::from_msgpack(snapshot_bytes) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("malformed tenant snapshot: {e}"),
                    },
                );
            }
        };

        // Arrays: the group's vShards take the snapshot's cell versions.
        if let Err(e) = self.install_group_arrays(group_vshards, &snap.arrays) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("restore: array install failed: {e}"),
                },
            );
        }

        // Edges: the group's vShards take the snapshot's edge versions.
        let group_set: std::collections::HashSet<u32> = group_vshards.iter().copied().collect();
        let edges_purged = match self.edge_store.purge_homed(&group_set) {
            Ok(purged) => purged,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("restore: clearing the group's edges failed: {e}"),
                    },
                );
            }
        };

        let (docs_written, indexes_written) = match self.restore_sparse(&snap) {
            Ok(written) => written,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("restore: document install failed: {e}"),
                    },
                );
            }
        };
        // The snapshot carries no postings: index the restored rows' text.
        if let Err(e) = self.restore_text_index(&snap) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("restore: full-text reindex failed: {e}"),
                },
            );
        }

        let edges_written: u64;
        let mut vectors_written = 0u64;
        let mut kv_written = 0u64;
        let mut crdt_written = 0u64;
        let mut crdt_constraints_written = 0u64;
        let mut ts_written = 0u64;

        {
            // Restore graph edges. Keys are the versioned form
            // `"{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}"`.
            // The plain sections take the tenant from context. The sections
            // of a merged Raft snapshot carry their own database and tenant:
            // the dispatch context is the default database and tenant 0.
            edges_written = match self.install_snapshot_edges(
                task.request.database_id.as_u64(),
                tenant_id,
                &snap,
            ) {
                Ok(versions) => versions,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("restore: edge install failed: {e}"),
                        },
                    );
                }
            };
            // Rebuild CSR from restored edges. A rebuild failure is fatal to the
            // whole restore: leaving the stale CSR in place would make graph
            // traversals silently return wrong results over the just-installed
            // edges — the same silent-corruption class the durable-section
            // failures above treat as fatal.
            if edges_written > 0 || edges_purged > 0 {
                match crate::engine::graph::csr::rebuild::rebuild_sharded_from_store(
                    &self.edge_store,
                    Arc::clone(&self.governor),
                ) {
                    Ok(rebuilt) => self.csr = rebuilt,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "restore: CSR rebuild after edge install failed: {e}"
                                ),
                            },
                        );
                    }
                }
            }

            // Restore vector_params: re-populate HnswParams before the vector
            // collection restore so `restore_vector_collection` finds real params
            // instead of falling back to `HnswParams::default()`.
            for (key, bytes) in &snap.vector_params {
                let params: crate::engine::vector::hnsw::HnswParams =
                    match zerompk::from_msgpack(bytes) {
                        Ok(p) => p,
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!(
                                        "restore: vector params '{key}' do not decode: {e}"
                                    ),
                                },
                            );
                        }
                    };
                let (vp_db, vp_tid, coll_key) = parse_vector_snapshot_key(key, tenant_id);
                let map_key = (
                    nodedb_types::DatabaseId::new(vp_db),
                    crate::types::TenantId::new(vp_tid),
                    coll_key.to_string(),
                );
                self.vector_params.insert(map_key, params);
            }

            // Restore index_configs: re-populate IndexConfig before the vector
            // collection restore so index routing uses the correct type.
            for (key, bytes) in &snap.index_configs {
                let cfg: crate::engine::vector::index_config::IndexConfig =
                    match zerompk::from_msgpack(bytes) {
                        Ok(c) => c,
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!(
                                        "restore: vector index config '{key}' does not decode: {e}"
                                    ),
                                },
                            );
                        }
                    };
                let (ic_db, ic_tid, coll_key) = parse_vector_snapshot_key(key, tenant_id);
                let map_key = (
                    nodedb_types::DatabaseId::new(ic_db),
                    crate::types::TenantId::new(ic_tid),
                    coll_key.to_string(),
                );
                self.index_configs.insert(map_key, cfg);
            }

            // Restore vector collections.
            // Snapshot keys are `"{db}:{tid}:{coll_key}"` (new format) or, for
            // legacy snapshots, `"{tid}:{coll_key}"`. Parse the leading numeric
            // components back-compatibly; `coll_key` may itself contain `:`.
            for (key, bytes) in &snap.vectors {
                let vectors: Vec<(u32, Vec<f32>, Option<nodedb_types::Surrogate>)> =
                    match zerompk::from_msgpack(bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!(
                                        "restore: vector collection '{key}' does not decode: {e}"
                                    ),
                                },
                            );
                        }
                    };
                let count = vectors.len() as u64;
                let (database_id, vector_tid, coll_key) = parse_vector_snapshot_key(key, tenant_id);
                let multi_documents: std::collections::HashSet<nodedb_types::Surrogate> = snap
                    .vector_multi_documents
                    .iter()
                    .filter(|(members_key, _)| members_key == key)
                    .flat_map(|(_, documents)| documents.iter().copied())
                    .collect();
                if let Err(e) = self.restore_vector_collection(
                    database_id,
                    vector_tid,
                    coll_key,
                    vectors,
                    &multi_documents,
                    replace_mode,
                ) {
                    return self.response_error(task, e);
                }
                vectors_written += count;
            }

            // Restore KV tables, each under the database and tenant its key names.
            for (table_key, bytes) in &snap.kv_tables {
                let entries: Vec<crate::engine::kv::hash_table::KvSnapshotRow> =
                    match zerompk::from_msgpack(bytes) {
                        Ok(e) => e,
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!(
                                        "restore: KV table '{table_key}' does not decode: {e}"
                                    ),
                                },
                            );
                        }
                    };
                let count = entries.len() as u64;
                if let Err(e) = self.restore_kv_table(table_key, entries) {
                    return self.response_error(task, e);
                }
                kv_written += count;
            }

            // Restore CRDT state per collection (tenant carried explicitly so
            // both the per-group Raft snapshot, whose merged blob dispatches
            // with tenant 0, and the per-tenant user RESTORE path route the same
            // way). Loro import is a monotonic CRDT merge, so no replace_mode
            // handling is needed: the snapshot is >= the follower's committed
            // state and the merge converges to the correct result.
            for (database_raw, tid_raw, collection, bytes) in &snap.crdt_state {
                if let Err(e) = self.restore_crdt_state(*database_raw, *tid_raw, collection, bytes)
                {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!(
                                "restore: CRDT state of '{collection}' (tenant {tid_raw}) failed: {e}"
                            ),
                        },
                    );
                }
                crdt_written += 1;
            }

            // Restore CRDT constraint state per collection: reconstructs the
            // validator's installed constraint set + `installed_constraint_version`
            // so a snapshot-installed follower does not come up empty and
            // retry-fence every peer delta on constrained collections.
            for entry in &snap.crdt_constraints {
                if let Err(e) = self.restore_crdt_constraints(
                    entry.database_id,
                    entry.tenant_id,
                    &entry.collection,
                    entry.version,
                    &entry.constraints,
                ) {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!(
                                "restore: CRDT constraints of '{}' (tenant {}) failed: {e}",
                                entry.collection, entry.tenant_id
                            ),
                        },
                    );
                }
                crdt_constraints_written += 1;
            }

            // Restore timeseries memtables and flush each to an on-disk segment
            // for durability. A flush failure is fatal to the whole restore —
            // consistent with how `restore_flushed_ts_segments` treats durability
            // errors — because partial restore with non-durable data is worse than
            // a clean failure the operator can retry.
            for (key, bytes) in &snap.timeseries {
                if let Err(e) = self.restore_timeseries(key, bytes) {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("restore: timeseries collection {key} failed: {e}"),
                        },
                    );
                }
                ts_written += 1;
            }

            // Restore flushed on-disk timeseries segments.
            if !snap.flushed_ts_segments.is_empty()
                && let Err(e) =
                    self.restore_flushed_ts_segments(&snap.flushed_ts_segments, replace_mode)
            {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("restore: flushed ts segment restore failed: {e}"),
                    },
                );
            }

            // Restore plain-columnar engines.
            if !snap.columnar_engines.is_empty()
                && let Err(e) = self.restore_columnar_engines(&snap.columnar_engines, replace_mode)
            {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("restore: columnar engine restore failed: {e}"),
                    },
                );
            }
        }

        // The install wrote no WAL record, so it is durable only once every
        // memory-only engine is checkpointed. A failed checkpoint fails the
        // install, so it is never acknowledged memory-only.
        if let Err(e) = self.persist_snapshot_install() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("restore: checkpoint after the install failed: {e}"),
                },
            );
        }

        info!(
            tenant_id,
            docs_written,
            indexes_written,
            edges_written,
            vectors_written,
            kv_written,
            crdt_written,
            crdt_constraints_written,
            ts_written,
            flushed_ts_collections = snap.flushed_ts_segments.len(),
            columnar_engines = snap.columnar_engines.len(),
            "full tenant snapshot restored"
        );

        let result = serde_json::json!({
            "tenant_id": tenant_id,
            "documents_restored": docs_written,
            "indexes_restored": indexes_written,
            "edges_restored": edges_written,
            "vectors_restored": vectors_written,
            "kv_entries_restored": kv_written,
            "crdt_restored": crdt_written,
            "crdt_constraints_restored": crdt_constraints_written,
            "timeseries_restored": ts_written,
            "columnar_engines_restored": snap.columnar_engines.len(),
        });
        match crate::data::executor::response_codec::encode_json_as_msgpack(&result) {
            Ok(p) => self.response_with_payload(task, p),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("result serialization failed: {e}"),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::vector_checkpoint::{
        read_vector_manifest_at, vector_ckpt_dir, vector_ckpt_gen_dir,
    };
    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::MetaOp;

    fn open_core(dir: &std::path::Path) -> CoreLoop {
        let hlc = Arc::new(nodedb_types::OrdinalClock::new());
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx);
        drop(resp_rx);
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            hlc,
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    /// A msgpack-encoded `TenantDataSnapshot` carrying one vector collection
    /// entry under key `"0:0:emb"` (db=0, tenant=0, collection="emb").
    fn vector_snapshot_bytes() -> Vec<u8> {
        let vectors: Vec<(u32, Vec<f32>, Option<nodedb_types::Surrogate>)> = vec![(
            0,
            vec![1.0, 2.0, 3.0],
            Some(nodedb_types::Surrogate::new(1)),
        )];
        let vectors_bytes = zerompk::to_msgpack_vec(&vectors).expect("encode vectors");
        let snap = crate::types::TenantDataSnapshot {
            vectors: vec![("0:0:emb".to_string(), vectors_bytes)],
            ..Default::default()
        };
        zerompk::to_msgpack_vec(&snap).expect("encode snapshot")
    }

    /// The Raft install-snapshot path (`replace_mode = true`) installs
    /// vectors straight into the in-memory-only `vector_collections` map with
    /// no WAL record. Its only durable copy is the checkpoint the install
    /// takes before it answers — this proves the checkpoint happens
    /// SYNCHRONOUSLY within the restore call, not on the next periodic
    /// (5-minute) timer tick.
    #[test]
    fn raft_install_checkpoints_vectors_synchronously() {
        let dir = TempDir::new().expect("tempdir");
        let mut core = open_core(dir.path());

        let task = CoreLoop::replay_vector_task(
            crate::types::TenantId::new(0),
            nodedb_types::DatabaseId::DEFAULT,
            nodedb_types::CollectionKey::from_bare(nodedb_types::DatabaseId::DEFAULT, "emb")
                .vshard(),
            PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
            None,
        );

        let response = core.execute_restore_tenant_snapshot(
            &task,
            0,
            &vector_snapshot_bytes(),
            true, // replace_mode = true: the Raft-install signature.
            &[],
            &[],
        );
        assert_eq!(response.status, Status::Ok, "restore must succeed");

        // The checkpoint file must be PUBLISHED on disk immediately — no
        // periodic timer tick has run. Published means named by the manifest:
        // a generation nothing points at is not durable state.
        let ckpt_dir = vector_ckpt_dir(&core.data_dir, core.core_id);
        let manifest = read_vector_manifest_at(&ckpt_dir)
            .expect("the manifest must be readable")
            .expect("the restore must have published a generation synchronously");
        let gen_dir = vector_ckpt_gen_dir(&ckpt_dir, manifest.generation);
        let entries: Vec<_> = std::fs::read_dir(&gen_dir)
            .expect("the live generation dir must exist synchronously")
            .flatten()
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("ckpt"))
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "the restored vector collection must be checkpointed to disk synchronously"
        );

        // Reopen a fresh CoreLoop against the same data_dir with zero WAL
        // records: `load_vector_checkpoints()` alone must restore the vector,
        // proving durability came from the synchronous checkpoint, not replay.
        drop(core);
        let mut reopened = open_core(dir.path());
        reopened
            .load_vector_checkpoints()
            .expect("load vector checkpoints");
        let key = CoreLoop::vector_index_key(0, 0, "emb", "");
        let restored = reopened
            .vector_collections
            .get(&key)
            .expect("checkpoint must restore the vector collection on reopen");
        assert_eq!(
            restored.len(),
            1,
            "the restored collection must contain the one vector"
        );
    }

    /// A tenant snapshot whose KV section holds `rows` in collection `kvt`,
    /// with the task a Raft install dispatches it under.
    fn kv_snapshot(
        rows: Vec<crate::engine::kv::hash_table::KvSnapshotRow>,
    ) -> (Vec<u8>, crate::data::executor::task::ExecutionTask) {
        let snap = crate::types::TenantDataSnapshot {
            kv_tables: vec![(
                "0:0:kvt".to_string(),
                zerompk::to_msgpack_vec(&rows).expect("encode entries"),
            )],
            ..Default::default()
        };
        let bytes = zerompk::to_msgpack_vec(&snap).expect("encode snapshot");
        let task = CoreLoop::replay_vector_task(
            crate::types::TenantId::new(0),
            nodedb_types::DatabaseId::DEFAULT,
            nodedb_types::CollectionKey::from_bare(nodedb_types::DatabaseId::DEFAULT, "kvt")
                .vshard(),
            PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
            None,
        );
        (bytes, task)
    }

    /// A KV row installed by a Raft snapshot keeps the surrogate the snapshot
    /// carries, so a cross-engine lookup by surrogate finds it.
    #[test]
    fn a_snapshot_installed_kv_row_is_found_under_its_surrogate() {
        let dir = TempDir::new().expect("tempdir");
        let mut core = open_core(dir.path());
        let (bytes, task) = kv_snapshot(vec![(b"alice".to_vec(), b"v".to_vec(), 0, 4242)]);

        let response = core.execute_restore_tenant_snapshot(&task, 0, &bytes, true, &[], &[]);
        assert_eq!(response.status, Status::Ok, "restore must succeed");
        assert_eq!(
            core.kv_engine
                .key_for_surrogate(0, 0, "kvt", nodedb_types::Surrogate::new(4242)),
            Some(b"alice".to_vec())
        );
    }

    /// A snapshot KV row that carries no surrogate fails the install instead
    /// of installing a row no identity reaches.
    #[test]
    fn a_snapshot_kv_row_without_a_surrogate_fails_the_install() {
        let dir = TempDir::new().expect("tempdir");
        let mut core = open_core(dir.path());
        let (bytes, task) = kv_snapshot(vec![(b"alice".to_vec(), b"v".to_vec(), 0, 0)]);

        let response = core.execute_restore_tenant_snapshot(&task, 0, &bytes, true, &[], &[]);
        assert_eq!(response.status, Status::Error);
        assert_eq!(core.kv_engine.get(0, 0, "kvt", b"alice", 0), None);
    }

    /// KV has no store behind it and the install writes no WAL record, so a
    /// reopened core with no WAL must find the installed row in the KV
    /// checkpoint the install took before it answered.
    #[test]
    fn raft_install_checkpoints_kv_synchronously() {
        let dir = TempDir::new().expect("tempdir");
        let mut core = open_core(dir.path());
        let (bytes, task) = kv_snapshot(vec![(b"k1".to_vec(), b"v1".to_vec(), 0, 11)]);

        let response = core.execute_restore_tenant_snapshot(&task, 0, &bytes, true, &[], &[]);
        assert_eq!(response.status, Status::Ok, "restore must succeed");

        drop(core);
        let mut reopened = open_core(dir.path());
        reopened.load_kv_checkpoints().expect("load KV checkpoints");
        assert_eq!(
            reopened.kv_engine.get(0, 0, "kvt", b"k1", 0),
            Some(b"v1".to_vec()),
            "the installed KV row must survive a restart with no WAL"
        );
    }

    /// A one-vector multi-vector document installs as a multi-vector
    /// document, so a later `MultiVectorDelete` finds and removes it.
    #[test]
    fn a_one_vector_multi_vector_document_installs_as_multi_vector() {
        let dir = TempDir::new().expect("tempdir");
        let mut core = open_core(dir.path());
        let single = nodedb_types::Surrogate::new(1);
        let document = nodedb_types::Surrogate::new(9);
        let vectors: Vec<(u32, Vec<f32>, Option<nodedb_types::Surrogate>)> = vec![
            (0, vec![1.0, 0.0], Some(single)),
            (1, vec![0.0, 1.0], Some(document)),
        ];
        let snap = crate::types::TenantDataSnapshot {
            vectors: vec![(
                "0:0:emb".to_string(),
                zerompk::to_msgpack_vec(&vectors).expect("encode vectors"),
            )],
            vector_multi_documents: vec![("0:0:emb".to_string(), vec![document])],
            ..Default::default()
        };
        let bytes = zerompk::to_msgpack_vec(&snap).expect("encode snapshot");
        let task = CoreLoop::replay_vector_task(
            crate::types::TenantId::new(0),
            nodedb_types::DatabaseId::DEFAULT,
            nodedb_types::CollectionKey::from_bare(nodedb_types::DatabaseId::DEFAULT, "emb")
                .vshard(),
            PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
            None,
        );
        let response = core.execute_restore_tenant_snapshot(&task, 0, &bytes, true, &[], &[]);
        assert_eq!(response.status, Status::Ok, "restore must succeed");

        let key = CoreLoop::vector_index_key(0, 0, "emb", "");
        let installed = core.vector_collections.get(&key).expect("index installed");
        assert_eq!(installed.multi_vector_documents(), vec![document]);
        assert_eq!(installed.live_count(), 2);

        let deleted = core.execute_multi_vector_delete(&task, 0, "emb", "", document);
        assert_eq!(deleted.status, Status::Ok);
        let remaining = core
            .vector_collections
            .get(&key)
            .map(|coll| coll.live_count());
        assert_eq!(
            remaining,
            Some(1),
            "the delete removes the one-vector document and keeps the single row"
        );
    }

    /// Every stored vector is bound, so a snapshot vector without a surrogate
    /// fails the restore instead of installing an unbound row.
    #[test]
    fn a_snapshot_vector_without_a_surrogate_fails_the_restore() {
        let dir = TempDir::new().expect("tempdir");
        let mut core = open_core(dir.path());
        let vectors: Vec<(u32, Vec<f32>, Option<nodedb_types::Surrogate>)> =
            vec![(0, vec![1.0, 0.0], None)];
        let result = core.restore_vector_collection(
            0,
            0,
            "emb",
            vectors,
            &std::collections::HashSet::new(),
            true,
        );
        assert!(result.is_err(), "an unbound snapshot vector is refused");
        let key = CoreLoop::vector_index_key(0, 0, "emb", "");
        assert!(
            !core.vector_collections.contains_key(&key),
            "a refused restore installs no collection"
        );
    }
}
