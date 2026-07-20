// SPDX-License-Identifier: BUSL-1.1

//! Single dispatch entry point for a full-tenant snapshot restore, orchestrating
//! the per-engine install helpers in `engines.rs` across every engine.

use tracing::{info, warn};

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
        // Carried for symmetry with the applier; the per-collection list below
        // drives the actual clear. The applier populates this from the catalog.
        _clear_vshards: &[u32],
        collections_to_clear: &[(u64, String)],
    ) -> Response {
        info!(core = self.core_id, tenant_id, "restoring tenant snapshot");

        // Clear-then-install: drop stale state for the listed collections before
        // installing, so keys deleted before the snapshot index and dropped
        // collections do not linger on a lagging follower. Empty list = no-op.
        for (tid_raw, coll) in collections_to_clear {
            // Preserve the collection definition: clear-then-install replaces row
            // data from the snapshot, but the snapshot does not carry the schema,
            // so the reinstalled rows must land in the still-defined collection.
            // Fail-closed: if stale state cannot be cleared, abort the restore
            // rather than install the snapshot over rows that survived — those
            // would linger as un-owned data on this follower.
            if let Err(e) = self.clear_collection_all_engines(
                nodedb_types::DatabaseId::DEFAULT,
                crate::types::TenantId::new(*tid_raw),
                coll,
                true,
            ) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("clear-then-install purge failed for '{coll}': {e}"),
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

        let (docs_written, indexes_written) =
            self.restore_sparse(tenant_id, &snap.documents, &snap.indexes);

        let mut edges_written = 0u64;
        let mut vectors_written = 0u64;
        let mut kv_written = 0u64;
        let mut crdt_written = 0u64;
        let mut crdt_constraints_written = 0u64;
        let mut ts_written = 0u64;

        {
            // Restore graph edges. Keys are the versioned form
            // `"{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}"`;
            // tenant is supplied from context.
            let tid = crate::types::TenantId::new(tenant_id);
            let database_id = task.request.database_id.as_u64();
            for (key, props) in &snap.edges {
                if let Err(e) = self.edge_store.put_edge_raw(database_id, tid, key, props) {
                    warn!(key, error = %e, "failed to restore edge");
                    continue;
                }
                edges_written += 1;
            }
            // Restore tenant-aware edges from the multi-tenant merged Raft
            // snapshot. The edge key does NOT carry the tenant, so each entry
            // carries its owning `tid` explicitly — install it under THAT tenant
            // rather than the dispatch-context tenant (which is 0 for the merged
            // group snapshot). Shares `edges_written` with the legacy loop so the
            // CSR rebuild below runs if EITHER source installed edges.
            for (tid_raw, key, props) in &snap.tenant_edges {
                let edge_tid = crate::types::TenantId::new(*tid_raw);
                if let Err(e) = self
                    .edge_store
                    .put_edge_raw(database_id, edge_tid, key, props)
                {
                    warn!(key, error = %e, "failed to restore tenant edge");
                    continue;
                }
                edges_written += 1;
            }
            // Rebuild CSR from restored edges. A rebuild failure is fatal to the
            // whole restore: leaving the stale CSR in place would make graph
            // traversals silently return wrong results over the just-installed
            // edges — the same silent-corruption class the durable-section
            // failures above treat as fatal.
            if edges_written > 0 {
                match crate::engine::graph::csr::rebuild::rebuild_sharded_from_store(
                    &self.edge_store,
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
                            warn!(key, error = %e, "failed to decode vector_params snapshot entry");
                            continue;
                        }
                    };
                let (vp_db, coll_key) = parse_vector_snapshot_key(key, tenant_id);
                let map_key = (
                    nodedb_types::DatabaseId::new(vp_db),
                    crate::types::TenantId::new(tenant_id),
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
                            warn!(key, error = %e, "failed to decode index_configs snapshot entry");
                            continue;
                        }
                    };
                let (ic_db, coll_key) = parse_vector_snapshot_key(key, tenant_id);
                let map_key = (
                    nodedb_types::DatabaseId::new(ic_db),
                    crate::types::TenantId::new(tenant_id),
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
                            warn!(key, error = %e, "failed to decode vector snapshot");
                            continue;
                        }
                    };
                let count = vectors.len() as u64;
                let (database_id, coll_key) = parse_vector_snapshot_key(key, tenant_id);
                self.restore_vector_collection(
                    database_id,
                    tenant_id,
                    coll_key,
                    vectors,
                    replace_mode,
                );
                vectors_written += count;
            }

            // Vectors installed above land only in the in-memory
            // `vector_collections` map with no WAL record — the Raft
            // install-snapshot path (`replace_mode`) never re-issues them as
            // live writes (they are already Raft-committed; re-proposing
            // would be circular), so its only durability contract is Raft's
            // own fsynced `.snap` file plus this local checkpoint. Without a
            // synchronous checkpoint here, a crash before the next periodic
            // `checkpoint_vector_indexes()` run (every 5 minutes) would lose
            // the just-installed vectors. Checkpointing is cheap-idempotent
            // (skips empty collections), so this is unconditional rather than
            // gated on `replace_mode`: the user-RESTORE path drains vector
            // data before it ever reaches here (see
            // `control/backup/restore/orchestrate/mod.rs`), so `vectors_written`
            // is 0 and the call is a no-op on that path.
            if vectors_written > 0 {
                match self.checkpoint_vector_indexes() {
                    Ok(outcome) => {
                        info!(
                            core = self.core_id,
                            tenant_id,
                            files_written = outcome.files_written,
                            "vector snapshot install checkpointed synchronously"
                        );
                    }
                    // The install path has no LSN to clamp — it applies
                    // Raft-committed vectors with no WAL record behind them, so
                    // there is no truncation authority to narrow here. What a
                    // failure does mean is that this core's only local copy of
                    // the just-installed vectors is still in memory, which the
                    // operator needs to see rather than have it disappear into
                    // a discarded count.
                    Err(e) => {
                        warn!(
                            core = self.core_id,
                            tenant_id,
                            error = %e,
                            "vector snapshot install checkpoint failed; the installed \
                             vectors are memory-only on this core until the next \
                             successful checkpoint"
                        );
                    }
                }
            }

            // Restore KV tables.
            for (collection_name, bytes) in &snap.kv_tables {
                let entries: Vec<(Vec<u8>, Vec<u8>, u64)> = match zerompk::from_msgpack(bytes) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(collection_name, error = %e, "failed to decode kv snapshot");
                        continue;
                    }
                };
                let count = entries.len() as u64;
                self.restore_kv_table(tenant_id, collection_name, entries);
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
                    warn!(tid_raw, %collection, error = %e, "failed to restore crdt state");
                } else {
                    crdt_written += 1;
                }
            }

            // Restore CRDT constraint state per collection: reconstructs the
            // validator's installed constraint set + `installed_constraint_version`
            // so a snapshot-installed follower does not come up empty and
            // retry-fence every peer delta on constrained collections. Fail-safe
            // on error — warn and continue, matching the `crdt_state` loop, since
            // a failed reconstruction only reverts to the pre-fix (over-rejecting)
            // behavior rather than corrupting state.
            for entry in &snap.crdt_constraints {
                if let Err(e) = self.restore_crdt_constraints(
                    entry.database_id,
                    entry.tenant_id,
                    &entry.collection,
                    entry.version,
                    &entry.constraints,
                ) {
                    let (tid_raw, collection) = (entry.tenant_id, &entry.collection);
                    warn!(tid_raw, %collection, error = %e, "failed to restore crdt constraints");
                } else {
                    crdt_constraints_written += 1;
                }
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
        match crate::data::executor::response_codec::encode_json(&result) {
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
    use crate::data::executor::vector_checkpoint::vector_ckpt_dir;
    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::MetaOp;

    fn open_core(dir: &std::path::Path) -> CoreLoop {
        let hlc = Arc::new(nodedb_types::OrdinalClock::new());
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx);
        drop(resp_rx);
        CoreLoop::open(0, req_rx, resp_tx, dir, hlc).expect("CoreLoop::open")
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
    /// no WAL record. Its only durability contract is Raft's own fsynced
    /// `.snap` file plus an immediate local checkpoint — this proves the
    /// checkpoint happens SYNCHRONOUSLY within the restore call, not on the
    /// next periodic (5-minute) timer tick.
    #[test]
    fn raft_install_checkpoints_vectors_synchronously() {
        let dir = TempDir::new().expect("tempdir");
        let mut core = open_core(dir.path());

        let task = CoreLoop::replay_vector_task(
            crate::types::TenantId::new(0),
            nodedb_types::DatabaseId::DEFAULT,
            crate::types::VShardId::from_collection_in_database(
                nodedb_types::DatabaseId::DEFAULT,
                "emb",
            ),
            PhysicalPlan::Meta(MetaOp::WalAppend {
                payload: Vec::new(),
            }),
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

        // The checkpoint file must exist on disk immediately — no periodic
        // timer tick has run.
        let ckpt_dir = vector_ckpt_dir(&core.data_dir, core.core_id);
        let entries: Vec<_> = std::fs::read_dir(&ckpt_dir)
            .expect("checkpoint dir must exist synchronously")
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
}
