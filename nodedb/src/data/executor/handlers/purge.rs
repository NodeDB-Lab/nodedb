// SPDX-License-Identifier: BUSL-1.1

//! Tenant data purge handler.
//!
//! Removes a tenant's data on one data group's vShards from every engine and
//! cache on this Data Plane core. Called via `MetaOp::PurgeTenant` when a
//! replica applies the group's `ReplicatedWrite::TenantPurge` entry.
//!
//! Every replica of the group applies the entry at one log position, so the
//! purge names exactly the group's vShards. A node can host other groups of the
//! tenant, and those purge at their own log positions.
//!
//! A collection keeps its definition: the catalog row outlives its rows.
//! Idempotent: safe to re-run after a crash (missing data is a no-op).

use std::collections::{BTreeSet, HashSet};

use tracing::{info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId};

/// What one core's purge removed.
#[derive(Default)]
struct PurgeCounts {
    docs: usize,
    idxs: usize,
    inv: usize,
    vec: usize,
    ts: u64,
    kv: usize,
    crdt: usize,
    spatial: usize,
}

impl CoreLoop {
    /// Purge a tenant's data on `vshards` from every engine and cache.
    ///
    /// `collections` names every collection of the tenant by storage name.
    /// Only those `vshards` homes lose their rows, and every array of the
    /// tenant loses the cells `vshards` routes. Memory-only engines
    /// checkpoint before this answers, so a restart loads no purged row from an
    /// older checkpoint. The Control Plane fences WAL replay after every core
    /// answers.
    pub(in crate::data::executor) fn execute_purge_tenant(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collections: &[String],
        vshards: &[u32],
    ) -> Response {
        info!(core = self.core_id, tenant_id, "starting tenant purge");
        let group: HashSet<u32> = vshards.iter().copied().collect();
        let database_id = task.request.database_id;

        let counts =
            match self.purge_tenant_collections(database_id, tenant_id, collections, &group) {
                Ok(counts) => counts,
                Err(e) => {
                    warn!(tenant_id, error = %e, "tenant collection purge failed");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("tenant collection purge: {e}"),
                        },
                    );
                }
            };
        let edges = match self.purge_tenant_homed_graph(database_id, tenant_id, &group) {
            Ok(edges) => edges,
            Err(e) => {
                warn!(tenant_id, error = %e, "tenant graph purge failed");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("tenant graph purge: {e}"),
                    },
                );
            }
        };

        let tid_key = TenantId::new(tenant_id);
        if let Err(e) = self.clear_tenant_group_arrays(database_id, tid_key, &group) {
            warn!(tenant_id, error = %e, "tenant array purge failed");
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("tenant array purge: {e}"),
                },
            );
        }

        // A tenant re-created under this id starts with no quarantine records.
        // A record of another group's segment forgets too: its next read
        // detects the segment again, and that group's purge removes it.
        if let Some(registry) = &self.quarantine_registry {
            registry.release_tenant(database_id.as_u64(), tenant_id);
        }

        // Caches hold no authority: a miss reads the store.
        self.doc_cache.evict_tenant(database_id.as_u64(), tenant_id);
        self.aggregate_cache.retain(|(_, t, _), _| *t != tid_key);

        // Stored CRDT dead-letter entries: a tenant recreated under the same
        // id must not restore them.
        if let Err(e) = self.sparse.delete_crdt_dead_letters_for_tenant(tenant_id) {
            warn!(tenant_id, error = %e, "sparse crdt dead-letter purge failed");
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("sparse crdt dead-letter purge: {e}"),
                },
            );
        }

        // The purged collections' row-version history releases its charge.
        self.recharge_all_columnar_history();

        if let Err(e) = self.persist_snapshot_install() {
            warn!(tenant_id, error = %e, "tenant purge checkpoint failed");
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("tenant purge checkpoint: {e}"),
                },
            );
        }

        info!(
            core = self.core_id,
            tenant_id,
            docs = counts.docs,
            idxs = counts.idxs,
            edges,
            inv = counts.inv,
            vec_removed = counts.vec,
            ts_removed = counts.ts,
            kv_removed = counts.kv,
            crdt_removed = counts.crdt,
            spatial_removed = counts.spatial,
            "tenant purge complete"
        );

        let summary = serde_json::json!({
            "tenant_id": tenant_id,
            "documents_removed": counts.docs,
            "indexes_removed": counts.idxs,
            "edges_removed": edges,
            "inverted_entries_removed": counts.inv,
            "vector_collections_removed": counts.vec,
            "timeseries_collections_removed": counts.ts,
            "kv_tables_removed": counts.kv,
            "crdt_engines_removed": counts.crdt,
            "spatial_indexes_removed": counts.spatial,
        });

        match crate::data::executor::response_codec::encode_json_as_msgpack(&summary) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(_) => self.response_ok(task),
        }
    }

    /// Answer every collection this core stores rows of for the tenant, by
    /// storage name. A store can hold a collection the catalog no longer names:
    /// a purge that lists only the catalog leaves those rows behind.
    pub(in crate::data::executor) fn execute_list_tenant_collections(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
    ) -> Response {
        match self
            .stored_tenant_collections(task.request.database_id, TenantId::new(tenant_id))
            .and_then(|names| {
                zerompk::to_msgpack_vec(&names).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("tenant collections: {e}"),
                })
            }) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Every collection name any engine of this core holds for the tenant.
    fn stored_tenant_collections(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
    ) -> crate::Result<Vec<String>> {
        let db = database_id.as_u64();
        let mine = |d: &DatabaseId, t: &TenantId| *d == database_id && *t == tid;
        let mut names: BTreeSet<String> = self
            .sparse
            .versioned_newest_by_collection()?
            .into_iter()
            .filter(|row| row.database_id == db && row.tenant == tid.as_u64())
            .map(|row| row.collection)
            .collect();
        names.extend(self.inverted.tenant_collections(db, tid)?);
        let keyed = self
            .columnar_engines
            .keys()
            .chain(self.columnar_memtables.keys())
            .map(|(d, t, c)| (d, t, c));
        let keyed = keyed
            .chain(self.ts_registries.keys().map(|(d, t, c)| (d, t, c)))
            .chain(self.vector_collections.keys().map(|(d, t, c)| (d, t, c)))
            .chain(
                self.sparse_vector_indexes
                    .keys()
                    .map(|(d, t, c, _)| (d, t, c)),
            )
            .chain(self.spatial_indexes.keys().map(|(d, t, c, _)| (d, t, c)));
        for (d, t, collection) in keyed {
            if mine(d, t) {
                // A vector index key can carry a `:field` suffix.
                let stored = collection.split(':').next().unwrap_or(collection.as_str());
                names.insert(stored.to_string());
            }
        }
        names.extend(
            self.kv_engine
                .live_collections()
                .filter(|row| row.database_id == database_id && row.tenant_id == tid.as_u64())
                .map(|row| row.collection.to_string()),
        );
        if let Some(engine) = self.crdt_engines.get(&(database_id, tid)) {
            names.extend(engine.collection_names());
        }
        Ok(names.into_iter().collect())
    }

    /// Clear every collection of `collections` that `group` homes. Each keeps
    /// its definition. A chain head goes with its rows, so the next write of a
    /// hash-chained collection starts at genesis.
    fn purge_tenant_collections(
        &mut self,
        database_id: DatabaseId,
        tenant_id: u64,
        collections: &[String],
        group: &HashSet<u32>,
    ) -> crate::Result<PurgeCounts> {
        let tid = TenantId::new(tenant_id);
        let mut counts = PurgeCounts::default();
        for collection in collections {
            let home =
                nodedb_types::CollectionKey::from_qualified_str(database_id, collection)?.vshard();
            if !group.contains(&home.as_u32()) {
                continue;
            }
            // Only the home core unlinks the shared on-disk files. An edge
            // lives on its endpoints' vShards, so edges purge by vShard below.
            let reclaim_l1_files = self.owns_collection(database_id, collection)?;
            let stats = self.clear_collection_all_engines(
                database_id,
                tid,
                collection,
                true,
                reclaim_l1_files,
                false,
            )?;
            counts.docs += stats.docs_removed;
            counts.idxs += stats.idxs_removed;
            counts.inv += stats.inv_removed;
            counts.vec += stats.vec_removed;
            counts.ts += stats.ts_removed;
            counts.kv += stats.kv_removed;
            counts.crdt += stats.crdt_rows_removed;
            counts.spatial += stats.spatial_removed;
            self.chain
                .heads
                .remove(&(database_id, tid, collection.clone()));
            self.sparse
                .delete_chain_head(database_id.as_u64(), tenant_id, collection)?;
        }
        Ok(counts)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{CollectionKey, StorageKey, Surrogate};

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, task_with_vshard};
    use crate::engine::sparse::btree_versioned::VersionedPut;
    use crate::types::VShardId;

    const TENANT: u64 = 1;
    /// A collection with rows and no catalog row.
    const ORPHAN: &str = "orphan";

    fn put(core: &CoreLoop, tenant: u64, coll: &str) {
        core.sparse
            .versioned_put(VersionedPut {
                database_id: 0,
                tenant,
                coll,
                doc_id: &StorageKey::for_surrogate(Surrogate::new(7)),
                sys_from: 10,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                body: b"row",
            })
            .expect("put");
    }

    fn listed(core: &mut CoreLoop, tenant: u64) -> Vec<String> {
        let task = task_with_vshard(VShardId::new(0));
        let response = core.execute_list_tenant_collections(&task, tenant);
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        zerompk::from_msgpack(response.payload.as_ref()).expect("names")
    }

    /// The listing finds what the catalog no longer names. The purge the
    /// listing feeds removes it, and another tenant's row of that name stays.
    #[test]
    fn a_row_with_no_catalog_row_is_listed_and_purged() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        put(&core, TENANT, ORPHAN);
        put(&core, TENANT + 1, ORPHAN);
        assert_eq!(listed(&mut core, TENANT), vec![ORPHAN.to_string()]);

        let home = CollectionKey::from_bare(DatabaseId::DEFAULT, ORPHAN)
            .vshard()
            .as_u32();
        let task = task_with_vshard(VShardId::new(home));
        let answer = core.execute_purge_tenant(&task, TENANT, &[ORPHAN.to_string()], &[home]);
        assert_eq!(answer.status, Status::Ok, "{:?}", answer.error_code);

        assert!(listed(&mut core, TENANT).is_empty());
        assert_eq!(listed(&mut core, TENANT + 1), vec![ORPHAN.to_string()]);
    }

    /// A purge of another group's vShards leaves the row for that group's purge.
    #[test]
    fn a_purge_leaves_a_collection_another_group_homes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        put(&core, TENANT, ORPHAN);
        let home = CollectionKey::from_bare(DatabaseId::DEFAULT, ORPHAN)
            .vshard()
            .as_u32();
        let other = home.wrapping_add(1);
        let task = task_with_vshard(VShardId::new(other));
        let answer = core.execute_purge_tenant(&task, TENANT, &[ORPHAN.to_string()], &[other]);
        assert_eq!(answer.status, Status::Ok, "{:?}", answer.error_code);
        assert_eq!(listed(&mut core, TENANT), vec![ORPHAN.to_string()]);
    }
}
