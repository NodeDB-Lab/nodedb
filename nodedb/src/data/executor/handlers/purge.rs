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

use std::collections::HashSet;

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
    /// Only those `vshards` homes lose their rows. Memory-only engines
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

        // Caches hold no authority: a miss reads the store.
        let tid_key = TenantId::new(tenant_id);
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
