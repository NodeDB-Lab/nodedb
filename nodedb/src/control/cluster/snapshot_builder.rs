// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane-facing [`SnapshotBuilder`] implementation for the Raft snapshot
//! SEND path.
//!
//! `nodedb-cluster` defines the [`nodedb_cluster::SnapshotBuilder`] trait but
//! cannot depend on `nodedb` (circular), so the host crate supplies this
//! implementation. The Raft tick loop calls it on the LEADER before framing the
//! chunked `InstallSnapshot` RPC for a lagging/new follower.
//!
//! The build reuses the existing Data-Plane snapshot builder
//! (`MetaOp::CreateTenantSnapshot`) per tenant, then FILTERS every section down
//! to the collections whose vshard belongs to the target Raft group, and merges
//! the per-tenant slices into one `TenantDataSnapshot` for the wire.
//!
//! The vshard-partitioned engines are filtered and shipped, including graph
//! `edges` (the edge key already embeds the collection, so it is routed through
//! the same vshard filter as every other section). CRDT is one Loro doc per
//! (tenant, collection); each `crdt_state` entry carries its single collection
//! and is shipped to the group that owns that collection's vshard — the same
//! per-collection vshard filter as every other section.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use nodedb_types::id::DatabaseId;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::backup::snapshot_keys::{
    extract_db_scoped_collection, extract_db_tenant_scoped_collection,
};
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::engine::graph::edge_store::parse_versioned_edge_key;
use crate::types::{SurrogateBindEntry, TenantDataSnapshot, TenantId};
use nodedb_physical::physical_plan::MetaOp;

/// Per-tenant snapshot dispatch timeout (mirrors the backup orchestrator).
const TENANT_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// Builds per-group snapshot payloads from the local Data Plane for the Raft
/// snapshot SEND path.
pub struct DataPlaneSnapshotBuilder {
    shared: Arc<SharedState>,
}

impl DataPlaneSnapshotBuilder {
    /// Construct a builder bound to the node's shared state.
    pub fn new(shared: Arc<SharedState>) -> Self {
        Self { shared }
    }

    /// Compute the vshard for a `(DEFAULT db, collection)` pair.
    ///
    /// One helper, used uniformly by every section's filter so the
    /// vshard-of-key logic is never duplicated. Matches the canonical routing
    /// function (`vshard_for_collection`) used by the RESTORE topology splitter.
    fn vshard_of(collection: &str) -> u32 {
        nodedb_cluster::routing::vshard_for_collection(DatabaseId::DEFAULT, collection)
    }

    /// Capture PK→surrogate bindings for every active collection whose vshard
    /// belongs to the target group, for each enumerated tenant.
    ///
    /// Uses the SAME `vshard_of` membership filter every section uses (one
    /// source of truth), so only in-group collections' identities ship — never
    /// more, never less than the data sections carry.
    fn capture_surrogates(
        catalog: &SystemCatalog,
        tenants: &[u64],
        group_vshards: &HashSet<u32>,
        merged: &mut TenantDataSnapshot,
    ) -> Result<(), Error> {
        let collections = catalog.load_all_collections(DatabaseId::DEFAULT)?;
        let tenant_set: HashSet<u64> = tenants.iter().copied().collect();
        for coll in collections
            .iter()
            .filter(|c| c.is_active && tenant_set.contains(&c.tenant_id))
            .filter(|c| group_vshards.contains(&Self::vshard_of(&c.name)))
        {
            let bindings = catalog.scan_surrogates_for_collection(
                DatabaseId::DEFAULT,
                TenantId::new(coll.tenant_id),
                &coll.name,
            )?;
            for (pk, surrogate) in bindings {
                merged.surrogate_pk.push(SurrogateBindEntry {
                    tenant_id: coll.tenant_id,
                    collection: coll.name.clone(),
                    pk,
                    surrogate: surrogate.as_u32(),
                });
            }
        }
        Ok(())
    }

    /// Build the merged, group-filtered snapshot for `tenant_id`.
    async fn build_tenant_filtered(
        &self,
        tenant_id: u64,
        group_vshards: &HashSet<u32>,
        merged: &mut TenantDataSnapshot,
    ) -> Result<(), Error> {
        let plan = PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot { tenant_id });
        let bytes = crate::control::server::shared::ddl::sync_dispatch::dispatch_async(
            &self.shared,
            TenantId::new(tenant_id),
            DatabaseId::DEFAULT,
            "__system",
            plan,
            TENANT_SNAPSHOT_TIMEOUT,
        )
        .await?;

        let snap: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Internal {
                detail: format!("snapshot build: decode tenant {tenant_id} snapshot: {e}"),
            })?;

        // db-tenant-scoped sections: key shape "{db}:{tid}:{collection}[:suffix]"
        let in_group_db_tenant_scoped = |key: &str| {
            extract_db_tenant_scoped_collection(key, tenant_id)
                .map(|c| group_vshards.contains(&Self::vshard_of(c)))
                .unwrap_or(false)
        };
        // db-scoped sections: key shape "{db}:{tid}:{collection}" (coll may contain ':')
        let in_group_db_scoped = |key: &str| {
            extract_db_scoped_collection(key, tenant_id)
                .map(|c| group_vshards.contains(&Self::vshard_of(c)))
                .unwrap_or(false)
        };

        for (k, v) in snap.documents {
            if in_group_db_tenant_scoped(&k) {
                merged.documents.push((k, v));
            }
        }
        for (k, v) in snap.indexes {
            if in_group_db_tenant_scoped(&k) {
                merged.indexes.push((k, v));
            }
        }
        for (k, v) in snap.vectors {
            if in_group_db_tenant_scoped(&k) {
                merged.vectors.push((k, v));
            }
        }
        for (k, v) in snap.timeseries {
            if in_group_db_tenant_scoped(&k) {
                merged.timeseries.push((k, v));
            }
        }
        // kv_tables: the key IS the collection name → route directly.
        for (k, v) in snap.kv_tables {
            if group_vshards.contains(&Self::vshard_of(&k)) {
                merged.kv_tables.push((k, v));
            }
        }
        // flushed_ts_segments / columnar_engines: db-scoped keys.
        for blob in snap.flushed_ts_segments {
            if in_group_db_scoped(&blob.collection_key) {
                merged.flushed_ts_segments.push(blob);
            }
        }
        for (k, v) in snap.columnar_engines {
            if in_group_db_scoped(&k) {
                merged.columnar_engines.push((k, v));
            }
        }
        for (k, v) in snap.vector_params {
            if in_group_db_tenant_scoped(&k) {
                merged.vector_params.push((k, v));
            }
        }
        for (k, v) in snap.index_configs {
            if in_group_db_tenant_scoped(&k) {
                merged.index_configs.push((k, v));
            }
        }

        // Graph edges: the versioned edge key embeds the collection as its
        // FIRST `\x00`-delimited component, and edge writes are homed at
        // `vshard_for_collection(DEFAULT, collection)` — the SAME routing
        // function `Self::vshard_of` uses. So edges route through the identical
        // vshard filter every other section uses. The restore path parses the
        // key and rebuilds CSR, so no key transformation is needed here.
        //
        // Unlike every other section, the edge key does NOT carry the tenant,
        // so the merged multi-tenant snapshot (applied ONCE with no per-tenant
        // dispatch) carries edges tenant-aware via `tenant_edges` — pushing to
        // the no-tenant `edges` field here would install them under the wrong
        // tenant on apply.
        for (key, value) in snap.edges {
            match parse_versioned_edge_key(&key) {
                Some((collection, ..)) => {
                    if group_vshards.contains(&Self::vshard_of(collection)) {
                        merged.tenant_edges.push((tenant_id, key, value));
                    }
                }
                None => {
                    // All edge keys are the versioned format; an unparseable
                    // key has no determinable group, and restore would reject
                    // it via `put_edge_raw`. Do NOT silently drop it — surface
                    // it. Log only a short prefix, never the full key.
                    let key_prefix: String = key.chars().take(32).collect();
                    tracing::warn!(key_prefix, "snapshot build: unparseable edge key, skipping");
                }
            }
        }

        // CRDT: one Loro doc per (tenant, collection). Each entry carries its
        // single collection; include it iff that collection's vshard belongs to
        // this group — the same per-collection vshard filter every other engine
        // uses.
        for (database_id, tid, collection, bytes) in snap.crdt_state {
            if group_vshards.contains(&Self::vshard_of(&collection)) {
                merged
                    .crdt_state
                    .push((database_id, tid, collection, bytes));
            }
        }

        // CRDT constraints: same per-collection vshard filter as `crdt_state`
        // — each entry is routed by its single collection's vshard.
        for (database_id, tid, collection, version, encoded) in snap.crdt_constraints {
            if group_vshards.contains(&Self::vshard_of(&collection)) {
                merged
                    .crdt_constraints
                    .push((database_id, tid, collection, version, encoded));
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::SnapshotBuilder for DataPlaneSnapshotBuilder {
    async fn build_group_snapshot(
        &self,
        group_id: u64,
        _last_included_index: u64,
        _last_included_term: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        // Resolve the group's vshards. Single-node (no routing) or an
        // empty/ownerless group → nothing to ship; the sender falls back to the
        // stub chunk.
        let group_vshards: HashSet<u32> = match self.shared.cluster_routing.as_ref() {
            Some(routing) => {
                let table = routing.read().map_err(|_| {
                    Box::new(Error::Internal {
                        detail: "snapshot build: cluster_routing RwLock poisoned".into(),
                    }) as Box<dyn std::error::Error + Send + Sync>
                })?;
                table.vshards_for_group(group_id).into_iter().collect()
            }
            None => return Ok(Vec::new()),
        };
        if group_vshards.is_empty() {
            return Ok(Vec::new());
        }

        // Enumerate tenants from the system catalog — the same source the backup
        // orchestrator's catalog sections use. Every active collection carries
        // its `tenant_id`; the distinct set is the tenants to snapshot. When no
        // catalog is configured there is nothing durable to enumerate, so ship
        // an empty (well-formed) snapshot.
        let tenants: Vec<u64> = {
            let catalog = self.shared.credentials.catalog();

            let collections = catalog
                .load_all_collections(DatabaseId::DEFAULT)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let mut set: HashSet<u64> = HashSet::new();
            for coll in collections.iter().filter(|c| c.is_active) {
                set.insert(coll.tenant_id);
            }
            let mut v: Vec<u64> = set.into_iter().collect();
            v.sort_unstable();
            v
        };

        let mut merged = TenantDataSnapshot::default();
        for tenant_id in &tenants {
            self.build_tenant_filtered(*tenant_id, &group_vshards, &mut merged)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        }

        // Capture the PK→surrogate identity map for every in-group collection.
        // The surrogate map is DATA-derived and travels with the data-group
        // snapshot (not the metadata group): without it a snapshot-installed
        // follower has documents but cannot resolve PK point-lookups. The
        // catalog is Control-Plane state (the Data-Plane snapshot handler can't
        // see it), so it is captured here and rebound on the apply side.
        {
            let catalog = self.shared.credentials.catalog();
            Self::capture_surrogates(catalog, &tenants, &group_vshards, &mut merged)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        }

        // Always return a well-formed serialized struct (even when empty) so the
        // follower-apply unit receives a decodable payload rather than a stub.
        let out = zerompk::to_msgpack_vec(&merged).map_err(|e| {
            Box::new(Error::Internal {
                detail: format!("snapshot build: encode merged group {group_id} snapshot: {e}"),
            }) as Box<dyn std::error::Error + Send + Sync>
        })?;
        Ok(out)
    }
}
