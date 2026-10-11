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
//! (`MetaOp::CreateTenantSnapshot`) per tenant and per database the tenant has
//! collections or arrays in, then FILTERS every section down to the records with a home
//! vShard in the target Raft group, and merges the slices into one
//! `TenantDataSnapshot` for the wire. Every section entry names its database,
//! so the follower installs each row in the database it came from.
//!
//! A row is homed on its collection's vShard. A graph edge is homed on both
//! endpoint vShards, `from_key(src)` and `from_key(dst)`, so a group ships
//! every edge with an endpoint home among its vShards and no other edge. CRDT
//! is one Loro doc per (tenant, collection); each `crdt_state` entry carries
//! its single collection and is shipped to the group that owns that
//! collection's vshard. An array cell is homed on the vShard its Hilbert
//! prefix routes to.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use nodedb_types::id::DatabaseId;

use crate::Error;
use crate::control::backup::snapshot_keys::{
    StoredRecord, extract_db_scoped_collection, extract_db_tenant_scoped_collection,
    homes_of_stored,
};
use crate::control::cluster::calvin_snapshot::{await_calvin_cut, capture_calvin_cut};
use crate::control::cluster::snapshot_builder_filter::{bind_in_group, push_group_edges};
use crate::control::cluster::snapshot_cut::settled_cut;
use crate::control::security::catalog::SystemCatalog;
use crate::control::state::SharedState;
use crate::event::trigger::lane::snapshot as lane_snapshot;
use crate::types::{SurrogateBindEntry, TenantDataSnapshot, TenantId};

/// Per-tenant snapshot dispatch timeout (mirrors the backup orchestrator).
const TENANT_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// How long the build waits for the group's apply gate. A Calvin install
/// holds it shared until the install finishes on its core.
const FENCE_WAIT: Duration = Duration::from_secs(5);

/// Builds per-group snapshot payloads from the local Data Plane for the Raft
/// snapshot SEND path.
pub struct DataPlaneSnapshotBuilder {
    shared: Arc<SharedState>,
    /// The Calvin sequencer group's snapshot, or `None` on a builder for
    /// data groups only.
    sequencer: Option<Arc<crate::control::cluster::sequencer_snapshot::SequencerSnapshotStore>>,
}

impl DataPlaneSnapshotBuilder {
    /// Construct a builder bound to the node's shared state.
    pub fn new(shared: Arc<SharedState>) -> Self {
        Self {
            shared,
            sequencer: None,
        }
    }

    /// Capture the sequencer group's snapshot from `store`.
    #[must_use]
    pub fn with_sequencer(
        mut self,
        store: Arc<crate::control::cluster::sequencer_snapshot::SequencerSnapshotStore>,
    ) -> Self {
        self.sequencer = Some(store);
        self
    }

    /// Every tenant with an active collection or an array, and the databases
    /// it has them in.
    fn tenant_databases(catalog: &SystemCatalog) -> Result<BTreeMap<u64, BTreeSet<u64>>, Error> {
        let mut tenants: BTreeMap<u64, BTreeSet<u64>> = BTreeMap::new();
        for coll in catalog
            .load_all_collections_across_databases()?
            .iter()
            .filter(|c| c.is_active)
        {
            tenants
                .entry(coll.tenant_id)
                .or_default()
                .insert(coll.database_id.as_u64());
        }
        for array in catalog.load_all_arrays()? {
            tenants
                .entry(array.array_id.tenant_id.as_u64())
                .or_default()
                .insert(array.array_id.database_id.as_u64());
        }
        Ok(tenants)
    }

    /// Capture the PK→surrogate binds with a home in the target group, for
    /// every active collection of each enumerated tenant, in every database.
    ///
    /// A bind homes on its collection's vShard and, in an edge-bearing
    /// collection, on the key vShard of its key, where a live edge write binds
    /// an endpoint. A group whose vShards hold an endpoint therefore ships
    /// that endpoint's bind even when the collection homes elsewhere.
    fn capture_surrogates(
        catalog: &SystemCatalog,
        tenants: &BTreeMap<u64, BTreeSet<u64>>,
        group_vshards: &HashSet<u32>,
        merged: &mut TenantDataSnapshot,
    ) -> Result<(), Error> {
        let collections = catalog.load_all_collections_across_databases()?;
        for coll in collections
            .iter()
            .filter(|c| c.is_active && tenants.contains_key(&c.tenant_id))
        {
            let key = nodedb_types::CollectionKey::from_bare(coll.database_id, &coll.name);
            let holds_edges = coll.has_implicit_edges;
            // Without edges every bind homes on the collection's vShard.
            if !holds_edges && !group_vshards.contains(&key.vshard().as_u32()) {
                continue;
            }
            let bindings =
                catalog.scan_surrogates_for_collection(key, TenantId::new(coll.tenant_id))?;
            for (pk, surrogate) in bindings {
                if !bind_in_group(key, &pk, holds_edges, group_vshards) {
                    continue;
                }
                merged.surrogate_pk.push(SurrogateBindEntry {
                    database_id: coll.database_id.as_u64(),
                    tenant_id: coll.tenant_id,
                    collection: coll.name.clone(),
                    pk,
                    surrogate: surrogate.as_u32(),
                });
            }
        }
        Ok(())
    }

    /// Build the group-filtered snapshot of `tenant_id` in `database_id` and
    /// merge it into `merged`.
    async fn build_tenant_filtered(
        &self,
        tenant_id: u64,
        database_id: DatabaseId,
        group_vshards: &HashSet<u32>,
        merged: &mut TenantDataSnapshot,
    ) -> Result<(), Error> {
        let bytes = crate::control::server::exchange::snapshot_tenant_on_local_cores(
            &self.shared,
            TenantId::new(tenant_id),
            database_id,
            TENANT_SNAPSHOT_TIMEOUT,
            true,
        )
        .await?;

        let snap: TenantDataSnapshot =
            zerompk::from_msgpack(&bytes).map_err(|e| Error::Internal {
                detail: format!(
                    "snapshot build: decode tenant {tenant_id} snapshot of database {}: {e}",
                    database_id.as_u64()
                ),
            })?;
        // Every section names its collection as the Data Plane stores it in
        // `database_id`.
        let in_group = |collection: &str| {
            homes_of_stored(database_id, StoredRecord::Row { collection }).intersects(group_vshards)
        };

        // db-tenant-scoped sections: key shape "{db}:{tid}:{collection}[:suffix]"
        let in_group_db_tenant_scoped =
            |key: &str| extract_db_tenant_scoped_collection(key, tenant_id).is_some_and(in_group);
        // db-scoped sections: key shape "{db}:{tid}:{collection}" (coll can contain ':')
        let in_group_db_scoped =
            |key: &str| extract_db_scoped_collection(key, tenant_id).is_some_and(in_group);

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
        for (k, v) in snap.documents_versioned {
            if in_group_db_tenant_scoped(&k) {
                merged.documents_versioned.push((k, v));
            }
        }
        for (k, v) in snap.indexes_versioned {
            if in_group_db_tenant_scoped(&k) {
                merged.indexes_versioned.push((k, v));
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
        // kv_tables / flushed_ts_segments / columnar_engines: db-scoped keys.
        for (k, v) in snap.kv_tables {
            if in_group_db_scoped(&k) {
                merged.kv_tables.push((k, v));
            }
        }
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

        push_group_edges(
            snap.edges,
            database_id,
            tenant_id,
            group_vshards,
            &mut merged.tenant_edges,
        )?;
        // A version a TRUNCATE hides is still history, so the follower gets
        // it with the cuts and applied ordinals and resolves every read as
        // the leader does. A cut covers every vShard of its collection.
        push_group_edges(
            snap.edge_hidden,
            database_id,
            tenant_id,
            group_vshards,
            &mut merged.tenant_edges,
        )?;
        push_group_edges(
            snap.edge_applied,
            database_id,
            tenant_id,
            group_vshards,
            &mut merged.tenant_edge_applied,
        )?;
        merged.tenant_edge_cuts.extend(
            snap.edge_cuts
                .into_iter()
                .map(|(collection, cut)| (database_id.as_u64(), tenant_id, collection, cut)),
        );

        // CRDT: one Loro doc per (tenant, collection). Each entry carries its
        // single collection; include it iff that collection's vshard belongs to
        // this group — the same per-collection vshard filter every other engine
        // uses.
        for (crdt_db, tid, collection, bytes) in snap.crdt_state {
            if in_group(collection.as_str()) {
                merged.crdt_state.push((crdt_db, tid, collection, bytes));
            }
        }

        // CRDT constraints: same per-collection vshard filter as `crdt_state`
        // — each entry is routed by its single collection's vshard.
        for entry in snap.crdt_constraints {
            if in_group(entry.collection.as_str()) {
                merged.crdt_constraints.push(entry);
            }
        }

        // Array cells: each blob holds the cells that route to one vShard.
        merged.arrays.extend(
            snap.arrays
                .into_iter()
                .filter(|blob| group_vshards.contains(&blob.vshard)),
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::SnapshotBuilder for DataPlaneSnapshotBuilder {
    async fn build_group_snapshot(
        &self,
        group_id: u64,
        last_included_index: u64,
        _last_included_term: u64,
    ) -> Result<nodedb_cluster::BuiltGroupSnapshot, Box<dyn std::error::Error + Send + Sync>> {
        // No routing table or an empty group ships nothing: the sender falls
        // back to the stub chunk, at the index asked for.
        let nothing = nodedb_cluster::BuiltGroupSnapshot {
            bytes: Vec::new(),
            cut_index: last_included_index,
        };
        let group_vshards: HashSet<u32> = match self.shared.cluster_routing.as_ref() {
            Some(routing) => {
                let table = routing.read().map_err(|_| {
                    Box::new(Error::Internal {
                        detail: "snapshot build: cluster_routing RwLock poisoned".into(),
                    }) as Box<dyn std::error::Error + Send + Sync>
                })?;
                table.vshards_for_group(group_id).into_iter().collect()
            }
            None => return Ok(nothing),
        };
        if group_vshards.is_empty() {
            return Ok(nothing);
        }

        // Every Calvin input of the group's vShards sequenced up to the
        // marker this places finished here before the capture starts.
        let calvin_through = await_calvin_cut(&self.shared, group_id, &group_vshards)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Fence the group's apply for the whole capture, so every section,
        // the write marks and the proposal keys hold the same entries: those
        // at or below the cut. No entry of the group starts until the fence
        // drops, and no Calvin install of its vShards runs: a scheduler
        // installs under a shared hold of the same gate. Entries of other
        // groups apply as usual.
        let _fence = match self.shared.raft_apply_gates.get() {
            Some(gates) => Some(
                tokio::time::timeout(FENCE_WAIT, gates.install(group_id))
                    .await
                    .map_err(|_| {
                        Box::new(Error::Internal {
                            detail: format!(
                                "snapshot build: group {group_id}: an install in flight held \
                                 the group's apply gate past {FENCE_WAIT:?}; the build \
                                 retries on the next heartbeat"
                            ),
                        }) as Box<dyn std::error::Error + Send + Sync>
                    })?,
            ),
            None => None,
        };
        let cut_index = settled_cut(&self.shared, group_id, last_included_index)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        // Read under the fence: the storage captured below holds exactly the
        // Calvin positions this names.
        let calvin_cut = capture_calvin_cut(&self.shared, &group_vshards, calvin_through)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // Enumerate tenants and their databases from the system catalog — the
        // same source the backup orchestrator's catalog sections use. Every
        // active collection carries its `tenant_id` and `database_id`; each
        // distinct pair is one snapshot to take. With no collection there is
        // nothing durable to enumerate, so ship an empty (well-formed) snapshot.
        let tenants = Self::tenant_databases(self.shared.credentials.catalog())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut merged = TenantDataSnapshot::default();
        for (tenant_id, databases) in &tenants {
            for database_id in databases {
                self.build_tenant_filtered(
                    *tenant_id,
                    DatabaseId::new(*database_id),
                    &group_vshards,
                    &mut merged,
                )
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            }
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

        // Read after the capture. A row reached this node's storage only
        // after its collection's incarnation applied here, so every row the
        // snapshot holds belongs to an incarnation at or below this index.
        merged.metadata_floor = self
            .shared
            .applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID)
            .floor();

        // The group's write marks travel with its data: the follower never
        // applies the entries the snapshot covers. So does its lane state.
        merged.group_write_marks = self.shared.tenant_marks.group_entries(group_id);

        // The keys of the entries the snapshot covers travel with it too: the
        // follower learns from them which proposals committed, for its
        // waiters and its duplicate check. They stop at the cut: an entry
        // above it committed but is not in the captured state, so the
        // follower must apply it.
        // With no tracker no covered index is known.
        match self.shared.propose_tracker.get() {
            Some(tracker) => {
                let carried = tracker.committed_keys_through(group_id, cut_index);
                merged.group_proposal_keys = carried.keys;
                merged.group_proposal_keys_complete_from = carried.complete_from;
            }
            None => {
                merged.group_proposal_keys_complete_from = cut_index.saturating_add(1);
            }
        }
        merged.group_cut_index = cut_index;
        merged.group_calvin = Some(calvin_cut);
        // Read under the fence, so the streams hold the chunks of the entries
        // at or below the cut.
        merged.group_redo_streams = self.shared.redo_chunks.capture_group(group_id);
        merged.group_event_lane = lane_snapshot::capture(&self.shared, &group_vshards)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        // A well-formed struct, even when empty, so the follower can decode it.
        let out = zerompk::to_msgpack_vec(&merged).map_err(|e| {
            Box::new(Error::Internal {
                detail: format!("snapshot build: encode merged group {group_id} snapshot: {e}"),
            }) as Box<dyn std::error::Error + Send + Sync>
        })?;
        // Labelled with its cut: the follower resumes the log right after it.
        Ok(nodedb_cluster::BuiltGroupSnapshot {
            bytes: out,
            cut_index,
        })
    }

    fn capture_metadata(
        &self,
        applied_index: u64,
        applied_term: u64,
    ) -> Result<
        Box<dyn nodedb_cluster::MetadataSnapshotCapture>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let capture = crate::control::cluster::metadata_image::MetadataImageCapture::from_live(
            &self.shared,
            applied_index,
            applied_term,
        )?;
        Ok(Box::new(capture))
    }

    fn capture_sequencer(
        &self,
        applied_index: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let store = self.sequencer.as_ref().ok_or_else(|| {
            Box::new(Error::Internal {
                detail: "this snapshot builder captures no sequencer snapshot".into(),
            }) as Box<dyn std::error::Error + Send + Sync>
        })?;
        store
            .capture(applied_index)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}
