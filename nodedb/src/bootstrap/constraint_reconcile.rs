// SPDX-License-Identifier: BUSL-1.1

//! Singleton CRDT constraint reconcile loop.
//!
//! Exactly one node runs it — the metadata-group leader in a cluster, and the
//! sole node in a standalone deployment, which has no group to elect from.
//! That node periodically re-derives each collection's
//! constraint set from the catalog and replicates it to every data-group
//! replica via a `ConstraintChange` entry on the collection's vshard data
//! Raft log. Each replica installs the set into its per-core CRDT validator,
//! fenced by `constraint_version` so a stale set can never clobber a newer
//! one. `constraint_version` bumps only when the derived constraint set
//! actually changes (not on every catalog descriptor bump), so an unrelated
//! ALTER never re-proposes.
//!
//! Why a recurring reconcile rather than a one-shot DDL hook: leadership can
//! move (election, crash). A new metadata leader re-derives and re-delivers
//! the current catalog state, so a collection created or altered under a
//! previous leader still converges on every surviving replica without the
//! original proposer being alive. Delivery is idempotent — the per-collection
//! version fence makes re-proposing the same set a no-op on every replica.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nodedb_types::{DatabaseId, TenantId};
use tracing::{debug, warn};

use crate::control::security::catalog::{
    ReadOnlySystemCatalog, StoredCollection, SystemCatalog, collection_constraints,
};
use crate::control::state::SharedState;
use crate::control::wal_replication::{
    ConstraintChangeOp, ReplicatedEntry, ReplicatedWrite, propose_replicated_entry,
};

/// Maximum number of constraint deliveries proposed in a single reconcile
/// pass. Remaining changed collections are delivered on subsequent ticks so a
/// large catalog churn cannot monopolize the loop or the Raft proposer.
const MAX_RECONCILE_PROPOSALS_PER_PASS: usize = 64;

/// Spawn the singleton constraint reconcile loop.
///
/// Control-Plane task (Tokio): it reads the catalog (Control Plane owns it) and
/// dispatches Control → Data proposes. The catalog read runs in
/// `spawn_blocking` so a synchronous redb scan never stalls the reactor, and no
/// lock is ever held across an `.await`.
pub fn spawn_constraint_reconcile(shared: Arc<SharedState>, interval_ms: u64) {
    // Clone for the task body so the original `shared` remains available to
    // borrow `loop_registry`/`shutdown` for the `spawn_loop` call itself.
    let task_shared = Arc::clone(&shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "constraint_reconcile",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let shared = task_shared;
            // Task-local delivered-version map, persisted across ticks (NOT in
            // SharedState): records the highest `constraint_version` already
            // accepted by Raft for each `(tenant, collection)`. Skipping equal
            // or older versions keeps steady-state ticks proposal-free.
            let mut delivered: HashMap<(TenantId, String), u64> = HashMap::new();
            let mut tick = tokio::time::interval(Duration::from_millis(interval_ms));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                reconcile_once(&shared, &mut delivered).await;
            }
        },
    );
}

/// Run one reconcile pass: if this node is the metadata leader, re-derive every
/// collection's constraint set from the catalog and propose a `ConstraintChange`
/// for each collection whose `constraint_version` exceeds what `delivered`
/// records. `delivered` is updated in place on each accepted proposal. Returns
/// the number of proposals accepted this pass. A no-op (returns 0) when this
/// node is not the one elected to reconcile, the catalog is absent, or the
/// async Raft proposer is not yet installed.
///
/// Exposed (crate-public) so tests can drive constraint installation
/// deterministically instead of waiting on the background timer.
pub async fn reconcile_once(
    shared: &Arc<SharedState>,
    delivered: &mut HashMap<(TenantId, String), u64>,
) -> usize {
    // Only one node reconciles — every replica installing would duplicate
    // proposals onto the data log for no gain. In a cluster that node is the
    // metadata leader; standalone has no group to elect from, so the sole
    // node does it.
    if !shared.is_singleton_worker() {
        return 0;
    }
    let catalog = shared.credentials.catalog().clone();
    // Read every owned database's collections off the reactor.
    let loaded = match tokio::task::spawn_blocking(move || load_collections(&catalog)).await {
        Ok(Ok(rows)) => rows,
        Ok(Err(e)) => {
            warn!(error = %e, "constraint reconcile: catalog read failed");
            return 0;
        }
        Err(e) => {
            warn!(error = %e, "constraint reconcile: catalog read task panicked");
            return 0;
        }
    };
    // No proposer installed yet (still bootstrapping the Raft layer):
    // skip this pass and retry next tick.
    let Some(proposer) = shared.async_raft_proposer() else {
        return 0;
    };
    let proposer = Arc::clone(proposer);

    let mut proposed = 0usize;
    for (database_id, stored) in loaded {
        if proposed >= MAX_RECONCILE_PROPOSALS_PER_PASS {
            break;
        }
        let key = (TenantId::new(stored.tenant_id), stored.name.clone());
        // Already delivered this version (or newer) — fence skip.
        if delivered
            .get(&key)
            .is_some_and(|&v| v >= stored.constraint_version)
        {
            continue;
        }

        let constraints = collection_constraints(&stored);
        let mut blobs = Vec::with_capacity(constraints.len());
        let mut encode_failed = false;
        for constraint in &constraints {
            match zerompk::to_msgpack_vec(constraint) {
                Ok(bytes) => blobs.push(bytes),
                Err(e) => {
                    warn!(
                        collection = %stored.name,
                        error = %e,
                        "constraint reconcile: encode failed; skipping collection"
                    );
                    encode_failed = true;
                    break;
                }
            }
        }
        if encode_failed {
            continue;
        }

        let vshard_id = nodedb_cluster::routing::vshard_for_collection(database_id, &stored.name);
        let entry = ReplicatedEntry::new(
            stored.tenant_id,
            database_id.as_u64(),
            vshard_id,
            ReplicatedWrite::ConstraintChange {
                collection: stored.name.clone(),
                op: ConstraintChangeOp::Set,
                constraint_version: stored.constraint_version,
                constraints: blobs,
            },
        );

        match propose_replicated_entry(shared, &proposer, entry).await {
            Ok(_) => {
                // Record only on commit. A transient / NotLeader error
                // leaves the map untouched so the next tick retries.
                delivered.insert(key, stored.constraint_version);
                proposed += 1;
            }
            Err(e) => {
                debug!(
                    collection = %stored.name,
                    error = %e,
                    "constraint reconcile: propose failed; will retry next tick"
                );
            }
        }
    }
    proposed
}

/// Load every collection across every database the node owns, tagged with its
/// owning [`DatabaseId`]. `StoredCollection` does not carry its database id, so
/// it is paired here from the enumeration that produced it.
/// The two catalog reads [`load_collections`] needs, so it can run against
/// either a read-write [`SystemCatalog`] or a [`ReadOnlySystemCatalog`].
pub trait CollectionSource {
    fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>>;
    fn collections_in(&self, database_id: DatabaseId) -> crate::Result<Vec<StoredCollection>>;
    fn collections_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCollection>>;
}

impl CollectionSource for SystemCatalog {
    fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>> {
        Ok(self.list_databases()?.into_iter().map(|db| db.id).collect())
    }
    fn collections_in(&self, database_id: DatabaseId) -> crate::Result<Vec<StoredCollection>> {
        self.load_all_collections(database_id)
    }
    fn collections_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCollection>> {
        self.load_collections_for_tenant(database_id, tenant_id)
    }
}

impl CollectionSource for ReadOnlySystemCatalog {
    fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>> {
        Ok(self.list_databases()?.into_iter().map(|db| db.id).collect())
    }
    fn collections_in(&self, database_id: DatabaseId) -> crate::Result<Vec<StoredCollection>> {
        self.load_all_collections(database_id)
    }
    fn collections_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCollection>> {
        self.load_collections_for_tenant(database_id, tenant_id)
    }
}

pub(crate) fn load_collections<S: CollectionSource + ?Sized>(
    catalog: &S,
) -> crate::Result<Vec<(DatabaseId, StoredCollection)>> {
    // Always enumerate the default database, then any explicitly-created ones.
    // Collections created without a `CREATE DATABASE` live under
    // `DatabaseId::DEFAULT`, which has no row in the DATABASES table and so never
    // appears in `list_databases()`; every other catalog consumer hardcodes the
    // default id for the same reason. Routing solely through `list_databases()`
    // would silently deliver nothing for those collections.
    let mut db_ids: Vec<DatabaseId> = vec![DatabaseId::DEFAULT];
    for id in catalog.list_database_ids()? {
        if id != DatabaseId::DEFAULT {
            db_ids.push(id);
        }
    }
    let mut out = Vec::new();
    for db_id in db_ids {
        for stored in catalog.collections_in(db_id)? {
            out.push((db_id, stored));
        }
    }
    Ok(out)
}
