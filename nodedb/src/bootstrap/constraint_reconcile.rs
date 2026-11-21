// SPDX-License-Identifier: BUSL-1.1

//! Singleton CRDT constraint reconcile loop.
//!
//! Exactly one node runs it — the metadata-group leader. A one-node cluster
//! leads its own metadata group. That node periodically re-derives each collection's
//! constraint set from the catalog and replicates it to every data-group
//! replica via a `ConstraintChange` entry on the collection's vshard data
//! Raft log. Each replica installs the set into its per-core CRDT validator,
//! fenced by `constraint_version` so a stale set can never clobber a newer
//! one. `constraint_version` bumps only when the derived constraint set
//! actually changes (not on every catalog descriptor bump), so an unrelated
//! ALTER never re-proposes.
//!
//! The DDL that changes a constraint set installs it before it returns
//! (`control::constraint_install`). This loop is the safety net: leadership
//! can move (election, crash) between a DDL's commit and its install. A new
//! metadata leader re-derives and re-delivers the current catalog state, so a
//! collection created or altered under a previous leader still converges on
//! every surviving replica without the original proposer being alive.
//! Delivery is idempotent — the per-collection version fence makes
//! re-proposing the same set a no-op on every replica.
//!
//! The same pass delivers the document index steps each active document
//! collection owes: the top-level build of a schemaless collection, each
//! declared path of a strict one, and each `Ready` explicit index. A DDL
//! proposes them after its commit, and this pass covers a step that
//! proposal lost. A replica that already holds a step's owner with the same
//! spec reads no row for it. A collection is delivered again only when its
//! incarnation or descriptor version moves.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nodedb_types::{DatabaseId, Hlc, TenantId};
use tracing::{debug, warn};

use crate::control::security::catalog::{
    ReadOnlySystemCatalog, StoredCollection, StoredIndexRecord, SystemCatalog,
};
use crate::control::state::SharedState;

/// A collection the reconcile loop delivers to: the same name in two
/// databases is two collections.
pub type DeliveredKey = (DatabaseId, TenantId, String);

/// What one leader's reconcile loop delivered, kept across its passes and
/// never in `SharedState`. A new leader starts empty and delivers
/// everything once.
#[derive(Debug, Default)]
pub struct ReconcileDelivered {
    /// The highest `constraint_version` Raft accepted per collection.
    pub constraints: HashMap<DeliveredKey, u64>,
    /// The `(incarnation, descriptor_version)` whose document index steps
    /// Raft accepted per collection.
    pub doc_indexes: HashMap<DeliveredKey, (Hlc, u64)>,
}

/// The proposals one reconcile pass had Raft accept.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcilePass {
    /// Constraint sets delivered, one per collection.
    pub constraints: usize,
    /// Document index steps delivered.
    pub doc_index_steps: usize,
}

impl ReconcilePass {
    /// Every proposal of the pass, as the per-pass cap counts them.
    pub fn total(&self) -> usize {
        self.constraints + self.doc_index_steps
    }
}

/// Maximum number of proposals, constraint sets and document index steps
/// together, in a single reconcile pass. Remaining collections are delivered
/// on subsequent ticks so a large catalog churn cannot monopolize the loop or
/// the Raft proposer.
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
            // Task-local delivered state, persisted across ticks. Skipping
            // what Raft already accepted keeps steady-state ticks
            // proposal-free.
            let mut delivered = ReconcileDelivered::default();
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
/// records. Then propose the document index steps of each active document
/// collection whose `(incarnation, descriptor_version)` `delivered` does not
/// hold. `delivered` is updated in place on each accepted delivery. Returns
/// the proposals accepted this pass: one per constraint set, and one per
/// document index step. A no-op (returns zero counts) when this node is not
/// the one elected to reconcile or the catalog is absent. A delivery that
/// fails is retried on the next pass.
///
/// Exposed (crate-public) so tests can drive delivery deterministically
/// instead of waiting on the background timer.
pub async fn reconcile_once(
    shared: &Arc<SharedState>,
    delivered: &mut ReconcileDelivered,
) -> ReconcilePass {
    let mut pass = ReconcilePass::default();
    // Only one node reconciles — every replica installing would duplicate
    // proposals onto the data log for no gain. That node is the metadata
    // leader; a one-node cluster leads its own metadata group.
    if !shared.is_singleton_worker() {
        return pass;
    }
    let catalog = shared.credentials.catalog().clone();
    // Read every owned database's collections off the reactor.
    let loaded = match tokio::task::spawn_blocking(move || load_collections(&catalog)).await {
        Ok(Ok(rows)) => rows,
        Ok(Err(e)) => {
            warn!(error = %e, "constraint reconcile: catalog read failed");
            return pass;
        }
        Err(e) => {
            warn!(error = %e, "constraint reconcile: catalog read task panicked");
            return pass;
        }
    };
    for (database_id, stored) in loaded {
        if pass.total() >= MAX_RECONCILE_PROPOSALS_PER_PASS {
            break;
        }
        // The source's delivery reaches a mirror through the log. Delivery
        // starts at PROMOTE, and `delivered` holds nothing for this database.
        if shared.mirror.fence.is_read_only(database_id) {
            continue;
        }
        let key = (
            database_id,
            TenantId::new(stored.tenant_id),
            stored.name.clone(),
        );
        pass.constraints +=
            deliver_constraints_if_due(shared, database_id, &stored, &key, delivered).await;
        if pass.total() >= MAX_RECONCILE_PROPOSALS_PER_PASS {
            break;
        }
        pass.doc_index_steps += deliver_doc_indexes_if_due(shared, &stored, key, delivered).await;
    }
    pass
}

/// Propose `stored`'s constraint set unless `delivered` holds its version
/// or a newer one. Returns how many proposals Raft accepted.
async fn deliver_constraints_if_due(
    shared: &Arc<SharedState>,
    database_id: DatabaseId,
    stored: &StoredCollection,
    key: &DeliveredKey,
    delivered: &mut ReconcileDelivered,
) -> usize {
    // Already delivered this version (or newer) — fence skip.
    if delivered
        .constraints
        .get(key)
        .is_some_and(|&v| v >= stored.constraint_version)
    {
        return 0;
    }
    // The delivery routes to the leader of the collection's data group,
    // which need not be this node.
    match crate::control::constraint_install::deliver_constraints(shared, database_id, stored).await
    {
        Ok(()) => {
            // Record only on commit. A transient / NotLeader error leaves
            // the map untouched so the next tick retries.
            delivered
                .constraints
                .insert(key.clone(), stored.constraint_version);
            1
        }
        Err(e) => {
            debug!(
                collection = %stored.name,
                error = %e,
                "constraint reconcile: propose failed; will retry next tick"
            );
            0
        }
    }
}

/// Propose the document index steps of an active document collection
/// unless `delivered` holds its `(incarnation, descriptor_version)`.
/// Returns how many steps Raft accepted.
async fn deliver_doc_indexes_if_due(
    shared: &Arc<SharedState>,
    stored: &StoredCollection,
    key: DeliveredKey,
    delivered: &mut ReconcileDelivered,
) -> usize {
    if !stored.is_active || !stored.collection_type.is_document() {
        return 0;
    }
    let version = (stored.incarnation, stored.descriptor_version);
    if delivered.doc_indexes.get(&key) == Some(&version) {
        return 0;
    }
    // The steps carry `stored`'s fence. A DDL that commits after this read
    // stamps a higher fence, so a step of this pass that reaches a core
    // after that DDL's steps changes nothing there.
    #[cfg(feature = "failpoints")]
    crate::control::fail_gate::before_doc_index_delivery(shared.node_id).await;
    match crate::control::doc_index_install::deliver_doc_indexes(shared, stored).await {
        Ok(steps) => {
            // Record only once every step applied. An error leaves the map
            // untouched so the next tick retries.
            delivered.doc_indexes.insert(key, version);
            steps
        }
        Err(e) => {
            debug!(
                collection = %stored.name,
                error = %e,
                "document index reconcile: propose failed; will retry next tick"
            );
            0
        }
    }
}

/// Load every collection across every database the node owns, tagged with its
/// owning [`DatabaseId`]. `StoredCollection` does not carry its database id, so
/// it is paired here from the enumeration that produced it.
/// The catalog reads [`load_collections`] and the doc-config builder need, so
/// both run against either a read-write [`SystemCatalog`] or a
/// [`ReadOnlySystemCatalog`].
pub trait CollectionSource {
    fn list_database_ids(&self) -> crate::Result<Vec<DatabaseId>>;
    fn collections_in(&self, database_id: DatabaseId) -> crate::Result<Vec<StoredCollection>>;
    fn collections_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredCollection>>;
    /// Every index record of one (database, tenant), of every kind.
    fn index_records_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredIndexRecord>>;
    /// Every index record of one collection, of every kind. The read
    /// covers the collection's records only.
    fn index_records_for_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<StoredIndexRecord>>;
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
    fn index_records_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        self.list_index_records(database_id.as_u64(), tenant_id)
    }
    fn index_records_for_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        self.list_index_records_for_collection(database_id.as_u64(), tenant_id, collection)
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
    fn index_records_for_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        self.list_committed_index_records(database_id.as_u64(), tenant_id)
    }
    fn index_records_for_collection(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<Vec<StoredIndexRecord>> {
        self.list_committed_index_records_for_collection(
            database_id.as_u64(),
            tenant_id,
            collection,
        )
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
