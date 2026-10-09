// SPDX-License-Identifier: BUSL-1.1

//! Host-side apply logic for `DdlPendingPropose` / `DdlPendingFinalize` /
//! `DdlPendingCancel`.
//!
//! Applies the entries `ddl_flush::begin_commit` / `finalize_pending` propose
//! at COMMIT, and the cancel `metadata_proposer::ddl_reclaim` proposes for a
//! reclaimed owner's stranded record. Propose and finalize apply only while
//! their token owns the DDL preparation lease. Finalize and cancel
//! are idempotent: applying either twice, or applying either for a token
//! with no pending record, is a no-op. Raft replay relies on exactly that
//! shape.

use tracing::{debug, error};

use nodedb_cluster::{MetadataEntry, PendingDdlObject};
use nodedb_types::Hlc;

use crate::control::catalog_entry;
use crate::control::security::catalog::StoredPendingReclaim;

use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    /// `DdlPendingPropose`: insert the pending record. Re-delivery of the
    /// same propose overwrites with an identical record, so no ordering
    /// hazard exists.
    ///
    /// Applies only while `token` owns the preparation lease. An owner whose
    /// lease the metadata leader reclaimed reserves nothing: its propose is a
    /// deterministic no-op on every replica, and the proposer sees no record.
    pub(super) fn apply_ddl_pending_propose(
        &self,
        token: u64,
        objects: &[PendingDdlObject],
        proposed_at: Hlc,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        if !crate::control::metadata_proposer::ddl_owner::owns_ddl_lease(&shared, token) {
            debug!(
                token,
                "pending DDL propose: token does not own the lease, no-op"
            );
            return Ok(());
        }
        // `proposed_at` is the only remote HLC observation the metadata group
        // carries — every other `Hlc` on a `MetadataEntry` is a future
        // deadline, and folding one will jump this node's clock forward.
        //
        // The entry is already committed, so a refused fold must not stop the
        // apply: refusing to move the clock IS the protection. Applying still
        // has to happen or the state machine wedges.
        if let Err(skew) = shared.hlc_clock.update_checked(proposed_at) {
            error!(
                token,
                skew_ms = skew.skew_ns / 1_000_000,
                remote_wall_ns = skew.remote_wall_ns,
                local_wall_ns = skew.local_wall_ns,
                "refusing to fold a proposer's HLC: {skew}"
            );
        }
        self.credentials.catalog().put_pending_ddl(
            &crate::control::security::catalog::StoredPendingDdl {
                token,
                objects: objects.to_vec(),
                proposed_at,
            },
        )?;
        shared
            .pending_ddl
            .insert(token, objects.to_vec(), proposed_at);
        Ok(())
    }

    /// `DdlPendingFinalize`: replay every reserved object's host-side
    /// effects, then drop the pending record. The record is peeked rather
    /// than removed up front, so a mid-replay failure leaves it in place
    /// for the next re-delivery instead of silently skipping the rest.
    ///
    /// Applies only while `token` owns the preparation lease. A finalize
    /// that applies records `token` in `metadata_ddl.applied_token`, which
    /// is how its proposer learns the objects landed.
    pub(super) async fn apply_ddl_pending_finalize(
        &self,
        token: u64,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        let Some(record) = shared.pending_ddl.get(token) else {
            debug!(token, "pending DDL finalize: no pending record, no-op");
            return Ok(());
        };
        if !crate::control::metadata_proposer::ddl_owner::owns_ddl_lease(&shared, token) {
            debug!(
                token,
                "pending DDL finalize: token does not own the lease, no-op"
            );
            return Ok(());
        }
        for object in &record.objects {
            self.apply_host_side_effects(object_entry(object), raft_index)
                .await?;
        }
        self.credentials.catalog().remove_pending_ddl(token)?;
        shared.pending_ddl.take(token);
        shared
            .metadata_ddl
            .applied_token
            .store(token, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// `DdlPendingCancel`: tear down the Data Plane engine registered for
    /// every `Create`-shaped reserved object, then drop the pending
    /// record. A collection's engine is registered eagerly at CREATE
    /// statement time, independent of buffering, so an abandoned create
    /// still needs the same `UnregisterCollection` teardown a real purge
    /// uses. A name with any committed row keeps its engine.
    ///
    /// Each teardown is queued as a durable `_system.pending_reclaim` row
    /// before the record is dropped, so a crash before the teardown finishes
    /// leaves it to the boot drain. The dispatch is spawned rather than
    /// awaited inline — apply runs on the raft loop task, and blocking here
    /// will deadlock the applied-index watcher (same reasoning as the
    /// `TopologyChange::Leave` lease-GC spawn in `dispatch.rs`).
    pub(super) fn apply_ddl_pending_cancel(&self, token: u64) -> Result<(), crate::Error> {
        let shared = self.shared_state()?;
        let Some(record) = shared.pending_ddl.get(token) else {
            debug!(token, "pending DDL cancel: no pending record, no-op");
            return Ok(());
        };
        // Decide every teardown before dropping the record, so a failed catalog
        // read leaves the record for the re-delivered cancel.
        let catalog = self.credentials.catalog();
        let mut teardown = Vec::new();
        for object in &record.objects {
            let PendingDdlObject::Create { entry } = object else {
                continue;
            };
            let Some(target) = created_collection_target(entry.as_ref()) else {
                continue;
            };
            if cancel_owns_engine(&target, catalog)? {
                teardown.push(target);
            } else {
                debug!(
                    collection = %target.name,
                    tenant = target.tenant_id,
                    "pending DDL cancel: a later incarnation holds the name, teardown skipped"
                );
            }
        }
        let queued = queue_teardowns(
            catalog,
            teardown,
            shared.wal.next_lsn().as_u64(),
            crate::control::lease::wall_now_ns(),
        )?;
        // A same-name CREATE waits on the pending-reclaim path's hold until
        // the teardown finishes. A re-delivered cancel finds it already held.
        for entry in &queued {
            shared.quiesce.ensure_reclaim_hold(&entry.owner());
        }
        catalog.remove_pending_ddl(token)?;
        shared.pending_ddl.take(token);
        for entry in queued {
            let shared = std::sync::Arc::clone(&shared);
            tokio::spawn(async move {
                if let Err(error) =
                    crate::event::collection_gc::pending_reclaim::retry_one(&shared, &entry).await
                {
                    tracing::warn!(
                        collection = %entry.name,
                        tenant = entry.tenant_id,
                        error = %error,
                        "pending DDL cancel: Data Plane teardown failed; the pending-reclaim \
                         worker retries it"
                    );
                }
            });
        }
        Ok(())
    }
}

/// A collection a pending create registered.
struct CreatedCollection {
    database_id: u64,
    tenant_id: u64,
    name: String,
    /// The create's own clock, the incarnation the teardown reclaims.
    hlc: Hlc,
}

/// Record one durable pending-reclaim row per teardown. The boot drain and the
/// pending-reclaim worker finish any teardown these rows still name.
fn queue_teardowns(
    catalog: &crate::control::security::catalog::SystemCatalog,
    teardown: Vec<CreatedCollection>,
    purge_lsn: u64,
    enqueued_at_ns: u64,
) -> crate::Result<Vec<StoredPendingReclaim>> {
    let mut queued = Vec::with_capacity(teardown.len());
    for CreatedCollection {
        database_id,
        tenant_id,
        name,
        hlc,
    } in teardown
    {
        let entry = StoredPendingReclaim {
            database_id,
            tenant_id,
            name,
            purge_lsn,
            enqueued_at_ns,
            last_error: String::new(),
            attempts: 0,
            target_hlc: Some(hlc),
            cancelled_create: true,
        };
        catalog.enqueue_pending_reclaim(&entry)?;
        queued.push(entry);
    }
    Ok(queued)
}

/// Whether the engine registered under `target`'s name still belongs to the
/// cancelled create. A cancelled create never commits its row, so any
/// committed row under the name belongs to a finalized create or a later
/// incarnation, and its engine stays.
fn cancel_owns_engine(
    target: &CreatedCollection,
    catalog: &crate::control::security::catalog::SystemCatalog,
) -> crate::Result<bool> {
    let row = catalog.get_committed_collection(
        crate::types::DatabaseId::new(target.database_id),
        target.tenant_id,
        &target.name,
    )?;
    Ok(row.is_none())
}

/// The `MetadataEntry` wrapped by a pending object, regardless of shape.
fn object_entry(object: &PendingDdlObject) -> &MetadataEntry {
    match object {
        PendingDdlObject::Create { entry } | PendingDdlObject::Alter { entry, .. } => {
            entry.as_ref()
        }
    }
}

/// `(database_id, tenant_id, name)` when `entry` is a collection create —
/// the only shape that registers a Data Plane engine eagerly at DDL time.
fn created_collection_target(entry: &MetadataEntry) -> Option<CreatedCollection> {
    let payload = match entry {
        MetadataEntry::CatalogDdl { payload }
        | MetadataEntry::CatalogDdlAudited { payload, .. } => payload,
        _ => return None,
    };
    match catalog_entry::decode(payload).ok()? {
        catalog_entry::CatalogEntry::PutCollection(stored)
        | catalog_entry::CatalogEntry::PutCollectionIfAbsent(stored) => Some(CreatedCollection {
            database_id: stored.database_id.as_u64(),
            tenant_id: stored.tenant_id,
            hlc: stored.modification_hlc,
            name: stored.name,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::DatabaseId;

    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;

    fn target() -> CreatedCollection {
        CreatedCollection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "orders".to_string(),
            hlc: Hlc::new(10, 0),
        }
    }

    fn open() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    fn seed(store: &CredentialStore, hlc: Hlc) {
        let mut row = StoredCollection::stamped_for_test(1, "orders", "tester");
        row.modification_hlc = hlc;
        store
            .catalog()
            .put_collection(DatabaseId::DEFAULT, &row)
            .expect("seed collection");
    }

    /// A cancel replayed after the same name was created for real must leave
    /// the later collection's engine registered.
    #[test]
    fn replayed_cancel_spares_a_later_same_name_collection() {
        let (store, _tmp) = open();
        seed(&store, Hlc::new(30, 0));
        assert!(!cancel_owns_engine(&target(), store.catalog()).expect("read"));
    }

    #[test]
    fn cancel_tears_down_when_no_row_holds_the_name() {
        let (store, _tmp) = open();
        assert!(cancel_owns_engine(&target(), store.catalog()).expect("read"));
    }

    /// The cancel records the teardown durably before it spawns the dispatch,
    /// so the boot drain finishes a teardown a crash interrupted.
    #[test]
    fn cancel_queues_a_durable_reclaim_for_each_teardown() {
        let (store, _tmp) = open();
        let queued =
            queue_teardowns(store.catalog(), vec![target()], 42, 7).expect("queue teardown");
        assert_eq!(queued.len(), 1);

        let rows = store
            .catalog()
            .load_pending_reclaim_queue()
            .expect("load queue");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].database_id, DatabaseId::DEFAULT.as_u64());
        assert_eq!(rows[0].tenant_id, 1);
        assert_eq!(rows[0].name, "orders");
        assert_eq!(rows[0].purge_lsn, 42);
        assert_eq!(rows[0].target_hlc, Some(Hlc::new(10, 0)));
        assert!(rows[0].cancelled_create);
    }

    /// A committed row at the create's own clock means the create was
    /// finalized. Its engine is live.
    #[test]
    fn cancel_spares_a_finalized_create() {
        let (store, _tmp) = open();
        seed(&store, Hlc::new(10, 0));
        assert!(!cancel_owns_engine(&target(), store.catalog()).expect("read"));
    }
}
