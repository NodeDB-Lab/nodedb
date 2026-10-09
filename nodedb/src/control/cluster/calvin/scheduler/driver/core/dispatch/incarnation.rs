// SPDX-License-Identifier: BUSL-1.1

//! The incarnation check a Calvin transaction passes when a replica stages it.
//!
//! The coordinator stamps the incarnation of every collection the transaction
//! names. Each replica checks all of them, not only its own slice's, against
//! its committed catalog under the collections' gates, held shared until the
//! transaction leaves `pending`. A mismatch makes the leader's vote an abort,
//! and the sequencer's global verdict then aborts every participant alike: no
//! part of the transaction applies. A follower with a mismatch under a COMMIT
//! verdict halts rather than flush into a recreated collection.
//!
//! A halt releases every gate no flush in flight needs, so a purge never waits
//! on a halted scheduler. A txn whose gates a halt released checks again, and
//! takes them back, before its flush.

use std::collections::BTreeSet;

use nodedb_cluster::calvin::types::TxClass;
use nodedb_types::{CollectionKey, Hlc};
use tracing::error;

use super::super::super::types::CommitState;
use super::super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::write_gate::{self, GateKey, SharedGate};

impl Scheduler {
    /// Hold the gate of every collection `tx_class` names, and report whether
    /// one no longer holds its planned incarnation on this replica.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn check_incarnations(
        &self,
        tx_class: &TxClass,
    ) -> (bool, Vec<SharedGate>) {
        let tenant_id = tx_class.tenant_id.as_u64();
        let database_id = tx_class.database_id;
        let keys: BTreeSet<GateKey> = tx_class
            .incarnations
            .iter()
            .map(|named| {
                let key = CollectionKey::from_qualified_str(database_id, &named.collection)
                    .unwrap_or_else(|_| CollectionKey::from_bare(database_id, &named.collection));
                GateKey::Collection {
                    database_id: key.database_id().as_u64(),
                    tenant_id,
                    name: key.name().to_string(),
                }
            })
            .collect();
        let mut superseded = false;
        let mut gates = Vec::with_capacity(keys.len());
        for key in keys {
            // A reclaim holds a gate exclusive only after the catalog dropped
            // the collection's row, so its incarnation is gone.
            match write_gate::try_shared(key) {
                Some(gate) => gates.push(gate),
                None => superseded = true,
            }
        }
        let catalog = self.shared.credentials.catalog();
        for named in &tx_class.incarnations {
            if named.incarnation == Hlc::ZERO {
                continue;
            }
            match catalog.holds_incarnation(
                database_id,
                tenant_id,
                &named.collection,
                named.incarnation,
            ) {
                Ok(true) => {}
                Ok(false) => superseded = true,
                Err(e) => {
                    error!(
                        vshard_id = self.vshard_id,
                        collection = %named.collection,
                        error = %e,
                        "calvin scheduler: incarnation check could not read the catalog; \
                         voting abort"
                    );
                    superseded = true;
                }
            }
        }
        (superseded, gates)
    }

    /// Release the gates a halt leaves nothing to protect.
    ///
    /// A halted scheduler never re-sends a parked request, and `stuck` never
    /// applies before restart. Only a committed flush the Data Plane already
    /// holds can still write, and it keeps its gates until it answers. Every
    /// other txn gives its gates back, so a purge never waits on this
    /// scheduler.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn release_gates_on_halt(
        &mut self,
        stuck: TxnId,
    ) {
        let release: Vec<TxnId> = self
            .pending
            .iter()
            .filter(|(txn_id, pending)| {
                **txn_id == stuck
                    || !writes_in_flight(pending.commit_state)
                    || self.has_parked_dispatch(**txn_id)
            })
            .map(|(txn_id, _)| *txn_id)
            .collect();
        for txn_id in release {
            if let Some(pending) = self.pending.get_mut(&txn_id)
                && !pending.ungated
            {
                pending.gates.clear();
                pending.ungated = true;
            }
        }
    }

    /// Take back the gates of `txn_id` before its flush, when a halt released
    /// them. Fails when a collection the txn names no longer holds its planned
    /// incarnation: the flush cannot apply here.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn regate(
        &mut self,
        txn_id: TxnId,
    ) -> crate::Result<()> {
        let Some(tx_class) = self
            .pending
            .get(&txn_id)
            .filter(|pending| pending.ungated)
            .map(|pending| pending.txn.tx_class.clone())
        else {
            return Ok(());
        };
        let (superseded, gates) = self.check_incarnations(&tx_class);
        if superseded {
            return Err(crate::Error::Internal {
                detail: format!(
                    "calvin txn ({}, {}) committed, but a collection it writes was purged \
                     after the halt released its gates",
                    txn_id.epoch, txn_id.position
                ),
            });
        }
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            pending.gates = gates;
            pending.ungated = false;
        }
        Ok(())
    }
}

/// Whether a txn in `state` can have a write in the Data Plane's hands: a
/// committed flush.
fn writes_in_flight(state: CommitState) -> bool {
    matches!(
        state,
        CommitState::AwaitingResolve {
            committed: true,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::CalvinIncarnation;
    use nodedb_types::DatabaseId;

    use super::super::super::test_support::{
        make_sequenced_txn, scheduler_with_pending, staged_response,
    };
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
    use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
    use crate::control::security::catalog::StoredCollection;

    fn named(collection: &str, incarnation: Hlc) -> CalvinIncarnation {
        CalvinIncarnation {
            collection: collection.into(),
            incarnation,
        }
    }

    /// A transaction staged against `orders` before a purge and a same-name
    /// create is superseded on this replica, though `ledger` is unchanged. The
    /// leader's vote is an abort, which the global verdict applies to every
    /// participant, `ledger`'s included. A replica that staged it under a
    /// COMMIT verdict halts instead of flushing.
    #[test]
    fn a_txn_staged_before_a_recreate_is_superseded_everywhere() {
        let txn_id = TxnId::new(3, 0);
        let (mut scheduler, _dir) = scheduler_with_pending(txn_id, CommitState::Staged);
        let catalog = scheduler.shared.credentials.catalog();
        let incarnation = |name: &str| {
            catalog
                .get_committed_collection(DatabaseId::DEFAULT, 1, name)
                .expect("read")
                .expect("row")
                .incarnation
        };
        for name in ["orders", "ledger"] {
            catalog
                .put_collection(
                    DatabaseId::DEFAULT,
                    &StoredCollection::stamped_for_test(1, name, "admin"),
                )
                .expect("create");
        }
        let planned = incarnation("orders");
        let ledger = incarnation("ledger");

        let mut tx_class = make_sequenced_txn(3, 0).tx_class;
        tx_class.set_incarnations(vec![named("orders", planned), named("ledger", ledger)]);
        let (superseded, gates) = scheduler.check_incarnations(&tx_class);
        assert!(
            !superseded,
            "both collections hold their planned incarnations"
        );
        assert_eq!(gates.len(), 2);
        drop(gates);

        catalog
            .delete_collection(DatabaseId::DEFAULT, 1, "orders")
            .expect("purge");
        catalog
            .put_collection(
                DatabaseId::DEFAULT,
                &StoredCollection::stamped_for_test(1, "orders", "admin"),
            )
            .expect("recreate");
        assert_ne!(incarnation("orders"), planned);
        let (superseded, _gates) = scheduler.check_incarnations(&tx_class);
        assert!(
            superseded,
            "the recreated collection is not the planned one"
        );

        // The staged slice votes abort, whatever its own read-set says, and
        // cannot flush under a COMMIT verdict.
        if let Some(pending) = scheduler.pending.get_mut(&txn_id) {
            pending.superseded = true;
        }
        scheduler.resolve_staged_commit(txn_id, &staged_response(Status::Ok, Some(true)));
        let pending = scheduler.pending.get(&txn_id).expect("still parked");
        assert_eq!(pending.commit_state, CommitState::AwaitingVerdict);
        assert!(pending.stage_error.is_some(), "a COMMIT verdict halts it");
    }

    /// A reclaim in progress holds the gate exclusive: the check does not
    /// wait on it and reports the collection superseded.
    #[tokio::test]
    async fn a_reclaim_in_progress_supersedes_without_waiting() {
        let txn_id = TxnId::new(4, 0);
        let (scheduler, _dir) = scheduler_with_pending(txn_id, CommitState::Staged);
        let reclaim = write_gate::exclusive(GateKey::Collection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "purging".into(),
        })
        .await;
        let mut tx_class = make_sequenced_txn(4, 0).tx_class;
        tx_class.set_incarnations(vec![named("purging", Hlc::ZERO)]);
        let (superseded, gates) = scheduler.check_incarnations(&tx_class);
        assert!(superseded);
        assert!(gates.is_empty());
        drop(reclaim);
    }

    /// A replica that halts on a txn it never staged gives back the gates the
    /// txn held: a purge of its collection proceeds. The txn checks again
    /// before a flush and fails once the collection was recreated.
    #[tokio::test]
    async fn a_halt_releases_the_gates_and_a_later_flush_checks_again() {
        let txn_id = TxnId::new(5, 0);
        let (mut scheduler, _dir) = scheduler_with_pending(txn_id, CommitState::AwaitingVerdict);
        let shared = std::sync::Arc::clone(&scheduler.shared);
        let catalog = shared.credentials.catalog();
        catalog
            .put_collection(
                DatabaseId::DEFAULT,
                &StoredCollection::stamped_for_test(1, "orders", "admin"),
            )
            .expect("create");
        let planned = catalog
            .get_committed_collection(DatabaseId::DEFAULT, 1, "orders")
            .expect("read")
            .expect("row")
            .incarnation;
        let mut tx_class = make_sequenced_txn(5, 0).tx_class;
        tx_class.set_incarnations(vec![named("orders", planned)]);
        let (superseded, gates) = scheduler.check_incarnations(&tx_class);
        assert!(!superseded);
        if let Some(pending) = scheduler.pending.get_mut(&txn_id) {
            pending.txn.tx_class = tx_class;
            pending.gates = gates;
            pending.stage_error = Some("stage refused on this replica".into());
        }

        scheduler.resume_on_verdict(txn_id, true);
        assert!(scheduler.is_apply_halted());
        let pending = scheduler.pending.get(&txn_id).expect("held unapplied");
        assert!(pending.gates.is_empty(), "the halt released the gates");
        assert!(pending.ungated);

        let key = GateKey::Collection {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: "orders".into(),
        };
        let reclaim = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            write_gate::exclusive(key),
        )
        .await
        .expect("a purge never waits on a halted scheduler");
        drop(reclaim);

        scheduler.regate(txn_id).expect("nothing changed yet");
        let pending = scheduler.pending.get(&txn_id).expect("pending");
        assert_eq!(pending.gates.len(), 1, "the check took the gate back");
        assert!(!pending.ungated);

        scheduler.release_gates_on_halt(txn_id);
        catalog
            .delete_collection(DatabaseId::DEFAULT, 1, "orders")
            .expect("purge");
        catalog
            .put_collection(
                DatabaseId::DEFAULT,
                &StoredCollection::stamped_for_test(1, "orders", "admin"),
            )
            .expect("recreate");
        assert!(
            scheduler.regate(txn_id).is_err(),
            "a flush never lands in the recreated collection"
        );
    }
}
