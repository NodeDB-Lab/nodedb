// SPDX-License-Identifier: BUSL-1.1

//! `CatalogDdl` / `CatalogDdlAudited` host-side effects: decode the
//! opaque payload as a `CatalogEntry`, write through to `SystemCatalog`
//! redb, run synchronous post-apply side effects, run the post-apply
//! dispatch, and emit the DDL audit record.

use tracing::{debug, warn};

use nodedb_cluster::MetadataEntry;

use crate::control::catalog_entry;

use super::audit::emit_ddl_audit;
use crate::control::state::SharedState;

use super::types::MetadataCommitApplier;

/// The rule for a catalog entry's descriptor version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DescriptorCheck {
    /// The entry is the next one of its descriptor's log.
    Log,
    /// The entry is a row of a mirror's bootstrap image: with no prior row, it
    /// starts its descriptor's history at the version it carries.
    Image,
}

impl MetadataCommitApplier {
    /// Release the descriptor drain a `Put*` DDL installed, and no drain
    /// another owner holds on the same descriptor.
    ///
    /// A drain is proposed *before* the DDL and is meant to end when that DDL
    /// concludes. Concluding includes the outcomes that write nothing — an
    /// entry superseded during replay, or an if-absent create for a descriptor
    /// that already exists. The drain is keyed to the DDL, not to whether the
    /// catalog changed, so every path that finishes handling the entry must
    /// clear it.
    ///
    /// Missing one of those paths does not fail loudly: the drain
    /// survives, and `is_draining` then rejects every plan for that
    /// descriptor as a retryable schema change with no error explaining
    /// why. The drain has no self-healing wall-clock expiry (see
    /// `lease::drain`) — the only backstop is the proposer's own wait
    /// loop timing out and proposing `DescriptorDrainEnd` explicitly, so
    /// every code path here must clear the drain it opened.
    ///
    /// The drain rows go first, so a crash after them never leaves a drain
    /// that boot will seed back.
    fn clear_implicit_drain(
        &self,
        shared: &SharedState,
        stamped: &catalog_entry::CatalogEntry,
    ) -> Result<(), crate::Error> {
        crate::control::lease::clear_implicit_drains(shared, stamped)
    }

    pub(super) async fn apply_catalog_ddl(
        &self,
        entry: &MetadataEntry,
        raft_index: u64,
    ) -> Result<(), crate::Error> {
        self.apply_catalog_ddl_as(entry, raft_index, DescriptorCheck::Log)
            .await
    }

    /// [`Self::apply_catalog_ddl`] under `check`, the rule for the entry's
    /// descriptor version.
    pub(super) async fn apply_catalog_ddl_as(
        &self,
        entry: &MetadataEntry,
        raft_index: u64,
        check: DescriptorCheck,
    ) -> Result<(), crate::Error> {
        let catalog = self.credentials.catalog();
        let (payload, audit) = match entry {
            MetadataEntry::CatalogDdl { payload } => (payload, None),
            MetadataEntry::CatalogDdlAudited {
                payload,
                auth_user_id,
                auth_user_name,
                sql_text,
            } => (
                payload,
                Some((
                    auth_user_id.clone(),
                    auth_user_name.clone(),
                    sql_text.clone(),
                )),
            ),
            _ => return Ok(()),
        };
        let stamped = match catalog_entry::decode(payload) {
            Ok(e) => e,
            Err(e) => {
                // Deterministic poison: a corrupt payload will not decode on
                // retry either, so skip it (advance) rather than wedge the
                // group. Loud because a committed-but-undecodable entry is a
                // serious version-skew / corruption signal.
                warn!(error = %e, "metadata applier: failed to decode CatalogEntry payload");
                return Ok(());
            }
        };
        let shared = self.shared_state()?;

        // Slows one collection's create on one node, as a long apply does.
        #[cfg(feature = "failpoints")]
        if let catalog_entry::CatalogEntry::PutCollection(stored)
        | catalog_entry::CatalogEntry::PutCollectionIfAbsent(stored) = &stamped
        {
            crate::control::fail_gate::before_catalog_ddl_apply(shared.node_id, &stored.name).await;
        }

        // Holds back one node's apply of backup schedule marks with a
        // transient error, so a test can make that node's catalog lag the
        // metadata group. Raft re-delivers the entry once it is cleared.
        #[cfg(feature = "failpoints")]
        if matches!(
            stamped,
            catalog_entry::CatalogEntry::PutBackupScheduleMark(_)
        ) {
            nodedb_types::fail_point_err!(
                nodedb_types::fail_point::FailScope::Node(shared.node_id),
                BACKUP_MARK_FAIL_POINT,
                |detail| {
                    crate::Error::Storage {
                        engine: "catalog".into(),
                        detail,
                    }
                }
            );
        }

        // Descriptor versions (and the constraint_version /
        // modification_hlc that travel with them) are frozen at PROPOSE
        // time and replicated verbatim (see `metadata_proposer`). The
        // applier persists exactly what the entry carries and never
        // re-derives from local state, so full-log replay on restart and
        // re-delivery during learner catch-up write the same frozen
        // value — idempotent by construction, with no per-node drift.
        //
        // Before persisting, validate the carried version against this
        // node's local prior. Historical entries encountered during a full-log
        // replay are acknowledged without overwriting newer state or repeating
        // post-apply side effects. Forward gaps and same-version divergent
        // payloads remain loud typed errors. A version of `0` (unit-test
        // fixtures that bypass the proposer) is applied without version
        // fencing.
        let validated = match check {
            DescriptorCheck::Log => catalog_entry::descriptor_validate::validate(&stamped, catalog),
            DescriptorCheck::Image => {
                catalog_entry::descriptor_validate::validate_image_row(&stamped, catalog)
            }
        };
        // A re-delivered entry whose catalog write landed and whose post-apply
        // did not finish finds its row written. It runs the post-apply again
        // rather than counting as applied. The marker is a catalog row, so
        // the full-log replay after a restart finds it too.
        let owed = self.owes_post_apply(raft_index, payload)?;
        if matches!(
            validated?,
            catalog_entry::descriptor_validate::ValidationOutcome::AlreadyApplied
        ) {
            if !owed {
                debug!(
                    kind = stamped.kind(),
                    "catalog_entry: descriptor entry already superseded or applied"
                );
                // The DDL that installed the drain is over even though this
                // entry changed nothing — release it, or every read of the
                // descriptor stays rejected indefinitely (no wall-clock
                // expiry backstops this path; see `lease::drain`).
                self.clear_implicit_drain(&shared, &stamped)?;
                return Ok(());
            }
            debug!(
                kind = stamped.kind(),
                "catalog_entry: re-running the post-apply a failed attempt owes"
            );
        } else {
            debug!(kind = stamped.kind(), "catalog_entry: applying to redb");
            // The marker lands before the catalog write. A crash or an error
            // anywhere between that write and the end of the post-apply
            // leaves it, and the re-delivery runs the post-apply again.
            if !owed {
                self.owe_post_apply(raft_index, payload)?;
            }
            let outcome = catalog_entry::apply::apply_to(&stamped, catalog)?;
            let concluded = match outcome {
                catalog_entry::apply::ApplyOutcome::Applied => false,
                // An earlier delivery wrote the row, so this one writes
                // nothing and still owes that write's post-apply.
                catalog_entry::apply::ApplyOutcome::Unchanged => !owed,
                // A refusal depends on rows this entry does not write, so an
                // earlier delivery was refused alike and wrote nothing.
                catalog_entry::apply::ApplyOutcome::Refused(_) => true,
            };
            if concluded {
                // A `Put*` that wrote nothing (e.g. an if-absent create for a
                // descriptor that already exists) still concludes its DDL. So
                // does a refused entry: every node refuses it at this
                // position, and the proposer reports the refusal to its
                // client.
                self.clear_implicit_drain(&shared, &stamped)?;
                return self.settle_post_apply(raft_index, payload);
            }
        }
        // The catalog holds the entry's row. Until every effect below
        // finished, the marker stays and a re-delivery runs them again.
        self.finish_applied(&shared, &stamped, raft_index, audit.as_ref())
            .await?;
        self.settle_post_apply(raft_index, payload)
    }

    /// Every effect of a catalog entry whose row the catalog holds: the
    /// purge-log row, the drain release, the post-apply, the purge broadcast
    /// and the audit record.
    async fn finish_applied(
        &self,
        shared: &std::sync::Arc<SharedState>,
        stamped: &catalog_entry::CatalogEntry,
        raft_index: u64,
        audit: Option<&(String, String, String)>,
    ) -> Result<(), crate::Error> {
        // The purge-log row lands before the reclaim, so an entry that
        // re-delivers after a crash writes it again.
        let purges = super::purge_log::record_purges(shared, stamped, raft_index)?;
        // Implicit drain clear: if the entry is a `Put*` for one
        // of the six stamped descriptor types, the DDL that was
        // waiting on drain has now committed — remove the drain
        // entry from every node's host tracker. Happens before
        // post_apply so a subsequent `acquire_lease` fired from
        // post_apply doesn't see a stale drain.
        {
            self.clear_implicit_drain(shared, stamped)?;
            // Run synchronous post-apply side effects INLINE so every
            // in-memory cache update (install_replicated_user,
            // install_replicated_owner, etc.) is visible before the
            // watcher bump. Any reader that observes `applied_index`
            // moving past `last` is guaranteed to see the sync side
            // effects of every entry up to `last`.
            //
            // The awaited post-apply lane is part of the same contract:
            // `PutCollection`'s Register dispatch completes on every core
            // before the entry counts as applied, so a later scan always
            // finds the schema.
            catalog_entry::post_apply::apply_post_apply_side_effects_sync(stamped, shared);

            // A reclaim that failed with no durable retry, or a Register a
            // core did not acknowledge, returns `Err`. The batch stops here
            // and the re-delivered entry runs the post-apply again.
            catalog_entry::post_apply::run_post_apply_async_side_effects(
                stamped.clone(),
                std::sync::Arc::clone(shared),
            )
            .await?;

            // The reclaim ran: online sessions drop their copy now.
            super::purge_log::broadcast_purges(shared, &purges);

            // Emit a DdlChange audit record on every replica, once the entry
            // fully applied, so a re-delivered entry is audited once.
            emit_ddl_audit(shared, raft_index, stamped, audit);
        }
        Ok(())
    }
}

/// The fail point that holds back a node's apply of backup schedule marks.
/// A test arms it for one node.
pub const BACKUP_MARK_FAIL_POINT: &str = "backup_schedule::mark_apply";

#[cfg(test)]
mod tests {
    use nodedb_cluster::{MetadataApplier, MetadataEntry, encode_entry};

    use super::super::test_fixture::applier_with_shared_at;
    use crate::control::catalog_entry::{self, CatalogEntry};
    use crate::control::security::catalog::StoredRole;

    fn put_role() -> Vec<u8> {
        catalog_entry::encode(&CatalogEntry::PutRole(Box::new(StoredRole {
            name: "auditor".into(),
            tenant_id: 1,
            parent: String::new(),
            created_at: 0,
        })))
        .unwrap()
    }

    fn ddl(payload: Vec<u8>) -> Vec<u8> {
        encode_entry(&MetadataEntry::CatalogDdl { payload }).unwrap()
    }

    /// An entry that finished its post-apply leaves no marker.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_applied_entry_leaves_no_marker() {
        let dir = tempfile::tempdir().unwrap();
        let (applier, state) = applier_with_shared_at(dir.path(), "test.wal");
        assert_eq!(applier.apply(&[(3, ddl(put_role()))]).await, 3);
        assert_eq!(
            state.credentials.catalog().load_owed_post_apply().unwrap(),
            None
        );
    }

    /// A marker a crash left behind survives the restart. The entry's
    /// re-delivery runs its post-apply and clears the marker.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_marker_left_by_a_crash_is_cleared_by_the_redelivery() {
        let dir = tempfile::tempdir().unwrap();
        let payload = put_role();
        {
            let (_applier, state) = applier_with_shared_at(dir.path(), "first.wal");
            state
                .credentials
                .catalog()
                .put_owed_post_apply(5, &payload)
                .unwrap();
        }
        let (applier, state) = applier_with_shared_at(dir.path(), "second.wal");
        let catalog = state.credentials.catalog();
        assert_eq!(
            catalog.load_owed_post_apply().unwrap(),
            Some((5, payload.clone()))
        );
        assert_eq!(applier.apply(&[(5, ddl(payload))]).await, 5);
        assert_eq!(catalog.load_owed_post_apply().unwrap(), None);
        assert!(
            catalog
                .load_all_roles()
                .unwrap()
                .iter()
                .any(|role| role.name == "auditor")
        );
    }
}
