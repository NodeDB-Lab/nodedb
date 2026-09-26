// SPDX-License-Identifier: BUSL-1.1

//! `CatalogDdl` / `CatalogDdlAudited` host-side effects: decode the
//! opaque payload as a `CatalogEntry`, write through to `SystemCatalog`
//! redb, run synchronous post-apply side effects, emit the DDL audit
//! record, and spawn async post-apply side effects.

use tracing::{debug, warn};

use nodedb_cluster::MetadataEntry;

use crate::control::catalog_entry;

use super::audit::emit_ddl_audit;
use super::types::MetadataCommitApplier;

impl MetadataCommitApplier {
    /// Release the descriptor drain a `Put*` DDL installed.
    ///
    /// A drain is proposed *before* the DDL and is meant to end when that DDL
    /// concludes. Concluding includes the outcomes that write nothing — an
    /// entry superseded during replay, or an if-absent create for a descriptor
    /// that already exists. The drain is keyed to the DDL, not to whether the
    /// catalog changed, so every path that finishes handling the entry must
    /// clear it.
    ///
    /// Missing one of those paths does not fail loudly: the drain simply
    /// survives, and `is_draining` then rejects every plan for that
    /// descriptor as a retryable schema change with no error explaining
    /// why. The drain has no self-healing wall-clock expiry (see
    /// `lease::drain`) — the only backstop is the proposer's own wait
    /// loop timing out and proposing `DescriptorDrainEnd` explicitly, so
    /// every code path here must clear the drain it opened.
    fn clear_implicit_drain(&self, stamped: &catalog_entry::CatalogEntry) {
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
            && let Some(drained_id) =
                crate::control::lease::descriptor_id_for_implicit_clear(stamped)
        {
            shared.lease_drain.install_end(&drained_id);
        }
    }

    pub(super) fn apply_catalog_ddl(
        &self,
        entry: &MetadataEntry,
        raft_index: u64,
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
        // payloads remain loud typed errors. A version of `0` (compat mode /
        // unit tests) is applied without version fencing.
        if matches!(
            catalog_entry::descriptor_validate::validate(&stamped, catalog)?,
            catalog_entry::descriptor_validate::ValidationOutcome::AlreadyApplied
        ) {
            debug!(
                kind = stamped.kind(),
                "catalog_entry: descriptor entry already superseded or applied"
            );
            // The DDL that installed the drain is over even though this entry
            // changed nothing — release it, or every read of the descriptor
            // stays rejected indefinitely (no wall-clock expiry backstops
            // this path; see `lease::drain`).
            self.clear_implicit_drain(&stamped);
            return Ok(());
        }

        debug!(kind = stamped.kind(), "catalog_entry: applying to redb");
        let outcome = catalog_entry::apply::apply_to(&stamped, catalog)?;
        if !outcome.wrote() {
            // A `Put*` that wrote nothing (e.g. an if-absent create for a
            // descriptor that already exists) still concludes its DDL. So
            // does a refused entry: every node refuses it at this position,
            // and the proposer reports the refusal to its client.
            self.clear_implicit_drain(&stamped);
            return Ok(());
        }
        // Implicit drain clear: if the entry is a `Put*` for one
        // of the six stamped descriptor types, the DDL that was
        // waiting on drain has now committed — remove the drain
        // entry from every node's host tracker. Happens before
        // post_apply so a subsequent `acquire_lease` fired from
        // post_apply doesn't see a stale drain.
        if let Some(weak) = self.shared.get()
            && let Some(shared) = weak.upgrade()
        {
            self.clear_implicit_drain(&stamped);
            // Run synchronous post-apply side effects INLINE so every
            // in-memory cache update (install_replicated_user,
            // install_replicated_owner, etc.) is visible before the
            // watcher bump. Any reader that observes `applied_index`
            // moving past `last` is guaranteed to see the sync side
            // effects of every entry up to `last`.
            //
            // `PutCollection` Register dispatch runs synchronously
            // (block_in_place) inside spawn_post_apply_async_side_effects
            // and IS part of the applied-index contract: the watcher
            // only bumps after doc_configs is populated on every core,
            // so subsequent scans always find the schema.
            catalog_entry::post_apply::apply_post_apply_side_effects_sync(&stamped, &shared);

            // Emit a DdlChange audit record on every replica.
            // Executed BEFORE spawning async post-apply side effects
            // so the audit entry lands synchronously with the rest of
            // the commit.
            emit_ddl_audit(&shared, raft_index, &stamped, audit.as_ref());

            catalog_entry::post_apply::spawn_post_apply_async_side_effects(
                stamped, shared, raft_index,
            );
        }
        Ok(())
    }
}
