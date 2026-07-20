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
        // node's local prior so a genuine regression or gap surfaces as a
        // loud typed error (halting the apply watermark for this entry and
        // forcing Raft re-delivery) rather than silently writing a
        // divergent value. A version of `0` (compat mode / unit tests) is
        // skipped by the validator.
        catalog_entry::descriptor_stamp::validate(&stamped, catalog)?;

        debug!(kind = stamped.kind(), "catalog_entry: applying to redb");
        if !catalog_entry::apply::apply_to(&stamped, catalog) {
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
            if let Some(drained_id) =
                crate::control::lease::drain_propose::descriptor_id_for_implicit_clear(&stamped)
            {
                shared.lease_drain.install_end(&drained_id);
            }
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
