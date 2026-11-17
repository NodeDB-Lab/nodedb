// SPDX-License-Identifier: BUSL-1.1

//! Capture of the stored sync-frame text versions for a tenant snapshot.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{SyncedTextEntry, TenantId};

impl CoreLoop {
    /// Every stored sync-frame text version of `tenant_id` in
    /// `database_id` on this core, tombstones included, in collection, then
    /// surrogate, then ordinal order.
    pub(super) fn capture_synced_text(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<SyncedTextEntry>> {
        let mut frames = Vec::new();
        self.inverted.for_each_synced_text(
            database_id,
            TenantId::new(tenant_id),
            None,
            |frame| {
                frames.push(SyncedTextEntry {
                    database_id,
                    tenant_id,
                    collection: frame.collection,
                    surrogate: frame.surrogate.as_u32(),
                    ordinal: frame.ordinal,
                    fields: frame.fields,
                });
                Ok(())
            },
        )?;
        Ok(frames)
    }
}
