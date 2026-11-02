// SPDX-License-Identifier: BUSL-1.1

//! Charging document history to the document engine's memory budget.
//!
//! Every document collection keeps every row version and index entry
//! version until purge. The sparse store counts the bytes each tenant's
//! versions hold in the same transaction that writes them. After a document
//! write, the core replaces the tenant's charge with the counter's value, so
//! the `DocumentSchemaless` budget, the one the document write gate checks,
//! tracks the history the tenant keeps.

use nodedb_mem::{EngineId, ScopedMemory};

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

impl CoreLoop {
    /// Replace the tenant's document-history charge with the bytes its
    /// versions hold now. The bytes are already stored, so the charge
    /// cannot be denied.
    ///
    /// `Err` when the counter does not read. The prior charge then stays.
    pub(in crate::data::executor) fn recharge_document_history(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> crate::Result<()> {
        let bytes = self
            .sparse
            .document_history_bytes(database_id.as_u64(), tenant_id.as_u64())?;
        let bytes = usize::try_from(bytes).unwrap_or(usize::MAX);
        let key = (database_id, tenant_id);
        if self
            .document_history_mem
            .get(&key)
            .is_some_and(|token| token.size() == bytes)
        {
            return Ok(());
        }
        // Release the prior charge first so an unchanged footprint nets to
        // zero rather than counting twice.
        self.document_history_mem.remove(&key);
        if bytes == 0 {
            return Ok(());
        }
        let mem = ScopedMemory::new(
            self.governor.clone(),
            database_id,
            tenant_id,
            EngineId::DocumentSchemaless,
        );
        self.document_history_mem.insert(key, mem.charge(bytes));
        Ok(())
    }

    /// Recharge every tenant whose document history holds bytes. Boot runs
    /// it once replay ends: the counter then holds the history the store
    /// kept across the restart plus every version replay wrote, and no
    /// charge exists yet.
    ///
    /// `Err` when the counter does not read.
    pub(in crate::data) fn recharge_all_document_history(&mut self) -> crate::Result<()> {
        for (database_id, tenant) in self.sparse.document_history_tenants()? {
            self.recharge_document_history(DatabaseId::new(database_id), TenantId::new(tenant))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use nodedb_types::{StorageKey, Surrogate};

    /// After a restart the core charges the history the store kept, before
    /// any write recharges it.
    #[test]
    fn a_restart_recharges_the_stored_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (core, _tx, _rx) = make_core_with_dir(dir.path());
            let key = StorageKey::for_surrogate(Surrogate::new(1));
            core.sparse
                .put(0, 4, "docs", &key, b"v1", 100)
                .expect("put");
            core.sparse
                .put(0, 4, "docs", &key, b"v2", 200)
                .expect("put");
        }
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let tenant = (DatabaseId::new(0), TenantId::new(4));
        assert!(!core.document_history_mem.contains_key(&tenant));

        core.recharge_all_document_history().expect("recharge");

        let stored = core.sparse.document_history_bytes(0, 4).expect("counter");
        assert!(stored > 0);
        assert_eq!(
            core.document_history_mem
                .get(&tenant)
                .map(|token| token.size()),
            Some(usize::try_from(stored).expect("fits"))
        );
    }
}
