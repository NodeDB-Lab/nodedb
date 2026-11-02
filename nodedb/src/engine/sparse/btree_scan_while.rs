// SPDX-License-Identifier: BUSL-1.1

//! A streaming current-state document scan its visitor can stop.

use std::ops::ControlFlow;

use nodedb_types::StorageKey;

use super::btree::SparseEngine;

impl SparseEngine {
    /// Visit the current version of every document of a collection in doc-id
    /// order, one row in memory at a time, until `f` returns
    /// `ControlFlow::Break`. Every redb error and every `f` error is
    /// propagated.
    pub fn scan_documents_while<F>(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        f: F,
    ) -> crate::Result<()>
    where
        F: FnMut(&StorageKey, &[u8]) -> crate::Result<ControlFlow<()>>,
    {
        self.for_each_current_document(
            database_id,
            tenant_id,
            collection,
            &super::scan_stop::never_stop,
            f,
        )
    }
}
