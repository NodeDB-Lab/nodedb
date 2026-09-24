// SPDX-License-Identifier: BUSL-1.1

//! Storing the dead-letter entry a rejected CRDT delta leaves, and the
//! refusal the writer receives.

use nodedb_types::DatabaseId;
use nodedb_types::sync::violation::ViolationType;

use crate::bridge::envelope::ErrorCode;
use crate::types::{Lsn, TenantId};

use super::CoreLoop;

impl CoreLoop {
    /// Bind the entry the latest validated apply on `(database_id,
    /// tenant_id)` enqueued to the record at `source_lsn`, and store it.
    ///
    /// Call it after every validated apply that returned `Rejected`. A
    /// record already bound keeps its one entry. A write with no record has
    /// nothing to replay, so its entry stays in memory only.
    ///
    /// A store error is returned: the entry is then in memory only, and the
    /// caller must not report the rejection as durable.
    pub(in crate::data::executor) fn store_crdt_dead_letter(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        source_lsn: Option<Lsn>,
    ) -> crate::Result<()> {
        let Some(source_lsn) = source_lsn.filter(|lsn| *lsn != Lsn::ZERO) else {
            return Ok(());
        };
        let Some(engine) = self.crdt_engines.get_mut(&(database_id, tenant_id)) else {
            return Ok(());
        };
        let Some(entry) = engine.bind_dead_letter_source(source_lsn.as_u64()) else {
            return Ok(());
        };
        self.sparse.put_crdt_dead_letter(
            database_id.as_u64(),
            tenant_id.as_u64(),
            source_lsn.as_u64(),
            &entry,
        )
    }
}

impl CoreLoop {
    /// Store the entry the rejection of the replayed record at `lsn`
    /// produced. The live apply of the record stored the same entry, so this
    /// keeps one. A store error is logged, and the record keeps the entry in
    /// memory.
    pub(in crate::data::executor) fn store_replayed_dead_letter(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        lsn: u64,
    ) {
        if let Err(error) = self.store_crdt_dead_letter(database_id, tenant_id, Some(Lsn::new(lsn)))
        {
            tracing::error!(
                core = self.core_id,
                %database_id,
                %tenant_id,
                lsn,
                %error,
                "a replayed CRDT rejection's dead-letter entry could not be stored"
            );
        }
    }
}

/// The refusal for a CRDT delta rejected by constraint `violation`.
///
/// Nothing applied, so the write's record is cancelled. The rejected delta
/// stays in the dead-letter queue.
pub(in crate::data::executor) fn crdt_rejection(
    collection: &str,
    target: &str,
    violation: &ViolationType,
) -> ErrorCode {
    let text = violation.to_string();
    let constraint = text
        .split_once(':')
        .map_or(text.as_str(), |(kind, _)| kind)
        .to_string();
    ErrorCode::RejectedConstraint {
        constraint,
        detail: format!(
            "delta for {collection}/{target} violates {violation}; nothing was applied, \
             and the delta is in the dead-letter queue"
        ),
    }
}
