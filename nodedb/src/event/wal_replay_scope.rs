// SPDX-License-Identifier: BUSL-1.1

//! Where a replayed WAL record sits, and which event source its rows carry.
//!
//! A write record's header stores the event source its write ran with. A
//! replayed event carries the source its live event carried:
//! - a raw Put or Delete record: every row carries the record's source;
//! - a `TransactionRedo` record: a document row carries
//!   [`EventSource::committed_row_source`] of the record's source, and a KV,
//!   graph edge or node-label row carries the record's source. The live
//!   committed-redo apply emits the same sources.

use crate::event::types::EventSource;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

/// The event source each kind of replayed row carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowSources {
    /// The source of a document row.
    pub document: EventSource,
    /// The source of a KV, graph edge or node-label row.
    pub other: EventSource,
}

impl RowSources {
    /// A raw write record: every row carries `source`.
    pub const fn uniform(source: EventSource) -> Self {
        Self {
            document: source,
            other: source,
        }
    }

    /// A committed transaction's redo record whose writes ran with `source`.
    pub const fn committed_redo(source: EventSource) -> Self {
        Self {
            document: source.committed_row_source(),
            other: source,
        }
    }
}

/// The header identity every event rebuilt from one record carries.
#[derive(Debug, Clone, Copy)]
pub struct ReplayScope {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub lsn: Lsn,
    pub sources: RowSources,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_transaction_redo_fires_deferred_document_rows_only() {
        let sources = RowSources::committed_redo(EventSource::User);
        assert_eq!(sources.document, EventSource::Deferred);
        assert_eq!(sources.other, EventSource::User);
    }

    #[test]
    fn a_restore_redo_keeps_restore_for_every_row() {
        let sources = RowSources::committed_redo(EventSource::Restore);
        assert_eq!(sources, RowSources::uniform(EventSource::Restore));
    }
}
