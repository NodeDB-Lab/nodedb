// SPDX-License-Identifier: BUSL-1.1

//! The version keys a record's apply forces on the rows and edges it writes.

use std::collections::HashMap;

use crate::data::executor::handlers::transaction::overlay::BitemporalStamp;

/// Scratch a committed or replayed record sets around its apply, so every
/// apply of the record writes the same version keys.
#[derive(Default)]
pub(in crate::data::executor) struct ApplyScope {
    /// Surrogate to resolve-time bitemporal stamp, read ONLY by
    /// `apply_point_put` and `apply_point_delete`. Set right before a
    /// bitemporal document apply, from a committing transaction's overlay
    /// sidecar or a decoded stamped redo put or delete, and cleared right
    /// after. A surrogate with an entry is forced onto the versioned store at
    /// the carried system time, so every apply of the record agrees on the
    /// version key even when `doc_configs` is empty, as at replay-time boot.
    pub(in crate::data::executor) bitemporal_stamps: HashMap<u32, BitemporalStamp>,
    /// The transaction-resolved graph system-time ordinal every edge
    /// mutation of the current apply or replay uses.
    pub(in crate::data::executor) graph_system_from: Option<i64>,
    /// The ordinal the edge version of the current apply or replay is
    /// applied at, when it differs from `graph_system_from`: a restored
    /// version's restore ordinal. Set beside `graph_system_from` and cleared
    /// with it.
    pub(in crate::data::executor) graph_applied: Option<i64>,
    /// The ordinal of the Calvin transaction a stage or resolve runs for,
    /// from its epoch instant and position. Set only for the length of a
    /// Calvin stage or resolve. Every participant of the transaction
    /// computes the same value, so a cross-shard edge's two homes stamp one
    /// version key, and every document version of the transaction stamps
    /// the edges' ordinal.
    pub(in crate::data::executor) calvin_txn_ordinal: Option<i64>,
    /// The system-time ordinal the applied write's WAL records carry, set
    /// for the length of the apply or the replay of a record: the one the
    /// leader of a replicated data-group entry assigned, or the one the local
    /// append reserved. Every replica, and every replay of the records,
    /// applies the write with the same value, so all of them stamp the same
    /// version keys.
    pub(in crate::data::executor) replicated_ordinal: Option<i64>,
}

impl super::CoreLoop {
    /// Scope the replay of `record` to the system ordinal it carries
    /// (`RecordHeader::system_ordinal`), the one its live apply stamped.
    /// Returns the ordinal the scope held, which the caller restores once
    /// the record applied. A record that carries none leaves the scope as
    /// it is: a committed-redo install keeps the ordinal of its entry.
    pub(in crate::data::executor) fn enter_record_ordinal(
        &mut self,
        record: &nodedb_wal::WalRecord,
    ) -> Option<i64> {
        let outer = self.apply_scope.replicated_ordinal;
        if let Some(ordinal) = record.system_ordinal() {
            self.apply_scope.replicated_ordinal = Some(ordinal);
        }
        outer
    }
}
