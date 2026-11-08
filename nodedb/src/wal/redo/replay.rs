// SPDX-License-Identifier: BUSL-1.1

//! Replay arm for [`RecordType::TransactionRedo`] WAL records.
//!
//! A `TransactionRedo` record groups an ordered set of engine-native
//! sub-records ([`RedoSubRecord`]) — each already in the exact payload shape
//! that engine's own per-op WAL record uses. This module turns each
//! `TransactionRedo` back into a set of per-op [`WalRecord`]s and feeds them to
//! the SAME per-engine replay paths the standalone (autocommit) records use.
//! A `WriteGroup` record, one write's rows journalled after apply, carries
//! sub-records of the same shapes and reconstitutes the same way.
//!
//! ## Why reconstitute rather than apply a blob
//!
//! There is no global "skip records ≤ checkpoint LSN" barrier — each engine
//! self-manages idempotency (KV rebuilds from empty; columnar/timeseries gate on
//! their flushed-LSN watermark; array on its manifest's `durable_lsn`; vector /
//! spatial / FTS restore a checkpoint then replay). Routing every sub-record
//! through its engine's existing `replay_*_wal` inherits that per-engine
//! discipline exactly. Applying the redo record as one monolithic blob, or
//! bypassing an engine's replay function, would defeat it — re-applying a
//! columnar append a checkpoint already absorbed duplicates the row.
//!
//! ## Why the reconstituted ops are MERGED, not appended
//!
//! Most redo ops are absolute overwrites. Replaying every standalone
//! (autocommit) record first and then every redo sub-record inverts LSN order
//! for any key written both inside a transaction and by a later autocommit: the
//! transaction's older post-image lands last and recovery comes up holding it.
//! So [`CoreLoop::replay_engines_in_lsn_order`] merges the reconstituted
//! sub-records into the standalone stream and sorts by LSN before any engine
//! sees a record. Every engine arm then observes exactly the order the WAL
//! recorded, which is the order that produced the acknowledged state.
//!
//! ## Dispatch
//!
//! Each `replay_*_wal` already filters the slice it is handed by
//! `RecordType`, so the reconstituted records are handed to every engine arm
//! and each self-selects its own records. `RecordType::Put` / `Delete` are
//! shared by KV, document, and graph; those three arms disambiguate by payload
//! shape (KV by its leading string discriminator; document and graph by their
//! mutually-exclusive tuple shapes — see the `replay_document_redo` and
//! `replay_graph_redo` arms in `crate::data::executor`).
//!
//! `calvin_stamp` is ignored here: the Calvin recovery scan reads it, and it
//! does not gate engine replay. A Calvin slice's materialized-sum fold rows
//! journal as parts of the record's group, so replay applies them like any
//! other write's rows.
//!
//! The committed-redo apply (`handlers::transaction::redo_apply`) drives this
//! same entry point with a one-record slice for every committed transaction,
//! so the live install and restart replay run the same arms. The arms settle
//! watermark skips and per-record errors by pass (`data::executor`'s
//! `replay_policy`): restart replay keeps its gates, the live install skips
//! none and fails on any error.

use std::borrow::Cow;

use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordStamp, RecordType, WalRecordArgs};

use super::replay_origin::GroupedReplayOrigins;
use super::{RedoRecord, WriteGroupRecord};
use crate::data::executor::core_loop::CoreLoop;

struct ReconstitutedReplay {
    records: Vec<WalRecord>,
    grouped_origins: GroupedReplayOrigins,
}

/// Reconstitute every sub-record of every `TransactionRedo` record into a flat,
/// LSN-ordered `Vec<WalRecord>` carrying each sub-record's own engine
/// `record_type` and payload, plus the enclosing redo record's header identity
/// (tenant / vshard / database / lsn).
///
/// Non-`TransactionRedo` records are skipped.
///
/// A redo record whose payload fails to decode aborts recovery. Its CRC was
/// already verified when the WAL was read, so a decode failure is not bit-rot
/// in the bytes — it means a transaction that was acknowledged as committed
/// cannot be applied. Skipping it would leave a hole in the replayed suffix and
/// bring the database up silently missing committed writes. The same holds for
/// a sub-record that cannot be reconstituted: the group's ops are one atomic
/// unit, so dropping one of them applies a torn transaction.
///
/// Each sub-record carries its enclosing record's system ordinal
/// (`RecordHeader::system_ordinal`), the ordinal the live apply of the record
/// stamped.
///
/// Reconstituted records are always plaintext (`encryption_key: None`): the
/// enclosing record was already decrypted when the WAL was read into memory, so
/// its sub-payloads are cleartext and these records never touch disk.
///
/// A `WriteGroup` record reconstitutes the same way: its sub-records are the
/// rows one write stored, in the shapes the standalone records use. Each
/// sub-record takes the LSN of its group's origin, the LSN the write ran at
/// on its core. Replay then applies a write's rows at the write's place in
/// the order, wherever its parts sit in the WAL: a part boot journals at the
/// tail never lands over a later write to the same row.
fn reconstitute_redo_records(records: &[WalRecord]) -> crate::Result<ReconstitutedReplay> {
    let mut out = Vec::new();
    let mut grouped_origins = GroupedReplayOrigins::default();
    for record in records {
        let (lsn, ops) = match RecordType::from_raw(record.logical_record_type()) {
            Some(RecordType::TransactionRedo) => {
                let redo = RedoRecord::from_bytes(&record.payload)?;
                (record.header.lsn, redo.ops)
            }
            Some(RecordType::WriteGroup) => {
                let group = WriteGroupRecord::from_bytes(&record.payload)?;
                (group.group.origin_at(record.header.lsn), group.ops)
            }
            _ => continue,
        };
        grouped_origins.insert(lsn);
        // Every sub-record takes the system ordinal of its record, so every
        // write of the record replays at the ordinal its live apply stamped.
        let stamp = RecordStamp {
            system_ordinal: record.header.system_ordinal,
            ..RecordStamp::NONE
        };
        for sub in ops {
            out.push(WalRecord::new_stamped(
                WalRecordArgs {
                    record_type: sub.record_type,
                    lsn,
                    tenant_id: record.header.tenant_id,
                    vshard_id: record.header.vshard_id,
                    database_id: record.header.database_id,
                    payload: sub.payload,
                    encryption_key: None,
                    preamble_bytes: None,
                },
                stamp,
            )?);
        }
    }
    Ok(ReconstitutedReplay {
        records: out,
        grouped_origins,
    })
}

/// Merge the standalone records with the reconstituted redo sub-records into
/// one LSN-ordered sequence.
///
/// Borrows when there are no redo groups — the overwhelmingly common shape of a
/// WAL tail — so the ordinary boot never copies the whole tail.
///
/// The sort is stable, so the sub-records of one redo group keep the
/// intra-transaction order the resolver emitted them in: they all carry the
/// enclosing record's LSN and nothing else distinguishes them. The enclosing
/// `TransactionRedo` record itself stays in the sequence at that same LSN and
/// simply matches no engine arm.
fn merge_by_lsn<'a>(standalone: &'a [WalRecord], redo_ops: &[WalRecord]) -> Cow<'a, [WalRecord]> {
    if redo_ops.is_empty() {
        return Cow::Borrowed(standalone);
    }
    let mut ordered = Vec::with_capacity(standalone.len() + redo_ops.len());
    ordered.extend_from_slice(standalone);
    ordered.extend_from_slice(redo_ops);
    ordered.sort_by_key(|record| record.header.lsn);
    Cow::Owned(ordered)
}

impl CoreLoop {
    /// Replay every engine-bearing WAL record in `records` — standalone
    /// (autocommit) records AND the sub-records of every committed
    /// `TransactionRedo` group — through the per-engine replay arms in one
    /// globally LSN-ordered pass.
    ///
    /// Ordering is the point: redo ops are absolute overwrites, so a key
    /// written at a low LSN inside a transaction and again at a high LSN by an
    /// autocommit must see the autocommit last. Dispatching the two record
    /// classes as separate passes cannot express that regardless of which pass
    /// runs first, so they are merged before dispatch.
    ///
    /// Within one LSN the arm order still matters and is preserved:
    /// `replay_vector_wal` registers `VectorParams` before
    /// `replay_vector_extended_wal` — the ONLY decoder for the
    /// `VectorDirectUpsert` / `SparseVectorPut` / `SparseVectorDelete` /
    /// `MultiVectorPut` / `MultiVectorDelete` sub-records the vector resolver
    /// emits — rebuilds any index from them.
    ///
    /// The document arm replays standalone records and redo sub-records alike
    /// from the merged stream. `apply_point_put` re-indexes a row's secondary
    /// vectors inline, gated by the vector checkpoint's stamp, so no separate
    /// pass indexes document vectors. It runs after `replay_vector_wal`, so the
    /// `VectorParams` records emitted by `CREATE VECTOR INDEX` have registered
    /// per-collection index params before a document row re-indexes.
    ///
    /// One arm takes the STANDALONE slice rather than the merged one:
    /// `replay_graph_node_label_wal`, whose redo counterpart is
    /// `replay_graph_node_labels_redo` below; handing both the merged stream
    /// applies every label delta twice.
    ///
    /// Returns `Err` when a committed redo group cannot be reconstituted; that
    /// is unrecoverable data loss, not a skippable record, and recovery must
    /// stop rather than bring the database up with a hole in the replayed
    /// suffix.
    pub(crate) fn replay_engines_in_lsn_order(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> crate::Result<()> {
        let ReconstitutedReplay {
            records: redo_ops,
            grouped_origins,
        } = reconstitute_redo_records(records)?;
        let ordered = merge_by_lsn(records, &redo_ops);

        self.replay_vector_wal(&ordered, num_cores, tombstones, &grouped_origins);
        crate::fail_point!(self.fail_scope(), "replay::between_engine_passes");
        self.replay_vector_extended_wal(&ordered, num_cores, tombstones);
        // Every document row the WAL names, standalone and redo alike, in LSN
        // order.
        self.replay_document_redo(&ordered, num_cores, tombstones);
        self.replay_kv_wal(&ordered, num_cores, tombstones);
        self.replay_timeseries_wal_with_origins(&ordered, num_cores, tombstones, &grouped_origins);
        self.replay_array_wal(&ordered, num_cores, tombstones);
        // CRDT deltas and document/list intents share Loro state, so replay
        // them together in global LSN order: the standalone records and the
        // intents a committed transaction journalled as redo sub-records.
        self.replay_crdt_wal_ordered(&ordered, num_cores, tombstones);
        self.replay_fts_wal(&ordered, num_cores, tombstones, &grouped_origins);
        self.replay_spatial_wal(&ordered, num_cores, tombstones, &grouped_origins);
        // Graph node labels have no redb-backed durability (unlike edges,
        // rebuilt into the CSR from the `EdgeStore` before this sequence runs) —
        // a WAL record is their only durable backing.
        self.replay_graph_node_label_wal(records, num_cores);

        crate::fail_point!(self.fail_scope(), "replay::between_standalone_and_redo");

        // Every graph edge version the WAL names, standalone and redo alike, in
        // LSN order. The edges' CSR is rebuilt from the `EdgeStore` at open, so
        // a restart re-applies versions it already holds at their exact keys,
        // and a point-in-time restore applies the versions its base lacks.
        self.replay_graph_redo(&ordered, num_cores, tombstones);
        // Node-label deltas staged inside a transaction resolve to the same
        // `GraphNodeLabelSet` / `GraphNodeLabelRemove` sub-record shape the
        // autocommit path produces (`resolve/graph.rs`'s
        // `serialize_node_label_deltas`); this is the ONLY decoder that
        // routes them from a reconstituted `TransactionRedo` record, mirroring
        // `replay_vector_extended_wal`'s role for the vector engine's extended
        // sub-records above.
        self.replay_graph_node_labels_redo(&redo_ops, num_cores);
        Ok(())
    }

    /// Apply a slice consisting only of `TransactionRedo` records end to end.
    ///
    /// Test-only: production recovery always has the standalone records in hand
    /// too and must merge them, which is what
    /// [`Self::replay_engines_in_lsn_order`] does. Handing that same function a
    /// redo-only slice is exactly this operation, so the two can never drift.
    #[cfg(test)]
    pub(crate) fn replay_transaction_redo_wal(
        &mut self,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> crate::Result<()> {
        self.replay_engines_in_lsn_order(records, num_cores, tombstones)
    }
}

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;
