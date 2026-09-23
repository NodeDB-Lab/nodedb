// SPDX-License-Identifier: BUSL-1.1

//! Replay arm for [`RecordType::TransactionRedo`] WAL records.
//!
//! A `TransactionRedo` record groups an ordered set of engine-native
//! sub-records ([`RedoSubRecord`]) — each already in the exact payload shape
//! that engine's own per-op WAL record uses. This module turns each
//! `TransactionRedo` back into a set of per-op [`WalRecord`]s and feeds them to
//! the SAME per-engine replay paths the standalone (autocommit) records use.
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
//! does not gate engine replay.
//!
//! The committed-redo apply (`handlers::transaction::redo_apply`) drives this
//! same entry point with a one-record slice for every committed transaction,
//! so the live install and restart replay run the same arms. The arms settle
//! watermark skips and per-record errors by pass (`data::executor`'s
//! `replay_policy`): restart replay keeps its gates, the live install skips
//! none and fails on any error.

use std::borrow::Cow;

use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordType, WalRecordArgs};

use super::RedoRecord;
use crate::data::executor::core_loop::CoreLoop;

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
/// Reconstituted records are always plaintext (`encryption_key: None`): the
/// enclosing record was already decrypted when the WAL was read into memory, so
/// its sub-payloads are cleartext and these records never touch disk.
fn reconstitute_redo_records(records: &[WalRecord]) -> crate::Result<Vec<WalRecord>> {
    let mut out = Vec::new();
    for record in records {
        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::TransactionRedo) {
            continue;
        }
        let redo = RedoRecord::from_bytes(&record.payload)?;
        for sub in redo.ops {
            out.push(WalRecord::new(WalRecordArgs {
                record_type: sub.record_type,
                lsn: record.header.lsn,
                tenant_id: record.header.tenant_id,
                vshard_id: record.header.vshard_id,
                database_id: record.header.database_id,
                payload: sub.payload,
                encryption_key: None,
                preamble_bytes: None,
            })?);
        }
    }
    Ok(out)
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
    /// Two arms take the STANDALONE slice rather than the merged one, at the
    /// exact positions they have always occupied so their ordering against the
    /// merged arms is unchanged:
    ///
    /// * `replay_document_vector_wal` — redo document puts rebuild their
    ///   secondary vector index inline inside `replay_document_redo`, so
    ///   feeding it the merged stream would index them a second time.
    /// * `replay_graph_node_label_wal` — its redo counterpart is
    ///   `replay_graph_node_labels_redo` below; handing both the merged stream
    ///   would apply every label delta twice.
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
        let redo_ops = reconstitute_redo_records(records)?;
        let ordered = merge_by_lsn(records, &redo_ops);

        self.replay_vector_wal(&ordered, num_cores, tombstones);
        crate::fail_point!("replay::between_engine_passes");
        self.replay_vector_extended_wal(&ordered, num_cores, tombstones);
        // Runs after `replay_vector_wal` so the `VectorParams` records emitted
        // by `CREATE VECTOR INDEX` have registered per-collection index params
        // before secondary vector indexes are rebuilt from document `Put`s.
        self.replay_document_vector_wal(records, num_cores, tombstones);
        self.replay_kv_wal(&ordered, num_cores, tombstones);
        self.replay_timeseries_wal(&ordered, num_cores, tombstones);
        self.replay_array_wal(&ordered, num_cores, tombstones);
        // CRDT deltas and document/list intents share Loro state, so replay
        // them together in global LSN order: the standalone records and the
        // intents a committed transaction journalled as redo sub-records.
        self.replay_crdt_wal_ordered(&ordered, num_cores, tombstones);
        self.replay_fts_wal(&ordered, num_cores, tombstones);
        self.replay_spatial_wal(&ordered, num_cores, tombstones);
        // Graph node labels have no redb-backed durability (unlike edges,
        // rebuilt into the CSR from the `EdgeStore` before this sequence runs) —
        // a WAL record is their only durable backing.
        self.replay_graph_node_label_wal(records, num_cores);

        crate::fail_point!("replay::between_standalone_and_redo");

        // Document and graph have no standalone replay — they survive today via
        // redb's synchronous commit at apply time. Under write-ahead-then-install
        // a crash between append and install loses them, so redo replays them.
        // These three arms take the reconstituted sub-records ALONE rather than
        // the merged stream: a standalone document `Put` is already installed in
        // redb, and re-applying it here would rebuild its secondary vector index
        // a second time on top of `replay_document_vector_wal`'s pass.
        // `apply_point_put` rebuilds any secondary vector index inline, so no
        // separate `replay_document_vector_wal` pass is needed for redo puts.
        self.replay_document_redo(&redo_ops, num_cores, tombstones);
        self.replay_graph_redo(&redo_ops, num_cores, tombstones);
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
mod tests {
    use super::*;
    use crate::wal::{RedoRecord, RedoSubRecord};

    fn redo_wal_record(lsn: u64, tenant_id: u64, vshard_id: u32, record: &RedoRecord) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn,
            tenant_id,
            vshard_id,
            database_id: 0,
            payload: record.to_bytes().expect("encode redo record"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    #[test]
    fn reconstitute_preserves_type_payload_and_header() {
        let redo = RedoRecord {
            version: 1,
            ops: vec![
                RedoSubRecord {
                    record_type: RecordType::VectorPut as u32,
                    payload: vec![1, 2, 3],
                },
                RedoSubRecord {
                    record_type: RecordType::SpatialPut as u32,
                    payload: vec![4, 5],
                },
            ],
            calvin_stamp: None,
        };
        let outer = redo_wal_record(77, 9, 3, &redo);

        let recon =
            reconstitute_redo_records(std::slice::from_ref(&outer)).expect("well-formed redo");
        assert_eq!(recon.len(), 2);
        assert_eq!(recon[0].logical_record_type(), RecordType::VectorPut as u32);
        assert_eq!(recon[0].payload, vec![1, 2, 3]);
        assert_eq!(
            recon[1].logical_record_type(),
            RecordType::SpatialPut as u32
        );
        assert_eq!(recon[1].payload, vec![4, 5]);
        // Enclosing header identity propagates to every sub-record.
        for r in &recon {
            assert_eq!(r.header.lsn, 77);
            assert_eq!(r.header.tenant_id, 9);
            assert_eq!(r.header.vshard_id, 3);
        }
    }

    #[test]
    fn reconstitute_skips_non_redo_records() {
        let put = WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: vec![9, 9, 9],
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        let recon = reconstitute_redo_records(&[put]).expect("non-redo records are ignored");
        assert!(recon.is_empty());
    }

    fn put_wal_record(lsn: u64, payload: Vec<u8>) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    /// With no redo groups the merge must not copy the tail at all — the
    /// ordinary boot walks the borrowed slice.
    #[test]
    fn merge_borrows_when_there_are_no_redo_groups() {
        let standalone = vec![put_wal_record(1, vec![1]), put_wal_record(2, vec![2])];
        let merged = merge_by_lsn(&standalone, &[]);
        assert!(matches!(merged, Cow::Borrowed(_)));
        assert_eq!(merged.len(), 2);
    }

    /// The defect this ordering exists to prevent: a redo op written at a LOWER
    /// LSN than a later autocommit write must be applied BEFORE it, or the
    /// absolute overwrite it carries resurrects the older value.
    #[test]
    fn lower_lsn_redo_op_is_ordered_before_a_higher_lsn_autocommit() {
        let redo = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: vec![0xAA],
            }],
            calvin_stamp: None,
        };
        // The transaction committed at LSN 50; an autocommit overwrote the same
        // key at LSN 100.
        let standalone = vec![
            redo_wal_record(50, 0, 0, &redo),
            put_wal_record(100, vec![0xBB]),
        ];
        let redo_ops = reconstitute_redo_records(&standalone).expect("well-formed redo");
        let merged = merge_by_lsn(&standalone, &redo_ops);

        let applied: Vec<(u64, Vec<u8>)> = merged
            .iter()
            .filter(|r| r.logical_record_type() == RecordType::Put as u32)
            .map(|r| (r.header.lsn, r.payload.clone()))
            .collect();
        assert_eq!(
            applied,
            vec![(50, vec![0xAA]), (100, vec![0xBB])],
            "the LSN-50 redo op must be applied before the LSN-100 autocommit"
        );
    }

    /// Sub-records of one group share the enclosing record's LSN, so the sort
    /// must be stable or an intra-transaction write order is lost.
    #[test]
    fn merge_preserves_intra_transaction_order() {
        let redo = RedoRecord {
            version: 1,
            ops: vec![
                RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload: vec![1],
                },
                RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload: vec![2],
                },
                RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload: vec![3],
                },
            ],
            calvin_stamp: None,
        };
        let standalone = vec![redo_wal_record(7, 0, 0, &redo)];
        let redo_ops = reconstitute_redo_records(&standalone).expect("well-formed redo");
        let merged = merge_by_lsn(&standalone, &redo_ops);
        let payloads: Vec<Vec<u8>> = merged
            .iter()
            .filter(|r| r.logical_record_type() == RecordType::Put as u32)
            .map(|r| r.payload.clone())
            .collect();
        assert_eq!(payloads, vec![vec![1], vec![2], vec![3]]);
    }

    /// Merging the same records twice yields the same sequence: replay of an
    /// already-replayed tail must present engines with an identical stream.
    #[test]
    fn merge_is_deterministic_across_repeated_replays() {
        let redo = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: vec![9],
            }],
            calvin_stamp: None,
        };
        let standalone = vec![
            put_wal_record(1, vec![1]),
            redo_wal_record(2, 0, 0, &redo),
            put_wal_record(3, vec![3]),
        ];
        let redo_ops = reconstitute_redo_records(&standalone).expect("well-formed redo");
        let first: Vec<u64> = merge_by_lsn(&standalone, &redo_ops)
            .iter()
            .map(|r| r.header.lsn)
            .collect();
        let second: Vec<u64> = merge_by_lsn(&standalone, &redo_ops)
            .iter()
            .map(|r| r.header.lsn)
            .collect();
        assert_eq!(first, second);
        assert_eq!(first, vec![1, 2, 2, 3]);
    }

    /// A `TransactionRedo` record whose payload is structurally invalid but
    /// CRC-valid represents a committed transaction that cannot be applied.
    /// Replay must fail rather than skip it and come up missing those writes.
    #[test]
    fn malformed_redo_payload_aborts_replay() {
        let bad = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 2,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload: vec![0xff, 0xff, 0xff],
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");
        // The CRC is intact — the payload, not the bytes, is what is wrong.
        bad.verify_checksum().expect("record bytes are consistent");

        assert!(
            reconstitute_redo_records(&[bad]).is_err(),
            "a committed redo group that cannot be decoded must abort recovery"
        );
    }
}
