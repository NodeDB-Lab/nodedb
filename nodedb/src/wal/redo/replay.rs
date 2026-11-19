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
use std::collections::HashMap;

use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordStamp, RecordType, WalRecordArgs};

use super::install_cursor::{InstallCursors, RedoInstallError};
use super::op_index::RedoOpIndex;
use super::replay_arm::RedoArm;
use super::replay_arms::ArmReplayInput;
use super::replay_origin::GroupedReplayOrigins;
use super::stream_expand::StreamContents;
use super::{RedoRecord, RedoStreamRef, WriteGroupRecord, read_stream_content};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::io::uring_wal_cursor::UringWalCursor;

struct ReconstitutedReplay {
    records: Vec<WalRecord>,
    /// Parallel to `records`: each sub-record's index among its
    /// `TransactionRedo` record's ops. A write-group part carries none.
    op_tags: Vec<Option<u64>>,
    grouped_origins: GroupedReplayOrigins,
    /// The sum targets each committed record's install folded with, by the
    /// record's LSN. Only records that carry targets appear.
    sum_targets: Vec<(u64, Vec<nodedb_physical::physical_plan::RedoSumTargets>)>,
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
///
/// A stream marker reconstitutes from the content its chunk records hold
/// (see [`super::stream_expand`]). A marker whose chunks lie below the
/// oldest retained segment was checkpointed by every engine, so it
/// reconstitutes no sub-record. A marker in `cursors` has an install that
/// ended part way, which no checkpoint passed: its chunks missing is
/// [`RedoInstallError::ChunksMissing`].
///
/// A committed record's sum targets come from where its install read them:
/// a whole record's WAL copy, or a stream marker's content header.
///
/// A `TransactionRedo` sub-record is tagged with its index among the
/// record's ops, the index the fold ledger counts. A continuation of a split
/// record carries the record's next ops, so its sub-records continue the
/// origin's count in continuation order: the order the live install applies
/// the whole record in. A part journalled after apply is tagged with none:
/// it is no op of the record at its origin.
fn reconstitute_redo_records(
    records: &[WalRecord],
    contents: &mut StreamContents<'_, '_, '_>,
    cursors: &InstallCursors,
) -> crate::Result<ReconstitutedReplay> {
    let mut out = Vec::new();
    let mut op_tags = Vec::new();
    let mut grouped_origins = GroupedReplayOrigins::default();
    let mut sum_targets = Vec::new();
    let mut continued: HashMap<u64, ContinuedOps> = HashMap::new();
    for record in records {
        let (lsn, ops, first_op) = match RecordType::from_raw(record.logical_record_type()) {
            Some(RecordType::TransactionRedo) => {
                let redo = RedoRecord::from_bytes(&record.payload)?;
                let installed = match RedoStreamRef::of_marker(&redo)? {
                    Some(reference) => match contents.installed(&redo, &reference)? {
                        Some(installed) => installed,
                        None if cursors.contains(record.header.lsn) => {
                            return Err(RedoInstallError::ChunksMissing {
                                lsn: record.header.lsn,
                            }
                            .into());
                        }
                        None => continue,
                    },
                    None => redo,
                };
                if !installed.sum_targets.is_empty() {
                    sum_targets.push((record.header.lsn, installed.sum_targets));
                }
                continued.insert(
                    record.header.lsn,
                    ContinuedOps {
                        next_op: installed.ops.len() as u64,
                        next_continuation: 1,
                    },
                );
                (record.header.lsn, installed.ops, Some(0))
            }
            Some(RecordType::WriteGroup) => {
                let group = WriteGroupRecord::from_bytes(&record.payload)?;
                let mut ops = group.ops;
                for sub in &mut ops {
                    if let Some(installed) =
                        super::installed_document::InstalledDocumentRedo::<u32>::decode(
                            &sub.payload,
                        )?
                    {
                        sub.payload = installed.resolve(&group.schemas)?.encode()?;
                    }
                }
                let first_op = continuation_first_op(&mut continued, &group.group, ops.len())?;
                (group.group.origin_at(record.header.lsn), ops, first_op)
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
        for (index, sub) in ops.into_iter().enumerate() {
            op_tags.push(first_op.map(|first| first + index as u64));
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
        op_tags,
        grouped_origins,
        sum_targets,
    })
}

/// The op count of a `TransactionRedo` record its continuations extend.
struct ContinuedOps {
    /// The record op index of the next continuation's first sub-record.
    next_op: u64,
    /// The index (from 1) of the record's next continuation.
    next_continuation: u32,
}

/// The record op index of the first sub-record of the `WriteGroup` record
/// `group`, which holds `len` sub-records. `None` unless it is a
/// continuation whose origin record this read holds: a part journalled
/// after apply is no op of the record, and a continuation without its
/// origin replays no fold.
///
/// A continuation out of order is refused: its ops would apply in another
/// order than the live install applied them.
fn continuation_first_op(
    continued: &mut HashMap<u64, ContinuedOps>,
    group: &super::WriteGroup,
    len: usize,
) -> crate::Result<Option<u64>> {
    let is_continuation = group.opens() && group.origin != 0 && group.parts > 0;
    if !is_continuation {
        return Ok(None);
    }
    let Some(ops) = continued.get_mut(&group.origin) else {
        return Ok(None);
    };
    if group.parts != ops.next_continuation {
        return Err(crate::Error::Internal {
            detail: format!(
                "continuation {} of the transaction record at lsn {} follows continuation {}",
                group.parts,
                group.origin,
                ops.next_continuation.saturating_sub(1)
            ),
        });
    }
    let first = ops.next_op;
    ops.next_op = first.saturating_add(len as u64);
    ops.next_continuation = ops.next_continuation.saturating_add(1);
    Ok(Some(first))
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
/// matches no engine arm.
///
/// The same stable sort carries `redo_op_tags`, parallel to `redo_ops`, into
/// the merged order. A standalone record carries no op index. Tags that do
/// not match `redo_ops` one to one are an error.
fn merge_by_lsn<'a>(
    standalone: &'a [WalRecord],
    redo_ops: &[WalRecord],
    redo_op_tags: &[Option<u64>],
) -> crate::Result<MergedReplay<'a>> {
    if redo_op_tags.len() != redo_ops.len() {
        return Err(crate::Error::Internal {
            detail: format!(
                "{} op tags name {} reconstituted redo sub-records",
                redo_op_tags.len(),
                redo_ops.len()
            ),
        });
    }
    if redo_ops.is_empty() {
        return Ok(MergedReplay {
            records: Cow::Borrowed(standalone),
            ops: RedoOpIndex::default(),
        });
    }
    let mut tagged: Vec<(&WalRecord, Option<u64>)> =
        Vec::with_capacity(standalone.len() + redo_ops.len());
    tagged.extend(standalone.iter().map(|record| (record, None)));
    tagged.extend(redo_ops.iter().zip(redo_op_tags.iter().copied()));
    tagged.sort_by_key(|(record, _)| record.header.lsn);
    let ops = RedoOpIndex::new(tagged.iter().map(|(_, op)| *op).collect());
    Ok(MergedReplay {
        records: Cow::Owned(
            tagged
                .into_iter()
                .map(|(record, _)| record.clone())
                .collect(),
        ),
        ops,
    })
}

/// The merged replay stream and the op index parallel to it.
struct MergedReplay<'a> {
    records: Cow<'a, [WalRecord]>,
    ops: RedoOpIndex,
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
    /// `replay_graph_node_labels_redo`; handing both the merged stream
    /// applies every label delta twice. [`RedoArm::DISPATCH`] is the arm
    /// order, and `replay_one_arm` holds each arm's body.
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
        self.replay_engines_within(records, records, num_cores, tombstones)
    }

    /// [`Self::replay_engines_in_lsn_order`] over `records`, a part of the
    /// replayed stream `stream`. A stream marker of `records` reads its
    /// chunk records from `stream`: they sit below the marker, and can lie
    /// outside the part.
    pub(crate) fn replay_engines_within(
        &mut self,
        records: &[WalRecord],
        stream: &[WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> crate::Result<()> {
        self.replay_reconstituted(records, stream, num_cores, tombstones, |core, input| {
            for arm in RedoArm::DISPATCH {
                core.replay_one_arm(arm, input);
                // A committed-redo apply stops at the first arm error: the
                // apply answers it, and a batched install stops there too.
                if core.committed_redo_failed() {
                    break;
                }
                match arm {
                    RedoArm::Vector => {
                        crate::fail_point!(core.fail_scope(), "replay::between_engine_passes");
                    }
                    RedoArm::GraphNodeLabel => {
                        crate::fail_point!(
                            core.fail_scope(),
                            "replay::between_standalone_and_redo"
                        );
                    }
                    RedoArm::Reconstitute
                    | RedoArm::VectorExtended
                    | RedoArm::Document
                    | RedoArm::Kv
                    | RedoArm::Timeseries
                    | RedoArm::Array
                    | RedoArm::Crdt
                    | RedoArm::Fts
                    | RedoArm::Spatial
                    | RedoArm::Graph
                    | RedoArm::GraphNodeLabelRedo => {}
                }
            }
        })
    }

    /// Replay the one arm `arm` over `records`, reconstituted and merged
    /// as [`Self::replay_engines_in_lsn_order`] does. A batched install runs
    /// each arm over each batch of a record this way, so the arms see the
    /// record's ops in the order a whole-record pass hands them.
    pub(crate) fn replay_arm_in_lsn_order(
        &mut self,
        arm: RedoArm,
        records: &[WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> crate::Result<()> {
        self.replay_reconstituted(records, records, num_cores, tombstones, |core, input| {
            core.replay_one_arm(arm, input);
        })
    }

    /// Reconstitute and merge `records`, then hand the arms' input to `run`.
    /// A stream marker's chunks come from `stream`, which holds `records`.
    fn replay_reconstituted(
        &mut self,
        records: &[WalRecord],
        stream: &[WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
        run: impl FnOnce(&mut Self, &ArmReplayInput<'_>),
    ) -> crate::Result<()> {
        // A stream marker's chunks the stream lacks are read back through
        // this core's io_uring cursor.
        let source = self.wal_source().clone();
        let mut fetch =
            |reference: &RedoStreamRef| read_stream_content::<UringWalCursor>(&source, reference);
        let mut contents = StreamContents::new(stream, &mut fetch);
        // An open committed-redo scope tags each error it records with the
        // step that ran (`RedoArm`).
        self.enter_redo_arm(RedoArm::Reconstitute);
        let ReconstitutedReplay {
            records: redo_ops,
            op_tags,
            grouped_origins,
            sum_targets,
        } = reconstitute_redo_records(records, &mut contents, self.boot_install_cursors())?;
        // A never-durable record folds with the targets its install used.
        self.keep_replay_fold_targets(sum_targets);
        let MergedReplay {
            records: ordered,
            ops,
        } = merge_by_lsn(records, &redo_ops, &op_tags)?;

        let input = ArmReplayInput {
            standalone: records,
            ordered: &ordered,
            ops: &ops,
            redo_ops: &redo_ops,
            grouped_origins: &grouped_origins,
            num_cores,
            tombstones,
        };
        run(self, &input);
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
