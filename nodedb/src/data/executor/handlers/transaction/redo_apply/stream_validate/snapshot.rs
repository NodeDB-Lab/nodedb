// SPDX-License-Identifier: BUSL-1.1

//! The serialization check of a snapshot commit's stream, run as the
//! content pages in.
//!
//! `check_redo_snapshot` runs the window check, then the read set, then the
//! keyed writes: every document row in record order, then every KV row,
//! then every edge. Each stops at its first row that moved or did not read.
//! The stream runs the window check once, before the first read, checks the
//! reads in bounded chunks as they arrive ahead of the ops, and checks each
//! op's writes in the lane the whole-record check lists them in. Each
//! result is offered under its rank, so the verdict names the row the
//! whole-record check names.
//!
//! Each read is checked once. A row that moves after its chunk passed is
//! not seen again. So a yielding install fences the collection of each
//! read from before the validate, as its marker declares, and checks the
//! collection of every read it checked before the slice ends
//! (`StreamWalk::read_keys`). A read that names no collection reads its
//! whole vShard: it checks only on the task's own vShard, whose writes the
//! fence holds from before the validate.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::install_fence::FenceKey;
use crate::data::executor::core_loop::txn_validate::TxnWrite;
use crate::wal::RedoSubRecord;

use super::super::snapshot_check::{
    read_moved_past_snapshot, snapshot_outside_window, snapshot_store_unreadable,
    write_moved_past_snapshot,
};
use super::super::write_keys::sub_record_writes;
use super::verdict::{Rank, WriteLane};
use super::walk::{READ_CHUNK, StreamValidateInput, StreamWalk};

impl CoreLoop {
    /// Refuse a snapshot that trails the commit ordinal by more than the
    /// window. Runs once, when the header arrives.
    pub(super) fn check_stream_window(
        &self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
    ) {
        let Some(snapshot) = input.snapshot else {
            return;
        };
        let ordinal = self.apply_scope.replicated_ordinal;
        if !self.snapshot_within_window(snapshot, ordinal) {
            let vshard = input.task.request.vshard_id.as_u32();
            walk.verdict.offer(
                Rank::SnapshotWindow,
                snapshot_outside_window(snapshot, ordinal, vshard),
            );
        }
    }

    /// Hold one read of the read set, and check the held reads once a chunk
    /// is full.
    pub(super) fn push_stream_read(
        &mut self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
        read: nodedb_types::calvin::VersionedReadEntry,
    ) {
        if input.snapshot.is_none() || !walk.verdict.admits(&Rank::SnapshotReads) {
            return;
        }
        walk.reads.push(read);
        if walk.reads.len() >= READ_CHUNK {
            self.flush_stream_reads(walk, input);
        }
    }

    /// Check the held reads. A read that moved, or a store that did not
    /// read, refuses the record at the reads' rank. The collection of each
    /// read checked joins the walk's read keys.
    pub(super) fn flush_stream_reads(
        &mut self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
    ) {
        let reads = std::mem::take(&mut walk.reads);
        let Some(snapshot) = input.snapshot else {
            return;
        };
        if reads.is_empty() || !walk.verdict.admits(&Rank::SnapshotReads) {
            return;
        }
        let db = input.task.request.database_id;
        walk.read_keys.extend(
            reads
                .iter()
                .filter_map(|read| FenceKey::of_read(db, &read.collection)),
        );
        let vshard = input.task.request.vshard_id.as_u32();
        match self.txn_reads_unchanged(input.task, input.tid, &reads, snapshot) {
            Ok(true) => {}
            Ok(false) => walk.verdict.offer(
                Rank::SnapshotReads,
                read_moved_past_snapshot(snapshot, vshard),
            ),
            Err(error) => walk.verdict.offer(
                Rank::SnapshotReads,
                snapshot_store_unreadable(vshard, error),
            ),
        }
    }

    /// Check the keyed rows op `op` writes, each in its lane.
    pub(super) fn check_stream_writes(
        &self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
        op: u64,
        sub: &RedoSubRecord,
    ) {
        let Some(snapshot) = input.snapshot else {
            return;
        };
        if !walk
            .verdict
            .admits(&Rank::SnapshotWrites(WriteLane::Document, op))
        {
            return;
        }
        let writes = sub_record_writes(sub);
        let lanes: [(WriteLane, Vec<TxnWrite>); 3] = [
            (WriteLane::Document, writes.document.into_iter().collect()),
            (WriteLane::Kv, writes.kv),
            (WriteLane::Edge, writes.edge.into_iter().collect()),
        ];
        let vshard = input.task.request.vshard_id.as_u32();
        for (lane, rows) in lanes {
            let rank = Rank::SnapshotWrites(lane, op);
            if rows.is_empty() || !walk.verdict.admits(&rank) {
                continue;
            }
            match self.txn_writes_unchanged(input.task, input.tid, &rows, snapshot) {
                Ok(true) => {}
                Ok(false) => walk
                    .verdict
                    .offer(rank, write_moved_past_snapshot(snapshot, vshard)),
                Err(error) => walk
                    .verdict
                    .offer(rank, snapshot_store_unreadable(vshard, error)),
            }
        }
    }
}
