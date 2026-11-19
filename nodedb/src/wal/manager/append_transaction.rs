// SPDX-License-Identifier: BUSL-1.1

//! WAL appends for transactions and CRDT operations.

use nodedb_wal::record::RecordType;

use super::appender::WalAppender;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalAppender<'_> {
    pub fn append_transaction(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::Transaction, tid, vs, db, p)
    }

    /// Append a `TransactionRedo` record wrapping an ordered set of
    /// engine-native sub-records as one durable, replayable unit.
    ///
    /// The record is serialized here; the returned LSN is the write's WAL
    /// position, which the caller uses to write-ahead the transaction before
    /// installing its effects. A record over the WAL record limit splits into
    /// the origin record and its continuations (see
    /// `crate::wal::redo::continuation`); the LSN is the origin's.
    pub fn append_transaction_redo(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        record: &crate::wal::RedoRecord,
    ) -> crate::Result<Lsn> {
        self.append_redo_split(tid, vs, db, record, false, None)
    }

    /// [`Self::append_transaction_redo`] for a record whole at append: no
    /// announcement and no part follows it. Each continuation names the
    /// continuation count, so a group that shows every continuation is whole.
    pub fn append_whole_transaction_redo(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        record: &crate::wal::RedoRecord,
    ) -> crate::Result<Lsn> {
        self.append_redo_split(tid, vs, db, record, true, None)
    }

    fn append_redo_split(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        record: &crate::wal::RedoRecord,
        whole: bool,
        boundary: Option<crate::wal::CommitBoundary>,
    ) -> crate::Result<Lsn> {
        let payload = record.to_bytes()?;
        if payload.len() <= self.max_payload() {
            return self.append_redo_origin(tid, vs, db, &payload, boundary);
        }
        let split = crate::wal::split_redo(record, self.max_payload())?;
        let count =
            u32::try_from(split.continuations.len()).map_err(|_| crate::Error::Internal {
                detail: format!(
                    "a transaction record splits into {} continuations, more than a group counts",
                    split.continuations.len()
                ),
            })?;
        let origin = self.append_redo_origin(tid, vs, db, &split.origin.to_bytes()?, boundary)?;
        for (index, ops) in (1u32..).zip(split.continuations) {
            let group = if whole {
                crate::wal::WriteGroup::continuing_closed(origin.as_u64(), index, count)
            } else {
                crate::wal::WriteGroup::continuing(origin.as_u64(), index)
            };
            self.append_write_group(
                tid,
                vs,
                db,
                &crate::wal::WriteGroupRecord {
                    schemas: Vec::new(),
                    group,
                    ops,
                    redo: Some(split.metadata.clone()),
                },
            )?;
        }
        Ok(origin)
    }

    /// Append the origin `TransactionRedo` record `payload`. The pieces of
    /// `boundary` land right below it in one run under the WAL lock, each
    /// naming the LSN the record takes, so the WAL is never durable through
    /// the record without its whole boundary. The stored boundary's
    /// `redo_crc` is the CRC32C of `payload`.
    ///
    /// Only the record carries this appender's apply key. A piece left over
    /// from a run that failed before its record landed marks no proposal
    /// applied.
    fn append_redo_origin(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        payload: &[u8],
        boundary: Option<crate::wal::CommitBoundary>,
    ) -> crate::Result<Lsn> {
        let Some(boundary) = boundary else {
            return self.append_row_record(RecordType::TransactionRedo, tid, vs, db, payload);
        };
        let boundary = boundary.with_crc(crc32c::crc32c(payload));
        let bodies = boundary.piece_bodies(self.max_payload())?;
        // Every record of the run: its payload, the piece envelope and the
        // record header, with room for an authentication tag.
        let per_record = nodedb_wal::record::HEADER_SIZE + 16;
        let run_bytes = bodies
            .iter()
            .map(|body| 2 * body.len() + crate::wal::redo::continuation::RECORD_OVERHEAD)
            .chain(std::iter::once(payload.len()))
            .map(|bytes| bytes + per_record)
            .sum::<usize>();
        let mut named = 0;
        let lsns = self.append_contiguous(tid, vs, db, run_bytes as u64, |first| {
            named = first.saturating_add(bodies.len() as u64);
            let mut items = crate::wal::RedoBoundaryPiece::of(named, bodies)?
                .iter()
                .map(|piece| Ok((RecordType::RedoBoundary, false, piece.to_bytes()?)))
                .collect::<crate::Result<Vec<_>>>()?;
            items.push((RecordType::TransactionRedo, true, payload.to_vec()));
            Ok(items)
        })?;
        match lsns.last() {
            Some(lsn) if lsn.as_u64() == named => Ok(*lsn),
            last => Err(crate::Error::Internal {
                detail: format!(
                    "the commit boundary names the redo at lsn {named}, and the WAL placed it \
                     at {last:?}"
                ),
            }),
        }
    }

    /// Append one `WriteGroup` record: a write's opening record or one part
    /// of the rows it stored after apply.
    pub fn append_write_group(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        record: &crate::wal::WriteGroupRecord,
    ) -> crate::Result<Lsn> {
        let payload = record.to_bytes()?;
        self.append_row_record(RecordType::WriteGroup, tid, vs, db, &payload)
    }

    /// Append a `TransactionRedo` record whose payload is an already-encoded
    /// redo record. Used by the committed-redo apply path, which carries the
    /// record encoded on the plan it dispatches. A payload over the WAL
    /// record limit splits as [`Self::append_transaction_redo`] splits it.
    pub fn append_transaction_redo_bytes(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_redo_bytes(tid, vs, db, payload, None)
    }

    /// [`Self::append_transaction_redo_bytes`] for a committed record whose
    /// install validates it against `boundary`. The boundary's pieces land
    /// right below the record. Returns the record's LSN.
    pub fn append_bounded_transaction_redo_bytes(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        payload: &[u8],
        boundary: crate::wal::CommitBoundary,
    ) -> crate::Result<Lsn> {
        self.append_redo_bytes(tid, vs, db, payload, Some(boundary))
    }

    fn append_redo_bytes(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        payload: &[u8],
        boundary: Option<crate::wal::CommitBoundary>,
    ) -> crate::Result<Lsn> {
        if payload.len() <= self.max_payload() {
            return self.append_redo_origin(tid, vs, db, payload, boundary);
        }
        let record = crate::wal::RedoRecord::from_bytes(payload)?;
        self.append_redo_split(tid, vs, db, &record, false, boundary)
    }

    pub fn append_crdt_delta(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        delta: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::CrdtDelta, tid, vs, db, delta)
    }

    /// Append a `CrdtListOp` record. Payload is a zerompk-encoded
    /// `CrdtListOpWalRecord` carrying the list-mutation intent (see that
    /// type's doc comment for why intent, not a Loro delta, is logged).
    pub fn append_crdt_list_op(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::CrdtListOp, tid, vs, db, p)
    }

    /// Append a `CrdtDocOp` record. Payload is a zerompk-encoded
    /// `CrdtDocOpWalRecord` carrying the document-row mutation intent (see that
    /// type's doc comment for why intent, not a Loro delta, is logged).
    pub fn append_crdt_doc_op(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::CrdtDocOp, tid, vs, db, p)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::RedoOrigin;
    use nodedb_wal::WalRecord;
    use nodedb_wal::record::RecordType;

    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use crate::wal::manager::{NO_APPLY_KEY, WalManager};
    use crate::wal::{CommitBoundary, RedoBoundaryPiece, RedoRecord, boundaries_in, boundary_of};

    const KEY: u64 = 0xB0D;

    fn record(calvin: bool) -> RedoRecord {
        RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: calvin.then_some(crate::wal::CalvinStamp {
                epoch: 1,
                position: 0,
                vshard_id: 0,
                mirror_database: None,
            }),
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        }
    }

    fn boundary() -> CommitBoundary {
        CommitBoundary {
            origin: RedoOrigin::Commit,
            snapshot_ordinal: Some(3),
            reads: Vec::new(),
            judged_at_ms: 1,
        }
    }

    fn home() -> (TenantId, VShardId, DatabaseId) {
        (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT)
    }

    /// A piece of a run that failed before its record landed: it names
    /// `redo_lsn` and sits at the place a boundary of `count` pieces holds
    /// piece `index`.
    fn leftover(wal: &WalManager, redo_lsn: u64, index: u32, count: u32) {
        let (t, v, d) = home();
        let piece = RedoBoundaryPiece {
            redo_lsn,
            index,
            count,
            // A whole boundary whose CRC names no payload here.
            bytes: boundary().with_crc(0).to_bytes().expect("encode boundary"),
        };
        wal.appender(NO_APPLY_KEY)
            .append_record(
                RecordType::RedoBoundary,
                t,
                v,
                d,
                &piece.to_bytes().expect("encode piece"),
            )
            .expect("append piece");
    }

    fn at(records: &[WalRecord], lsn: u64) -> &WalRecord {
        records
            .iter()
            .find(|record| record.header.lsn == lsn)
            .expect("record")
    }

    /// Only the record of a bounded append carries the apply key. With the
    /// record gone, its pieces mark no proposal applied.
    #[test]
    fn pieces_with_no_record_leave_the_proposal_unapplied() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let (t, v, d) = home();
        let lsn = wal
            .appender(KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_bounded_transaction_redo_bytes(
                t,
                v,
                d,
                &record(false).to_bytes().expect("encode"),
                boundary(),
            )
            .expect("append");
        wal.appender(NO_APPLY_KEY)
            .append_write_aborted(t, v, d, Lsn::new(lsn.as_u64()))
            .expect("cancel the record");
        wal.sync().expect("sync");
        let records = wal.replay().expect("replay");
        assert!(
            records
                .iter()
                .any(|record| RecordType::from_raw(record.logical_record_type())
                    == Some(RecordType::RedoBoundary)),
            "the pieces stay"
        );
        let ledger = crate::control::distributed_applier::ProposalLedger::from_records(&records, 8);
        assert!(ledger.prior(KEY).is_none());
    }

    /// A complete leftover boundary below a Calvin slice belongs to no
    /// record: its CRC names another payload, and a Calvin slice takes no
    /// boundary.
    #[test]
    fn a_leftover_boundary_below_a_calvin_slice_is_ignored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let (t, v, d) = home();
        let first = wal.next_lsn().as_u64();
        leftover(&wal, first + 1, 0, 1);
        let lsn = wal
            .appender(KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_transaction_redo(t, v, d, &record(true))
            .expect("calvin record")
            .as_u64();
        assert_eq!(lsn, first + 1, "the slice takes the LSN the piece names");
        wal.sync().expect("sync");
        let records = wal.replay().expect("replay");
        let found = boundaries_in(&records).expect("boundaries");
        assert!(boundary_of(&found, at(&records, lsn)).is_none());
        let scanned =
            crate::bootstrap::write_group_settle::WalGroups::scan(&records).expect("scan");
        let never_durable = crate::bootstrap::write_group_settle::never_durable_redos(
            &records,
            &scanned,
            &std::collections::HashSet::new(),
        )
        .expect("a Calvin slice needs no boundary");
        assert!(never_durable.judged(lsn).is_none());
    }

    /// A partial leftover of three pieces and a real boundary of one name
    /// the same record. They never merge: the record gets its own.
    #[test]
    fn a_partial_leftover_does_not_merge_with_a_real_boundary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let (t, v, d) = home();
        let first = wal.next_lsn().as_u64();
        let named = first + 3;
        leftover(&wal, named, 0, 3);
        leftover(&wal, named, 1, 3);
        let lsn = wal
            .appender(KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_bounded_transaction_redo_bytes(
                t,
                v,
                d,
                &record(false).to_bytes().expect("encode"),
                boundary(),
            )
            .expect("append")
            .as_u64();
        assert_eq!(lsn, named, "one piece, then the record");
        wal.sync().expect("sync");
        let records = wal.replay().expect("replay");
        let found = boundaries_in(&records).expect("boundaries");
        let stored = boundary_of(&found, at(&records, lsn)).expect("its own boundary");
        assert_eq!(stored.lsns, vec![lsn - 1]);
        assert_eq!(stored.boundary.snapshot_ordinal, Some(3));
    }
}
