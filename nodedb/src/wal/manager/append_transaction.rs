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
        self.append_redo_split(tid, vs, db, record, false)
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
        self.append_redo_split(tid, vs, db, record, true)
    }

    fn append_redo_split(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        record: &crate::wal::RedoRecord,
        whole: bool,
    ) -> crate::Result<Lsn> {
        let payload = record.to_bytes()?;
        if payload.len() <= self.max_payload() {
            return self.append_row_record(RecordType::TransactionRedo, tid, vs, db, &payload);
        }
        let split = crate::wal::split_redo(record, self.max_payload())?;
        let count =
            u32::try_from(split.continuations.len()).map_err(|_| crate::Error::Internal {
                detail: format!(
                    "a transaction record splits into {} continuations, more than a group counts",
                    split.continuations.len()
                ),
            })?;
        let origin = self.append_row_record(
            RecordType::TransactionRedo,
            tid,
            vs,
            db,
            &split.origin.to_bytes()?,
        )?;
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
        if payload.len() <= self.max_payload() {
            return self.append_row_record(RecordType::TransactionRedo, tid, vs, db, payload);
        }
        let record = crate::wal::RedoRecord::from_bytes(payload)?;
        self.append_transaction_redo(tid, vs, db, &record)
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
