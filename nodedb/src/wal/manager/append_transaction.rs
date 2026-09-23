// SPDX-License-Identifier: BUSL-1.1

//! WAL appends for transactions and CRDT operations.

use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalManager {
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
    /// The record is serialized here and appended atomically; the returned LSN
    /// is the write's WAL position, which the caller uses to write-ahead the
    /// transaction before installing its effects.
    pub fn append_transaction_redo(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        record: &crate::wal::RedoRecord,
    ) -> crate::Result<Lsn> {
        let payload = record.to_bytes()?;
        self.append_record(RecordType::TransactionRedo, tid, vs, db, &payload)
    }

    /// Append a `TransactionRedo` record whose payload is an already-encoded
    /// redo record. Used by the committed-redo apply path, which carries the
    /// record encoded on the plan it dispatches.
    pub fn append_transaction_redo_bytes(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        payload: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::TransactionRedo, tid, vs, db, payload)
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
